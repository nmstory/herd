// Package pgstore keeps lease state in Postgres, so leases survive restarts
// and several herd replicas can share one pool of devices.
//
// Exclusivity is enforced by the leases_one_active_per_device unique index.
// Acquire locks a candidate device row with FOR UPDATE SKIP LOCKED so
// concurrent claimers spread across devices instead of queueing on one, but
// the lock alone isn't enough under READ COMMITTED: the "is it free?" check
// reads a snapshot taken when the statement started, so it can miss a lease
// committed a moment earlier. The index catches that as a unique violation
// and Acquire retries with a fresh snapshot.
package pgstore

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
)

//go:embed migrations/*.sql
var migrations embed.FS

// LockMode picks how Acquire locks its candidate device. Only SkipLocked is
// meant for production; the others exist to measure the difference.
type LockMode int

const (
	SkipLocked LockMode = iota // FOR UPDATE SKIP LOCKED
	Wait                       // FOR UPDATE: claimers queue behind each other
	NoLock                     // no row lock: rely on the unique index alone
)

const maxAttempts = 100

type Store struct {
	pool *pgxpool.Pool
	Lock LockMode

	// Conflicts counts unique violations that forced Acquire to retry.
	Conflicts atomic.Int64
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate applies any migrations not yet recorded. An advisory lock makes it
// safe for several replicas to start at once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4831)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
			return err
		}

		for _, f := range files {
			tag, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, f)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				continue // already applied
			}

			sql, err := migrations.ReadFile(f)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("migration %s: %w", f, err)
			}
		}

		return nil
	})
}

// SyncDevices upserts the configured devices. Devices missing from the config
// are left in place, since old leases still reference them.
func SyncDevices(ctx context.Context, pool *pgxpool.Pool, devices []config.DeviceConfig) error {
	batch := &pgx.Batch{}
	for _, d := range devices {
		labels, err := json.Marshal(nonNil(d.Labels))
		if err != nil {
			return err
		}
		batch.Queue(`INSERT INTO devices (id, labels) VALUES ($1, $2::jsonb)
			ON CONFLICT (id) DO UPDATE SET labels = excluded.labels`, d.ID, string(labels))
	}

	return pool.SendBatch(ctx, batch).Close()
}

func (s *Store) ListDevices(ctx context.Context, selector map[string]string) ([]lease.Device, error) {
	sel, err := json.Marshal(nonNil(selector))
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.labels, EXISTS (
			SELECT 1 FROM leases l
			WHERE l.device_id = d.id AND l.released_at IS NULL AND l.expires_at > now()
		)
		FROM devices d
		WHERE d.labels @> $1::jsonb
		ORDER BY d.id`, string(sel))
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (lease.Device, error) {
		var d lease.Device
		err := row.Scan(&d.ID, &d.Labels, &d.Leased)
		return d, err
	})
}

func (s *Store) Acquire(ctx context.Context, p lease.AcquireParams) (lease.Lease, error) {
	if err := p.Validate(); err != nil {
		return lease.Lease{}, err
	}

	for range maxAttempts {
		l, err := s.tryAcquire(ctx, p)
		if isUniqueViolation(err) {
			// Either a concurrent claimer took this device, or a concurrent
			// retry of this same request committed first. Both resolve on the
			// next attempt.
			s.Conflicts.Add(1)
			continue
		}
		return l, err
	}

	return lease.Lease{}, fmt.Errorf("acquire: gave up after %d conflicting attempts", maxAttempts)
}

func (s *Store) tryAcquire(ctx context.Context, p lease.AcquireParams) (lease.Lease, error) {
	hash := p.Hash()
	sel, err := json.Marshal(nonNil(p.Selector))
	if err != nil {
		return lease.Lease{}, err
	}

	var l lease.Lease
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var storedHash []byte
		err := tx.QueryRow(ctx, `
			SELECT id, device_id, holder, expires_at, request_hash
			FROM leases WHERE holder = $1 AND idempotency_key = $2`,
			p.Holder, p.IdempotencyKey,
		).Scan(&l.ID, &l.DeviceID, &l.Holder, &l.ExpiresAt, &storedHash)
		switch {
		case err == nil:
			if !bytes.Equal(storedHash, hash) {
				return lease.ErrIdempotencyMismatch
			}
			return nil // replay: return the original lease unchanged
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		var deviceID string
		err = tx.QueryRow(ctx, `
			SELECT d.id FROM devices d
			WHERE d.labels @> $1::jsonb
			  AND NOT EXISTS (
				SELECT 1 FROM leases l
				WHERE l.device_id = d.id AND l.released_at IS NULL AND l.expires_at > now()
			  )
			ORDER BY d.id
			LIMIT 1 `+s.lockClause(), string(sel)).Scan(&deviceID)
		if errors.Is(err, pgx.ErrNoRows) {
			return lease.ErrNoDeviceAvailable
		}
		if err != nil {
			return err
		}

		// Close out any expired lease so the partial unique index lets a new
		// one in.
		if _, err := tx.Exec(ctx, `
			UPDATE leases SET released_at = expires_at
			WHERE device_id = $1 AND released_at IS NULL AND expires_at <= now()`, deviceID); err != nil {
			return err
		}

		l.DeviceID, l.Holder = deviceID, p.Holder
		return tx.QueryRow(ctx, `
			INSERT INTO leases (device_id, holder, idempotency_key, request_hash, expires_at)
			VALUES ($1, $2, $3, $4, now() + $5 * interval '1 microsecond')
			RETURNING id, expires_at`,
			deviceID, p.Holder, p.IdempotencyKey, hash, p.Duration.Microseconds(),
		).Scan(&l.ID, &l.ExpiresAt)
	})
	if err != nil {
		return lease.Lease{}, err
	}

	return l, nil
}

func (s *Store) lockClause() string {
	switch s.Lock {
	case Wait:
		return "FOR UPDATE OF d"
	case NoLock:
		return ""
	default:
		return "FOR UPDATE OF d SKIP LOCKED"
	}
}

func (s *Store) Release(ctx context.Context, leaseID, holder string) error {
	var id pgtype.UUID
	if err := id.Scan(leaseID); err != nil {
		return lease.ErrLeaseNotFound
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var (
			storedHolder string
			active       bool
		)
		err := tx.QueryRow(ctx, `
			SELECT holder, released_at IS NULL AND expires_at > now()
			FROM leases WHERE id = $1 FOR UPDATE`, id,
		).Scan(&storedHolder, &active)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return lease.ErrLeaseNotFound
		case err != nil:
			return err
		case storedHolder != holder:
			return lease.ErrNotHolder
		case !active:
			return nil
		}

		_, err = tx.Exec(ctx, `UPDATE leases SET released_at = now() WHERE id = $1`, id)
		return err
	})
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
