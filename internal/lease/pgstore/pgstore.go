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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
)

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

	// Billing charges leases to the holder's wallet. See billing.go.
	Billing bool

	// Events receives an event for every lease change and journal entry,
	// inside the transaction that makes it. nil disables events.
	Events outbox.Writer

	// Conflicts counts unique violations that forced Acquire to retry.
	Conflicts atomic.Int64
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Events: outbox.Table{}}
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
		if isRetryable(err) {
			// Usually a concurrent claimer took this device, or a concurrent
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
			SELECT id, device_id, holder, expires_at, credits_per_minute, held, request_hash
			FROM leases WHERE holder = $1 AND idempotency_key = $2`,
			p.Holder, p.IdempotencyKey,
		).Scan(&l.ID, &l.DeviceID, &l.Holder, &l.ExpiresAt, &l.CreditsPerMinute, &l.Held, &storedHash)
		switch {
		case err == nil:
			if !bytes.Equal(storedHash, hash) {
				return lease.ErrIdempotencyMismatch
			}
			return nil // replay: return the original lease unchanged
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		// Queue on the wallet before taking a device. Otherwise claimers
		// sharing a wallet each lock a device and then wait for the wallet,
		// and SKIP LOCKED makes newcomers see every device as taken.
		if s.Billing {
			if _, err := tx.Exec(ctx, `SELECT FROM account_balances WHERE account = $1 FOR UPDATE`,
				ledger.WalletAccount(p.Holder)); err != nil {
				return err
			}
		}

		var deviceID, model string
		err = tx.QueryRow(ctx, `
			SELECT d.id, COALESCE(d.labels->>'model', '') FROM devices d
			WHERE d.labels @> $1::jsonb
			  AND NOT EXISTS (
				SELECT 1 FROM leases l
				WHERE l.device_id = d.id AND l.released_at IS NULL AND l.expires_at > now()
			  )
			ORDER BY d.id
			LIMIT 1 `+s.lockClause(), string(sel)).Scan(&deviceID, &model)
		if errors.Is(err, pgx.ErrNoRows) {
			return lease.ErrNoDeviceAvailable
		}
		if err != nil {
			return err
		}

		// Close out any expired lease so the partial unique index lets a new
		// one in.
		expired, err := s.endLeases(ctx, tx, `
			UPDATE leases SET released_at = expires_at
			WHERE device_id = $1 AND released_at IS NULL AND expires_at <= now()
			RETURNING `+endedColumns, deviceID)
		if err != nil {
			return err
		}
		if s.Billing {
			for _, e := range expired {
				if _, err := settle(ctx, tx, s.Events, e.ID); err != nil {
					return err
				}
			}
		}

		l.DeviceID, l.Holder = deviceID, p.Holder
		if s.Billing {
			if l.CreditsPerMinute, err = priceFor(ctx, tx, model); err != nil {
				return err
			}
			l.Held = l.CreditsPerMinute * minutes(p.Duration)
		}

		var acquiredAt time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO leases (device_id, holder, idempotency_key, request_hash, expires_at, credits_per_minute, held)
			VALUES ($1, $2, $3, $4, now() + $5 * interval '1 microsecond', $6, $7)
			RETURNING id, expires_at, acquired_at`,
			deviceID, p.Holder, p.IdempotencyKey, hash, p.Duration.Microseconds(), l.CreditsPerMinute, l.Held,
		).Scan(&l.ID, &l.ExpiresAt, &acquiredAt); err != nil {
			return err
		}
		if err := s.leaseAcquired(ctx, tx, l, acquiredAt); err != nil {
			return err
		}

		return hold(ctx, tx, s.Events, l)
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

func (s *Store) Release(ctx context.Context, leaseID, holder string) (lease.Settlement, error) {
	var id pgtype.UUID
	if err := id.Scan(leaseID); err != nil {
		return lease.Settlement{}, lease.ErrLeaseNotFound
	}

	var settled lease.Settlement
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var storedHolder string
		err := tx.QueryRow(ctx, `SELECT holder FROM leases WHERE id = $1 FOR UPDATE`, id).Scan(&storedHolder)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return lease.ErrLeaseNotFound
		case err != nil:
			return err
		case storedHolder != holder:
			return lease.ErrNotHolder
		}

		// A lease that has already expired ends at its expiry, not now.
		if _, err := s.endLeases(ctx, tx, `
			UPDATE leases SET released_at = least(now(), expires_at)
			WHERE id = $1 AND released_at IS NULL
			RETURNING `+endedColumns, id); err != nil {
			return err
		}

		if s.Billing {
			settled, err = settle(ctx, tx, s.Events, leaseID)
		}
		return err
	})

	return settled, err
}

// isRetryable reports errors that a fresh attempt can resolve: a unique
// violation (see the package comment) or a deadlock, which Postgres resolves
// by aborting one of the transactions.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "40P01")
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
