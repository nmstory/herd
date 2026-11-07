package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/nmstory/herd/gen/go/herd/events/v1"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/outbox"
)

// endedLease is a row returned by an UPDATE that ends leases. Columns must
// match endedColumns.
type endedLease struct {
	ID         string
	DeviceID   string
	Holder     string
	ExpiresAt  time.Time
	ReleasedAt time.Time
	Held       int64
}

const endedColumns = `id::text, device_id, holder, expires_at, released_at, held`

// endLeases runs an UPDATE … RETURNING endedColumns and writes a released or
// expired event for every lease it ended.
func (s *Store) endLeases(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]endedLease, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	ended, err := pgx.CollectRows(rows, pgx.RowToStructByPos[endedLease])
	if err != nil {
		return nil, err
	}

	for _, l := range ended {
		typ := eventsv1.LeaseEvent_TYPE_RELEASED
		if !l.ReleasedAt.Before(l.ExpiresAt) {
			typ = eventsv1.LeaseEvent_TYPE_EXPIRED
		}
		if err := s.writeLeaseEvent(ctx, tx, &eventsv1.LeaseEvent{
			Type: typ, LeaseId: l.ID, DeviceId: l.DeviceID, Holder: l.Holder, Held: l.Held,
			ExpiresAt: timestamppb.New(l.ExpiresAt), OccurredAt: timestamppb.New(l.ReleasedAt),
		}); err != nil {
			return nil, err
		}
	}

	return ended, nil
}

func (s *Store) leaseAcquired(ctx context.Context, tx pgx.Tx, l lease.Lease, at time.Time) error {
	return s.writeLeaseEvent(ctx, tx, &eventsv1.LeaseEvent{
		Type: eventsv1.LeaseEvent_TYPE_ACQUIRED, LeaseId: l.ID, DeviceId: l.DeviceID, Holder: l.Holder, Held: l.Held,
		ExpiresAt: timestamppb.New(l.ExpiresAt), OccurredAt: timestamppb.New(at),
	})
}

func (s *Store) writeLeaseEvent(ctx context.Context, tx pgx.Tx, e *eventsv1.LeaseEvent) error {
	if s.Events == nil {
		return nil
	}
	return s.Events.Write(ctx, tx, outbox.LeaseTopic, e.DeviceId, e)
}
