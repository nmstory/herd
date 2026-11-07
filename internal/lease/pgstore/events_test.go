package pgstore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/nmstory/herd/gen/go/herd/events/v1"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/outbox"
)

// Every committed change writes exactly one event, and a change that rolls
// back writes none.
func TestEveryChangeWritesOneEvent(t *testing.T) {
	ctx := context.Background()
	s, book, pool := setupBilling(t, models(2, "rpi4"), map[string]int64{"rpi4": 1})

	if _, err := book.TopUp(ctx, "alice", 3, "t1"); err != nil {
		t.Fatal(err)
	}
	l, err := s.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(ctx, l.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(ctx, l.ID, "alice"); err != nil { // retry: no new events
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k2", Duration: 100 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	// Rolled back for lack of credits (2 left, 5 needed): no events.
	if _, err := s.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k3", Duration: 5 * time.Minute}); !errors.Is(err, lease.ErrInsufficientCredits) {
		t.Fatalf("got %v, want ErrInsufficientCredits", err)
	}
	time.Sleep(200 * time.Millisecond)
	if n, err := s.SettleExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("SettleExpired = %d, %v; want 1", n, err)
	}

	rows, err := pool.Query(ctx, `SELECT topic, key, payload FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	var entryIDs []int64
	for rows.Next() {
		var topic, key string
		var payload []byte
		if err := rows.Scan(&topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		switch topic {
		case outbox.LeaseTopic:
			var e eventsv1.LeaseEvent
			if err := proto.Unmarshal(payload, &e); err != nil {
				t.Fatal(err)
			}
			if key != e.DeviceId {
				t.Errorf("lease event keyed %q, want device %q", key, e.DeviceId)
			}
			got = append(got, "lease:"+e.Type.String())
		case outbox.LedgerTopic:
			var e eventsv1.LedgerEntry
			if err := proto.Unmarshal(payload, &e); err != nil {
				t.Fatal(err)
			}
			if key != e.Holder {
				t.Errorf("ledger event keyed %q, want holder %q", key, e.Holder)
			}
			got = append(got, "ledger:"+e.Kind)
			entryIDs = append(entryIDs, e.EntryId)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"ledger:topup",
		"lease:TYPE_ACQUIRED", "ledger:hold",
		"lease:TYPE_RELEASED", "ledger:capture",
		"lease:TYPE_ACQUIRED", "ledger:hold",
		"lease:TYPE_EXPIRED", "ledger:expire",
	}
	if !slices.Equal(got, want) {
		t.Errorf("events:\n got %v\nwant %v", got, want)
	}

	journal, err := pool.Query(ctx, `SELECT id FROM journal_entries ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(journal, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, entryIDs) {
		t.Errorf("journal entries %v, ledger events for %v; want one event per entry", ids, entryIDs)
	}
}
