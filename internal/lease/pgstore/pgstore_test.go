package pgstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/db"
	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/storetest"
)

func setup(t testing.TB, devices []config.DeviceConfig) (string, *pgxpool.Pool) {
	schema, pool := dbtest.Open(t)
	if err := SyncDevices(context.Background(), pool, devices); err != nil {
		t.Fatal(err)
	}

	return schema, pool
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T, devices []config.DeviceConfig) lease.Store {
		_, pool := setup(t, devices)
		return New(pool)
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	_, pool := setup(t, storetest.Devices(1))
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

// Leases live in the database, so a restarted herd still knows about them.
// The in-memory store fails this by design.
func TestLeasesSurviveRestart(t *testing.T) {
	ctx := context.Background()
	schema, pool := setup(t, storetest.Devices(1))

	p := lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: time.Minute}
	before, err := New(pool).Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	restarted := New(dbtest.OpenPool(t, schema))

	if _, err := restarted.Acquire(ctx, lease.AcquireParams{Holder: "bob", IdempotencyKey: "k1", Duration: time.Minute}); !errors.Is(err, lease.ErrNoDeviceAvailable) {
		t.Errorf("bob after restart: got %v, want ErrNoDeviceAvailable", err)
	}

	after, err := restarted.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if after.ID != before.ID {
		t.Errorf("alice's retry after restart got lease %s, want %s", after.ID, before.ID)
	}
}

// Two herd replicas with their own connection pools share one device pool.
func TestReplicasNeverDoubleLease(t *testing.T) {
	const devices, claimers = 20, 100
	schema, pool := setup(t, storetest.Devices(devices))
	replicas := []*Store{New(pool), New(dbtest.OpenPool(t, schema))}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		byDevice = map[string]int{}
	)
	for i := range claimers {
		wg.Go(func() {
			l, err := replicas[i%2].Acquire(context.Background(), lease.AcquireParams{
				Holder: fmt.Sprintf("client-%d", i), IdempotencyKey: "k", Duration: time.Minute,
			})
			if errors.Is(err, lease.ErrNoDeviceAvailable) {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			byDevice[l.DeviceID]++
			mu.Unlock()
		})
	}
	wg.Wait()

	if len(byDevice) != devices {
		t.Errorf("%d devices leased, want %d", len(byDevice), devices)
	}
	for id, n := range byDevice {
		if n != 1 {
			t.Errorf("%s leased %d times", id, n)
		}
	}
}
