package pgstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
)

func models(n int, model string) []config.DeviceConfig {
	out := make([]config.DeviceConfig, n)
	for i := range out {
		out[i] = config.DeviceConfig{ID: fmt.Sprintf("dev-%03d", i+1), Labels: map[string]string{"model": model}}
	}
	return out
}

func setupBilling(t *testing.T, devices []config.DeviceConfig, prices map[string]int64) (*Store, *ledger.Book, *pgxpool.Pool) {
	t.Helper()
	_, pool := setup(t, devices)
	if err := SyncPrices(context.Background(), pool, prices); err != nil {
		t.Fatal(err)
	}
	s := New(pool)
	s.Billing = true

	return s, ledger.NewBook(pool, outbox.Table{}), pool
}

func wallet(t *testing.T, book *ledger.Book, holder string) ledger.Wallet {
	t.Helper()
	w, err := book.Wallet(context.Background(), holder)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestHoldAndCapture(t *testing.T) {
	ctx := context.Background()
	s, book, _ := setupBilling(t, models(1, "jetson-orin"), map[string]int64{"jetson-orin": 5})

	if _, err := book.TopUp(ctx, "alice", 100, "t1"); err != nil {
		t.Fatal(err)
	}
	p := lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: 30 * time.Minute}

	// 5/min × 30m = 150 > 100: refused, and the device must not stay leased.
	if _, err := s.Acquire(ctx, p); !errors.Is(err, lease.ErrInsufficientCredits) {
		t.Fatalf("got %v, want ErrInsufficientCredits", err)
	}
	if devices, _ := s.ListDevices(ctx, nil); devices[0].Leased {
		t.Fatal("device leased by a refused acquire")
	}

	if _, err := book.TopUp(ctx, "alice", 400, "t2"); err != nil {
		t.Fatal(err)
	}
	p.IdempotencyKey = "k2"
	l, err := s.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if l.Held != 150 || l.CreditsPerMinute != 5 {
		t.Errorf("held %d at %d/min, want 150 at 5/min", l.Held, l.CreditsPerMinute)
	}
	if w := wallet(t, book, "alice"); w.Available != 350 || w.Held != 150 {
		t.Errorf("after acquire: %+v, want 350 available, 150 held", w)
	}

	// The retried acquire must not hold a second time.
	if _, err := s.Acquire(ctx, p); err != nil {
		t.Fatal(err)
	}
	if w := wallet(t, book, "alice"); w.Available != 350 {
		t.Errorf("after retried acquire: available %d, want 350", w.Available)
	}

	// Released within the first minute: charged one minute.
	settled, err := s.Release(ctx, l.ID, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if settled != (lease.Settlement{Charged: 5, Returned: 145}) {
		t.Errorf("settlement %+v, want charged 5, returned 145", settled)
	}

	// A retried release reports the same settlement and moves nothing.
	again, err := s.Release(ctx, l.ID, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if again != settled {
		t.Errorf("retried release reported %+v, want %+v", again, settled)
	}
	if w := wallet(t, book, "alice"); w.Available != 495 || w.Held != 0 {
		t.Errorf("after release: %+v, want 495 available, 0 held", w)
	}
}

// A lease nobody releases is charged in full when its device is next taken.
func TestExpiredLeaseSettlesOnReuse(t *testing.T) {
	ctx := context.Background()
	s, book, pool := setupBilling(t, models(1, "rpi5"), map[string]int64{"default": 2})

	for _, h := range []string{"alice", "bob"} {
		if _, err := book.TopUp(ctx, h, 10, "t1"); err != nil {
			t.Fatal(err)
		}
	}

	first, err := s.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if _, err := s.Acquire(ctx, lease.AcquireParams{Holder: "bob", IdempotencyKey: "k1", Duration: time.Minute}); err != nil {
		t.Fatal(err)
	}

	if w := wallet(t, book, "alice"); w.Available != 8 || w.Held != 0 {
		t.Errorf("alice: %+v, want 8 available (charged the full hold of 2), 0 held", w)
	}
	var kind string
	if err := pool.QueryRow(ctx, `SELECT kind FROM journal_entries WHERE idempotency_key = $1`, "settle:"+first.ID).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != "expire" {
		t.Errorf("settlement kind %q, want expire", kind)
	}
}

// 100 claimers share one wallet that can pay for 10 leases. Devices are
// plentiful, so credits are the limit: exactly 10 must succeed.
func TestSharedWalletNeverOverdraws(t *testing.T) {
	ctx := context.Background()
	s, book, _ := setupBilling(t, models(20, "rpi4"), map[string]int64{"rpi4": 1})

	if _, err := book.TopUp(ctx, "team", 10, "t1"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var ok, refused atomic.Int64
	for i := range 100 {
		wg.Go(func() {
			_, err := s.Acquire(ctx, lease.AcquireParams{Holder: "team", IdempotencyKey: fmt.Sprint(i), Duration: time.Minute})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, lease.ErrInsufficientCredits):
				refused.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if ok.Load() != 10 || refused.Load() != 90 {
		t.Errorf("%d leases and %d refusals, want 10 and 90", ok.Load(), refused.Load())
	}
	if w := wallet(t, book, "team"); w.Available != 0 || w.Held != 10 {
		t.Errorf("wallet %+v, want 0 available, 10 held", w)
	}

	leased := 0
	devices, _ := s.ListDevices(ctx, nil)
	for _, d := range devices {
		if d.Leased {
			leased++
		}
	}
	if leased != 10 {
		t.Errorf("%d devices leased, want 10 (refused acquires must roll back)", leased)
	}
}

func TestUnpricedModelIsFree(t *testing.T) {
	ctx := context.Background()
	s, _, _ := setupBilling(t, models(1, "rpi4"), nil)

	l, err := s.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: time.Minute})
	if err != nil {
		t.Fatalf("acquire with no prices and an empty wallet: %v", err)
	}
	if l.Held != 0 {
		t.Errorf("held %d, want 0", l.Held)
	}
	if settled, err := s.Release(ctx, l.ID, "alice"); err != nil || settled != (lease.Settlement{}) {
		t.Errorf("release: %+v, %v, want an empty settlement", settled, err)
	}
}

// Two replicas settling the same backlog at once must settle every lease
// exactly once.
func TestConcurrentSettlersSettleEachLeaseOnce(t *testing.T) {
	const leases = 30
	ctx := context.Background()
	schema, pool := setup(t, models(leases, "rpi4"))
	if err := SyncPrices(ctx, pool, map[string]int64{"rpi4": 1}); err != nil {
		t.Fatal(err)
	}
	book := ledger.NewBook(pool, outbox.Table{})
	if _, err := book.TopUp(ctx, "alice", 60*leases, "t1"); err != nil { // an hour's hold each
		t.Fatal(err)
	}

	replicas := []*Store{New(pool), New(dbtest.OpenPool(t, schema))}
	for _, r := range replicas {
		r.Billing = true
	}
	for i := range leases {
		if _, err := replicas[0].Acquire(ctx, lease.AcquireParams{
			Holder: "alice", IdempotencyKey: fmt.Sprint(i), Duration: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Expire them all at once. Waiting for short leases to run out is racy:
	// under load, early leases expire while later ones are still being taken,
	// and those acquires reclaim (and settle) the expired devices themselves.
	if _, err := pool.Exec(ctx, `UPDATE leases SET acquired_at = acquired_at - interval '1 hour', expires_at = acquired_at - interval '59 minutes'`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var settled [2]int
	for i, r := range replicas {
		wg.Go(func() {
			for {
				n, err := r.SettleExpired(ctx, 5)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
				settled[i] += n
			}
		})
	}
	wg.Wait()

	if settled[0]+settled[1] != leases {
		t.Errorf("settlers settled %d and %d leases, want %d in total", settled[0], settled[1], leases)
	}
	var entries int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM journal_entries WHERE kind = 'expire'`).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != leases {
		t.Errorf("%d expire entries, want %d", entries, leases)
	}
	// Each lease held 60 credits and ran for its one (backdated) minute.
	if w := wallet(t, book, "alice"); w.Available != 59*leases || w.Held != 0 {
		t.Errorf("wallet %+v, want %d available, 0 held", w, 59*leases)
	}
	t.Logf("replica 0 settled %d, replica 1 settled %d", settled[0], settled[1])
}
