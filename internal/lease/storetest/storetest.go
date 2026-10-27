// Package storetest is a conformance suite every lease.Store must pass. The
// in-memory and Postgres stores run the exact same tests, so any difference
// between them shows up here rather than in production.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
)

// Factory returns a fresh, empty store holding the given devices.
type Factory func(t *testing.T, devices []config.DeviceConfig) lease.Store

func Devices(n int) []config.DeviceConfig {
	out := make([]config.DeviceConfig, n)
	for i := range out {
		zone := "us-east"
		if i%2 == 1 {
			zone = "us-west"
		}
		out[i] = config.DeviceConfig{
			ID:     fmt.Sprintf("dev-%03d", i+1),
			Labels: map[string]string{"os": "linux", "zone": zone},
		}
	}

	return out
}

func params(holder, key string) lease.AcquireParams {
	return lease.AcquireParams{Holder: holder, IdempotencyKey: key, Duration: time.Minute}
}

func Run(t *testing.T, newStore Factory) {
	t.Run("Validation", func(t *testing.T) { testValidation(t, newStore) })
	t.Run("SelectorAndList", func(t *testing.T) { testSelectorAndList(t, newStore) })
	t.Run("NoDoubleLeaseUnderContention", func(t *testing.T) { testNoDoubleLease(t, newStore) })
	t.Run("IdempotentRetry", func(t *testing.T) { testIdempotentRetry(t, newStore) })
	t.Run("ConcurrentRetriesClaimOneDevice", func(t *testing.T) { testConcurrentRetries(t, newStore) })
	t.Run("IdempotencyMismatch", func(t *testing.T) { testIdempotencyMismatch(t, newStore) })
	t.Run("ExpiredLeaseIsReclaimed", func(t *testing.T) { testExpiry(t, newStore) })
	t.Run("Release", func(t *testing.T) { testRelease(t, newStore) })
}

func testValidation(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(1))
	ctx := context.Background()

	for name, p := range map[string]lease.AcquireParams{
		"no holder":     params("", "k"),
		"no key":        params("alice", ""),
		"zero duration": {Holder: "alice", IdempotencyKey: "k"},
	} {
		if _, err := s.Acquire(ctx, p); !errors.Is(err, lease.ErrInvalidArgument) {
			t.Errorf("%s: got %v, want ErrInvalidArgument", name, err)
		}
	}
}

func testSelectorAndList(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(4))
	ctx := context.Background()

	p := params("alice", "k1")
	p.Selector = map[string]string{"zone": "us-west"}
	l, err := s.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if l.DeviceID != "dev-002" {
		t.Errorf("leased %s, want dev-002 (first us-west device)", l.DeviceID)
	}

	devices, err := s.ListDevices(ctx, map[string]string{"zone": "us-west"})
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 2 {
		t.Fatalf("got %d us-west devices, want 2", len(devices))
	}
	for _, d := range devices {
		if want := d.ID == l.DeviceID; d.Leased != want {
			t.Errorf("%s Leased = %v, want %v", d.ID, d.Leased, want)
		}
	}

	all, err := s.ListDevices(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Errorf("got %d devices with empty selector, want 4", len(all))
	}
}

func testNoDoubleLease(t *testing.T, newStore Factory) {
	const devices, claimers = 20, 100
	s := newStore(t, Devices(devices))

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		byDevice = map[string]string{}
		failures int
	)
	for i := range claimers {
		wg.Go(func() {
			l, err := s.Acquire(context.Background(), params(fmt.Sprintf("client-%d", i), "k"))

			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, lease.ErrNoDeviceAvailable):
				failures++
			case err != nil:
				t.Errorf("client-%d: %v", i, err)
			default:
				if prev, ok := byDevice[l.DeviceID]; ok {
					t.Errorf("%s leased twice: %s and client-%d", l.DeviceID, prev, i)
				}
				byDevice[l.DeviceID] = fmt.Sprintf("client-%d", i)
			}
		})
	}
	wg.Wait()

	if len(byDevice) != devices || failures != claimers-devices {
		t.Errorf("got %d leases and %d failures, want %d and %d", len(byDevice), failures, devices, claimers-devices)
	}
}

func testIdempotentRetry(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(2))
	ctx := context.Background()

	first, err := s.Acquire(ctx, params("alice", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.Acquire(ctx, params("alice", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	if retry.ID != first.ID {
		t.Errorf("retry returned lease %s, want original %s", retry.ID, first.ID)
	}

	// The same key from a different holder is a different request.
	other, err := s.Acquire(ctx, params("bob", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	if other.DeviceID == first.DeviceID {
		t.Errorf("bob got alice's device %s", first.DeviceID)
	}
}

func testConcurrentRetries(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(5))

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = map[string]bool{}
	)
	for range 10 {
		wg.Go(func() {
			l, err := s.Acquire(context.Background(), params("alice", "same-key"))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			ids[l.ID] = true
			mu.Unlock()
		})
	}
	wg.Wait()

	if len(ids) != 1 {
		t.Errorf("10 concurrent retries produced %d leases, want 1", len(ids))
	}

	devices, err := s.ListDevices(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	leased := 0
	for _, d := range devices {
		if d.Leased {
			leased++
		}
	}
	if leased != 1 {
		t.Errorf("%d devices leased, want 1", leased)
	}
}

func testIdempotencyMismatch(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(2))
	ctx := context.Background()

	if _, err := s.Acquire(ctx, params("alice", "k1")); err != nil {
		t.Fatal(err)
	}

	p := params("alice", "k1")
	p.Duration = 2 * time.Minute
	if _, err := s.Acquire(ctx, p); !errors.Is(err, lease.ErrIdempotencyMismatch) {
		t.Errorf("got %v, want ErrIdempotencyMismatch", err)
	}
}

func testExpiry(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(1))
	ctx := context.Background()

	p := params("alice", "k1")
	p.Duration = 200 * time.Millisecond
	first, err := s.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Acquire(ctx, params("bob", "k1")); !errors.Is(err, lease.ErrNoDeviceAvailable) {
		t.Fatalf("before expiry: got %v, want ErrNoDeviceAvailable", err)
	}

	time.Sleep(300 * time.Millisecond)

	second, err := s.Acquire(ctx, params("bob", "k2"))
	if err != nil {
		t.Fatalf("after expiry: %v", err)
	}
	if second.ID == first.ID || second.DeviceID != first.DeviceID {
		t.Errorf("got lease %s on %s, want a new lease on %s", second.ID, second.DeviceID, first.DeviceID)
	}
}

func testRelease(t *testing.T, newStore Factory) {
	s := newStore(t, Devices(1))
	ctx := context.Background()

	l, err := s.Acquire(ctx, params("alice", "k1"))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Release(ctx, l.ID, "bob"); !errors.Is(err, lease.ErrNotHolder) {
		t.Errorf("release by bob: got %v, want ErrNotHolder", err)
	}
	if err := s.Release(ctx, "00000000-0000-4000-8000-000000000000", "alice"); !errors.Is(err, lease.ErrLeaseNotFound) {
		t.Errorf("release unknown: got %v, want ErrLeaseNotFound", err)
	}
	if err := s.Release(ctx, "not-a-uuid", "alice"); !errors.Is(err, lease.ErrLeaseNotFound) {
		t.Errorf("release malformed id: got %v, want ErrLeaseNotFound", err)
	}

	if err := s.Release(ctx, l.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, l.ID, "alice"); err != nil {
		t.Errorf("second release: got %v, want nil", err)
	}

	if _, err := s.Acquire(ctx, params("bob", "k1")); err != nil {
		t.Errorf("acquire after release: %v", err)
	}
}
