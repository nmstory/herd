// herdbench runs the lease experiments for the Postgres write-up directly
// against a store, without gRPC in the way.
//
//	herdbench -store memory   -mode latency
//	herdbench -store postgres -mode latency
//	herdbench -store postgres -mode contention -lock skip
//	herdbench -store postgres -mode contention -lock wait
//	herdbench -store postgres -mode contention -lock none -drop-unique-index
//
// Postgres runs use a throwaway schema that is dropped afterwards.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/memstore"
	"github.com/nmstory/herd/internal/lease/pgstore"
	"github.com/nmstory/herd/internal/registry"
)

func main() {
	var (
		storeKind   = flag.String("store", "memory", "memory or postgres")
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL")
		mode        = flag.String("mode", "latency", "latency or contention")
		lockMode    = flag.String("lock", "skip", "postgres row locking: skip, wait or none")
		dropIndex   = flag.Bool("drop-unique-index", false, "drop the one-active-lease index to show what it prevents")
		devices     = flag.Int("devices", 20, "devices in the pool")
		claimers    = flag.Int("claimers", 100, "concurrent claimers (contention mode)")
		iterations  = flag.Int("n", 2000, "acquire/release cycles (latency mode)")
	)
	flag.Parse()
	ctx := context.Background()

	store, conflicts, cleanup, err := openStore(ctx, *storeKind, *databaseURL, *lockMode, *dropIndex, makeDevices(*devices))
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	switch *mode {
	case "latency":
		latency(ctx, store, *iterations)
	case "contention":
		contention(ctx, store, *claimers)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	if conflicts != nil {
		fmt.Printf("unique-violation retries: %d\n", conflicts())
	}
}

func makeDevices(n int) []config.DeviceConfig {
	out := make([]config.DeviceConfig, n)
	for i := range out {
		out[i] = config.DeviceConfig{ID: fmt.Sprintf("dev-%03d", i+1), Labels: map[string]string{"os": "linux"}}
	}
	return out
}

func openStore(ctx context.Context, kind, url, lock string, dropIndex bool, devices []config.DeviceConfig) (lease.Store, func() int64, func(), error) {
	if kind == "memory" {
		return memstore.New(registry.New(&config.Config{DeviceConfigs: devices})), nil, func() {}, nil
	}

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, nil, nil, err
	}
	schema := "herdbench_" + rand.Text()[:12]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return nil, nil, nil, err
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, nil, nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 64
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	cleanup := func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	}

	if err := pgstore.Migrate(ctx, pool); err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	if err := pgstore.SyncDevices(ctx, pool, devices); err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	// Open every connection up front so timings measure locking, not
	// connection setup.
	conns := make([]*pgxpool.Conn, 0, cfg.MaxConns)
	for range cfg.MaxConns {
		c, err := pool.Acquire(ctx)
		if err != nil {
			cleanup()
			return nil, nil, nil, err
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Release()
	}

	if dropIndex {
		if _, err := pool.Exec(ctx, "DROP INDEX leases_one_active_per_device"); err != nil {
			cleanup()
			return nil, nil, nil, err
		}
	}

	s := pgstore.New(pool)
	switch lock {
	case "skip":
		s.Lock = pgstore.SkipLocked
	case "wait":
		s.Lock = pgstore.Wait
	case "none":
		s.Lock = pgstore.NoLock
	default:
		cleanup()
		return nil, nil, nil, fmt.Errorf("unknown lock mode %q", lock)
	}

	return s, s.Conflicts.Load, cleanup, nil
}

// latency times uncontended acquires, releasing each lease so the pool never
// runs dry.
func latency(ctx context.Context, s lease.Store, n int) {
	samples := make([]time.Duration, 0, n)
	for i := range n {
		start := time.Now()
		l, err := s.Acquire(ctx, lease.AcquireParams{
			Holder: "bench", IdempotencyKey: fmt.Sprint(i), Duration: time.Minute,
		})
		samples = append(samples, time.Since(start))
		if err != nil {
			log.Fatal(err)
		}
		if err := s.Release(ctx, l.ID, "bench"); err != nil {
			log.Fatal(err)
		}
	}

	report("acquire", samples)
}

// contention starts every claimer at once against a pool smaller than the
// number of claimers, then checks no device was handed out twice.
func contention(ctx context.Context, s lease.Store, claimers int) {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		samples  []time.Duration
		byDevice = map[string]int{}
		failed   int
		start    = make(chan struct{})
	)
	for i := range claimers {
		wg.Go(func() {
			<-start
			t := time.Now()
			l, err := s.Acquire(ctx, lease.AcquireParams{
				Holder: fmt.Sprintf("client-%d", i), IdempotencyKey: "k", Duration: time.Minute,
			})
			d := time.Since(t)

			mu.Lock()
			defer mu.Unlock()
			samples = append(samples, d)
			switch {
			case errors.Is(err, lease.ErrNoDeviceAvailable):
				failed++
			case err != nil:
				log.Printf("client-%d: %v", i, err)
				failed++
			default:
				byDevice[l.DeviceID]++
			}
		})
	}

	began := time.Now()
	close(start)
	wg.Wait()
	wall := time.Since(began)

	doubled := 0
	for _, n := range byDevice {
		if n > 1 {
			doubled += n - 1
		}
	}
	leases := 0
	for _, n := range byDevice {
		leases += n
	}

	fmt.Printf("claimers: %d  leases granted: %d  refused: %d  wall: %v\n", claimers, leases, failed, wall.Round(time.Microsecond))
	fmt.Printf("DOUBLE-LEASED: %d\n", doubled)
	report("acquire", samples)
}

func report(name string, samples []time.Duration) {
	slices.Sort(samples)
	pct := func(p float64) time.Duration { return samples[int(p*float64(len(samples)-1))] }

	var total time.Duration
	for _, d := range samples {
		total += d
	}
	fmt.Printf("%s n=%d  mean=%v  p50=%v  p99=%v  max=%v\n", name, len(samples),
		(total / time.Duration(len(samples))).Round(time.Microsecond),
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond),
		samples[len(samples)-1].Round(time.Microsecond))
}
