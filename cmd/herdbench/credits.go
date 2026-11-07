package main

// Experiments for the credits and Kafka write-up:
//
//	herdbench -mode wallet                       # shared wallet, wallet locked first
//	herdbench -mode wallet -no-wallet-lock       # what that lock prevents
//	herdbench -mode events                       # outbox, with injected crashes
//	herdbench -mode events -dual-write           # publish inside the transaction instead
//	herdbench -mode lag                          # commit-to-publish latency under load

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	crand "crypto/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/pgstore"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
	"github.com/nmstory/herd/internal/reconcile"
)

type creditsFlags struct {
	databaseURL  string
	brokers      string
	devices      int
	claimers     int
	n            int
	noWalletLock bool
	dualWrite    bool
	crashRate    float64
	duration     time.Duration
}

func openBilling(ctx context.Context, f creditsFlags) (*pgstore.Store, *pgxpool.Pool, func()) {
	pool, cleanup, err := openPostgres(ctx, f.databaseURL, makeDevices(f.devices))
	if err != nil {
		log.Fatal(err)
	}
	if err := pgstore.SyncPrices(ctx, pool, map[string]int64{"default": 1}); err != nil {
		log.Fatal(err)
	}
	s := pgstore.New(pool)
	s.Billing, s.SkipWalletLock = true, f.noWalletLock

	return s, pool, cleanup
}

// wallet: every claimer spends from one wallet that can pay for 10 one-minute
// leases. There are more devices than that, so credits are the only limit.
func walletDrain(ctx context.Context, f creditsFlags) {
	s, pool, cleanup := openBilling(ctx, f)
	defer cleanup()
	book := ledger.NewBook(pool, s.Events)
	if _, err := book.TopUp(ctx, "team", 10, "t1"); err != nil {
		log.Fatal(err)
	}

	var granted, broke, noDevice, other atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range f.claimers {
		wg.Go(func() {
			<-start
			_, err := s.Acquire(ctx, lease.AcquireParams{Holder: "team", IdempotencyKey: fmt.Sprint(i), Duration: time.Minute})
			switch {
			case err == nil:
				granted.Add(1)
			case errors.Is(err, lease.ErrInsufficientCredits):
				broke.Add(1)
			case errors.Is(err, lease.ErrNoDeviceAvailable):
				noDevice.Add(1)
			default:
				other.Add(1)
				log.Print(err)
			}
		})
	}
	began := time.Now()
	close(start)
	wg.Wait()

	w, err := book.Wallet(ctx, "team")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("claimers: %d  devices: %d  wallet funds 10 leases  wall: %v\n", f.claimers, f.devices, time.Since(began).Round(time.Millisecond))
	fmt.Printf("granted: %d  insufficient credits: %d  \"no device available\": %d  other errors: %d\n",
		granted.Load(), broke.Load(), noDevice.Load(), other.Load())
	fmt.Printf("wallet: %d available, %d held\n", w.Available, w.Held)
}

var errCrash = errors.New("simulated crash before commit")

// crashing wraps a Writer and, after a successful write, sometimes fails the
// transaction: the process dying between writing the event and committing.
type crashing struct {
	outbox.Writer
	rate    float64
	crashes *atomic.Int64
}

func (c crashing) Write(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error {
	if err := c.Writer.Write(ctx, tx, topic, key, msg); err != nil {
		return err
	}
	if rand.Float64() < c.rate {
		c.crashes.Add(1)
		return errCrash
	}
	return nil
}

// events: run top-ups and lease cycles while transactions randomly die after
// writing their events, retrying each operation until it succeeds as a real
// client would, then reconcile Kafka against Postgres.
func events(ctx context.Context, f creditsFlags) {
	s, pool, cleanup := openBilling(ctx, f)
	defer cleanup()
	client := kafkaClient(f.brokers)
	defer client.Close()

	prefix := "bench-" + strings.ToLower(crand.Text()[:8]) + "."
	if _, err := kadm.NewClient(client).CreateTopics(ctx, 3, -1, nil, prefix+outbox.LeaseTopic, prefix+outbox.LedgerTopic); err != nil {
		log.Fatal(err)
	}

	var base outbox.Writer = outbox.Table{TopicPrefix: prefix}
	name := "transactional outbox"
	if f.dualWrite {
		base, name = outbox.Direct{Client: client, TopicPrefix: prefix}, "dual write (publish, then commit)"
	}
	var crashes atomic.Int64
	s.Events = crashing{Writer: base, rate: f.crashRate, crashes: &crashes}
	book := ledger.NewBook(pool, s.Events)

	retry := func(op func() error) {
		for {
			err := op()
			if err == nil {
				return
			}
			if !errors.Is(err, errCrash) {
				log.Fatal(err)
			}
		}
	}
	for i := range f.n {
		holder := fmt.Sprint("holder-", i%5)
		retry(func() error { _, err := book.TopUp(ctx, holder, 10, fmt.Sprint(i)); return err })
		var l lease.Lease
		retry(func() (err error) {
			l, err = s.Acquire(ctx, lease.AcquireParams{Holder: holder, IdempotencyKey: fmt.Sprint(i), Duration: time.Minute})
			return err
		})
		retry(func() error { _, err := s.Release(ctx, l.ID, holder); return err })
	}

	if !f.dualWrite {
		relayCtx, stop := context.WithCancel(ctx)
		go (&outbox.Relay{Pool: pool, Client: client, Idle: 10 * time.Millisecond}).Run(relayCtx)
		waitForOutbox(ctx, pool)
		stop()
	}

	r, err := reconcile.Run(ctx, kafkaClient(f.brokers), pool, prefix+outbox.LedgerTopic)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s · %d operations · %d injected crashes (%.0f%%)\n", name, f.n*3, crashes.Load(), f.crashRate*100)
	fmt.Printf("journal entries committed: %d  ledger events in Kafka: %d\n", r.Entries+len(r.Missing)-len(r.Phantom), r.Entries)
	fmt.Printf("phantom entries (published, never committed): %d\n", len(r.Phantom))
	fmt.Printf("lost entries (committed, never published): %d\n", r.Lost())
	fmt.Printf("accounts whose balance disagrees: %d\n", len(r.Drift))
	if r.OK() {
		fmt.Println("books reconcile ✓")
	} else {
		fmt.Println("BOOKS DO NOT RECONCILE ✗")
	}
}

// lag: claimers cycle leases for a while with the relay running, then report
// how long events waited in the outbox between commit and publish.
func lag(ctx context.Context, f creditsFlags) {
	s, pool, cleanup := openBilling(ctx, f)
	defer cleanup()
	client := kafkaClient(f.brokers)
	defer client.Close()
	if err := outbox.EnsureTopics(ctx, client, 3); err != nil {
		log.Fatal(err)
	}
	book := ledger.NewBook(pool, s.Events)

	relayCtx, stop := context.WithCancel(ctx)
	defer stop()
	go (&outbox.Relay{Pool: pool, Client: client}).Run(relayCtx)

	deadline := time.Now().Add(f.duration)
	var cycles atomic.Int64
	var wg sync.WaitGroup
	for c := range f.claimers {
		holder := fmt.Sprint("holder-", c)
		if _, err := book.TopUp(ctx, holder, 1_000_000, "t1"); err != nil {
			log.Fatal(err)
		}
		wg.Go(func() {
			for i := 0; time.Now().Before(deadline); i++ {
				l, err := s.Acquire(ctx, lease.AcquireParams{Holder: holder, IdempotencyKey: fmt.Sprint(i), Duration: time.Minute})
				if errors.Is(err, lease.ErrNoDeviceAvailable) {
					continue
				}
				if err != nil {
					log.Fatal(err)
				}
				if _, err := s.Release(ctx, l.ID, holder); err != nil {
					log.Fatal(err)
				}
				cycles.Add(1)
			}
		})
	}
	wg.Wait()
	waitForOutbox(ctx, pool)

	var events int
	var p50, p99, worst float64
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
			percentile_cont(0.5) WITHIN GROUP (ORDER BY lag),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY lag),
			max(lag)
		FROM (SELECT extract(epoch FROM published_at - created_at) * 1000 AS lag FROM outbox) l`,
	).Scan(&events, &p50, &p99, &worst); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d claimers for %v: %d lease cycles, %d events (%.0f/s)\n",
		f.claimers, f.duration, cycles.Load(), events, float64(events)/f.duration.Seconds())
	fmt.Printf("commit-to-publish lag: p50=%.1fms  p99=%.1fms  max=%.1fms\n", p50, p99, worst)
}

func kafkaClient(brokers string) *kgo.Client {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func waitForOutbox(ctx context.Context, pool *pgxpool.Pool) {
	for {
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&left); err != nil {
			log.Fatal(err)
		}
		if left == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
