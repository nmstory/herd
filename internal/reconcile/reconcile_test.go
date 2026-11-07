package reconcile_test

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/nmstory/herd/gen/go/herd/events/v1"
	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/pgstore"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
	"github.com/nmstory/herd/internal/reconcile"
)

func newClient(t *testing.T) *kgo.Client {
	b := os.Getenv("HERD_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("HERD_TEST_KAFKA_BROKERS not set")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(b, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// world is herd writing to its own topics: a billing store and book whose
// events go to "<prefix>herd.ledger.v1" and friends, plus a relay.
type world struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	store  *pgstore.Store
	book   *ledger.Book
	topic  string
}

func newWorld(t *testing.T) *world {
	ctx := context.Background()
	client := newClient(t)
	_, pool := dbtest.Open(t)

	prefix := "t" + strings.ToLower(rand.Text()[:8]) + "."
	events := outbox.Table{TopicPrefix: prefix}
	resp, err := kadm.NewClient(client).CreateTopics(ctx, 3, -1, nil, prefix+outbox.LeaseTopic, prefix+outbox.LedgerTopic)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatal(err)
	}

	devices := []config.DeviceConfig{{ID: "dev-001", Labels: map[string]string{"model": "rpi4"}}}
	if err := pgstore.SyncDevices(ctx, pool, devices); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.SyncPrices(ctx, pool, map[string]int64{"rpi4": 2}); err != nil {
		t.Fatal(err)
	}
	store := pgstore.New(pool)
	store.Billing, store.Events = true, events

	return &world{pool: pool, client: client, store: store, book: ledger.NewBook(pool, events), topic: prefix + outbox.LedgerTopic}
}

// trade does a top-up, a released lease and an expired one.
func (w *world) trade(t *testing.T) {
	ctx := context.Background()
	if _, err := w.book.TopUp(ctx, "alice", 100, "t1"); err != nil {
		t.Fatal(err)
	}
	l, err := w.store.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k1", Duration: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Release(ctx, l.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Acquire(ctx, lease.AcquireParams{Holder: "alice", IdempotencyKey: "k2", Duration: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := w.store.SettleExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
}

func (w *world) relay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&outbox.Relay{Pool: w.pool, Client: w.client, Idle: 10 * time.Millisecond}).Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(10 * time.Second)
	for {
		var left int
		if err := w.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d outbox rows still unpublished", left)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (w *world) reconcile(t *testing.T) reconcile.Report {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := reconcile.Run(ctx, newClient(t), w.pool, w.topic)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBooksReconcile(t *testing.T) {
	w := newWorld(t)
	w.trade(t)
	w.relay(t)

	r := w.reconcile(t)
	if !r.OK() {
		t.Fatalf("report not OK: %+v", r)
	}
	// topup, hold, capture, hold, expire
	if r.Entries != 5 {
		t.Errorf("reconciled %d entries, want 5", r.Entries)
	}
}

// A redelivered event is a duplicate, not a second movement of money.
func TestDuplicatesAreDropped(t *testing.T) {
	w := newWorld(t)
	w.trade(t)
	w.relay(t)

	var payload []byte
	if err := w.pool.QueryRow(context.Background(), `SELECT payload FROM outbox WHERE topic = $1 ORDER BY id LIMIT 1`, w.topic).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if err := w.client.ProduceSync(context.Background(), &kgo.Record{Topic: w.topic, Key: []byte("alice"), Value: payload}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	r := w.reconcile(t)
	if !r.OK() || r.Duplicates != 1 {
		t.Errorf("got OK=%v with %d duplicates, want OK with 1: %+v", r.OK(), r.Duplicates, r)
	}
}

// An event for money that never moved in Postgres is caught, as is the
// drift it causes.
func TestPhantomEntryIsCaught(t *testing.T) {
	w := newWorld(t)
	w.trade(t)
	w.relay(t)

	phantom, err := proto.Marshal(&eventsv1.LedgerEntry{
		EntryId: 999, Kind: "topup", Holder: "mallory",
		Postings: []*eventsv1.Posting{{Account: "external", Amount: -1000}, {Account: "wallet:mallory", Amount: 1000}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.client.ProduceSync(context.Background(), &kgo.Record{Topic: w.topic, Key: []byte("mallory"), Value: phantom}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	r := w.reconcile(t)
	if r.OK() || len(r.Phantom) != 1 || r.Phantom[0] != 999 || len(r.Drift) == 0 {
		t.Errorf("want a failed report with phantom entry 999 and drift, got %+v", r)
	}
}

// Committed entries the relay hasn't sent yet are lag, not loss.
func TestUnrelayedEntriesAreLagNotLoss(t *testing.T) {
	w := newWorld(t)
	w.trade(t)

	r := w.reconcile(t)
	if !r.OK() || len(r.Missing) != 5 || r.Unrelayed != 5 {
		t.Errorf("want OK with 5 missing, all still in the outbox; got %+v", r)
	}
}
