package outbox_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/outbox"
)

// Relay tests also need Kafka, e.g. from docker-compose.yml:
//
//	HERD_TEST_KAFKA_BROKERS=localhost:19092
func brokers(t *testing.T) []string {
	b := os.Getenv("HERD_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("HERD_TEST_KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

// newTopic creates a topic only this test uses, so tests sharing a broker
// don't read each other's records.
func newTopic(t *testing.T, client *kgo.Client) string {
	topic := "herd-test-" + strings.ToLower(rand.Text()[:10])
	resp, err := kadm.NewClient(client).CreateTopics(context.Background(), 3, -1, nil, topic)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

func newClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers(t)...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// writeEvents commits n events across a few keys, as herd's transactions do.
func writeEvents(t *testing.T, pool *pgxpool.Pool, topic string, n int) {
	ctx := context.Background()
	for i := range n {
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return outbox.Table{}.Write(ctx, tx, topic, fmt.Sprint("key-", i%4), wrapperspb.Int64(int64(i)))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// consume reads n records from topic, failing if they don't arrive in time.
func consume(t *testing.T, topic string, n int) []*kgo.Record {
	client := newClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var out []*kgo.Record
	for len(out) < n {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("got %d of %d records before timing out", len(out), n)
		}
		out = append(out, fetches.Records()...)
	}
	return out
}

func eventID(t *testing.T, r *kgo.Record) int64 {
	for _, h := range r.Headers {
		if h.Key == outbox.EventIDHeader {
			id, err := strconv.ParseInt(string(h.Value), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return id
		}
	}
	t.Fatal("record has no event id header")
	return 0
}

func runRelay(t *testing.T, pool *pgxpool.Pool, client *kgo.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&outbox.Relay{Pool: pool, Client: client, Batch: 7, Idle: 10 * time.Millisecond}).Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// Two relays share one outbox. Only the leader publishes, so every event
// arrives exactly once and each key's events arrive in commit order.
func TestRelayPublishesEachEventOnceInOrder(t *testing.T) {
	const n = 60
	ctx := context.Background()
	_, pool := dbtest.Open(t)
	producer := newClient(t)
	topic := newTopic(t, producer)

	runRelay(t, pool, producer)
	runRelay(t, pool, newClient(t))
	writeEvents(t, pool, topic, n)

	records := consume(t, topic, n)
	time.Sleep(500 * time.Millisecond) // give any duplicate a chance to show up

	seen := map[int64]bool{}
	last := map[string]int64{}
	for _, r := range records {
		id := eventID(t, r)
		if seen[id] {
			t.Errorf("event %d published twice", id)
		}
		seen[id] = true
		if id <= last[string(r.Key)] {
			t.Errorf("%s: event %d arrived after %d", r.Key, id, last[string(r.Key)])
		}
		last[string(r.Key)] = id
	}

	var unpublished int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 0 {
		t.Errorf("%d rows still unpublished", unpublished)
	}
}

// Events committed while no relay is running are published once one starts:
// the outbox is durable, so Kafka being down or the relay crashing loses
// nothing.
func TestRelayCatchesUpAfterDowntime(t *testing.T) {
	const n = 25
	_, pool := dbtest.Open(t)
	producer := newClient(t)
	topic := newTopic(t, producer)

	writeEvents(t, pool, topic, n)
	runRelay(t, pool, producer)

	if got := len(consume(t, topic, n)); got != n {
		t.Errorf("got %d records, want %d", got, n)
	}
}
