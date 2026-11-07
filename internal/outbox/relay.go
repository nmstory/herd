package outbox

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// relayLock is the advisory lock that elects one relay among herd replicas.
const relayLock = 4832

// Relay publishes outbox rows to Kafka in id order and marks them published.
//
// Delivery is at-least-once: if the relay dies after Kafka accepts a batch
// but before the rows are marked, the next leader publishes them again, and
// consumers drop the repeats using EventIDHeader.
//
// Only one relay publishes at a time. Two relays working through the outbox
// in parallel would interleave batches and break per-key ordering, so each
// relay first takes a session-level advisory lock on its own connection and
// the others wait as standbys. Losing that connection loses the lock, and the
// relay steps down. This is leader election, not fencing: a leader that
// stalls just as its connection drops can still publish one stale batch,
// which consumers again absorb as duplicates.
type Relay struct {
	Pool   *pgxpool.Pool
	Client *kgo.Client
	Batch  int           // rows per batch; default 200
	Idle   time.Duration // poll interval when the outbox is empty; default 100ms
}

// EnsureTopics creates herd's topics if they don't exist yet.
func EnsureTopics(ctx context.Context, client *kgo.Client, partitions int32) error {
	resp, err := kadm.NewClient(client).CreateTopics(ctx, partitions, -1, nil, LeaseTopic, LedgerTopic)
	if err != nil {
		return err
	}
	for _, t := range resp {
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Topic, t.Err)
		}
	}

	return nil
}

// Run publishes until ctx is cancelled, taking over as leader whenever the
// current one goes away.
func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := r.lead(ctx); err != nil && ctx.Err() == nil {
			log.Printf("relay: %v", err)
		}
		sleep(ctx, time.Second)
	}
}

// lead publishes for as long as this relay holds the leader lock. It
// returns without error if another relay is already leading.
func (r *Relay) lead(ctx context.Context) error {
	pooled, err := r.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// Take the connection out of the pool and close it when done, rather
	// than returning it: closing releases the session lock for certain.
	conn := pooled.Hijack()
	defer conn.Close(context.Background())

	var leader bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, relayLock).Scan(&leader); err != nil || !leader {
		return err
	}
	log.Printf("relay: leading")

	for ctx.Err() == nil {
		n, err := r.publishBatch(ctx, conn)
		if err != nil {
			return err
		}
		if n == 0 {
			sleep(ctx, r.idle())
		}
	}

	return nil
}

// publishBatch runs on the leader's own connection, so if that connection
// (and with it the lock) is gone, publishing stops too.
func (r *Relay) publishBatch(ctx context.Context, conn *pgx.Conn) (int, error) {
	rows, err := conn.Query(ctx, `
		SELECT id, topic, key, payload FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT $1`, r.batch())
	if err != nil {
		return 0, err
	}

	var (
		ids     []int64
		records []*kgo.Record
	)
	for rows.Next() {
		var (
			id         int64
			topic, key string
			payload    []byte
		)
		if err := rows.Scan(&id, &topic, &key, &payload); err != nil {
			return 0, err
		}
		ids = append(ids, id)
		records = append(records, &kgo.Record{
			Topic:   topic,
			Key:     []byte(key),
			Value:   payload,
			Headers: []kgo.RecordHeader{{Key: EventIDHeader, Value: []byte(strconv.FormatInt(id, 10))}},
		})
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	// The client is idempotent by default, so Kafka won't duplicate or
	// reorder records within a partition when the client retries.
	if err := r.Client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, err
	}

	_, err = conn.Exec(ctx, `UPDATE outbox SET published_at = clock_timestamp() WHERE id = ANY($1)`, ids)
	return len(ids), err
}

func (r *Relay) batch() int {
	if r.Batch > 0 {
		return r.Batch
	}
	return 200
}

func (r *Relay) idle() time.Duration {
	if r.Idle > 0 {
		return r.Idle
	}
	return 100 * time.Millisecond
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
