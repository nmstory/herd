// Package outbox gets events from Postgres to Kafka without dual writes.
// Writers insert events into the outbox table inside the transaction that
// makes the change; the Relay publishes them afterwards.
package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	LeaseTopic  = "herd.lease.v1"
	LedgerTopic = "herd.ledger.v1"

	// EventIDHeader carries the outbox row id on every Kafka record, for
	// consumers to drop the duplicates at-least-once delivery produces.
	EventIDHeader = "herd-event-id"
)

// Writer records an event as part of the transaction tx.
type Writer interface {
	Write(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error
}

// Table is the standard Writer: it inserts into the outbox table, so the
// event commits or rolls back with everything else in tx.
type Table struct {
	// TopicPrefix is prepended to every topic, so tests sharing a broker can
	// keep their events apart.
	TopicPrefix string
}

func (t Table) Write(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`, t.TopicPrefix+topic, key, payload)

	return err
}

// Direct publishes straight to Kafka from inside the transaction: the dual
// write the outbox exists to avoid. If the transaction then rolls back, the
// event is already out. It's here only so herdbench can show that happening.
type Direct struct {
	Client      *kgo.Client
	TopicPrefix string
}

func (d Direct) Write(ctx context.Context, _ pgx.Tx, topic, key string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return d.Client.ProduceSync(ctx, &kgo.Record{Topic: d.TopicPrefix + topic, Key: []byte(key), Value: payload}).FirstErr()
}
