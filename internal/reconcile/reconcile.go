// Package reconcile checks herd's books from the outside. It rebuilds every
// balance from the ledger events in Kafka alone, then compares the result
// with what Postgres says. It shares no code path with the writer beyond
// the event format, which is the point: a bug that corrupts the ledger
// shouldn't also be able to hide it.
package reconcile

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/nmstory/herd/gen/go/herd/events/v1"
)

type Report struct {
	Events     int // records read from Kafka
	Duplicates int // repeats of an entry already seen: expected with at-least-once delivery
	Entries    int // distinct journal entries in Kafka
	Accounts   int

	Conflicts  []int64 // entry ids seen again with different contents: never a harmless redelivery
	Unbalanced []int64 // entries in Kafka whose postings don't sum to zero
	Phantom    []int64 // entries in Kafka that never committed in Postgres
	Missing    []int64 // committed entries absent from Kafka
	Unrelayed  int     // ledger rows still waiting in the outbox, which explain some of Missing
	Drift      []Drift // accounts whose balances disagree

	// InternalDrift counts running balances in Postgres that disagree with
	// Postgres's own postings.
	InternalDrift int
}

type Drift struct {
	Account         string
	Kafka, Postgres int64
}

// Lost is the number of committed entries that will never reach Kafka:
// those missing beyond what the outbox still has to send.
func (r Report) Lost() int {
	return max(len(r.Missing)-r.Unrelayed, 0)
}

func (r Report) OK() bool {
	return len(r.Conflicts) == 0 && len(r.Unbalanced) == 0 && len(r.Phantom) == 0 && r.Lost() == 0 && len(r.Drift) == 0 && r.InternalDrift == 0
}

// Run reads topic from the beginning up to its current end and reconciles it
// against the database. It's safe to run against live traffic: see snapshot.
func Run(ctx context.Context, client *kgo.Client, pool *pgxpool.Pool, topic string) (Report, error) {
	committed, unrelayed, err := snapshot(ctx, pool)
	if err != nil {
		return Report{}, err
	}

	entries, report, err := readTopic(ctx, client, topic)
	if err != nil {
		return report, err
	}
	report.Unrelayed = unrelayed
	for _, id := range committed {
		if entries[id] == nil {
			report.Missing = append(report.Missing, id)
		}
	}

	balances := map[string]int64{}
	for id, e := range entries {
		var sum int64
		for _, p := range e.Postings {
			sum += p.Amount
			balances[p.Account] += p.Amount
		}
		if sum != 0 {
			report.Unbalanced = append(report.Unbalanced, id)
		}
	}
	report.Entries = len(entries)
	report.Accounts = len(balances)

	if err := compare(ctx, pool, entries, balances, &report); err != nil {
		return report, err
	}
	slices.Sort(report.Conflicts)
	slices.Sort(report.Unbalanced)
	slices.Sort(report.Phantom)
	slices.Sort(report.Missing)

	return report, nil
}

// readTopic consumes every partition up to the end offset it had when we
// started, deduplicating entries by entry id.
func readTopic(ctx context.Context, client *kgo.Client, topic string) (map[int64]*eventsv1.LedgerEntry, Report, error) {
	var report Report
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, report, err
	}

	partitions := map[int32]kgo.Offset{}
	remaining := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			partitions[o.Partition] = kgo.NewOffset().AtStart()
			remaining[o.Partition] = o.Offset
		}
	})

	entries := map[int64]*eventsv1.LedgerEntry{}
	if len(partitions) == 0 {
		return entries, report, nil
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions})

	for len(remaining) > 0 {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, report, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, report, fmt.Errorf("%s/%d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}

		for _, r := range fetches.Records() {
			if end, ok := remaining[r.Partition]; !ok || r.Offset >= end {
				continue // written after we started
			} else if r.Offset == end-1 {
				delete(remaining, r.Partition)
			}

			report.Events++
			e := &eventsv1.LedgerEntry{}
			if err := proto.Unmarshal(r.Value, e); err != nil {
				return nil, report, fmt.Errorf("offset %d: %w", r.Offset, err)
			}
			if first, seen := entries[e.EntryId]; seen {
				if proto.Equal(first, e) {
					report.Duplicates++
				} else {
					report.Conflicts = append(report.Conflicts, e.EntryId)
				}
				continue
			}
			entries[e.EntryId] = e
		}
	}

	return entries, report, nil
}

// snapshot reads, at one instant, which entries have committed and how many
// ledger events are still waiting in the outbox. It must run before Kafka is
// read: every entry it sees has then either reached Kafka by the time we
// read it or is still in the outbox, so Missing beyond Unrelayed really is
// lost. (Read Postgres after Kafka instead and entries published in between
// look lost.)
func snapshot(ctx context.Context, pool *pgxpool.Pool) (committed []int64, unrelayed int, err error) {
	err = pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id FROM journal_entries`)
		if err != nil {
			return err
		}
		if committed, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM outbox WHERE topic LIKE '%herd.ledger.v1' AND published_at IS NULL`,
		).Scan(&unrelayed)
	})

	return committed, unrelayed, err
}

// compare runs after Kafka has been read, so every genuine entry in Kafka has
// committed by now and anything Postgres lacks is a phantom.
func compare(ctx context.Context, pool *pgxpool.Pool, entries map[int64]*eventsv1.LedgerEntry, balances map[string]int64, report *Report) error {
	seen := slices.Collect(maps.Keys(entries))
	var inDB []int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(id), '{}') FROM journal_entries WHERE id = ANY($1)`, seen).Scan(&inDB); err != nil {
		return err
	}
	committed := map[int64]bool{}
	for _, id := range inDB {
		committed[id] = true
	}
	for _, id := range seen {
		if !committed[id] {
			report.Phantom = append(report.Phantom, id)
		}
	}

	// Compare against Postgres's view of the same entries, so relay lag
	// (entries not in Kafka yet) doesn't show up as drift.
	rows, err := pool.Query(ctx, `
		SELECT account, sum(amount) FROM postings WHERE entry_id = ANY($1) GROUP BY account`, seen)
	if err != nil {
		return err
	}
	dbBalances := map[string]int64{}
	for rows.Next() {
		var account string
		var sum int64
		if err := rows.Scan(&account, &sum); err != nil {
			return err
		}
		dbBalances[account] = sum
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, account := range slices.Sorted(maps.Keys(balances)) {
		if balances[account] != dbBalances[account] {
			report.Drift = append(report.Drift, Drift{Account: account, Kafka: balances[account], Postgres: dbBalances[account]})
		}
	}
	for account, sum := range dbBalances {
		if _, ok := balances[account]; !ok && sum != 0 {
			report.Drift = append(report.Drift, Drift{Account: account, Postgres: sum})
		}
	}

	return pool.QueryRow(ctx, `
		SELECT count(*) FROM account_balances b
		WHERE b.balance <> (SELECT COALESCE(sum(amount), 0) FROM postings p WHERE p.account = b.account)`,
	).Scan(&report.InternalDrift)
}
