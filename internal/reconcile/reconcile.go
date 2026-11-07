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
	return len(r.Unbalanced) == 0 && len(r.Phantom) == 0 && r.Lost() == 0 && len(r.Drift) == 0 && r.InternalDrift == 0
}

// Run reads topic from the beginning up to its current end and reconciles it
// against the database.
func Run(ctx context.Context, client *kgo.Client, pool *pgxpool.Pool, topic string) (Report, error) {
	entries, report, err := readTopic(ctx, client, topic)
	if err != nil {
		return report, err
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
			if _, seen := entries[e.EntryId]; seen {
				report.Duplicates++
				continue
			}
			entries[e.EntryId] = e
		}
	}

	return entries, report, nil
}

func compare(ctx context.Context, pool *pgxpool.Pool, entries map[int64]*eventsv1.LedgerEntry, balances map[string]int64, report *Report) error {
	rows, err := pool.Query(ctx, `SELECT id FROM journal_entries`)
	if err != nil {
		return err
	}
	committed, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}

	inDB := map[int64]bool{}
	for _, id := range committed {
		inDB[id] = true
		if entries[id] == nil {
			report.Missing = append(report.Missing, id)
		}
	}
	for id := range entries {
		if !inDB[id] {
			report.Phantom = append(report.Phantom, id)
		}
	}

	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox WHERE topic LIKE '%herd.ledger.v1' AND published_at IS NULL`,
	).Scan(&report.Unrelayed); err != nil {
		return err
	}

	// Compare against Postgres's view of the same entries, so relay lag
	// (entries not in Kafka yet) doesn't show up as drift.
	seen := slices.Collect(maps.Keys(entries))
	rows, err = pool.Query(ctx, `
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
