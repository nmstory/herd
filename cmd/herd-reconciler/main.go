// herd-reconciler rebuilds every balance from the ledger events in Kafka and
// checks them against Postgres. It exits non-zero if the books don't agree.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nmstory/herd/internal/outbox"
	"github.com/nmstory/herd/internal/reconcile"
)

func main() {
	var (
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL")
		brokers     = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		topic       = flag.String("topic", outbox.LedgerTopic, "ledger topic to replay")
		timeout     = flag.Duration("timeout", time.Minute, "give up after this long")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	r, err := reconcile.Run(ctx, client, pool, *topic)
	if err != nil {
		log.Fatalf("reconcile: %v", err)
	}

	fmt.Printf("replayed %d ledger events (%d duplicates dropped) · %d entries · %d accounts\n",
		r.Events, r.Duplicates, r.Entries, r.Accounts)
	check(len(r.Conflicts) == 0, "no conflicting duplicates",
		fmt.Sprintf("%d entry ids seen twice with different contents %s; is another database writing to this topic?", len(r.Conflicts), some(r.Conflicts)))
	check(len(r.Unbalanced) == 0, "every entry balances", fmt.Sprintf("%d unbalanced entries %s", len(r.Unbalanced), some(r.Unbalanced)))
	check(len(r.Phantom) == 0, "no phantom entries", fmt.Sprintf("%d entries in Kafka never committed %s", len(r.Phantom), some(r.Phantom)))
	check(r.Lost() == 0,
		fmt.Sprintf("nothing lost (%d entries still in the outbox)", r.Unrelayed),
		fmt.Sprintf("%d committed entries never reached Kafka", r.Lost()))
	driftMsg := fmt.Sprintf("%d accounts drifted:", len(r.Drift))
	for _, d := range r.Drift[:min(len(r.Drift), 10)] {
		driftMsg += fmt.Sprintf("\n      %s  kafka %d  postgres %d", d.Account, d.Kafka, d.Postgres)
	}
	if len(r.Drift) > 10 {
		driftMsg += "\n      …"
	}
	check(len(r.Drift) == 0, "balances match Postgres", driftMsg)
	check(r.InternalDrift == 0, "running balances match postings", fmt.Sprintf("%d running balances disagree with postings", r.InternalDrift))

	if !r.OK() {
		fmt.Println("BOOKS DO NOT RECONCILE ✗")
		os.Exit(1)
	}
	fmt.Println("books reconcile ✓")
}

func check(ok bool, pass, fail string) {
	if ok {
		fmt.Println("  ✓", pass)
	} else {
		fmt.Println("  ✗", fail)
	}
}

// some formats the first few ids of a list.
func some(ids []int64) string {
	if len(ids) > 10 {
		return fmt.Sprintf("%v…", ids[:10])
	}
	return fmt.Sprint(ids)
}
