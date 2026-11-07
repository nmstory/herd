package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/nmstory/herd/gen/go/herd/events/v1"
	"github.com/nmstory/herd/internal/outbox"
)

// events tails herd's Kafka topics and prints each event on one line.
func events(args []string) {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
	fromStart := fs.Bool("from-start", false, "print every event, not just new ones")
	fs.Parse(args)
	if fs.Arg(0) != "tail" {
		flag.Usage()
		os.Exit(2)
	}

	offset := kgo.NewOffset().AtEnd()
	if *fromStart {
		offset = kgo.NewOffset().AtStart()
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(outbox.LeaseTopic, outbox.LedgerTopic),
		kgo.ConsumeResetOffset(offset),
	)
	if err != nil {
		log.Fatalf("failed to connect to Kafka: %v", err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(context.Background())
		fetches.EachError(func(topic string, _ int32, err error) { log.Printf("%s: %v", topic, err) })
		fetches.EachRecord(func(r *kgo.Record) { fmt.Println(formatEvent(r)) })
	}
}

func formatEvent(r *kgo.Record) string {
	at := r.Timestamp.Local().Format(time.TimeOnly)

	switch r.Topic {
	case outbox.LeaseTopic:
		var e eventsv1.LeaseEvent
		if err := proto.Unmarshal(r.Value, &e); err != nil {
			return fmt.Sprintf("%s  %s  undecodable: %v", at, r.Topic, err)
		}
		typ := strings.ToLower(strings.TrimPrefix(e.Type.String(), "TYPE_"))
		line := fmt.Sprintf("%s  %-15s %-8s %-9s lease=%s holder=%s", at, r.Topic, e.DeviceId, typ, short(e.LeaseId), e.Holder)
		if e.Type == eventsv1.LeaseEvent_TYPE_ACQUIRED && e.Held > 0 {
			line += fmt.Sprintf(" held=%d", e.Held)
		}
		return line

	case outbox.LedgerTopic:
		var e eventsv1.LedgerEntry
		if err := proto.Unmarshal(r.Value, &e); err != nil {
			return fmt.Sprintf("%s  %s  undecodable: %v", at, r.Topic, err)
		}
		var postings []string
		for _, p := range e.Postings {
			account := p.Account
			if id, ok := strings.CutPrefix(account, "hold:"); ok {
				account = "hold:" + short(id)
			}
			postings = append(postings, fmt.Sprintf("%s %+d", account, p.Amount))
		}
		return fmt.Sprintf("%s  %-15s %-8s %-9s entry=%d  %s", at, r.Topic, e.Holder, e.Kind, e.EntryId, strings.Join(postings, "  "))
	}

	return fmt.Sprintf("%s  %s  %d bytes", at, r.Topic, len(r.Value))
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
