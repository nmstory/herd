package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	herdv1 "github.com/nmstory/herd/gen/go/herd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

const usage = `usage:
  herdctl [-addr host:port] list [-selector k=v,...]
  herdctl [-addr host:port] acquire [-selector k=v,...] [-duration 5m] [-holder name] [-key idempotency-key]
  herdctl [-addr host:port] release [-holder name] <lease-id>`

func main() {
	addr := flag.String("addr", "localhost:9000", "herd address")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials())) // no TLS for now
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := herdv1.NewHerdServiceClient(conn)
	ctx := context.Background()

	cmd, args := flag.Arg(0), flag.Args()[1:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	selector := fs.String("selector", "", "comma-separated label selector, e.g. os=linux,zone=us-east")
	holder := fs.String("holder", os.Getenv("USER"), "lease holder")

	switch cmd {
	case "list":
		fs.Parse(args)
		resp, err := client.ListDevices(ctx, &herdv1.ListDevicesRequest{Selector: parseSelector(*selector)})
		if err != nil {
			log.Fatalf("ListDevices failed: %v", err)
		}
		if len(resp.Devices) == 0 {
			fmt.Println("No devices.")
			return
		}
		for _, d := range resp.Devices {
			fmt.Printf("%s\t%s\t%v\n", d.Id, strings.TrimPrefix(d.State.String(), "DEVICE_STATE_"), d.Labels)
		}

	case "acquire":
		duration := fs.Duration("duration", 5*time.Minute, "lease duration")
		key := fs.String("key", "", "idempotency key; reuse it to retry safely (default: random)")
		fs.Parse(args)
		if *key == "" {
			*key = rand.Text()
		}
		resp, err := client.AcquireLease(ctx, &herdv1.AcquireLeaseRequest{
			Selector:       parseSelector(*selector),
			Duration:       durationpb.New(*duration),
			Holder:         *holder,
			IdempotencyKey: *key,
		})
		if err != nil {
			log.Fatalf("AcquireLease failed: %v", err)
		}
		l := resp.Lease
		fmt.Printf("lease %s on %s until %s (key %s)\n", l.Id, l.DeviceId, l.ExpiresAt.AsTime().Local().Format(time.TimeOnly), *key)

	case "release":
		fs.Parse(args)
		if fs.NArg() != 1 {
			flag.Usage()
			os.Exit(2)
		}
		if _, err := client.ReleaseLease(ctx, &herdv1.ReleaseLeaseRequest{LeaseId: fs.Arg(0), Holder: *holder}); err != nil {
			log.Fatalf("ReleaseLease failed: %v", err)
		}
		fmt.Println("released", fs.Arg(0))

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func parseSelector(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}
