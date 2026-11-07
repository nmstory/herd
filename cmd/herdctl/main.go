package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
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
  herdctl [-addr host:port] release [-holder name] <lease-id>
  herdctl [-addr host:port] wallet topup [-holder name] [-key idempotency-key] <amount>
  herdctl [-addr host:port] wallet show [-holder name]
  herdctl [-addr host:port] wallet statement [-holder name] [-n 50]
  herdctl events [-brokers localhost:19092] [-from-start] tail`

func main() {
	addr := flag.String("addr", "localhost:9000", "herd address")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if flag.Arg(0) == "events" {
		events(flag.Args()[1:]) // talks to Kafka, not herd
		return
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
		fmt.Printf("lease %s on %s until %s (key %s)", l.Id, l.DeviceId, l.ExpiresAt.AsTime().Local().Format(time.TimeOnly), *key)
		if l.Held > 0 {
			fmt.Printf("  ·  held %d credits (%d/min × %v)", l.Held, l.CreditsPerMinute, *duration)
		}
		fmt.Println()

	case "release":
		fs.Parse(args)
		if fs.NArg() != 1 {
			flag.Usage()
			os.Exit(2)
		}
		resp, err := client.ReleaseLease(ctx, &herdv1.ReleaseLeaseRequest{LeaseId: fs.Arg(0), Holder: *holder})
		if err != nil {
			log.Fatalf("ReleaseLease failed: %v", err)
		}
		fmt.Print("released ", fs.Arg(0))
		if resp.Charged > 0 || resp.Returned > 0 {
			fmt.Printf("  ·  charged %d, returned %d", resp.Charged, resp.Returned)
		}
		fmt.Println()

	case "wallet":
		wallet(ctx, client, args)

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func wallet(ctx context.Context, client herdv1.HerdServiceClient, args []string) {
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	fs := flag.NewFlagSet("wallet "+args[0], flag.ExitOnError)
	holder := fs.String("holder", os.Getenv("USER"), "wallet holder")

	switch args[0] {
	case "topup":
		key := fs.String("key", "", "idempotency key; reuse it to retry safely (default: random)")
		fs.Parse(args[1:])
		amount, err := strconv.ParseInt(fs.Arg(0), 10, 64)
		if fs.NArg() != 1 || err != nil {
			flag.Usage()
			os.Exit(2)
		}
		if *key == "" {
			*key = rand.Text()
		}
		w, err := client.TopUp(ctx, &herdv1.TopUpRequest{Holder: *holder, Amount: amount, IdempotencyKey: *key})
		if err != nil {
			log.Fatalf("TopUp failed: %v", err)
		}
		printWallet(w)

	case "show":
		fs.Parse(args[1:])
		w, err := client.GetWallet(ctx, &herdv1.GetWalletRequest{Holder: *holder})
		if err != nil {
			log.Fatalf("GetWallet failed: %v", err)
		}
		printWallet(w)

	case "statement":
		n := fs.Int("n", 50, "number of most recent entries")
		fs.Parse(args[1:])
		resp, err := client.ListStatement(ctx, &herdv1.ListStatementRequest{Holder: *holder, Limit: int32(*n)})
		if err != nil {
			log.Fatalf("ListStatement failed: %v", err)
		}
		for _, l := range resp.Lines {
			lease := ""
			if l.LeaseId != "" {
				lease = "lease " + l.LeaseId[:8]
			}
			charged := ""
			if l.Charged != 0 {
				charged = fmt.Sprintf("charged %d", l.Charged)
			}
			fmt.Printf("%s  %-8s %-14s %+6d  %-12s balance %d\n",
				l.At.AsTime().Local().Format(time.TimeOnly), l.Kind, lease, l.Amount, charged, l.Balance)
		}

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func printWallet(w *herdv1.Wallet) {
	fmt.Printf("wallet %s: %d credits available, %d held\n", w.Holder, w.Available, w.Held)
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
