package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	herdv1 "github.com/nmstory/herd/gen/go/herd/v1"
	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/db"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/memstore"
	"github.com/nmstory/herd/internal/lease/pgstore"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
	"github.com/nmstory/herd/internal/registry"
)

type HerdServiceServer struct {
	herdv1.UnimplementedHerdServiceServer
	store lease.Store
	book  *ledger.Book // nil with the in-memory store, which has no credits
}

func (s *HerdServiceServer) ListDevices(ctx context.Context, req *herdv1.ListDevicesRequest) (*herdv1.ListDevicesResponse, error) {
	devices, err := s.store.ListDevices(ctx, req.GetSelector())
	if err != nil {
		return nil, toStatus(err)
	}

	resp := &herdv1.ListDevicesResponse{}
	for _, d := range devices {
		state := herdv1.DeviceState_DEVICE_STATE_IDLE
		if d.Leased {
			state = herdv1.DeviceState_DEVICE_STATE_LEASED
		}
		resp.Devices = append(resp.Devices, &herdv1.Device{Id: d.ID, Labels: d.Labels, State: state})
	}

	return resp, nil
}

func (s *HerdServiceServer) AcquireLease(ctx context.Context, req *herdv1.AcquireLeaseRequest) (*herdv1.AcquireLeaseResponse, error) {
	l, err := s.store.Acquire(ctx, lease.AcquireParams{
		Selector:       req.GetSelector(),
		Duration:       req.GetDuration().AsDuration(),
		Holder:         req.GetHolder(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	return &herdv1.AcquireLeaseResponse{Lease: &herdv1.Lease{
		Id:               l.ID,
		DeviceId:         l.DeviceID,
		ExpiresAt:        timestamppb.New(l.ExpiresAt),
		Holder:           l.Holder,
		CreditsPerMinute: l.CreditsPerMinute,
		Held:             l.Held,
	}}, nil
}

func (s *HerdServiceServer) ReleaseLease(ctx context.Context, req *herdv1.ReleaseLeaseRequest) (*herdv1.ReleaseLeaseResponse, error) {
	settled, err := s.store.Release(ctx, req.GetLeaseId(), req.GetHolder())
	if err != nil {
		return nil, toStatus(err)
	}

	return &herdv1.ReleaseLeaseResponse{Charged: settled.Charged, Returned: settled.Returned}, nil
}

func (s *HerdServiceServer) TopUp(ctx context.Context, req *herdv1.TopUpRequest) (*herdv1.Wallet, error) {
	if s.book == nil {
		return nil, errNoCredits
	}
	w, err := s.book.TopUp(ctx, req.GetHolder(), req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}

	return walletProto(w), nil
}

func (s *HerdServiceServer) GetWallet(ctx context.Context, req *herdv1.GetWalletRequest) (*herdv1.Wallet, error) {
	if s.book == nil {
		return nil, errNoCredits
	}
	w, err := s.book.Wallet(ctx, req.GetHolder())
	if err != nil {
		return nil, toStatus(err)
	}

	return walletProto(w), nil
}

func (s *HerdServiceServer) ListStatement(ctx context.Context, req *herdv1.ListStatementRequest) (*herdv1.ListStatementResponse, error) {
	if s.book == nil {
		return nil, errNoCredits
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}

	lines, err := s.book.Statement(ctx, req.GetHolder(), limit)
	if err != nil {
		return nil, toStatus(err)
	}

	resp := &herdv1.ListStatementResponse{}
	for _, l := range lines {
		resp.Lines = append(resp.Lines, &herdv1.StatementLine{
			EntryId: l.EntryID,
			Kind:    string(l.Kind),
			LeaseId: l.LeaseID,
			Amount:  l.Amount,
			Charged: l.Charged,
			Balance: l.Balance,
			At:      timestamppb.New(l.At),
		})
	}

	return resp, nil
}

var errNoCredits = status.Error(codes.Unimplemented, "credits need the postgres store")

func walletProto(w ledger.Wallet) *herdv1.Wallet {
	return &herdv1.Wallet{Holder: w.Holder, Available: w.Available, Held: w.Held}
}

func toStatus(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, lease.ErrInvalidArgument), errors.Is(err, ledger.ErrInvalidArgument):
		code = codes.InvalidArgument
	case errors.Is(err, lease.ErrNoDeviceAvailable):
		code = codes.ResourceExhausted
	case errors.Is(err, lease.ErrLeaseNotFound):
		code = codes.NotFound
	case errors.Is(err, lease.ErrNotHolder):
		code = codes.PermissionDenied
	case errors.Is(err, lease.ErrIdempotencyMismatch), errors.Is(err, ledger.ErrIdempotencyMismatch),
		errors.Is(err, lease.ErrInsufficientCredits), errors.Is(err, ledger.ErrInsufficientFunds):
		code = codes.FailedPrecondition
	}

	return status.Error(code, err.Error())
}

// openPostgres migrates the database and loads devices and prices into it.
func openPostgres(ctx context.Context, databaseURL string, cfg *config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Migrate(ctx, pool); err != nil {
		return nil, err
	}
	if err := pgstore.SyncDevices(ctx, pool, cfg.DeviceConfigs); err != nil {
		return nil, err
	}
	if err := pgstore.SyncPrices(ctx, pool, cfg.Prices); err != nil {
		return nil, err
	}

	return pool, nil
}

// startRelay publishes the outbox to Kafka in the background.
func startRelay(ctx context.Context, pool *pgxpool.Pool, brokers string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	if err := outbox.EnsureTopics(ctx, client, 3); err != nil {
		return err
	}

	go (&outbox.Relay{Pool: pool, Client: client}).Run(ctx)
	return nil
}

func main() {
	var (
		addr        = flag.String("addr", ":9000", "gRPC listen address")
		configPath  = flag.String("config", "devices.yaml", "device config file")
		storeKind   = flag.String("store", "memory", "lease store: memory or postgres")
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL (with -store=postgres)")
		settleEvery = flag.Duration("settle-interval", 5*time.Second, "how often to charge expired leases (with -store=postgres)")
		kafka       = flag.String("kafka", "", "comma-separated Kafka brokers to publish events to (with -store=postgres)")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx := context.Background()
	server := &HerdServiceServer{}
	switch *storeKind {
	case "memory":
		server.store = memstore.New(registry.New(cfg))
	case "postgres":
		pool, err := openPostgres(ctx, *databaseURL, cfg)
		if err != nil {
			log.Fatalf("failed to open postgres: %v", err)
		}
		store := pgstore.New(pool)
		store.Billing = true
		server.store, server.book = store, ledger.NewBook(pool, outbox.Table{})

		go store.RunSettler(ctx, *settleEvery)
		if *kafka != "" {
			if err := startRelay(ctx, pool, *kafka); err != nil {
				log.Fatalf("failed to start relay: %v", err)
			}
		}
	default:
		log.Fatalf("unknown store %q", *storeKind)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	herdv1.RegisterHerdServiceServer(grpcServer, server)

	log.Printf("herd listening on %s with %s store (%d devices)", *addr, *storeKind, len(cfg.DeviceConfigs))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
