package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	herdv1 "github.com/nmstory/herd/gen/go/herd/v1"
	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/memstore"
	"github.com/nmstory/herd/internal/lease/pgstore"
	"github.com/nmstory/herd/internal/registry"
)

type HerdServiceServer struct {
	herdv1.UnimplementedHerdServiceServer
	store lease.Store
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
		Id:        l.ID,
		DeviceId:  l.DeviceID,
		ExpiresAt: timestamppb.New(l.ExpiresAt),
		Holder:    l.Holder,
	}}, nil
}

func (s *HerdServiceServer) ReleaseLease(ctx context.Context, req *herdv1.ReleaseLeaseRequest) (*herdv1.ReleaseLeaseResponse, error) {
	if err := s.store.Release(ctx, req.GetLeaseId(), req.GetHolder()); err != nil {
		return nil, toStatus(err)
	}

	return &herdv1.ReleaseLeaseResponse{}, nil
}

func toStatus(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, lease.ErrInvalidArgument):
		code = codes.InvalidArgument
	case errors.Is(err, lease.ErrNoDeviceAvailable):
		code = codes.ResourceExhausted
	case errors.Is(err, lease.ErrLeaseNotFound):
		code = codes.NotFound
	case errors.Is(err, lease.ErrNotHolder):
		code = codes.PermissionDenied
	case errors.Is(err, lease.ErrIdempotencyMismatch):
		code = codes.FailedPrecondition
	}

	return status.Error(code, err.Error())
}

func openStore(ctx context.Context, kind, databaseURL string, cfg *config.Config) (lease.Store, error) {
	switch kind {
	case "memory":
		return memstore.New(registry.New(cfg)), nil
	case "postgres":
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			return nil, err
		}
		if err := pgstore.Migrate(ctx, pool); err != nil {
			return nil, err
		}
		if err := pgstore.SyncDevices(ctx, pool, cfg.DeviceConfigs); err != nil {
			return nil, err
		}
		return pgstore.New(pool), nil
	default:
		return nil, errors.New("unknown store " + kind)
	}
}

func main() {
	var (
		addr        = flag.String("addr", ":9000", "gRPC listen address")
		configPath  = flag.String("config", "devices.yaml", "device config file")
		storeKind   = flag.String("store", "memory", "lease store: memory or postgres")
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL (with -store=postgres)")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	store, err := openStore(context.Background(), *storeKind, *databaseURL, cfg)
	if err != nil {
		log.Fatalf("failed to open %s store: %v", *storeKind, err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	herdv1.RegisterHerdServiceServer(grpcServer, &HerdServiceServer{store: store})

	log.Printf("herd listening on %s with %s store (%d devices)", *addr, *storeKind, len(cfg.DeviceConfigs))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
