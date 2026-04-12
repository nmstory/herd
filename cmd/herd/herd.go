package main

import (
	"context"
	"log"
	"net"

	herdv1 "github.com/nmstory/herd/gen/go/herd/v1"
	"google.golang.org/grpc"
)

type HerdServiceServer struct {
	herdv1.UnimplementedHerdServiceServer
}

func (s *HerdServiceServer) ListDevices(ctx context.Context, req *herdv1.ListDevicesRequest) (*herdv1.ListDevicesResponse, error) {
	return &herdv1.ListDevicesResponse{}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	herdv1.RegisterHerdServiceServer(grpcServer, &HerdServiceServer{})

	log.Println("herd listening on :9000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
