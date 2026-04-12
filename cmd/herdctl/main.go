package main

import (
	"context"
	"fmt"
	"log"

	herdv1 "github.com/nmstory/herd/gen/go/herd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials())) // no TLS for now
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := herdv1.NewHerdServiceClient(conn)

	resp, err := client.ListDevices(context.Background(), &herdv1.ListDevicesRequest{})
	if err != nil {
		log.Fatalf("ListDevices failed: %v", err)
	}

	if len(resp.Devices) == 0 {
		fmt.Println("No devices.")
		return
	}
	for _, d := range resp.Devices {
		fmt.Println(d)
	}
}
