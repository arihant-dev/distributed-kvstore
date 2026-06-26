package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/arihant/kvstore/api"
	"github.com/arihant/kvstore/server"
	"github.com/arihant/kvstore/store"
)

func main() {
	var id string
	var httpPort string
	var grpcPort string
	var peersStr string

	// Parse flags for testing locally
	flag.StringVar(&id, "id", "node1", "Node ID")
	flag.StringVar(&httpPort, "http", "8081", "HTTP Port")
	flag.StringVar(&grpcPort, "grpc", "9081", "gRPC Port")
	flag.StringVar(&peersStr, "peers", "", "Comma separated list of peer gRPC addresses")
	flag.Parse()

	// Alternatively, parse from ENV (for Docker Compose)
	if envId := os.Getenv("NODE_ID"); envId != "" {
		id = envId
	}
	if envHttp := os.Getenv("HTTP_PORT"); envHttp != "" {
		httpPort = envHttp
	}
	if envGrpc := os.Getenv("GRPC_PORT"); envGrpc != "" {
		grpcPort = envGrpc
	}
	if envPeers := os.Getenv("PEERS"); envPeers != "" {
		peersStr = envPeers
	}

	var peers []string
	if peersStr != "" {
		peers = strings.Split(peersStr, ",")
	}

	fmt.Printf("Starting Node %s (HTTP: %s, gRPC: %s)\n", id, httpPort, grpcPort)
	fmt.Printf("Peers: %v\n", peers)

	// 1. Initialize the Store (WAL + Map)
	walPath := fmt.Sprintf("/tmp/%s_wal.log", id)
	s, err := store.NewStore(walPath)
	if err != nil {
		log.Fatalf("Failed to initialize store: %v", err)
	}

	// 2. Initialize the gRPC Server Node
	node := server.NewNode(id, peers, s)

	// 3. Start the gRPC server in the background
	go func() {
		if err := node.Start(":" + grpcPort); err != nil {
			log.Fatalf("gRPC Server failed: %v", err)
		}
	}()

	// 4. Start the HTTP API Server (blocking)
	apiServer := api.NewAPIServer(node, ":"+httpPort)
	if err := apiServer.Start(); err != nil {
		log.Fatalf("HTTP Server failed: %v", err)
	}
}
