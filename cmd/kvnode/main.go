package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/arihant/kvstore/api"
	"github.com/arihant/kvstore/proto"
	"github.com/arihant/kvstore/server"
	"github.com/arihant/kvstore/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var id string
	var httpPort string
	var grpcPort string
	var peersStr string
	var seedNodesStr string

	// Parse flags for testing locally
	flag.StringVar(&id, "id", "node1", "Node ID")
	flag.StringVar(&httpPort, "http", "8081", "HTTP Port")
	flag.StringVar(&grpcPort, "grpc", "9081", "gRPC Port")
	flag.StringVar(&peersStr, "peers", "", "Comma separated list of peer gRPC addresses")
	flag.StringVar(&seedNodesStr, "seeds", "", "Comma separated list of seed HTTP addresses")
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
	if envSeeds := os.Getenv("SEED_NODES"); envSeeds != "" {
		seedNodesStr = envSeeds
	}

	var peers []string
	if peersStr != "" {
		peers = strings.Split(peersStr, ",")
	}

	fmt.Printf("Starting Node %s (HTTP: %s, gRPC: %s)\n", id, httpPort, grpcPort)
	fmt.Printf("Peers: %v\n", peers)
	fmt.Printf("Seed Nodes: %s\n", seedNodesStr)

	// 1. Initialize the Store (WAL + Map)
	walPath := fmt.Sprintf("/tmp/%s_wal.log", id)
	s, err := store.NewStore(walPath)
	if err != nil {
		log.Fatalf("Failed to initialize store: %v", err)
	}

	// 2. Initialize the gRPC Server Node
	node := server.NewNode(id, grpcPort, peers, s)

	// 3. Start the gRPC server in the background
	go func() {
		if err := node.Start(":" + grpcPort); err != nil {
			log.Fatalf("gRPC Server failed: %v", err)
		}
	}()

	// 4. If Peers is empty, but Seed Nodes are specified, start dynamic join process
	if len(peers) == 0 && seedNodesStr != "" {
		seeds := strings.Split(seedNodesStr, ",")
		go joinCluster(id, node.GetGRPCAddress(), seeds)
	}

	// 5. Start the HTTP API Server (blocking)
	apiServer := api.NewAPIServer(node, ":"+httpPort)
	if err := apiServer.Start(); err != nil {
		log.Fatalf("HTTP Server failed: %v", err)
	}
}

func joinCluster(nodeID string, myGRPCAddr string, seeds []string) {
	// Give the server a moment to start up and allow local ports to bind
	time.Sleep(3 * time.Second)

	log.Printf("Node %s: Attempting to join cluster via seeds: %v", nodeID, seeds)

	type LeaderInfo struct {
		IsLeader          bool   `json:"is_leader"`
		LeaderID          string `json:"leader_id"`
		LeaderHTTPAddress string `json:"leader_http_address"`
		LeaderGRPCAddress string `json:"leader_grpc_address"`
	}

	var leaderGRPCAddr string
	for _, seed := range seeds {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		url := fmt.Sprintf("http://%s/leader", seed)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Node %s: Failed to contact seed %s: %v", nodeID, seed, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Node %s: Failed to read leader response from %s: %v", nodeID, seed, err)
			continue
		}

		var info LeaderInfo
		if err := json.Unmarshal(body, &info); err != nil {
			log.Printf("Node %s: Failed to parse leader response from %s: %v", nodeID, seed, err)
			continue
		}

		if info.LeaderGRPCAddress != "" {
			leaderGRPCAddr = info.LeaderGRPCAddress
			log.Printf("Node %s: Discovered leader gRPC address: %s", nodeID, leaderGRPCAddr)
			break
		}
	}

	if leaderGRPCAddr == "" {
		log.Printf("Node %s: No active leader found among seeds. Retrying in 5 seconds...", nodeID)
		go func() {
			time.Sleep(5 * time.Second)
			joinCluster(nodeID, myGRPCAddr, seeds)
		}()
		return
	}

	// Send gRPC Join call to the leader
	conn, err := grpc.NewClient(leaderGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Node %s: Failed to connect to leader %s: %v. Retrying in 5 seconds...", nodeID, leaderGRPCAddr, err)
		go func() {
			time.Sleep(5 * time.Second)
			joinCluster(nodeID, myGRPCAddr, seeds)
		}()
		return
	}
	defer conn.Close()

	client := proto.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.Join(ctx, &proto.JoinRequest{
		NodeId:      nodeID,
		GrpcAddress: myGRPCAddr,
	})
	if err != nil {
		log.Printf("Node %s: gRPC Join call to leader failed: %v. Retrying in 5 seconds...", nodeID, err)
		go func() {
			time.Sleep(5 * time.Second)
			joinCluster(nodeID, myGRPCAddr, seeds)
		}()
		return
	}

	if !res.Success {
		log.Printf("Node %s: Join failed (leader success=false, redirect leader ID: %s, redirect leader addr: %s). Retrying in 5 seconds...",
			nodeID, res.LeaderId, res.LeaderAddress)
		go func() {
			time.Sleep(5 * time.Second)
			joinCluster(nodeID, myGRPCAddr, seeds)
		}()
		return
	}

	log.Printf("Node %s: Successfully joined the cluster!", nodeID)
}
