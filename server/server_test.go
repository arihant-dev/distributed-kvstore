package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/arihant/kvstore/store"
)

func TestNode_InitializationAndHelpers(t *testing.T) {
	// Create a temp dir for the store's WAL
	tmpDir, err := os.MkdirTemp("", "server_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "node_test.log")
	s, err := store.NewStore(walPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	id := "node1"
	grpcPort := "9081"
	peersList := []string{
		"node2:9082",
		"node3:9083",
		" ", // should be trimmed and ignored
	}

	node := NewNode(id, grpcPort, peersList, s)

	if node == nil {
		t.Fatal("NewNode returned nil")
	}

	// Test GetID
	if node.GetID() != id {
		t.Errorf("expected ID %q, got %q", id, node.GetID())
	}

	// Test GetGRPCAddress
	expectedAddr := "node1:9081"
	if node.GetGRPCAddress() != expectedAddr {
		t.Errorf("expected GRPC Address %q, got %q", expectedAddr, node.GetGRPCAddress())
	}

	// Test GetStore
	if node.GetStore() != s {
		t.Error("GetStore did not return the expected store instance")
	}

	// Test GetPeers
	expectedPeers := []string{"node2:9082", "node3:9083"}
	gotPeers := node.GetPeers()
	// Sort or compare elements
	if len(gotPeers) != len(expectedPeers) {
		t.Errorf("expected peers %v, got %v", expectedPeers, gotPeers)
	} else {
		// Create a map to compare since order in map iteration is non-deterministic
		peerMap := make(map[string]bool)
		for _, p := range gotPeers {
			peerMap[p] = true
		}
		for _, p := range expectedPeers {
			if !peerMap[p] {
				t.Errorf("missing expected peer %q in gotPeers %v", p, gotPeers)
			}
		}
	}

	// Test adding a peer
	node.AddPeer("node4", "node4:9084")
	gotPeersAfterAdd := node.GetPeers()
	foundNode4 := false
	for _, p := range gotPeersAfterAdd {
		if p == "node4:9084" {
			foundNode4 = true
			break
		}
	}
	if !foundNode4 {
		t.Error("expected to find node4:9084 in peers list after AddPeer")
	}

	// Test removing a peer
	node.RemovePeer("node4")
	gotPeersAfterRemove := node.GetPeers()
	for _, p := range gotPeersAfterRemove {
		if p == "node4:9084" {
			t.Error("expected node4:9084 to be removed from peers list")
		}
	}
}

func TestNode_ConnectionPool(t *testing.T) {
	// Create a temp dir for the store's WAL
	tmpDir, err := os.MkdirTemp("", "server_conn_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "node_conn_test.log")
	s, err := store.NewStore(walPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	node := NewNode("node1", "9081", []string{"node2:9082"}, s)
	if node == nil {
		t.Fatal("NewNode returned nil")
	}

	addr := "node2:9082"

	// 1. Dial peer for the first time
	conn1, err := node.dialPeer(addr)
	if err != nil {
		t.Fatalf("dialPeer failed: %v", err)
	}
	if conn1 == nil {
		t.Fatal("expected non-nil connection")
	}

	// 2. Dial again, should return the same cached pointer
	conn2, err := node.dialPeer(addr)
	if err != nil {
		t.Fatalf("second dialPeer failed: %v", err)
	}
	if conn1 != conn2 {
		t.Error("expected dialPeer to return the cached connection, got different instances")
	}

	// 3. Reset connection, should evict from pool
	node.resetConn(addr)

	// 4. Dial third time, should establish a new connection instance
	conn3, err := node.dialPeer(addr)
	if err != nil {
		t.Fatalf("third dialPeer failed: %v", err)
	}
	if conn3 == conn1 {
		t.Error("expected dialPeer to return a new connection after resetConn, got the same instance")
	}
}
