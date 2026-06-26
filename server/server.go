package server

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/arihant/kvstore/proto"
	"github.com/arihant/kvstore/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Timing Constants ---
const (
	HeartbeatInterval  = 100 * time.Millisecond // Leader pings followers every 100ms
	ElectionTimeoutMin = 500                    // Minimum election timeout in ms
	ElectionTimeoutMax = 800                    // Maximum election timeout in ms (randomized to avoid split votes)
	RPCTimeout         = 150 * time.Millisecond // How long to wait for a gRPC response
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Node struct {
	proto.UnimplementedReplicationServer

	mu sync.RWMutex

	id    string
	peers []string // gRPC addresses of other nodes

	role        Role
	currentTerm uint64
	votedFor    string
	leaderId    string

	lastHeartbeat time.Time

	store      *store.Store
	grpcServer *grpc.Server
}

// NewNode initializes a new server node
func NewNode(id string, peers []string, s *store.Store) *Node {
	return &Node{
		id:            id,
		peers:         peers,
		role:          Follower,
		store:         s,
		lastHeartbeat: time.Now(),
	}
}

// Start begins listening for gRPC requests, starts election timer, and triggers recovery sync
func (n *Node) Start(port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	n.grpcServer = grpc.NewServer()
	proto.RegisterReplicationServer(n.grpcServer, n)

	// Start the election timer loop in the background
	go n.electionTicker()

	// After a short delay, try to sync with peers to catch up on missed WAL entries
	go func() {
		time.Sleep(3 * time.Second) // Give the cluster a moment to stabilize
		n.attemptRecoverySync()
	}()

	log.Printf("Node %s starting gRPC server on %s", n.id, port)
	return n.grpcServer.Serve(lis)
}

// --- gRPC Implementations ---

// AppendEntries is called by the Leader to replicate data or send heartbeats.
func (n *Node) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reject if the leader's term is older than ours
	if req.Term < n.currentTerm {
		return &proto.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// 2. We recognize the leader, reset our election timer
	n.lastHeartbeat = time.Now()
	n.leaderId = req.LeaderId

	// If we were a candidate or had an older term, step down to follower
	if req.Term > n.currentTerm || n.role == Candidate {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = req.LeaderId
	}

	// 3. Apply replicated entries using ApplyEntry (preserves leader's index)
	for _, pbEntry := range req.Entries {
		entry := store.LogEntry{
			Index: pbEntry.Index,
			Op:    store.OpType(pbEntry.Op),
			Key:   pbEntry.Key,
			Value: pbEntry.Value,
		}
		if err := n.store.ApplyEntry(entry); err != nil {
			log.Printf("Node %s: failed to apply entry %d: %v", n.id, entry.Index, err)
		}
	}

	return &proto.AppendEntriesResponse{Term: n.currentTerm, Success: true}, nil
}

// RequestVote is called by a Candidate during an election.
func (n *Node) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &proto.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
	}

	// Safety: candidate's log must be at least as up-to-date as ours
	if req.LastLogIndex < n.store.GetIndex() {
		return &proto.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// Grant vote if we haven't voted yet in this term
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		n.votedFor = req.CandidateId
		n.lastHeartbeat = time.Now()
		return &proto.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}

	return &proto.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
}

// SyncWAL is called by a recovering follower to catch up on missed entries.
func (n *Node) SyncWAL(ctx context.Context, req *proto.SyncWALRequest) (*proto.SyncWALResponse, error) {
	log.Printf("Node %s: SyncWAL request from %s (their last index: %d, our index: %d)",
		n.id, req.FollowerId, req.LastIndex, n.store.GetIndex())

	entries, err := n.store.GetEntriesAfter(req.LastIndex)
	if err != nil {
		return nil, err
	}

	// Convert store.LogEntry -> proto.LogEntry
	var pbEntries []*proto.LogEntry
	for _, e := range entries {
		pbEntries = append(pbEntries, &proto.LogEntry{
			Index: e.Index,
			Op:    uint32(e.Op),
			Key:   e.Key,
			Value: e.Value,
		})
	}

	log.Printf("Node %s: sending %d entries to %s for recovery", n.id, len(pbEntries), req.FollowerId)

	return &proto.SyncWALResponse{
		Entries:     pbEntries,
		LeaderIndex: n.store.GetIndex(),
	}, nil
}

// --- Background Tasks ---

// electionTicker runs continuously. If 5-7 seconds pass without a heartbeat, it triggers an election.
func (n *Node) electionTicker() {
	for {
		timeout := time.Duration(ElectionTimeoutMin+rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond
		time.Sleep(timeout)

		n.mu.RLock()
		role := n.role
		timeSinceHeartbeat := time.Since(n.lastHeartbeat)
		n.mu.RUnlock()

		if role != Leader && timeSinceHeartbeat > timeout {
			log.Printf("Node %s: election timeout reached (%v)! Starting election...", n.id, timeout)
			n.startElection()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderId = ""
	n.lastHeartbeat = time.Now()

	term := n.currentTerm
	lastLogIndex := n.store.GetIndex()
	peers := n.peers
	n.mu.Unlock()

	votes := 1 // We vote for ourselves

	req := &proto.RequestVoteRequest{
		CandidateId:  n.id,
		Term:         term,
		LastLogIndex: lastLogIndex,
	}

	var wg sync.WaitGroup
	var voteMu sync.Mutex

	for _, peer := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			client := proto.NewReplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			defer cancel()

			res, err := client.RequestVote(ctx, req)
			if err == nil && res.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	// Check if we won
	n.mu.Lock()
	defer n.mu.Unlock()

	// Majority = (total_nodes / 2) + 1. With 3 nodes, we need 2 votes.
	totalNodes := len(peers) + 1
	if n.role == Candidate && n.currentTerm == term && votes > totalNodes/2 {
		log.Printf("Node %s WON election for term %d (%d/%d votes)", n.id, n.currentTerm, votes, totalNodes)
		n.role = Leader
		n.leaderId = n.id

		go n.heartbeatTicker()
	} else {
		log.Printf("Node %s LOST election for term %d (%d/%d votes)", n.id, term, votes, totalNodes)
	}
}

// heartbeatTicker runs only on the Leader, sending pings every 1 second.
func (n *Node) heartbeatTicker() {
	for {
		n.mu.RLock()
		if n.role != Leader {
			n.mu.RUnlock()
			return
		}

		term := n.currentTerm
		leaderID := n.id
		peers := n.peers
		n.mu.RUnlock()

		req := &proto.AppendEntriesRequest{
			LeaderId: leaderID,
			Term:     term,
			Entries:  nil, // Empty = Heartbeat
		}

		for _, peer := range peers {
			go func(peerAddr string) {
				conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				client := proto.NewReplicationClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
				defer cancel()

				client.AppendEntries(ctx, req)
			}(peer)
		}

		time.Sleep(HeartbeatInterval)
	}
}

// attemptRecoverySync tries to contact any peer and pull WAL entries we missed.
// This runs once on startup after a crash/restart.
func (n *Node) attemptRecoverySync() {
	myIndex := n.store.GetIndex()
	if myIndex == 0 {
		log.Printf("Node %s: fresh start (index 0), no recovery needed", n.id)
		return
	}

	log.Printf("Node %s: attempting recovery sync (local index: %d)", n.id, myIndex)

	for _, peer := range n.peers {
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := proto.NewReplicationClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		res, err := client.SyncWAL(ctx, &proto.SyncWALRequest{
			FollowerId: n.id,
			LastIndex:  myIndex,
		})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("Node %s: SyncWAL from %s failed: %v", n.id, peer, err)
			continue
		}

		if len(res.Entries) == 0 {
			log.Printf("Node %s: already up to date with %s", n.id, peer)
			return
		}

		// Apply all missing entries
		applied := 0
		for _, pbEntry := range res.Entries {
			entry := store.LogEntry{
				Index: pbEntry.Index,
				Op:    store.OpType(pbEntry.Op),
				Key:   pbEntry.Key,
				Value: pbEntry.Value,
			}
			if err := n.store.ApplyEntry(entry); err != nil {
				log.Printf("Node %s: failed to apply recovery entry %d: %v", n.id, entry.Index, err)
			} else {
				applied++
			}
		}

		log.Printf("Node %s: recovery complete! Applied %d entries from %s (new index: %d)",
			n.id, applied, peer, n.store.GetIndex())
		return
	}

	log.Printf("Node %s: could not reach any peer for recovery sync", n.id)
}

// --- API Helpers ---

func (n *Node) GetStore() *store.Store {
	return n.store
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == Leader
}

func (n *Node) GetRoleString() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	switch n.role {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Follower"
	}
}

func (n *Node) GetPeers() []string {
	return n.peers
}

func (n *Node) GetLeaderHTTPAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.leaderId == "" {
		return ""
	}

	// Map node IDs to their HTTP addresses inside Docker
	switch n.leaderId {
	case "node1":
		return "node1:8081"
	case "node2":
		return "node2:8082"
	case "node3":
		return "node3:8083"
	default:
		return n.leaderId + ":8080"
	}
}

// ReplicateEntry sends a single new entry to all followers and waits for a majority to acknowledge.
// Returns true if successfully replicated to a majority.
func (n *Node) ReplicateEntry(op store.OpType, key string, val []byte) bool {
	n.mu.RLock()
	if n.role != Leader {
		n.mu.RUnlock()
		return false
	}
	term := n.currentTerm
	leaderID := n.id
	peers := n.peers
	index := n.store.GetIndex()
	n.mu.RUnlock()

	req := &proto.AppendEntriesRequest{
		LeaderId: leaderID,
		Term:     term,
		Entries: []*proto.LogEntry{
			{
				Index: index,
				Op:    uint32(op),
				Key:   key,
				Value: val,
			},
		},
	}

	neededAcks := (len(peers)+1)/2
	if neededAcks == 0 {
		return true
	}

	ackChan := make(chan bool, len(peers))
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				ackChan <- false
				return
			}
			defer conn.Close()

			client := proto.NewReplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			defer cancel()

			res, err := client.AppendEntries(ctx, req)
			if err == nil && res.Success {
				ackChan <- true
			} else {
				ackChan <- false
			}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(ackChan)
	}()

	acks := 0
	failures := 0
	for ack := range ackChan {
		if ack {
			acks++
			if acks >= neededAcks {
				return true
			}
		} else {
			failures++
			if len(peers)-failures < neededAcks {
				return false
			}
		}
	}

	return acks >= neededAcks
}
