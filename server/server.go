package server

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
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
	PeerDeadTimeout    = 5 * time.Second        // Peer considered unhealthy after this
	PeerEvictTimeout   = 15 * time.Second       // Peer evicted from cluster after this
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

	id               string
	grpcPort         string
	peers            map[string]string // nodeID -> gRPC address (e.g. "node2" -> "node2:9082")

	role        Role
	currentTerm uint64
	votedFor    string
	leaderId    string

	lastHeartbeat    time.Time
	lastPeerResponse map[string]time.Time
	isCatchingUp     bool

	store      *store.Store
	grpcServer *grpc.Server
}

// NewNode initializes a new server node
func NewNode(id string, grpcPort string, peersList []string, s *store.Store) *Node {
	peers := make(map[string]string)
	for _, p := range peersList {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		parts := strings.Split(p, ":")
		peerID := parts[0]
		peers[peerID] = p
	}

	return &Node{
		id:               id,
		grpcPort:         grpcPort,
		peers:            peers,
		role:             Follower,
		store:            s,
		lastHeartbeat:    time.Now(),
		lastPeerResponse: make(map[string]time.Time),
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
	n.lastPeerResponse[req.LeaderId] = time.Now()

	// If we were a candidate or had an older term, step down to follower
	if req.Term > n.currentTerm || n.role == Candidate {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = req.LeaderId
	}

	// 3. Check for log gap/inconsistency (lag)
	myIndex := n.store.GetIndex()
	if myIndex < req.PrevLogIndex {
		log.Printf("Node %s: detected lag (local index: %d, leader index: %d). Triggering catchup...", n.id, myIndex, req.PrevLogIndex)
		go n.catchUpFromLeader(req.LeaderId, myIndex)
		return &proto.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// 4. Apply replicated entries using ApplyEntry (preserves leader's index)
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

	n.lastPeerResponse[req.CandidateId] = time.Now()

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

	n.mu.Lock()
	n.lastPeerResponse[req.FollowerId] = time.Now()
	n.mu.Unlock()

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
	var peers []string
	for _, p := range n.peers {
		peers = append(peers, p)
	}
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
		go n.peerHealthMonitor()
	} else {
		log.Printf("Node %s LOST election for term %d (%d/%d votes)", n.id, term, votes, totalNodes)
	}
}

// heartbeatTicker runs only on the Leader, sending pings every 100ms.
func (n *Node) heartbeatTicker() {
	for {
		n.mu.RLock()
		if n.role != Leader {
			n.mu.RUnlock()
			return
		}

		term := n.currentTerm
		leaderID := n.id
		lastIndex := n.store.GetIndex()
		peersMap := make(map[string]string)
		for id, addr := range n.peers {
			peersMap[id] = addr
		}
		n.mu.RUnlock()

		req := &proto.AppendEntriesRequest{
			LeaderId:     leaderID,
			Term:         term,
			PrevLogIndex: lastIndex,
			Entries:      nil, // Empty = Heartbeat
		}

		for pID, pAddr := range peersMap {
			go func(peerID string, peerAddr string) {
				conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				client := proto.NewReplicationClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
				defer cancel()

				res, err := client.AppendEntries(ctx, req)
				if err == nil {
					n.mu.Lock()
					n.lastPeerResponse[peerID] = time.Now()
					if res.Term > n.currentTerm {
						log.Printf("Node %s (Leader): Stepping down in heartbeat response because term %d > %d", n.id, res.Term, n.currentTerm)
						n.currentTerm = res.Term
						n.role = Follower
						n.votedFor = ""
					}
					n.mu.Unlock()
				}
			}(pID, pAddr)
		}

		time.Sleep(HeartbeatInterval)
	}
}

// attemptRecoverySync tries to contact any peer and pull WAL entries we missed.
// This runs once on startup after a crash/restart.
func (n *Node) attemptRecoverySync() {
	myIndex := n.store.GetIndex()
	log.Printf("Node %s: attempting recovery sync (local index: %d)", n.id, myIndex)

	for _, peer := range n.GetPeers() {
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

// --- Peer Health Monitor ---

// peerHealthMonitor runs only on the Leader. It checks peer liveness every 2 seconds
// and evicts peers that haven't responded within PeerEvictTimeout.
func (n *Node) peerHealthMonitor() {
	for {
		time.Sleep(2 * time.Second)

		n.mu.RLock()
		if n.role != Leader {
			n.mu.RUnlock()
			log.Printf("Node %s: peerHealthMonitor exiting (no longer leader)", n.id)
			return
		}

		// Snapshot peers and lastPeerResponse under read lock
		peersSnapshot := make(map[string]string)
		for id, addr := range n.peers {
			peersSnapshot[id] = addr
		}
		lastResponseSnapshot := make(map[string]time.Time)
		for id, t := range n.lastPeerResponse {
			lastResponseSnapshot[id] = t
		}
		currentTerm := n.currentTerm
		leaderID := n.id
		n.mu.RUnlock()

		var peersToEvict []string

		for peerID := range peersSnapshot {
			lastResp, exists := lastResponseSnapshot[peerID]
			if !exists {
				// No response ever recorded; skip (newly added peer may not have responded yet)
				continue
			}

			silence := time.Since(lastResp)
			if silence > PeerEvictTimeout {
				log.Printf("Node %s (Leader): Peer %s exceeded evict timeout (%v silent). Removing from cluster.", leaderID, peerID, silence)
				peersToEvict = append(peersToEvict, peerID)
			} else if silence > PeerDeadTimeout {
				log.Printf("Node %s (Leader): Peer %s is unhealthy (%v since last response)", leaderID, peerID, silence)
			}
		}

		// Evict dead peers
		for _, peerID := range peersToEvict {
			n.mu.Lock()
			delete(n.peers, peerID)
			delete(n.lastPeerResponse, peerID)
			// Snapshot remaining peers for broadcast
			remainingPeers := make(map[string]string)
			for id, addr := range n.peers {
				remainingPeers[id] = addr
			}
			n.mu.Unlock()

			log.Printf("Node %s (Leader): Peer %s evicted from membership", leaderID, peerID)

			// Broadcast removal to remaining followers
			for followerID, followerAddr := range remainingPeers {
				go func(fID string, fAddr string) {
					conn, err := grpc.NewClient(fAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						log.Printf("Node %s (Leader): Failed to connect to %s for eviction broadcast: %v", leaderID, fID, err)
						return
					}
					defer conn.Close()

					client := proto.NewReplicationClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
					defer cancel()

					_, err = client.UpdatePeers(ctx, &proto.UpdatePeersRequest{
						LeaderId:    leaderID,
						Term:        currentTerm,
						PeerId:      peerID,
						PeerAddress: "",
						IsAdd:       false,
					})
					if err != nil {
						log.Printf("Node %s (Leader): Failed to broadcast eviction of %s to %s: %v", leaderID, peerID, fID, err)
					}
				}(followerID, followerAddr)
			}
		}
	}
}

// --- API Helpers ---

func (n *Node) GetStore() *store.Store {
	return n.store
}

func (n *Node) GetID() string {
	return n.id
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == Leader
}

func (n *Node) GetLeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderId
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
	n.mu.RLock()
	defer n.mu.RUnlock()
	var addrs []string
	for _, addr := range n.peers {
		addrs = append(addrs, addr)
	}
	return addrs
}

// GetGRPCAddress returns this node's own gRPC address.
func (n *Node) GetGRPCAddress() string {
	return n.id + ":" + n.grpcPort
}

// GetLeaderGRPCAddress returns the gRPC address of the current leader.
func (n *Node) GetLeaderGRPCAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.leaderId == "" {
		return ""
	}
	if n.leaderId == n.id {
		return n.id + ":" + n.grpcPort
	}
	return n.peers[n.leaderId]
}

func (n *Node) AddPeer(peerID, address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peerID] = address
	log.Printf("Node %s: Peer %s (%s) added to membership", n.id, peerID, address)
}

func (n *Node) RemovePeer(peerID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peers, peerID)
	log.Printf("Node %s: Peer %s removed from membership", n.id, peerID)
}

func (n *Node) GetLeaderHTTPAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.leaderId == "" {
		return ""
	}
	if n.leaderId == n.id {
		var grpcPortInt int
		_, err := fmt.Sscanf(n.grpcPort, "%d", &grpcPortInt)
		if err == nil {
			return fmt.Sprintf("%s:%d", n.id, grpcPortInt-1000)
		}
		return n.id + ":8080"
	}

	addr, ok := n.peers[n.leaderId]
	if !ok {
		return ""
	}

	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		var port int
		_, err := fmt.Sscanf(parts[1], "%d", &port)
		if err == nil {
			return fmt.Sprintf("%s:%d", parts[0], port-1000)
		}
	}
	return addr
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
	peersMap := make(map[string]string)
	for id, addr := range n.peers {
		peersMap[id] = addr
	}
	index := n.store.GetIndex()
	n.mu.RUnlock()

	req := &proto.AppendEntriesRequest{
		LeaderId:     leaderID,
		Term:         term,
		PrevLogIndex: index - 1,
		Entries: []*proto.LogEntry{
			{
				Index: index,
				Op:    uint32(op),
				Key:   key,
				Value: val,
			},
		},
	}

	neededAcks := (len(peersMap)+1)/2
	if neededAcks == 0 {
		return true
	}

	ackChan := make(chan bool, len(peersMap))
	var wg sync.WaitGroup

	for pID, pAddr := range peersMap {
		wg.Add(1)
		go func(peerID string, peerAddr string) {
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
				n.mu.Lock()
				n.lastPeerResponse[peerID] = time.Now()
				n.mu.Unlock()
				ackChan <- true
			} else {
				if err == nil {
					n.mu.Lock()
					if res.Term > n.currentTerm {
						log.Printf("Node %s (Leader): Stepping down in replication response because term %d > %d", n.id, res.Term, n.currentTerm)
						n.currentTerm = res.Term
						n.role = Follower
						n.votedFor = ""
					}
					n.mu.Unlock()
				}
				ackChan <- false
			}
		}(pID, pAddr)
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
			if len(peersMap)-failures < neededAcks {
				return false
			}
		}
	}

	return acks >= neededAcks
}

func (n *Node) catchUpFromLeader(leaderID string, startIndex uint64) {
	n.mu.Lock()
	if n.isCatchingUp {
		n.mu.Unlock()
		return
	}
	n.isCatchingUp = true
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		n.isCatchingUp = false
		n.mu.Unlock()
	}()

	n.mu.RLock()
	leaderAddr, ok := n.peers[leaderID]
	n.mu.RUnlock()

	if !ok {
		log.Printf("Node %s: catchUp: leader %s address unknown, trying general recovery sync", n.id, leaderID)
		n.attemptRecoverySync()
		return
	}

	log.Printf("Node %s: catchUp: contacting leader %s at %s starting from index %d", n.id, leaderID, leaderAddr, startIndex)

	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Node %s: catchUp: failed to connect to leader: %v", n.id, err)
		return
	}
	defer conn.Close()

	client := proto.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.SyncWAL(ctx, &proto.SyncWALRequest{
		FollowerId: n.id,
		LastIndex:  startIndex,
	})
	if err != nil {
		log.Printf("Node %s: catchUp: SyncWAL from leader failed: %v", n.id, err)
		return
	}

	if len(res.Entries) == 0 {
		log.Printf("Node %s: catchUp: already up to date with leader", n.id)
		return
	}

	applied := 0
	for _, pbEntry := range res.Entries {
		entry := store.LogEntry{
			Index: pbEntry.Index,
			Op:    store.OpType(pbEntry.Op),
			Key:   pbEntry.Key,
			Value: pbEntry.Value,
		}
		if err := n.store.ApplyEntry(entry); err != nil {
			log.Printf("Node %s: catchUp: failed to apply entry %d: %v", n.id, entry.Index, err)
		} else {
			applied++
		}
	}

	log.Printf("Node %s: catchUp complete! Applied %d entries from leader %s (new index: %d)",
		n.id, applied, leaderID, n.store.GetIndex())
}

// Join is called by a new node to request entry into the cluster.
func (n *Node) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		leaderAddr := ""
		if n.leaderId != "" && n.leaderId != n.id {
			leaderAddr = n.peers[n.leaderId]
		}
		return &proto.JoinResponse{
			Success:       false,
			LeaderId:      n.leaderId,
			LeaderAddress: leaderAddr,
		}, nil
	}

	// We are the leader!
	n.peers[req.NodeId] = req.GrpcAddress
	log.Printf("Node %s (Leader): Peer %s (%s) joined cluster", n.id, req.NodeId, req.GrpcAddress)

	// Broadcast the membership change to other followers
	for peerID, peerAddr := range n.peers {
		if peerID == req.NodeId {
			continue
		}
		go func(pID string, pAddr string) {
			conn, err := grpc.NewClient(pAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Node %s (Leader): Failed to connect to follower %s for UpdatePeers: %v", n.id, pID, err)
				return
			}
			defer conn.Close()

			client := proto.NewReplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			defer cancel()

			_, err = client.UpdatePeers(ctx, &proto.UpdatePeersRequest{
				LeaderId:    n.id,
				Term:        n.currentTerm,
				PeerId:      req.NodeId,
				PeerAddress: req.GrpcAddress,
				IsAdd:       true,
			})
			if err != nil {
				log.Printf("Node %s (Leader): Failed to UpdatePeers on follower %s: %v", n.id, pID, err)
			}
		}(peerID, peerAddr)
	}

	// Trigger async full WAL sync to the new node so it catches up immediately
	newNodeAddr := req.GrpcAddress
	newNodeID := req.NodeId
	go func() {
		entries, err := n.store.GetEntriesAfter(0)
		if err != nil {
			log.Printf("Node %s (Leader): Failed to get WAL entries for sync to %s: %v", n.id, newNodeID, err)
			return
		}
		if len(entries) == 0 {
			log.Printf("Node %s (Leader): No WAL entries to sync to %s", n.id, newNodeID)
			return
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

		conn, err := grpc.NewClient(newNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Node %s (Leader): Failed to connect to new node %s (%s) for WAL sync: %v", n.id, newNodeID, newNodeAddr, err)
			return
		}
		defer conn.Close()

		n.mu.RLock()
		term := n.currentTerm
		leaderID := n.id
		n.mu.RUnlock()

		client := proto.NewReplicationClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		res, err := client.AppendEntries(ctx, &proto.AppendEntriesRequest{
			LeaderId:     leaderID,
			Term:         term,
			PrevLogIndex: 0,
			Entries:      pbEntries,
		})
		if err != nil {
			log.Printf("Node %s (Leader): WAL sync to new node %s failed: %v", n.id, newNodeID, err)
		} else {
			log.Printf("Node %s (Leader): WAL sync to new node %s succeeded (sent %d entries, response term: %d, success: %v)",
				n.id, newNodeID, len(pbEntries), res.Term, res.Success)
		}
	}()

	return &proto.JoinResponse{
		Success: true,
	}, nil
}

// Leave is called by a node to request clean exit from the cluster.
func (n *Node) Leave(ctx context.Context, req *proto.LeaveRequest) (*proto.LeaveResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return &proto.LeaveResponse{
			Success: false,
		}, nil
	}

	// We are the leader!
	delete(n.peers, req.NodeId)
	log.Printf("Node %s (Leader): Peer %s left cluster", n.id, req.NodeId)

	// Broadcast the membership change to other followers
	for peerID, peerAddr := range n.peers {
		go func(pID string, pAddr string) {
			conn, err := grpc.NewClient(pAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Node %s (Leader): Failed to connect to follower %s for UpdatePeers: %v", n.id, pID, err)
				return
			}
			defer conn.Close()

			client := proto.NewReplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
			defer cancel()

			_, err = client.UpdatePeers(ctx, &proto.UpdatePeersRequest{
				LeaderId:    n.id,
				Term:        n.currentTerm,
				PeerId:      req.NodeId,
				PeerAddress: "",
				IsAdd:       false,
			})
			if err != nil {
				log.Printf("Node %s (Leader): Failed to UpdatePeers on follower %s: %v", n.id, pID, err)
			}
		}(peerID, peerAddr)
	}

	return &proto.LeaveResponse{
		Success: true,
	}, nil
}

// UpdatePeers is called by the Leader to broadcast membership changes (add/remove peer) to followers.
func (n *Node) UpdatePeers(ctx context.Context, req *proto.UpdatePeersRequest) (*proto.UpdatePeersResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &proto.UpdatePeersResponse{Success: false}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = req.LeaderId
	}
	n.leaderId = req.LeaderId
	n.lastHeartbeat = time.Now()

	if req.IsAdd {
		n.peers[req.PeerId] = req.PeerAddress
		log.Printf("Node %s (Follower): Peer %s (%s) added to membership via leader %s", n.id, req.PeerId, req.PeerAddress, req.LeaderId)
	} else {
		delete(n.peers, req.PeerId)
		log.Printf("Node %s (Follower): Peer %s removed from membership via leader %s", n.id, req.PeerId, req.LeaderId)
	}

	return &proto.UpdatePeersResponse{Success: true}, nil
}
