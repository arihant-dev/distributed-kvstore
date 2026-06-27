package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

// NodeStatus tracks the health and identity of a single KV store node.
type NodeStatus struct {
	NodeID        string    `json:"node_id"`
	ContainerName string    `json:"container_name"`
	HTTPPort      string    `json:"http_port"`
	GRPCPort      string    `json:"grpc_port"`
	Healthy       bool      `json:"healthy"`
	LastSeen      time.Time `json:"last_seen"`
	ContainerID   string    `json:"container_id,omitempty"`
}

// ClusterStatus is the JSON response returned by GET /chaos/cluster.
type ClusterStatus struct {
	Nodes              map[string]*NodeStatus `json:"nodes"`
	AutoProvisionEnabled bool                 `json:"auto_provision_enabled"`
	TargetNodeCount    int                    `json:"target_node_count"`
}

// NodeProvisioner monitors cluster health and auto-provisions replacement nodes.
type NodeProvisioner struct {
	engine          *ChaosEngine
	mu              sync.Mutex
	targetNodeCount int
	nextNodeNum     int
	enabled         bool
	knownNodes      map[string]*NodeStatus
}

// NewNodeProvisioner creates a provisioner initialized with the 3 default KV store nodes.
func NewNodeProvisioner(engine *ChaosEngine) *NodeProvisioner {
	nodes := map[string]*NodeStatus{
		"node1": {
			NodeID:        "node1",
			ContainerName: "kvstore-node1",
			HTTPPort:      "8081",
			GRPCPort:      "9081",
		},
		"node2": {
			NodeID:        "node2",
			ContainerName: "kvstore-node2",
			HTTPPort:      "8082",
			GRPCPort:      "9082",
		},
		"node3": {
			NodeID:        "node3",
			ContainerName: "kvstore-node3",
			HTTPPort:      "8083",
			GRPCPort:      "9083",
		},
	}

	return &NodeProvisioner{
		engine:          engine,
		targetNodeCount: 3,
		nextNodeNum:     4,
		enabled:         false,
		knownNodes:      nodes,
	}
}

// Start begins the background health monitoring loop. Should be called in a goroutine.
func (p *NodeProvisioner) Start() {
	log.Println("Health monitor started (checking every 10s)")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		p.checkClusterHealth()
		<-ticker.C
	}
}

// checkClusterHealth pings every known node and provisions replacements if needed.
func (p *NodeProvisioner) checkClusterHealth() {
	p.mu.Lock()
	defer p.mu.Unlock()

	healthyCount := 0
	totalCount := len(p.knownNodes)

	for _, node := range p.knownNodes {
		healthy := p.pingNode(node)
		node.Healthy = healthy
		if healthy {
			node.LastSeen = time.Now()
			healthyCount++
		}
	}

	log.Printf("Cluster health: %d/%d nodes healthy", healthyCount, totalCount)

	if p.enabled && healthyCount < p.targetNodeCount {
		log.Printf("Cluster health: %d/%d nodes healthy. Provisioning replacement...", healthyCount, p.targetNodeCount)
		if err := p.provisionNode(); err != nil {
			log.Printf("Failed to provision replacement node: %v", err)
		}
	}
}

// pingNode performs a health check against a single node using Docker-internal DNS.
func (p *NodeProvisioner) pingNode(node *NodeStatus) bool {
	url := fmt.Sprintf("http://%s:%s/health", node.ContainerName, node.HTTPPort)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// provisionNode creates a new replacement KV store container.
// Must be called with p.mu held.
func (p *NodeProvisioner) provisionNode() error {
	// 1. Generate new node identity
	nodeID := fmt.Sprintf("node%d", p.nextNodeNum)
	httpPort := fmt.Sprintf("%d", 8080+p.nextNodeNum)
	grpcPort := fmt.Sprintf("%d", 9080+p.nextNodeNum)
	containerName := fmt.Sprintf("kvstore-%s", nodeID)
	p.nextNodeNum++

	// 2. Find surviving healthy nodes for seed addresses
	var seeds []string
	for _, node := range p.knownNodes {
		if node.Healthy {
			seeds = append(seeds, fmt.Sprintf("%s:%s", node.ContainerName, node.HTTPPort))
		}
	}
	if len(seeds) == 0 {
		return fmt.Errorf("no healthy nodes available to use as seeds")
	}
	seedNodesStr := strings.Join(seeds, ",")

	// 3. Find the Docker image name from an existing container
	imageName, networkName, err := p.discoverImageAndNetwork()
	if err != nil {
		return fmt.Errorf("failed to discover image/network: %w", err)
	}

	// 4. Create and start the new container
	ctx := context.Background()

	containerConfig := &container.Config{
		Image: imageName,
		Env: []string{
			"NODE_ID=" + nodeID,
			"HTTP_PORT=" + httpPort,
			"GRPC_PORT=" + grpcPort,
			"SEED_NODES=" + seedNodesStr,
		},
		ExposedPorts: nat.PortSet{
			nat.Port(httpPort + "/tcp"): struct{}{},
			nat.Port(grpcPort + "/tcp"): struct{}{},
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			nat.Port(httpPort + "/tcp"): []nat.PortBinding{{HostPort: httpPort}},
			nat.Port(grpcPort + "/tcp"): []nat.PortBinding{{HostPort: grpcPort}},
		},
	}

	networkingConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			networkName: {
				Aliases: []string{nodeID},
			},
		},
	}

	// Clean up any existing container with the same name to prevent naming conflicts
	_ = p.engine.docker.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true})

	resp, err := p.engine.docker.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, containerName)
	if err != nil {
		return fmt.Errorf("failed to create container %s: %w", containerName, err)
	}

	if err := p.engine.docker.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container %s: %w", containerName, err)
	}

	// 5. Track the new node
	p.knownNodes[nodeID] = &NodeStatus{
		NodeID:        nodeID,
		ContainerName: containerName,
		HTTPPort:      httpPort,
		GRPCPort:      grpcPort,
		Healthy:       false, // will be confirmed on next health check
		ContainerID:   resp.ID,
	}

	log.Printf("Provisioned replacement node %s (HTTP: %s, gRPC: %s)", nodeID, httpPort, grpcPort)
	return nil
}

// discoverImageAndNetwork inspects existing kvstore containers to determine the
// Docker image name and network to use for new containers.
func (p *NodeProvisioner) discoverImageAndNetwork() (string, string, error) {
	ctx := context.Background()

	for _, node := range p.knownNodes {
		inspect, err := p.engine.docker.ContainerInspect(ctx, node.ContainerName)
		if err != nil {
			continue // try the next node
		}

		imageName := inspect.Config.Image
		if imageName == "" {
			continue
		}

		// Pick the first non-"bridge" network, falling back to "bridge"
		networkName := "bridge"
		for name := range inspect.NetworkSettings.Networks {
			if name != "bridge" {
				networkName = name
				break
			}
		}

		return imageName, networkName, nil
	}

	return "", "", fmt.Errorf("could not inspect any existing kvstore container to determine image/network")
}

// GetClusterStatus returns a snapshot of the current cluster state.
func (p *NodeProvisioner) GetClusterStatus() ClusterStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Deep copy the nodes map
	nodesCopy := make(map[string]*NodeStatus, len(p.knownNodes))
	for k, v := range p.knownNodes {
		cp := *v
		nodesCopy[k] = &cp
	}

	return ClusterStatus{
		Nodes:                nodesCopy,
		AutoProvisionEnabled: p.enabled,
		TargetNodeCount:      p.targetNodeCount,
	}
}

// SetEnabled enables or disables auto-provisioning.
func (p *NodeProvisioner) SetEnabled(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = enabled

	if enabled {
		log.Println("Auto-provisioning ENABLED")
	} else {
		log.Println("Auto-provisioning DISABLED")
	}
}

// autoProvisionRequest is the JSON body for POST /chaos/auto-provision.
type autoProvisionRequest struct {
	Enabled bool `json:"enabled"`
}

// --- Handler methods for provisioner endpoints ---

// handleClusterStatus handles GET /chaos/cluster
func (h *handler) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}

	status := h.provisioner.GetClusterStatus()
	h.writeJSON(w, http.StatusOK, status)
}

// handleAutoProvision handles POST /chaos/auto-provision
func (h *handler) handleAutoProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req autoProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'enabled' field required")
		return
	}

	h.provisioner.SetEnabled(req.Enabled)

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"auto_provision": req.Enabled,
		"status":         "updated",
	})
}
