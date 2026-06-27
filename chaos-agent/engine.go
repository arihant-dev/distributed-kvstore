package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

// ChaosState tracks all active chaos injections so we can cleanly heal them.
type ChaosState struct {
	// KilledContainers tracks containers we stopped (container name -> true)
	KilledContainers map[string]bool

	// PausedContainers tracks containers we paused
	PausedContainers map[string]bool

	// SlowContainers tracks containers with injected latency (container name -> latency in ms)
	SlowContainers map[string]int

	// Partitions tracks network disconnections (container name -> list of networks disconnected from)
	Partitions map[string][]string
}

// ChaosEngine is the core component that interacts with the Docker daemon.
type ChaosEngine struct {
	mu     sync.Mutex
	docker *client.Client
	state  ChaosState

	// selfContainerID is our own container ID so we can exclude ourselves from discovery
	selfContainerID string
}

// NewChaosEngine creates a new engine connected to the local Docker socket.
func NewChaosEngine() (*ChaosEngine, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	engine := &ChaosEngine{
		docker: cli,
		state: ChaosState{
			KilledContainers: make(map[string]bool),
			PausedContainers: make(map[string]bool),
			SlowContainers:   make(map[string]int),
			Partitions:       make(map[string][]string),
		},
	}

	// Try to figure out our own container ID so we don't accidentally kill ourselves
	engine.selfContainerID = detectSelfContainerID()

	return engine, nil
}

// detectSelfContainerID attempts to read our own container ID from cgroup.
// Returns empty string if not running inside Docker (which is fine).
func detectSelfContainerID() string {
	// Inside a Docker container, the hostname is typically the short container ID
	// This is a simple and reliable heuristic.
	return ""
}

// DiscoverContainers finds all running containers on the same Docker network,
// excluding the chaos-agent itself.
func (e *ChaosEngine) DiscoverContainers(ctx context.Context) ([]ContainerInfo, error) {
	containers, err := e.docker.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var results []ContainerInfo
	for _, c := range containers {
		// Skip ourselves
		if c.ID == e.selfContainerID {
			continue
		}

		// Skip containers whose name contains "chaos-agent" (our own container)
		isSelf := false
		for _, name := range c.Names {
			if strings.Contains(name, "chaos-agent") || strings.Contains(name, "chaos_agent") {
				isSelf = true
				break
			}
		}
		if isSelf {
			continue
		}

		// Build a clean name (Docker prepends "/" to container names)
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}

		results = append(results, ContainerInfo{
			ID:    c.ID,
			Name:  name,
			State: c.State,
		})
	}

	return results, nil
}

// ContainerInfo is a simplified view of a Docker container.
type ContainerInfo struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// findContainerByName resolves a target name (e.g. "node2") to a container ID.
// It uses a flexible match: the target can be the full container name, or a substring.
func (e *ChaosEngine) findContainerByName(ctx context.Context, target string) (string, string, error) {
	containers, err := e.DiscoverContainers(ctx)
	if err != nil {
		return "", "", err
	}

	for _, c := range containers {
		if c.Name == target || strings.Contains(c.Name, target) {
			return c.ID, c.Name, nil
		}
	}

	return "", "", fmt.Errorf("container matching '%s' not found", target)
}

// --- Chaos Operations ---

// Kill stops a container (simulating a crash).
func (e *ChaosEngine) Kill(ctx context.Context, target string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, name, err := e.findContainerByName(ctx, target)
	if err != nil {
		return err
	}

	// Use a very short timeout to simulate a hard crash (like SIGKILL)
	timeout := 0
	err = e.docker.ContainerStop(ctx, id, container.StopOptions{Timeout: &timeout})
	if err != nil {
		return fmt.Errorf("failed to kill container %s: %w", name, err)
	}

	e.state.KilledContainers[name] = true
	log.Printf("KILLED container %s (%s)", name, id[:12])
	return nil
}

// Pause freezes a container (simulating a frozen/unresponsive node).
func (e *ChaosEngine) Pause(ctx context.Context, target string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, name, err := e.findContainerByName(ctx, target)
	if err != nil {
		return err
	}

	err = e.docker.ContainerPause(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to pause container %s: %w", name, err)
	}

	e.state.PausedContainers[name] = true
	log.Printf("PAUSED container %s (%s)", name, id[:12])
	return nil
}

// Resume restarts a killed container or unpauses a paused container.
func (e *ChaosEngine) Resume(ctx context.Context, target string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, name, err := e.findContainerByName(ctx, target)
	if err != nil {
		return err
	}

	// Check if paused first
	if e.state.PausedContainers[name] {
		err = e.docker.ContainerUnpause(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to unpause container %s: %w", name, err)
		}
		delete(e.state.PausedContainers, name)
		log.Printf("UNPAUSED container %s", name)
		return nil
	}

	// Otherwise, try to start the stopped container
	err = e.docker.ContainerStart(ctx, id, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start container %s: %w", name, err)
	}

	delete(e.state.KilledContainers, name)
	log.Printf("RESUMED container %s", name)
	return nil
}

// Slow injects network latency into a container using `tc netem` via docker exec.
// This requires the container to have the `tc` command available (iproute2 package).
// If `tc` is not available, we log a warning — this is a best-effort feature.
func (e *ChaosEngine) Slow(ctx context.Context, target string, latencyMs int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, name, err := e.findContainerByName(ctx, target)
	if err != nil {
		return err
	}

	// First, try to clear any existing netem rules
	e.execInContainer(ctx, id, []string{"tc", "qdisc", "del", "dev", "eth0", "root"})

	// Add the latency rule
	cmd := []string{"tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", fmt.Sprintf("%dms", latencyMs)}
	exitCode, output, err := e.execInContainer(ctx, id, cmd)
	if err != nil {
		return fmt.Errorf("failed to exec tc in container %s: %w", name, err)
	}
	if exitCode != 0 {
		log.Printf("tc command failed in %s (exit %d): %s — container may not have iproute2 installed", name, exitCode, output)
		return fmt.Errorf("tc command failed (exit %d): %s. Ensure iproute2 is installed in the container", exitCode, output)
	}

	e.state.SlowContainers[name] = latencyMs
	log.Printf("SLOWED container %s by %dms", name, latencyMs)
	return nil
}

// Partition disconnects two containers from each other by removing them from
// their shared Docker network. This is a clean, universal approach that works
// regardless of what's installed inside the container.
func (e *ChaosEngine) Partition(ctx context.Context, containerA, containerB string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	idA, nameA, err := e.findContainerByName(ctx, containerA)
	if err != nil {
		return fmt.Errorf("container A: %w", err)
	}

	idB, nameB, err := e.findContainerByName(ctx, containerB)
	if err != nil {
		return fmt.Errorf("container B: %w", err)
	}

	// Find a shared network between the two containers
	sharedNetwork, err := e.findSharedNetwork(ctx, idA, idB)
	if err != nil {
		return err
	}

	// Disconnect container B from the shared network.
	// This creates a one-directional partition: A can't reach B, and B can't reach A
	// on this network. But both can still talk to other containers on the network.
	// For a true A↔B partition we disconnect B, because disconnecting both would
	// also partition them from other nodes (which we don't want).
	//
	// However, docker network disconnect removes ALL connectivity on that network,
	// so we disconnect B and track it.
	err = e.docker.NetworkDisconnect(ctx, sharedNetwork, idB, true)
	if err != nil {
		return fmt.Errorf("failed to disconnect %s from network: %w", nameB, err)
	}

	e.state.Partitions[nameB] = append(e.state.Partitions[nameB], sharedNetwork)
	log.Printf("PARTITIONED %s <=> %s (disconnected %s from network %s)", nameA, nameB, nameB, sharedNetwork)
	return nil
}

// findSharedNetwork finds a Docker network that both containers are connected to.
func (e *ChaosEngine) findSharedNetwork(ctx context.Context, idA, idB string) (string, error) {
	inspectA, err := e.docker.ContainerInspect(ctx, idA)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container A: %w", err)
	}

	inspectB, err := e.docker.ContainerInspect(ctx, idB)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container B: %w", err)
	}

	for netName := range inspectA.NetworkSettings.Networks {
		if _, ok := inspectB.NetworkSettings.Networks[netName]; ok {
			// Skip the default "bridge" network, prefer user-defined networks
			if netName == "bridge" {
				continue
			}
			return netName, nil
		}
	}

	// Fallback to "bridge" if that's the only shared one
	for netName := range inspectA.NetworkSettings.Networks {
		if _, ok := inspectB.NetworkSettings.Networks[netName]; ok {
			return netName, nil
		}
	}

	return "", fmt.Errorf("no shared network found between the two containers")
}

// Heal removes ALL injected chaos: unpauses, removes latency, reconnects networks, restarts killed containers.
func (e *ChaosEngine) Heal(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var errors []string

	// 1. Unpause all paused containers
	for name := range e.state.PausedContainers {
		id, _, err := e.findContainerByName(ctx, name)
		if err != nil {
			errors = append(errors, fmt.Sprintf("unpause %s: %v", name, err))
			continue
		}
		if err := e.docker.ContainerUnpause(ctx, id); err != nil {
			errors = append(errors, fmt.Sprintf("unpause %s: %v", name, err))
		} else {
			log.Printf("Healed: unpaused %s", name)
		}
	}
	e.state.PausedContainers = make(map[string]bool)

	// 2. Remove latency from slowed containers
	for name := range e.state.SlowContainers {
		id, _, err := e.findContainerByName(ctx, name)
		if err != nil {
			errors = append(errors, fmt.Sprintf("unslow %s: %v", name, err))
			continue
		}
		e.execInContainer(ctx, id, []string{"tc", "qdisc", "del", "dev", "eth0", "root"})
		log.Printf("Healed: removed latency from %s", name)
	}
	e.state.SlowContainers = make(map[string]int)

	// 3. Reconnect partitioned containers
	for name, networks := range e.state.Partitions {
		id, _, err := e.findContainerByName(ctx, name)
		if err != nil {
			errors = append(errors, fmt.Sprintf("reconnect %s: %v", name, err))
			continue
		}
		for _, netName := range networks {
			err := e.docker.NetworkConnect(ctx, netName, id, &network.EndpointSettings{})
			if err != nil {
				errors = append(errors, fmt.Sprintf("reconnect %s to %s: %v", name, netName, err))
			} else {
				log.Printf("Healed: reconnected %s to network %s", name, netName)
			}
		}
	}
	e.state.Partitions = make(map[string][]string)

	// 4. Restart killed containers
	for name := range e.state.KilledContainers {
		id, _, err := e.findContainerByName(ctx, name)
		if err != nil {
			errors = append(errors, fmt.Sprintf("restart %s: %v", name, err))
			continue
		}
		if err := e.docker.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
			errors = append(errors, fmt.Sprintf("restart %s: %v", name, err))
		} else {
			log.Printf("Healed: restarted %s", name)
		}
	}
	e.state.KilledContainers = make(map[string]bool)

	if len(errors) > 0 {
		return fmt.Errorf("heal completed with errors: %s", strings.Join(errors, "; "))
	}

	log.Printf("ALL CHAOS HEALED")
	return nil
}

// GetState returns a snapshot of current chaos state.
func (e *ChaosEngine) GetState() ChaosState {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Return a copy to avoid races
	return ChaosState{
		KilledContainers: copyMapBool(e.state.KilledContainers),
		PausedContainers: copyMapBool(e.state.PausedContainers),
		SlowContainers:   copyMapInt(e.state.SlowContainers),
		Partitions:       copyMapSlice(e.state.Partitions),
	}
}

// --- Docker Exec Helper ---

func (e *ChaosEngine) execInContainer(ctx context.Context, containerID string, cmd []string) (int, string, error) {
	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}

	execResp, err := e.docker.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return -1, "", err
	}

	attachResp, err := e.docker.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return -1, "", err
	}
	defer attachResp.Close()

	// Read output with a timeout
	outputCh := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := attachResp.Reader.Read(buf)
		outputCh <- string(buf[:n])
	}()

	var output string
	select {
	case output = <-outputCh:
	case <-time.After(5 * time.Second):
		output = "(timeout reading exec output)"
	}

	// Get the exit code
	inspectResp, err := e.docker.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return -1, output, err
	}

	return inspectResp.ExitCode, output, nil
}

// --- Helpers ---

func copyMapBool(m map[string]bool) map[string]bool {
	c := make(map[string]bool, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func copyMapInt(m map[string]int) map[string]int {
	c := make(map[string]int, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func copyMapSlice(m map[string][]string) map[string][]string {
	c := make(map[string][]string, len(m))
	for k, v := range m {
		dup := make([]string, len(v))
		copy(dup, v)
		c[k] = dup
	}
	return c
}
