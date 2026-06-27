# Distributed KV Store Hackathon

This repository contains the template and grading toolkit for a distributed systems hackathon. The goal of the challenge is to build a fault-tolerant, replicated key-value store that survives network partitions, node crashes, and high concurrent load.

## Project Structure

*   `cmd/kvnode/main.go`: Entry point for starting an individual key-value store node.
*   `api/`: HTTP API server handling client PUT, GET, and DELETE requests, including leader proxying.
*   `server/`: Consensus and replication logic implementing election timeouts, heartbeats, and synchronous quorum replication.
*   `store/`: Write-Ahead Log (WAL) engine and local memory map store.
*   `chaos-agent/`: Docker API controller that orchestrates node failures, network partitions, and latency injection.
*   `traffic-sim/`: Standalone traffic generation tool simulating realistic user workloads against the cluster.
*   `orchestrator/`: Master grading runner that coordinates the chaos agent and writes the final scorecard.

## Repository Setup Options

To distribute this challenge to contestants, you can organize the files in one of two ways:

### Option 1: Multi-Repository Split (Recommended)
This approach keeps the grading tests hidden from contestants, preventing hardcoded solutions.

1.  **Contestant Repo (`kvstore-template`)**:
    *   Include: `api/`, `server/`, `store/`, `cmd/`, `go.mod`, `go.sum`, `Dockerfile`, and `docker-compose.yml`.
    *   The `docker-compose.yml` should pull the `chaos-agent` Docker image from your public registry (`ghcr.io/username/chaos-agent:latest`).
2.  **Organizer Repo (`kvstore-grader`)**:
    *   Include: `chaos-agent/`, `traffic-sim/`, `orchestrator/`.
    *   Use this repo to build the `chaos-agent` Docker image and run the master `orchestrator` scoring suite against contestant submissions.

### Option 2: Monorepo Setup
Keep everything in a single repository but separate directories:
*   `template/`: Distributed KV store source code.
*   `grader/`: Chaos agent, traffic simulator, and scoring orchestrator.

## Dynamic Node Auto-Provisioning & Discovery

The system supports automatic replacement of failed nodes:
1. **Detection**: The chaos-agent monitors cluster health via `GET /health` endpoints.
2. **Provisioning**: If auto-provisioning is enabled and healthy node count falls below 3, the chaos-agent inspects surviving containers to discover the network name and image, then starts a new container (e.g. `node4`) passing the survivors' addresses in `SEED_NODES`.
3. **Leader Discovery & Join**: The new node boots up, queries `/leader` on the seed nodes, and sends a gRPC `Join` call to the active leader.
4. **State Catch-up**: The leader adds the new node to the cluster membership, broadcasts the updated peers list to surviving followers, and executes a full WAL state transfer to the new node to sync all historical records.
