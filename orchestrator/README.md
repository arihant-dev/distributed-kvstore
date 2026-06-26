# Scoring Orchestrator

The Scoring Orchestrator coordinates the automated evaluation suite for contestants. It connects to the cluster nodes and the chaos agent to perform grading across six scenarios.

## Usage

Build the binary:
```bash
go build .
```

Run the suite:
```bash
./orchestrator --target localhost --kv-ports "8081,8082,8083" --chaos-port 9090 --participant "Contestant-Name" --output score.json
```

## Flags

*   `--target`: Hostname or IP of the host machine running the Docker containers (required).
*   `--kv-ports`: Comma-separated list of the exposed HTTP ports for the key-value nodes (default: `8081,8082,8083`).
*   `--chaos-port`: Exposed port of the running chaos-agent container (default: `9090`).
*   `--participant`: Name or identifier of the contestant (default: `unknown`).
*   `--output`: File path to output the JSON scorecard (default: `score.json`).

## Scenarios Run

1.  **Correctness (10 pts)**: Writes 500 unique keys to random nodes and verifies they match when read back.
2.  **Replication (10 pts)**: Writes 100 keys to one node, waits 3 seconds, and verifies they are readable from a different node.
3.  **Availability (15 pts)**: Kills `node2` (the active leader), waits 2 seconds, and measures write success rate across surviving nodes.
4.  **Durability (10 pts)**: Verifies all keys written in previous steps remain readable from the surviving nodes after the crash.
5.  **Recovery (10 pts)**: Resumes `node2`, waits 5 seconds, writes 200 new keys, and verifies they are replicated and readable from the recovered node.
6.  **Load Test (5 pts)**: Sends 1,000 requests spread over 60 seconds. Measures failure rate (must be less than 5% for full points).
