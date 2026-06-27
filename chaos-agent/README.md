# Chaos Agent

The Chaos Agent is a Docker API controller designed to inject faults, disconnect network bridges, and disrupt running contestant nodes during the grading process.

## HTTP Endpoints

*   `POST /chaos/kill {"target": "node2"}`: Instantly stops the container for the target node.
*   `POST /chaos/pause {"target": "node2"}`: Freezes the target container's processes.
*   `POST /chaos/resume {"target": "node2"}`: Resumes or restarts a stopped/paused container.
*   `POST /chaos/slow {"target": "node2", "ms": 3000}`: Injects network latency (in milliseconds) into the container.
*   `POST /chaos/partition {"a": "node1", "b": "node3"}`: Severs the network connection between node A and node B.
*   `POST /chaos/heal`: Restores all containers and network interfaces to their healthy state.
*   `GET /chaos/status`: Returns current status of all chaos operations and active network partitions.
*   `GET /chaos/containers`: Returns a list of discovered containers and their current states.
*   `GET /chaos/cluster`: Returns the active nodes list, their ports, and their health status.
*   `POST /chaos/auto-provision {"enabled": true|false}`: Enables or disables the auto-provisioning monitor loop.
