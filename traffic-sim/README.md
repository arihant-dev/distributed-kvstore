# Traffic Simulator

The Traffic Simulator is a standalone CLI tool that simulates real-world client workloads against a running key-value store cluster.

## Usage

Build the binary:
```bash
go build .
```

Run the simulator:
```bash
./traffic-sim --endpoints "localhost:8081,localhost:8082,localhost:8083" --duration 60 --rps 50 --read-ratio 0.5 --output report.json
```

## Flags

*   `--endpoints`: Comma-separated list of HTTP addresses for the key-value nodes (default: `localhost:8081,localhost:8082,localhost:8083`).
*   `--duration`: Total test duration in seconds (default: `60`).
*   `--rps`: Target requests per second (default: `50`).
*   `--read-ratio`: Ratio of GET vs PUT requests, from `0.0` (all writes) to `1.0` (all reads) (default: `0.5`).
*   `--output`: Path to write the JSON results report (default: `report.json`).

## Traffic Profile Phases

1.  **Ramp-up (10% of duration)**: Gradually increases traffic to the target RPS.
2.  **Steady State (60% of duration)**: Sustained traffic at the target RPS.
3.  **Spike (20% of duration)**: Doubles the traffic rate (2x target RPS).
4.  **Cooldown (10% of duration)**: Returns traffic to the baseline target RPS.
