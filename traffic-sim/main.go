package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	endpointsFlag := flag.String("endpoints", "localhost:8081,localhost:8082,localhost:8083", "Comma-separated list of node HTTP addresses")
	duration := flag.Int("duration", 60, "Total test duration in seconds")
	rps := flag.Int("rps", 50, "Requests per second")
	readRatio := flag.Float64("read-ratio", 0.5, "Ratio of reads vs writes (0.0 = all writes, 1.0 = all reads)")
	output := flag.String("output", "report.json", "Path to write JSON report")

	flag.Parse()

	// Parse endpoints
	raw := strings.Split(*endpointsFlag, ",")
	var endpoints []string
	for _, ep := range raw {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		// Ensure endpoints have scheme
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "http://" + ep
		}
		endpoints = append(endpoints, ep)
	}

	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "Error: no endpoints provided")
		os.Exit(1)
	}

	if *readRatio < 0 || *readRatio > 1 {
		fmt.Fprintln(os.Stderr, "Error: --read-ratio must be between 0 and 1")
		os.Exit(1)
	}

	config := SimConfig{
		Endpoints:    endpoints,
		Duration:     time.Duration(*duration) * time.Second,
		RPS:          *rps,
		ReadRatio:    *readRatio,
		OutputPath:   *output,
	}

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║       🚦 Traffic Simulator for Distributed KV   ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Endpoints:   %s\n", strings.Join(endpoints, ", "))
	fmt.Printf("  Duration:    %ds\n", *duration)
	fmt.Printf("  Base RPS:    %d\n", *rps)
	fmt.Printf("  Read Ratio:  %.0f%%\n", *readRatio*100)
	fmt.Printf("  Output:      %s\n", *output)
	fmt.Println()

	results := RunSimulation(config)

	report := GenerateReport(results, config)

	if err := WriteReportJSON(report, *output); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing report: %v\n", err)
		os.Exit(1)
	}

	PrintSummary(report)
}
