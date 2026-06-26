package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	target := flag.String("target", "", "Participant IP or hostname (required)")
	kvPorts := flag.String("kv-ports", "8081,8082,8083", "Comma-separated KV store ports")
	chaosPort := flag.String("chaos-port", "9090", "Chaos agent port")
	output := flag.String("output", "score.json", "Output path for score JSON")
	participant := flag.String("participant", "unknown", "Participant name")

	flag.Parse()

	if *target == "" {
		fmt.Fprintln(os.Stderr, "Error: --target is required")
		flag.Usage()
		os.Exit(1)
	}

	ports := strings.Split(*kvPorts, ",")
	for i := range ports {
		ports[i] = strings.TrimSpace(ports[i])
	}

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║          DISTRIBUTED KV STORE — SCORING ORCHESTRATOR       ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("  Participant: %s\n", *participant)
	fmt.Printf("  Target:      %s\n", *target)
	fmt.Printf("  KV Ports:    %v\n", ports)
	fmt.Printf("  Chaos Port:  %s\n", *chaosPort)
	fmt.Printf("  Output:      %s\n", *output)
	fmt.Println()

	cfg := ScenarioConfig{
		Target:      *target,
		KVPorts:     ports,
		ChaosPort:   *chaosPort,
		Participant: *participant,
	}

	scorecard := RunScenario(cfg)

	// Print summary to stdout
	scorecard.PrintSummary()

	// Write JSON report
	if err := scorecard.WriteJSON(*output); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing score JSON: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Score JSON written to: %s\n", *output)
}
