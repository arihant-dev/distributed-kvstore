package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	log.SetFlags(log.Ltime | log.Lshortfile)

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║         🔥 CHAOS AGENT v1.0 🔥          ║")
	fmt.Println("║  Distributed Systems Hackathon Toolkit   ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()

	engine, err := NewChaosEngine()
	if err != nil {
		log.Fatalf("❌ Failed to initialize chaos engine: %v", err)
	}

	h := newHandler(engine)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	fmt.Printf("Chaos agent listening on :%s\n", port)
	fmt.Println()
	fmt.Println("Available endpoints:")
	fmt.Println("  POST /chaos/kill       {\"target\": \"node2\"}")
	fmt.Println("  POST /chaos/pause      {\"target\": \"node2\"}")
	fmt.Println("  POST /chaos/resume     {\"target\": \"node2\"}")
	fmt.Println("  POST /chaos/slow       {\"target\": \"node2\", \"ms\": 3000}")
	fmt.Println("  POST /chaos/partition  {\"a\": \"node1\", \"b\": \"node3\"}")
	fmt.Println("  POST /chaos/heal")
	fmt.Println("  GET  /chaos/status")
	fmt.Println("  GET  /chaos/containers")
	fmt.Println()

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("❌ HTTP server failed: %v", err)
	}
}
