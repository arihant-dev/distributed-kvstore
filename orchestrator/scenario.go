package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ScenarioConfig holds the configuration for a scoring run.
type ScenarioConfig struct {
	Target      string   // Base IP/hostname of the participant
	KVPorts     []string // Ports for KV store nodes (e.g., ["8081","8082","8083"])
	ChaosPort   string   // Port for the chaos agent
	Participant string   // Participant name
}

// kvEndpoint returns the full URL for a KV node by index.
func (sc *ScenarioConfig) kvEndpoint(nodeIdx int) string {
	port := sc.KVPorts[nodeIdx%len(sc.KVPorts)]
	return fmt.Sprintf("http://%s:%s", sc.Target, port)
}

// chaosEndpoint returns the full URL for the chaos agent.
func (sc *ScenarioConfig) chaosEndpoint() string {
	return fmt.Sprintf("http://%s:%s", sc.Target, sc.ChaosPort)
}

// randomKVEndpoint returns a random KV node endpoint.
func (sc *ScenarioConfig) randomKVEndpoint() string {
	return sc.kvEndpoint(rand.Intn(len(sc.KVPorts)))
}

// RunScenario executes the full scoring scenario and returns the scorecard.
func RunScenario(cfg ScenarioConfig) *Scorecard {
	scorecard := NewScorecard(cfg.Participant)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Track all written keys across steps for durability checks
	allKeys := make(map[string]string) // key -> value

	// ─── Step 1: Correctness under normal conditions (10 pts) ───
	fmt.Println("\n[Step 1/6] Correctness under normal conditions...")
	step1Keys := make(map[string]string)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("s1-key-%d", i)
		value := fmt.Sprintf("val-%d-%d", i, rng.Int63())
		step1Keys[key] = value
		allKeys[key] = value
	}

	// Write all keys to random nodes
	writeErrors := 0
	for key, value := range step1Keys {
		if err := PutKey(cfg.randomKVEndpoint(), key, value); err != nil {
			writeErrors++
		}
	}

	// Read all keys from random nodes
	matchCount := 0
	readErrors := 0
	for key, expected := range step1Keys {
		got, err := GetKey(cfg.randomKVEndpoint(), key)
		if err != nil {
			readErrors++
			continue
		}
		if got == expected {
			matchCount++
		}
	}
	step1Score := (float64(matchCount) / 500.0) * 10.0
	step1Details := fmt.Sprintf("Wrote 500 keys (%d write errors), read back %d/500 correctly (%d read errors)",
		writeErrors, matchCount, readErrors)
	scorecard.AddCategory("Correctness (normal)", 10, step1Score, step1Details)
	fmt.Printf("  → %.1f/10 pts: %s\n", step1Score, step1Details)

	// ─── Step 2: Replication correctness (10 pts) ───
	fmt.Println("\n[Step 2/6] Replication correctness...")
	step2Keys := make(map[string]string)
	writeNode := 0 // Write to node 0
	readNode := 1  // Read from node 1
	if len(cfg.KVPorts) < 2 {
		readNode = 0
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("s2-repl-%d", i)
		value := fmt.Sprintf("replval-%d-%d", i, rng.Int63())
		step2Keys[key] = value
		allKeys[key] = value
	}

	writeErrors = 0
	for key, value := range step2Keys {
		if err := PutKey(cfg.kvEndpoint(writeNode), key, value); err != nil {
			writeErrors++
		}
	}

	// Wait for replication
	fmt.Println("  Waiting 3 seconds for replication...")
	time.Sleep(3 * time.Second)

	replMatch := 0
	readErrors = 0
	for key, expected := range step2Keys {
		got, err := GetKey(cfg.kvEndpoint(readNode), key)
		if err != nil {
			readErrors++
			continue
		}
		if got == expected {
			replMatch++
		}
	}
	step2Score := (float64(replMatch) / 100.0) * 10.0
	step2Details := fmt.Sprintf("Wrote 100 keys to node %d (%d errs), read %d/100 correctly from node %d (%d read errs)",
		writeNode, writeErrors, replMatch, readNode, readErrors)
	scorecard.AddCategory("Replication correctness", 10, step2Score, step2Details)
	fmt.Printf("  → %.1f/10 pts: %s\n", step2Score, step2Details)

	// ─── Step 3: Availability during node crash (15 pts) ───
	fmt.Println("\n[Step 3/6] Availability during node crash...")
	killTarget := "node2" // Kill node index 2 (third node)
	fmt.Printf("  Killing %s via chaos agent...\n", killTarget)

	if err := ChaosKill(cfg.chaosEndpoint(), killTarget); err != nil {
		fmt.Printf("  ⚠ Chaos kill failed: %v\n", err)
	}
	fmt.Println("  Waiting 2 seconds for crash to take effect...")
	time.Sleep(2 * time.Second)

	// Send 200 requests to surviving nodes (node 0 and node 2)
	survivingNodes := []int{0, 2}
	if len(cfg.KVPorts) < 3 {
		survivingNodes = []int{0}
	}
	successCount := 0
	totalAvailRequests := 200
	for i := 0; i < totalAvailRequests; i++ {
		nodeIdx := survivingNodes[i%len(survivingNodes)]
		key := fmt.Sprintf("s3-avail-%d", i)
		value := fmt.Sprintf("av-%d", i)
		if err := PutKey(cfg.kvEndpoint(nodeIdx), key, value); err == nil {
			successCount++
			allKeys[key] = value
		}
	}
	availRate := float64(successCount) / float64(totalAvailRequests)
	step3Score := availRate * 15.0
	step3Details := fmt.Sprintf("%d/%d requests succeeded (%.1f%% availability) with %s down",
		successCount, totalAvailRequests, availRate*100, killTarget)
	scorecard.AddCategory("Availability (crash)", 15, step3Score, step3Details)
	fmt.Printf("  → %.1f/15 pts: %s\n", step3Score, step3Details)

	// ─── Step 4: Data durability after crash (10 pts) ───
	fmt.Println("\n[Step 4/6] Data durability after crash...")
	durableCount := 0
	durableTotal := 0
	durableErrors := 0

	// Check all previously-written keys from surviving nodes
	for key, expected := range allKeys {
		durableTotal++
		nodeIdx := survivingNodes[durableTotal%len(survivingNodes)]
		got, err := GetKey(cfg.kvEndpoint(nodeIdx), key)
		if err != nil {
			durableErrors++
			continue
		}
		if got == expected {
			durableCount++
		}
	}
	durableRate := float64(durableCount) / float64(durableTotal)
	step4Score := durableRate * 10.0
	step4Details := fmt.Sprintf("%d/%d keys still readable from surviving nodes (%d errors)",
		durableCount, durableTotal, durableErrors)
	scorecard.AddCategory("Durability (post-crash)", 10, step4Score, step4Details)
	fmt.Printf("  → %.1f/10 pts: %s\n", step4Score, step4Details)

	// ─── Step 5: Recovery correctness (10 pts) ───
	fmt.Println("\n[Step 5/6] Recovery correctness...")
	fmt.Printf("  Resuming %s via chaos agent...\n", killTarget)
	if err := ChaosResume(cfg.chaosEndpoint(), killTarget); err != nil {
		fmt.Printf("  ⚠ Chaos resume failed: %v\n", err)
	}
	fmt.Println("  Waiting 5 seconds for recovery...")
	time.Sleep(5 * time.Second)

	// Write 200 new keys to random nodes
	step5Keys := make(map[string]string)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("s5-recover-%d", i)
		value := fmt.Sprintf("rec-%d-%d", i, rng.Int63())
		step5Keys[key] = value
	}

	writeErrors = 0
	for key, value := range step5Keys {
		if err := PutKey(cfg.randomKVEndpoint(), key, value); err != nil {
			writeErrors++
		}
	}

	// Wait briefly for replication to recovered node
	time.Sleep(2 * time.Second)

	// Read from the recovered node (node index 1)
	recoveredNodeIdx := 1
	if len(cfg.KVPorts) < 2 {
		recoveredNodeIdx = 0
	}
	recoveryMatch := 0
	readErrors = 0
	for key, expected := range step5Keys {
		got, err := GetKey(cfg.kvEndpoint(recoveredNodeIdx), key)
		if err != nil {
			readErrors++
			continue
		}
		if got == expected {
			recoveryMatch++
		}
	}
	step5Score := (float64(recoveryMatch) / 200.0) * 10.0
	step5Details := fmt.Sprintf("Wrote 200 keys (%d errs), read %d/200 from recovered node %d (%d read errs)",
		writeErrors, recoveryMatch, recoveredNodeIdx, readErrors)
	scorecard.AddCategory("Recovery correctness", 10, step5Score, step5Details)
	fmt.Printf("  → %.1f/10 pts: %s\n", step5Score, step5Details)

	// ─── Step 6: Load test survival (5 pts) ───
	fmt.Println("\n[Step 6/6] Load test survival (1000 requests in 60s)...")
	var loadSuccess int64
	var loadTotal int64 = 1000
	var loadWg sync.WaitGroup

	// Calculate delay to spread requests over 60 seconds
	interval := 60 * time.Second / time.Duration(loadTotal)

	for i := int64(0); i < loadTotal; i++ {
		loadWg.Add(1)
		go func(idx int64) {
			defer loadWg.Done()
			key := fmt.Sprintf("s6-load-%d", idx)
			value := fmt.Sprintf("load-%d", idx)
			endpoint := cfg.randomKVEndpoint()
			if err := PutKey(endpoint, key, value); err != nil {
				return
			}
			// Verify read
			got, err := GetKey(endpoint, key)
			if err != nil {
				return
			}
			if got == value {
				atomic.AddInt64(&loadSuccess, 1)
			}
		}(i)
		time.Sleep(interval)
	}

	loadWg.Wait()
	loadErrorRate := 1.0 - (float64(loadSuccess) / float64(loadTotal))
	var step6Score float64
	if loadErrorRate < 0.05 {
		step6Score = 5.0
	} else if loadErrorRate < 0.10 {
		step6Score = 3.0
	} else if loadErrorRate < 0.20 {
		step6Score = 1.0
	} else {
		step6Score = 0.0
	}
	step6Details := fmt.Sprintf("%d/%d successful (%.1f%% error rate)",
		loadSuccess, loadTotal, loadErrorRate*100)
	scorecard.AddCategory("Load test survival", 5, step6Score, step6Details)
	fmt.Printf("  → %.1f/5 pts: %s\n", step6Score, step6Details)

	// ─── Step 7: Auto-Provisioning & Dynamic Catch-up (10 pts) ───
	fmt.Println("\n[Step 7/7] Auto-Provisioning & Dynamic Catch-up...")
	
	// 1. Enable Auto-Provisioning on Chaos Agent
	fmt.Println("  Enabling auto-provisioning on chaos agent...")
	if err := ChaosEnableAutoProvision(cfg.chaosEndpoint(), true); err != nil {
		fmt.Printf("  ⚠ Failed to enable auto-provisioning: %v\n", err)
	}

	// 2. Kill node2 (our target) - again, but this time we leave it dead and let chaos-agent auto-provision node4!
	fmt.Println("  Killing node2 via chaos agent to trigger auto-provisioning...")
	if err := ChaosKill(cfg.chaosEndpoint(), "node2"); err != nil {
		fmt.Printf("  ⚠ Chaos kill failed: %v\n", err)
	}

	// 3. Wait up to 20 seconds for chaos-agent to detect and provision node4
	fmt.Println("  Waiting 15 seconds for chaos-agent to detect outage and start replacement (node4)...")
	time.Sleep(15 * time.Second)

	// 4. Try to read from node4 (http://localhost:8084) to verify it came online, joined the cluster, and synced
	fmt.Println("  Verifying recovered state and WAL sync on node4 (http://localhost:8084)...")
	
	// Write 50 new keys to one of the surviving nodes (node 0 / port 8081)
	step7Keys := make(map[string]string)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("s7-repl-%d", i)
		value := fmt.Sprintf("val-s7-%d-%d", i, rng.Int63())
		step7Keys[key] = value
	}

	writeErrors = 0
	for key, value := range step7Keys {
		if err := PutKey(cfg.kvEndpoint(0), key, value); err != nil {
			writeErrors++
		}
	}

	// Wait 3 seconds for replication/sync to catch up on node4
	fmt.Println("  Waiting 3 seconds for replication/sync to catch up on node4...")
	time.Sleep(3 * time.Second)

	node4Endpoint := "http://localhost:8084"
	node4Match := 0
	node4ReadErrors := 0
	for key, expected := range step7Keys {
		got, err := GetKey(node4Endpoint, key)
		if err != nil {
			node4ReadErrors++
			continue
		}
		if got == expected {
			node4Match++
		}
	}

	step7Score := (float64(node4Match) / 50.0) * 10.0
	step7Details := fmt.Sprintf("Wrote 50 keys, read %d/50 correctly from auto-provisioned node4 (%d read errors)",
		node4Match, node4ReadErrors)
	scorecard.AddCategory("Auto-Provisioning & catchup", 10, step7Score, step7Details)
	fmt.Printf("  → %.1f/10 pts: %s\n", step7Score, step7Details)

	// Disable auto-provisioning at the end
	ChaosEnableAutoProvision(cfg.chaosEndpoint(), false)

	// ─── Cleanup: heal any remaining chaos ───
	fmt.Println("\n[Cleanup] Healing chaos state...")
	if err := ChaosHeal(cfg.chaosEndpoint()); err != nil {
		fmt.Printf("  ⚠ Chaos heal failed: %v\n", err)
	}

	return scorecard
}
