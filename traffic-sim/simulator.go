package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// SimConfig holds the simulation configuration.
type SimConfig struct {
	Endpoints  []string
	Duration   time.Duration
	RPS        int
	ReadRatio  float64
	OutputPath string
}

// RequestResult captures the outcome of a single HTTP request.
type RequestResult struct {
	Timestamp   time.Time     `json:"timestamp"`
	Method      string        `json:"method"`
	Key         string        `json:"key"`
	Endpoint    string        `json:"endpoint"`
	StatusCode  int           `json:"status_code"`
	Latency     time.Duration `json:"latency_ns"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
	Phase       string        `json:"phase"`
}

// ConsistencyViolation records a case where a read from a different node
// returned an unexpected value after a write.
type ConsistencyViolation struct {
	Timestamp    time.Time `json:"timestamp"`
	Key          string    `json:"key"`
	WriteNode    string    `json:"write_node"`
	ReadNode     string    `json:"read_node"`
	ExpectedVal  string    `json:"expected_value"`
	ActualVal    string    `json:"actual_value"`
}

// SimResults aggregates all results from the simulation run.
type SimResults struct {
	Results              []RequestResult        `json:"results"`
	ConsistencyViolations []ConsistencyViolation `json:"consistency_violations"`
	StartTime            time.Time              `json:"start_time"`
	EndTime              time.Time              `json:"end_time"`
}

// phase describes a traffic phase with its name, fraction of total duration, and RPS multiplier.
type phase struct {
	Name       string
	Fraction   float64
	RPSFactor  float64
}

// writtenEntry tracks a key that was successfully written along with its value and target node.
type writtenEntry struct {
	Key      string
	Value    string
	Endpoint string
}

// RunSimulation executes the traffic simulation against the configured endpoints.
func RunSimulation(cfg SimConfig) SimResults {
	phases := []phase{
		{Name: "ramp-up", Fraction: 0.10, RPSFactor: 0.5},
		{Name: "steady", Fraction: 0.60, RPSFactor: 1.0},
		{Name: "spike", Fraction: 0.20, RPSFactor: 2.0},
		{Name: "cooldown", Fraction: 0.10, RPSFactor: 0.5},
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	var (
		allResults   []RequestResult
		allViolations []ConsistencyViolation
		resultsMu    sync.Mutex
	)

	// Thread-safe storage for written keys
	var (
		writtenKeys []writtenEntry
		writtenMu   sync.Mutex
	)

	var keyCounter atomic.Int64
	var endpointIdx atomic.Int64

	// Pick the next endpoint in round-robin fashion
	nextEndpoint := func() string {
		idx := endpointIdx.Add(1) - 1
		return cfg.Endpoints[int(idx)%len(cfg.Endpoints)]
	}

	// Pick a different endpoint than the given one (for consistency checks)
	differentEndpoint := func(exclude string) string {
		if len(cfg.Endpoints) == 1 {
			return exclude // only one endpoint available
		}
		for {
			ep := cfg.Endpoints[rand.Intn(len(cfg.Endpoints))]
			if ep != exclude {
				return ep
			}
		}
	}

	startTime := time.Now()

	for _, p := range phases {
		phaseDuration := time.Duration(float64(cfg.Duration) * p.Fraction)
		phaseRPS := int(float64(cfg.RPS) * p.RPSFactor)
		if phaseRPS < 1 {
			phaseRPS = 1
		}

		fmt.Printf("  ▶ Phase: %-10s | Duration: %5s | RPS: %d\n", p.Name, phaseDuration.Round(time.Second), phaseRPS)

		interval := time.Second / time.Duration(phaseRPS)
		ticker := time.NewTicker(interval)
		phaseDeadline := time.After(phaseDuration)

		var wg sync.WaitGroup

	phaseLoop:
		for {
			select {
			case <-phaseDeadline:
				ticker.Stop()
				break phaseLoop
			case <-ticker.C:
				wg.Add(1)
				go func(phaseName string) {
					defer wg.Done()

					isRead := rand.Float64() < cfg.ReadRatio

					if isRead {
						// Attempt to read a previously written key from a different node
						writtenMu.Lock()
						if len(writtenKeys) == 0 {
							writtenMu.Unlock()
							// Nothing written yet — do a write instead
							isRead = false
						} else {
							// Pick a random written entry
							entry := writtenKeys[rand.Intn(len(writtenKeys))]
							writtenMu.Unlock()

							readEP := differentEndpoint(entry.Endpoint)
							result, body := doGet(client, readEP, entry.Key, phaseName)

							resultsMu.Lock()
							allResults = append(allResults, result)
							resultsMu.Unlock()

							// Consistency check
							if result.Success && body != entry.Value {
								v := ConsistencyViolation{
									Timestamp:   time.Now(),
									Key:         entry.Key,
									WriteNode:   entry.Endpoint,
									ReadNode:    readEP,
									ExpectedVal: entry.Value,
									ActualVal:   body,
								}
								resultsMu.Lock()
								allViolations = append(allViolations, v)
								resultsMu.Unlock()
							}
							return
						}
					}

					if !isRead {
						// Write
						ep := nextEndpoint()
						num := keyCounter.Add(1)
						key := fmt.Sprintf("test-key-%04d", num)
						value := randomString(16)

						result := doPut(client, ep, key, value, phaseName)

						resultsMu.Lock()
						allResults = append(allResults, result)
						resultsMu.Unlock()

						if result.Success {
							writtenMu.Lock()
							writtenKeys = append(writtenKeys, writtenEntry{
								Key:      key,
								Value:    value,
								Endpoint: ep,
							})
							writtenMu.Unlock()
						}
					}
				}(p.Name)
			}
		}

		wg.Wait()
	}

	endTime := time.Now()

	return SimResults{
		Results:               allResults,
		ConsistencyViolations: allViolations,
		StartTime:             startTime,
		EndTime:               endTime,
	}
}

// doPut sends a PUT /store/{key} request with a JSON body.
func doPut(client *http.Client, endpoint, key, value, phaseName string) RequestResult {
	url := fmt.Sprintf("%s/store/%s", endpoint, key)
	payload, _ := json.Marshal(map[string]string{"value": value})

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return RequestResult{
			Timestamp: time.Now(),
			Method:    "PUT",
			Key:       key,
			Endpoint:  endpoint,
			Success:   false,
			Error:     err.Error(),
			Phase:     phaseName,
		}
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	result := RequestResult{
		Timestamp: start,
		Method:    "PUT",
		Key:       key,
		Endpoint:  endpoint,
		Latency:   latency,
		Phase:     phaseName,
	}

	if err != nil {
		result.Success = false
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	result.StatusCode = resp.StatusCode
	result.Success = resp.StatusCode >= 200 && resp.StatusCode < 300
	if !result.Success {
		result.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}

	return result
}

// doGet sends a GET /store/{key} request and returns the result and parsed value.
func doGet(client *http.Client, endpoint, key, phaseName string) (RequestResult, string) {
	url := fmt.Sprintf("%s/store/%s", endpoint, key)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return RequestResult{
			Timestamp: time.Now(),
			Method:    "GET",
			Key:       key,
			Endpoint:  endpoint,
			Success:   false,
			Error:     err.Error(),
			Phase:     phaseName,
		}, ""
	}

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	result := RequestResult{
		Timestamp: start,
		Method:    "GET",
		Key:       key,
		Endpoint:  endpoint,
		Latency:   latency,
		Phase:     phaseName,
	}

	if err != nil {
		result.Success = false
		result.Error = err.Error()
		return result, ""
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	result.StatusCode = resp.StatusCode
	result.Success = resp.StatusCode >= 200 && resp.StatusCode < 300

	if !result.Success {
		result.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return result, ""
	}

	// Try to parse {"value": "..."} response
	var parsed map[string]string
	if err := json.Unmarshal(bodyBytes, &parsed); err == nil {
		return result, parsed["value"]
	}

	// Fallback: return raw body
	return result, string(bodyBytes)
}

// randomString generates a random alphanumeric string of the given length.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
