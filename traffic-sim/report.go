package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Report is the final JSON report structure.
type Report struct {
	Summary            ReportSummary          `json:"summary"`
	PhaseBreakdown     map[string]PhaseStat   `json:"phase_breakdown"`
	EndpointBreakdown  map[string]EndpointStat `json:"endpoint_breakdown"`
	ConsistencyReport  ConsistencyReport      `json:"consistency"`
	Config             ReportConfig           `json:"config"`
}

// ReportSummary holds the top-level summary statistics.
type ReportSummary struct {
	TotalRequests       int     `json:"total_requests"`
	SuccessfulRequests  int     `json:"successful_requests"`
	FailedRequests      int     `json:"failed_requests"`
	SuccessRate         float64 `json:"success_rate_pct"`
	ErrorRate           float64 `json:"error_rate_pct"`
	TotalDuration       string  `json:"total_duration"`
	AvgLatencyMs        float64 `json:"avg_latency_ms"`
	P50LatencyMs        float64 `json:"p50_latency_ms"`
	P95LatencyMs        float64 `json:"p95_latency_ms"`
	P99LatencyMs        float64 `json:"p99_latency_ms"`
	MinLatencyMs        float64 `json:"min_latency_ms"`
	MaxLatencyMs        float64 `json:"max_latency_ms"`
	ActualRPS           float64 `json:"actual_rps"`
}

// PhaseStat holds per-phase statistics.
type PhaseStat struct {
	Requests    int     `json:"requests"`
	Successes   int     `json:"successes"`
	Failures    int     `json:"failures"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
}

// EndpointStat holds per-endpoint statistics.
type EndpointStat struct {
	Requests    int     `json:"requests"`
	Successes   int     `json:"successes"`
	Failures    int     `json:"failures"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

// ConsistencyReport holds consistency verification results.
type ConsistencyReport struct {
	TotalChecks    int     `json:"total_read_checks"`
	Violations     int     `json:"violations"`
	ViolationRate  float64 `json:"violation_rate_pct"`
}

// ReportConfig records the configuration used for the run.
type ReportConfig struct {
	Endpoints  []string `json:"endpoints"`
	DurationS  int      `json:"duration_seconds"`
	TargetRPS  int      `json:"target_rps"`
	ReadRatio  float64  `json:"read_ratio"`
}

// GenerateReport computes all statistics from the simulation results.
func GenerateReport(sim SimResults, cfg SimConfig) Report {
	total := len(sim.Results)
	var successes, failures int
	var latencies []float64
	var totalLatency float64

	phaseStats := make(map[string]*struct {
		count    int
		success  int
		fail     int
		latTotal float64
		lats     []float64
	})

	epStats := make(map[string]*struct {
		count    int
		success  int
		fail     int
		latTotal float64
	})

	readChecks := 0

	for _, r := range sim.Results {
		latMs := float64(r.Latency) / float64(time.Millisecond)
		latencies = append(latencies, latMs)
		totalLatency += latMs

		if r.Success {
			successes++
		} else {
			failures++
		}

		// Phase stats
		ps, ok := phaseStats[r.Phase]
		if !ok {
			ps = &struct {
				count    int
				success  int
				fail     int
				latTotal float64
				lats     []float64
			}{}
			phaseStats[r.Phase] = ps
		}
		ps.count++
		ps.latTotal += latMs
		ps.lats = append(ps.lats, latMs)
		if r.Success {
			ps.success++
		} else {
			ps.fail++
		}

		// Endpoint stats
		es, ok := epStats[r.Endpoint]
		if !ok {
			es = &struct {
				count    int
				success  int
				fail     int
				latTotal float64
			}{}
			epStats[r.Endpoint] = es
		}
		es.count++
		es.latTotal += latMs
		if r.Success {
			es.success++
		} else {
			es.fail++
		}

		// Count read checks (successful GETs used for consistency verification)
		if r.Method == "GET" && r.Success {
			readChecks++
		}
	}

	sort.Float64s(latencies)

	var avgLat, p50, p95, p99, minLat, maxLat float64
	if total > 0 {
		avgLat = totalLatency / float64(total)
		p50 = percentile(latencies, 0.50)
		p95 = percentile(latencies, 0.95)
		p99 = percentile(latencies, 0.99)
		minLat = latencies[0]
		maxLat = latencies[len(latencies)-1]
	}

	dur := sim.EndTime.Sub(sim.StartTime)
	var actualRPS float64
	if dur.Seconds() > 0 {
		actualRPS = float64(total) / dur.Seconds()
	}

	successRate := 0.0
	errorRate := 0.0
	if total > 0 {
		successRate = float64(successes) / float64(total) * 100
		errorRate = float64(failures) / float64(total) * 100
	}

	// Build phase breakdown
	phaseBreakdown := make(map[string]PhaseStat)
	for name, ps := range phaseStats {
		sort.Float64s(ps.lats)
		pAvg := 0.0
		pP99 := 0.0
		if ps.count > 0 {
			pAvg = ps.latTotal / float64(ps.count)
			pP99 = percentile(ps.lats, 0.99)
		}
		phaseBreakdown[name] = PhaseStat{
			Requests:     ps.count,
			Successes:    ps.success,
			Failures:     ps.fail,
			AvgLatencyMs: pAvg,
			P99LatencyMs: pP99,
		}
	}

	// Build endpoint breakdown
	endpointBreakdown := make(map[string]EndpointStat)
	for ep, es := range epStats {
		eAvg := 0.0
		if es.count > 0 {
			eAvg = es.latTotal / float64(es.count)
		}
		endpointBreakdown[ep] = EndpointStat{
			Requests:     es.count,
			Successes:    es.success,
			Failures:     es.fail,
			AvgLatencyMs: eAvg,
		}
	}

	violationRate := 0.0
	if readChecks > 0 {
		violationRate = float64(len(sim.ConsistencyViolations)) / float64(readChecks) * 100
	}

	return Report{
		Summary: ReportSummary{
			TotalRequests:      total,
			SuccessfulRequests: successes,
			FailedRequests:     failures,
			SuccessRate:        successRate,
			ErrorRate:          errorRate,
			TotalDuration:      dur.Round(time.Millisecond).String(),
			AvgLatencyMs:       avgLat,
			P50LatencyMs:       p50,
			P95LatencyMs:       p95,
			P99LatencyMs:       p99,
			MinLatencyMs:       minLat,
			MaxLatencyMs:       maxLat,
			ActualRPS:          actualRPS,
		},
		PhaseBreakdown:    phaseBreakdown,
		EndpointBreakdown: endpointBreakdown,
		ConsistencyReport: ConsistencyReport{
			TotalChecks:   readChecks,
			Violations:    len(sim.ConsistencyViolations),
			ViolationRate: violationRate,
		},
		Config: ReportConfig{
			Endpoints: cfg.Endpoints,
			DurationS: int(cfg.Duration.Seconds()),
			TargetRPS: cfg.RPS,
			ReadRatio: cfg.ReadRatio,
		},
	}
}

// WriteReportJSON serializes the report to a JSON file.
func WriteReportJSON(report Report, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling report: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing report file: %w", err)
	}
	fmt.Printf("\n  📄 Report written to: %s\n\n", path)
	return nil
}

// PrintSummary outputs a formatted summary table to stdout.
func PrintSummary(r Report) {
	sep := strings.Repeat("─", 52)

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║                   📊 Test Results                   ║")
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Println("║  SUMMARY                                            ║")
	fmt.Printf("║  Total Requests:      %-30d║\n", r.Summary.TotalRequests)
	fmt.Printf("║  Successful:          %-30d║\n", r.Summary.SuccessfulRequests)
	fmt.Printf("║  Failed:              %-30d║\n", r.Summary.FailedRequests)
	fmt.Printf("║  Success Rate:        %-30s║\n", fmt.Sprintf("%.2f%%", r.Summary.SuccessRate))
	fmt.Printf("║  Actual RPS:          %-30s║\n", fmt.Sprintf("%.1f", r.Summary.ActualRPS))
	fmt.Printf("║  Duration:            %-30s║\n", r.Summary.TotalDuration)
	fmt.Println("║                                                      ║")
	fmt.Println("║  LATENCY                                             ║")
	fmt.Printf("║  Avg:    %-12s  P50:   %-23s║\n",
		fmt.Sprintf("%.2fms", r.Summary.AvgLatencyMs),
		fmt.Sprintf("%.2fms", r.Summary.P50LatencyMs))
	fmt.Printf("║  P95:    %-12s  P99:   %-23s║\n",
		fmt.Sprintf("%.2fms", r.Summary.P95LatencyMs),
		fmt.Sprintf("%.2fms", r.Summary.P99LatencyMs))
	fmt.Printf("║  Min:    %-12s  Max:   %-23s║\n",
		fmt.Sprintf("%.2fms", r.Summary.MinLatencyMs),
		fmt.Sprintf("%.2fms", r.Summary.MaxLatencyMs))
	fmt.Println("║                                                      ║")
	fmt.Println("║  CONSISTENCY                                         ║")
	fmt.Printf("║  Read Checks:         %-30d║\n", r.ConsistencyReport.TotalChecks)
	fmt.Printf("║  Violations:          %-30d║\n", r.ConsistencyReport.Violations)
	fmt.Printf("║  Violation Rate:      %-30s║\n", fmt.Sprintf("%.2f%%", r.ConsistencyReport.ViolationRate))
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Println("║  PHASE BREAKDOWN                                    ║")
	fmt.Printf("║  %s║\n", sep)

	phaseOrder := []string{"ramp-up", "steady", "spike", "cooldown"}
	for _, name := range phaseOrder {
		ps, ok := r.PhaseBreakdown[name]
		if !ok {
			continue
		}
		fmt.Printf("║  %-10s  reqs: %-6d  ok: %-6d  fail: %-6d  ║\n",
			name, ps.Requests, ps.Successes, ps.Failures)
	}

	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Println("║  ENDPOINT BREAKDOWN                                 ║")
	fmt.Printf("║  %s║\n", sep)

	for ep, es := range r.EndpointBreakdown {
		// Truncate long endpoints for display
		display := ep
		if len(display) > 28 {
			display = display[:28] + "…"
		}
		fmt.Printf("║  %-30s reqs: %-5d avg: %.1fms  ║\n",
			display, es.Requests, es.AvgLatencyMs)
	}

	fmt.Println("╚══════════════════════════════════════════════════════╝")
}

// percentile computes the p-th percentile from a sorted slice of float64.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
