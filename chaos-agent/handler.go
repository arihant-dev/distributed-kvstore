package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// handler holds a reference to the ChaosEngine and exposes HTTP endpoints.
type handler struct {
	engine *ChaosEngine
}

func newHandler(engine *ChaosEngine) *handler {
	return &handler{engine: engine}
}

// RegisterRoutes sets up all the chaos API routes.
func (h *handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/chaos/kill", h.withLogging(h.handleKill))
	mux.HandleFunc("/chaos/pause", h.withLogging(h.handlePause))
	mux.HandleFunc("/chaos/resume", h.withLogging(h.handleResume))
	mux.HandleFunc("/chaos/slow", h.withLogging(h.handleSlow))
	mux.HandleFunc("/chaos/partition", h.withLogging(h.handlePartition))
	mux.HandleFunc("/chaos/heal", h.withLogging(h.handleHeal))
	mux.HandleFunc("/chaos/status", h.withLogging(h.handleStatus))
	mux.HandleFunc("/chaos/containers", h.withLogging(h.handleContainers))
}

// --- Request/Response Types ---

type targetRequest struct {
	Target string `json:"target"`
}

type slowRequest struct {
	Target string `json:"target"`
	Ms     int    `json:"ms"`
}

type partitionRequest struct {
	A string `json:"a"`
	B string `json:"b"`
}

type statusResponse struct {
	Active  bool       `json:"active"`
	State   ChaosState `json:"state"`
	Uptime  string     `json:"uptime"`
}

type errorResponse struct {
	Error string `json:"error"`
}

var startTime = time.Now()

// --- Handlers ---

// POST /chaos/kill {"target": "node2"}
func (h *handler) handleKill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'target' field required")
		return
	}

	if err := h.engine.Kill(context.Background(), req.Target); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "killed", "target": req.Target})
}

// POST /chaos/pause {"target": "node2"}
func (h *handler) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'target' field required")
		return
	}

	if err := h.engine.Pause(context.Background(), req.Target); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "paused", "target": req.Target})
}

// POST /chaos/resume {"target": "node2"}
func (h *handler) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'target' field required")
		return
	}

	if err := h.engine.Resume(context.Background(), req.Target); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "resumed", "target": req.Target})
}

// POST /chaos/slow {"target": "node2", "ms": 3000}
func (h *handler) handleSlow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req slowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" || req.Ms <= 0 {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'target' (string) and 'ms' (positive int) required")
		return
	}

	if err := h.engine.Slow(context.Background(), req.Target, req.Ms); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "slowed",
		"target":     req.Target,
		"latency_ms": req.Ms,
	})
}

// POST /chaos/partition {"a": "node1", "b": "node3"}
func (h *handler) handlePartition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req partitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.A == "" || req.B == "" {
		h.writeError(w, http.StatusBadRequest, "JSON body with 'a' and 'b' fields required")
		return
	}

	if err := h.engine.Partition(context.Background(), req.A, req.B); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":    "partitioned",
		"between":   fmt.Sprintf("%s ↔ %s", req.A, req.B),
	})
}

// POST /chaos/heal
func (h *handler) handleHeal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	if err := h.engine.Heal(context.Background()); err != nil {
		// Partial heal — report as 207 Multi-Status
		h.writeJSON(w, http.StatusMultiStatus, map[string]string{
			"status":  "partially_healed",
			"details": err.Error(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "healed"})
}

// GET /chaos/status
func (h *handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}

	state := h.engine.GetState()

	hasActiveChaos := len(state.KilledContainers) > 0 ||
		len(state.PausedContainers) > 0 ||
		len(state.SlowContainers) > 0 ||
		len(state.Partitions) > 0

	h.writeJSON(w, http.StatusOK, statusResponse{
		Active: hasActiveChaos,
		State:  state,
		Uptime: time.Since(startTime).Round(time.Second).String(),
	})
}

// GET /chaos/containers — bonus endpoint to list discovered containers
func (h *handler) handleContainers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}

	containers, err := h.engine.DiscoverContainers(context.Background())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"containers": containers,
		"count":      len(containers),
	})
}

// --- Helpers ---

func (h *handler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *handler) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorResponse{Error: msg})
}

// withLogging wraps an HTTP handler with request logging.
func (h *handler) withLogging(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("→ %s %s from %s", r.Method, r.URL.Path, strings.Split(r.RemoteAddr, ":")[0])
		next(w, r)
		log.Printf("← %s %s completed in %v", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	}
}
