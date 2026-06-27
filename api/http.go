package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/arihant/kvstore/server"
	"github.com/arihant/kvstore/store"
)

type APIServer struct {
	node     *server.Node
	httpPort string
}

func NewAPIServer(n *server.Node, port string) *APIServer {
	return &APIServer{
		node:     n,
		httpPort: port,
	}
}

func (api *APIServer) Start() error {
	mux := http.NewServeMux()

	// Endpoints required by the hackathon contract
	mux.HandleFunc("/store/", api.handleStore)
	mux.HandleFunc("/health", api.handleHealth)
	mux.HandleFunc("/leader", api.handleLeader)

	fmt.Printf("HTTP API Server starting on %s\n", api.httpPort)
	return http.ListenAndServe(api.httpPort, mux)
}

func (api *APIServer) handleLeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"is_leader":           api.node.IsLeader(),
		"leader_id":           api.node.GetLeaderID(),
		"leader_http_address": api.node.GetLeaderHTTPAddress(),
		"leader_grpc_address": api.node.GetLeaderGRPCAddress(),
	})
}

func (api *APIServer) handleStore(w http.ResponseWriter, r *http.Request) {
	// Extract the key from "/store/{key}"
	key := strings.TrimPrefix(r.URL.Path, "/store/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// GET is always served locally (Eventual Consistency / AP)
	// We read directly from our local memory map, even if we are a follower.
	// This means reads are blazing fast, but might be slightly stale if we just got partitioned.
	if r.Method == http.MethodGet {
		val, exists := api.node.GetStore().Get(key)
		if !exists {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": string(val)})
		return
	}

	// Writes (PUT/DELETE) must go through the Leader
	if r.Method == http.MethodPut || r.Method == http.MethodDelete {
		if !api.node.IsLeader() {
			// If we are not the leader, proxy the request to the actual leader
			leaderAddr := api.node.GetLeaderHTTPAddress()
			if leaderAddr == "" {
				http.Error(w, "No Leader Elected Yet", http.StatusServiceUnavailable)
				return
			}
			api.proxyRequest(w, r, leaderAddr)
			return
		}

		// We ARE the leader! Let's process the write.
		if r.Method == http.MethodPut {
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}

			// Apply to our local WAL and memory
			api.node.GetStore().Put(key, []byte(body["value"]))

			// Trigger replication to followers!
			if !api.node.ReplicateEntry(store.OpPut, key, []byte(body["value"])) {
				http.Error(w, "Failed to replicate write to majority of nodes", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodDelete {
			api.node.GetStore().Delete(key)
			if !api.node.ReplicateEntry(store.OpDelete, key, nil) {
				http.Error(w, "Failed to replicate delete to majority of nodes", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (api *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	role := api.node.GetRoleString()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"nodes":  len(api.node.GetPeers()) + 1, // Peers + Self
		"role":   strings.ToLower(role),
	})
}

// proxyRequest forwards an HTTP request to the Leader
func (api *APIServer) proxyRequest(w http.ResponseWriter, r *http.Request, leaderAddr string) {
	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.Path)

	bodyBytes, _ := io.ReadAll(r.Body)
	proxyReq, err := http.NewRequest(r.Method, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusInternalServerError)
		return
	}

	proxyReq.Header = r.Header

	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Leader Unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
