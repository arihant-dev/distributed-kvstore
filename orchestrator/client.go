package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

var httpClient = &http.Client{
	Timeout: 5 * time.Second,
}

// PutKey writes a key-value pair to the given KV store endpoint.
// Expects the endpoint to accept PUT /store/{key} with JSON body {"value": "..."}.
func PutKey(endpoint, key, value string) error {
	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	url := fmt.Sprintf("%s/store/%s", endpoint, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s failed: %w", url, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s returned status %d", url, resp.StatusCode)
	}
	return nil
}

// GetKey reads a value for the given key from the KV store endpoint.
// Expects the endpoint to return JSON with a "value" field at GET /store/{key}.
func GetKey(endpoint, key string) (string, error) {
	url := fmt.Sprintf("%s/store/%s", endpoint, key)
	resp, err := httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("GET %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key %q not found on %s", key, endpoint)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, resp.Body)
		return "", fmt.Errorf("GET %s returned status %d", url, resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode response from %s: %w", url, err)
	}

	val, ok := result["value"]
	if !ok {
		return "", fmt.Errorf("response from %s missing 'value' field", url)
	}
	return val, nil
}

// ChaosKill sends a kill command to the chaos agent to take down a target node.
func ChaosKill(chaosEndpoint, target string) error {
	body, _ := json.Marshal(map[string]string{"target": target})
	url := fmt.Sprintf("%s/chaos/kill", chaosEndpoint)

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("chaos kill %s failed: %w", target, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("chaos kill %s returned status %d", target, resp.StatusCode)
	}
	return nil
}

// ChaosResume sends a resume command to the chaos agent to bring a target node back up.
func ChaosResume(chaosEndpoint, target string) error {
	body, _ := json.Marshal(map[string]string{"target": target})
	url := fmt.Sprintf("%s/chaos/resume", chaosEndpoint)

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("chaos resume %s failed: %w", target, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("chaos resume %s returned status %d", target, resp.StatusCode)
	}
	return nil
}

// ChaosHeal sends a heal command to the chaos agent to restore all network partitions.
func ChaosHeal(chaosEndpoint string) error {
	url := fmt.Sprintf("%s/chaos/heal", chaosEndpoint)

	resp, err := httpClient.Post(url, "application/json", nil)
	if err != nil {
		return fmt.Errorf("chaos heal failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("chaos heal returned status %d", resp.StatusCode)
	}
	return nil
}

// ChaosEnableAutoProvision enables or disables auto-provisioning in the chaos agent.
func ChaosEnableAutoProvision(chaosEndpoint string, enabled bool) error {
	body, _ := json.Marshal(map[string]bool{"enabled": enabled})
	url := fmt.Sprintf("%s/chaos/auto-provision", chaosEndpoint)

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("chaos auto-provision failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("chaos auto-provision returned status %d", resp.StatusCode)
	}
	return nil
}
