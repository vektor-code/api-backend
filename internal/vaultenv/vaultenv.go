// Package vaultenv loads KV secrets from HashiCorp Vault via AppRole and
// injects them into process environment variables before config loading.
//
// Enabled when VAULT_ADDR, VAULT_ROLE_ID, VAULT_SECRET_ID, and
// VAULT_SECRET_PATH are set. Existing non-empty env vars are left unchanged
// so chart/runtime overrides still win.
package vaultenv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxBody = 1 << 20

// Load authenticates with AppRole and sets env vars from the KV secret at
// VAULT_SECRET_PATH (KV v2 API path, e.g. development/data/crnet-apm/api-backend/dev).
func Load() error {
	addr := strings.TrimRight(strings.TrimSpace(os.Getenv("VAULT_ADDR")), "/")
	roleID := strings.TrimSpace(os.Getenv("VAULT_ROLE_ID"))
	secretID := strings.TrimSpace(os.Getenv("VAULT_SECRET_ID"))
	path := strings.Trim(strings.TrimSpace(os.Getenv("VAULT_SECRET_PATH")), "/")
	if addr == "" || roleID == "" || secretID == "" || path == "" {
		return nil
	}

	client := &http.Client{Timeout: 10 * time.Second}
	token, err := login(client, addr, roleID, secretID)
	if err != nil {
		return err
	}
	data, err := readSecret(client, addr, token, path)
	if err != nil {
		return err
	}
	for key, value := range data {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if existing, ok := os.LookupEnv(key); ok && strings.TrimSpace(existing) != "" {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("vaultenv: set %s: %w", key, err)
		}
	}
	return nil
}

func login(client *http.Client, addr, roleID, secretID string) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"role_id":   roleID,
		"secret_id": secretID,
	})
	req, err := http.NewRequest(http.MethodPost, addr+"/v1/auth/approle/login", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("vaultenv: login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("vaultenv: login: %w", err)
	}
	defer res.Body.Close()
	payload, err := readBody(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("vaultenv: login status %d: %s", res.StatusCode, truncate(string(payload)))
	}
	var parsed struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", fmt.Errorf("vaultenv: decode login: %w", err)
	}
	if parsed.Auth.ClientToken == "" {
		return "", fmt.Errorf("vaultenv: empty client token")
	}
	return parsed.Auth.ClientToken, nil
}

func readSecret(client *http.Client, addr, token, path string) (map[string]string, error) {
	req, err := http.NewRequest(http.MethodGet, addr+"/v1/"+path, nil)
	if err != nil {
		return nil, fmt.Errorf("vaultenv: read request: %w", err)
	}
	req.Header.Set("X-Vault-Token", token)
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vaultenv: read: %w", err)
	}
	defer res.Body.Close()
	payload, err := readBody(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("vaultenv: read status %d: %s", res.StatusCode, truncate(string(payload)))
	}
	var parsed struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, fmt.Errorf("vaultenv: decode secret: %w", err)
	}
	out := make(map[string]string, len(parsed.Data.Data))
	for key, raw := range parsed.Data.Data {
		switch v := raw.(type) {
		case string:
			out[key] = v
		case float64:
			out[key] = fmt.Sprintf("%v", v)
		case bool:
			out[key] = fmt.Sprintf("%t", v)
		default:
			if raw != nil {
				b, _ := json.Marshal(v)
				out[key] = string(b)
			}
		}
	}
	return out, nil
}

func readBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("vaultenv: read body: %w", err)
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("vaultenv: response too large")
	}
	return body, nil
}

func truncate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}
