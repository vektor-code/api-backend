// Package probe verifies Admin infrastructure connections without persisting settings.
package probe

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const timeout = 5 * time.Second

// Result is the Admin "Test connection" payload.
type Result struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// Settings are the form values for one tool. Keys match the Admin UI.
type Settings map[string]string

// Prober checks one third-party tool.
type Prober func(ctx context.Context, settings Settings) Result

var probers = map[string]Prober{
	"kafka":         kafkaProbe,
	"clickhouse":    clickhouseProbe,
	"minio":         minioProbe,
	"ldap":          ldapProbe,
	"prometheus":    prometheusProbe,
	"elasticsearch": elasticsearchProbe,
}

// SecretKeys are form fields that must be merged from stored config when masked.
var SecretKeys = map[string][]string{
	"clickhouse": {"password"},
	"minio":      {"secretKey"},
	"ldap":       {"bindPassword"},
}

// Supported reports whether Admin can probe the tool.
func Supported(tool string) bool {
	_, ok := probers[tool]
	return ok
}

// Run probes the named tool. Connection failures return Result, not an error.
func Run(ctx context.Context, tool string, settings Settings) Result {
	prober, ok := probers[tool]
	if !ok {
		return Result{OK: false, Message: "unsupported tool"}
	}
	if settings == nil {
		settings = Settings{}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return prober(ctx, settings)
}

func (s Settings) get(key string) string {
	return strings.TrimSpace(s[key])
}

func incomplete(message string) Result {
	if message == "" {
		message = "required fields are incomplete"
	}
	return Result{OK: false, Message: message}
}

func okResult(message string) Result {
	return Result{OK: true, Message: message}
}

func failResult(message string) Result {
	return Result{OK: false, Message: message}
}

var credentialInURL = regexp.MustCompile(`://[^/\s]+:[^@/\s]+@`)

func sanitize(err error) string {
	if err == nil {
		return "connection failed"
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "connection failed"
	}
	return credentialInURL.ReplaceAllString(msg, "://***@")
}

func withDefaultPort(hostPort, port string) string {
	hostPort = strings.TrimSpace(hostPort)
	if hostPort == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(hostPort); err == nil {
		return hostPort
	}
	return net.JoinHostPort(hostPort, port)
}

func parseHTTPURL(raw, defaultScheme string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("URL is required")
	}
	if !strings.Contains(raw, "://") {
		scheme := defaultScheme
		if scheme == "" {
			scheme = "http"
		}
		raw = scheme + "://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("URL is invalid")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("URL is required")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("URL must use http or https")
	}
	parsed.User = nil
	return parsed, nil
}

func firstCSV(raw string) string {
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// Merge overlays submitted form values onto stored settings, keeping secrets
// that the UI sent as a mask or left blank.
func Merge(incoming, stored Settings, secretKeys []string) Settings {
	out := Settings{}
	for key, value := range stored {
		out[key] = value
	}
	secrets := make(map[string]struct{}, len(secretKeys))
	for _, key := range secretKeys {
		secrets[key] = struct{}{}
	}
	for key, value := range incoming {
		if _, secret := secrets[key]; secret && IsMaskedSecret(value, stored[key]) {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	return out
}

// IsMaskedSecret reports placeholder values the Admin UI sends instead of a secret.
func IsMaskedSecret(value, stored string) bool {
	value = strings.TrimSpace(value)
	switch value {
	case "", "******", "****", "••••••••":
		return true
	}
	return stored != "" && value == maskPreview(stored)
}

func maskPreview(secret string) string {
	if secret == "" {
		return ""
	}
	if len(secret) <= 4 {
		return "****"
	}
	return secret[:2] + "****" + secret[len(secret)-2:]
}
