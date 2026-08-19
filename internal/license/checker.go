package license

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const maxBody = 1 << 20

type Status struct {
	Valid      bool       `json:"valid"`
	Status     string     `json:"status"`
	Code       string     `json:"code,omitempty"`
	Product    string     `json:"product"`
	InstanceID string     `json:"instance_id,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	Message    string     `json:"message"`
}

func (s Status) Allowed() bool {
	return s.Valid
}

type Config struct {
	Product      string
	InstanceID   string
	InstanceName string
	Component    string
	Version      string
	Interval     time.Duration
}

type Checker struct {
	cfg       Config
	logger    *slog.Logger
	client    *http.Client
	heartbeat string

	mu      sync.RWMutex
	current Status
	seen    bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func New(cfg Config, logger *slog.Logger) *Checker {
	product := strings.ToLower(strings.TrimSpace(cfg.Product))
	instanceID := strings.TrimSpace(cfg.InstanceID)
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}
	if instanceID == "" {
		instanceID = product
	}
	cfg.Product = product
	cfg.InstanceID = instanceID
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Checker{
		cfg:       cfg,
		logger:    logger,
		client:    &http.Client{Timeout: 8 * time.Second},
		heartbeat: strings.TrimRight(EndpointURL, "/") + "/api/v1/heartbeat",
		current: Status{
			Valid:      false,
			Status:     "pending",
			Code:       "LICENSE_PENDING",
			Product:    product,
			InstanceID: instanceID,
			Message:    "Waiting for license activation.",
		},
		done: make(chan struct{}),
	}
}

func (c *Checker) Name() string { return "license-heartbeat" }

func (c *Checker) Start(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.loop(ctx)
	return nil
}

func (c *Checker) Stop(context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
	return nil
}

func (c *Checker) Allowed() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current.Allowed()
}

func (c *Checker) Snapshot() Status {
	if c == nil {
		return Status{Valid: false, Status: "pending", Code: "LICENSE_PENDING", Message: "Waiting for license activation."}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

func (c *Checker) loop(ctx context.Context) {
	defer close(c.done)
	c.refresh(ctx)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *Checker) refresh(ctx context.Context) {
	snap, err := c.postHeartbeat(ctx)
	if err != nil {
		c.logger.Warn("license heartbeat", "error", err)
		return
	}
	c.mu.Lock()
	c.seen = true
	c.current = snap
	c.mu.Unlock()
}

func (c *Checker) postHeartbeat(ctx context.Context) (Status, error) {
	body, err := json.Marshal(map[string]string{
		"product":       c.cfg.Product,
		"instance_id":   c.cfg.InstanceID,
		"instance_name": firstNonEmpty(c.cfg.InstanceName, c.cfg.InstanceID),
		"component":     c.cfg.Component,
		"version":       c.cfg.Version,
	})
	if err != nil {
		return Status{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.heartbeat, bytes.NewReader(body))
	if err != nil {
		return Status{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+HeartbeatToken)
	res, err := c.client.Do(req)
	if err != nil {
		return Status{}, err
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
	if err != nil {
		return Status{}, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return Status{}, jsonError(res.StatusCode, payload)
	}
	var snap Status
	if err := json.Unmarshal(payload, &snap); err != nil {
		return Status{}, err
	}
	return snap, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type statusError struct {
	status int
	body   string
}

func (e statusError) Error() string {
	return e.body
}

func jsonError(status int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(status)
	}
	return statusError{status: status, body: msg}
}
