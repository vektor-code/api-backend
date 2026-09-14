package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Notifier delivers alert payloads to configured channels.
type Notifier struct {
	Client *http.Client
}

func NewNotifier(client *http.Client) *Notifier {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Notifier{Client: client}
}

type webhookPayload struct {
	Text      string      `json:"text"`
	Alert     ActiveAlert `json:"alert"`
	Severity  string      `json:"severity"`
	Service   string      `json:"service"`
	Namespace string      `json:"namespace"`
}

func (n *Notifier) Notify(ctx context.Context, channel Channel, alert ActiveAlert) error {
	switch channel.Type {
	case ChannelWebhook, ChannelSlack, "":
		return n.postJSON(ctx, channel.Target, webhookPayload{
			Text:      fmt.Sprintf("[CRNET APM] %s %s on %s/%s value=%g (%s)", alert.Severity, alert.RuleName, alert.Namespace, alert.Service, alert.Value, alert.Condition),
			Alert:     alert,
			Severity:  string(alert.Severity),
			Service:   alert.Service,
			Namespace: alert.Namespace,
		})
	case ChannelPagerDuty:
		return n.postJSON(ctx, channel.Target, map[string]interface{}{
			"routing_key": channel.Target,
			"event_action": "trigger",
			"payload": map[string]interface{}{
				"summary":  fmt.Sprintf("%s: %s %s", alert.Severity, alert.Service, alert.Condition),
				"severity": strings.ToLower(string(alert.Severity)),
				"source":   "crnet-apm",
				"custom_details": alert,
			},
		})
	case ChannelEmail:
		// Email requires an SMTP gateway; treat target as a webhook that accepts JSON mail.
		return n.postJSON(ctx, channel.Target, map[string]interface{}{
			"to":      channel.Target,
			"subject": fmt.Sprintf("[CRNET APM] %s %s", alert.Severity, alert.RuleName),
			"body":    fmt.Sprintf("%s/%s %s value=%g", alert.Namespace, alert.Service, alert.Condition, alert.Value),
			"alert":   alert,
		})
	default:
		return fmt.Errorf("unsupported channel type %q", channel.Type)
	}
}

func (n *Notifier) postJSON(ctx context.Context, url string, body interface{}) error {
	if strings.TrimSpace(url) == "" {
		return fmt.Errorf("empty channel target")
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return fmt.Errorf("channel target must be an http(s) URL")
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", resp.StatusCode)
	}
	return nil
}
