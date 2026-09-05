package probe

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

func probeHTTP(ctx context.Context, rawURL, username, password string, insecureTLS bool) (int, []byte, error) {
	parsed, err := parseHTTPURL(rawURL, "http")
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return 0, nil, err
	}
	if username != "" || password != "" {
		request.SetBasicAuth(username, password)
	}
	response, err := httpClient(insecureTLS).Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
	return response.StatusCode, body, nil
}

func probeHTTPPost(ctx context.Context, rawURL, username, password, body string) (int, []byte, error) {
	parsed, err := parseHTTPURL(rawURL, "http")
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if username != "" || password != "" {
		request.SetBasicAuth(username, password)
	}
	response, err := httpClient(false).Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
	return response.StatusCode, payload, nil
}

func httpClient(insecureTLS bool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		IdleConnTimeout:       timeout,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
	}
	if insecureTLS {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} //nolint:gosec
	} else {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("redirects are not followed")
		},
	}
}

func httpStatusMessage(status int) string {
	if status >= 200 && status < 300 {
		return fmt.Sprintf("Connected (HTTP %d)", status)
	}
	return fmt.Sprintf("Unexpected status %d", status)
}

func deadline(ctx context.Context) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(timeout)
}
