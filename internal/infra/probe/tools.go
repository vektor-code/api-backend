package probe

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
)

func kafkaProbe(ctx context.Context, settings Settings) Result {
	brokers := settings.get("brokers")
	if brokers == "" {
		return incomplete("bootstrap brokers are required")
	}
	address, err := firstBroker(brokers)
	if err != nil {
		return failResult(err.Error())
	}

	conn, err := kafka.DialContext(ctx, "tcp", address)
	if err != nil {
		return failResult(sanitize(err))
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline(ctx))

	topic := settings.get("topic")
	if topic == "" {
		if _, err := conn.Brokers(); err != nil {
			return failResult(sanitize(err))
		}
		return okResult("Broker connection succeeded")
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return failResult(sanitize(err))
	}
	if len(partitions) == 0 {
		return failResult("topic was not found")
	}
	return okResult("Broker connection succeeded, topic is reachable")
}

func firstBroker(raw string) (string, error) {
	for _, part := range strings.Split(raw, ",") {
		addr, err := normalizeBroker(part)
		if err != nil {
			return "", err
		}
		if addr != "" {
			return addr, nil
		}
	}
	return "", fmt.Errorf("bootstrap brokers are required")
}

func normalizeBroker(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("broker address is invalid")
		}
		raw = parsed.Host
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return raw, nil
	}
	if strings.Contains(raw, ":") {
		return "", fmt.Errorf("broker address is invalid")
	}
	return net.JoinHostPort(raw, "9092"), nil
}

func clickhouseProbe(ctx context.Context, settings Settings) Result {
	base, err := clickhouseBaseURL(settings)
	if err != nil {
		return incomplete(err.Error())
	}
	username := settings.get("username")
	password := settings.get("password")
	if parsed, err := url.Parse(base); err == nil && parsed.User != nil {
		if username == "" {
			username = parsed.User.Username()
		}
		if password == "" {
			password, _ = parsed.User.Password()
		}
		parsed.User = nil
		base = parsed.String()
	}
	queryURL := strings.TrimRight(base, "/") + "/"
	if database := settings.get("database"); database != "" {
		queryURL += "?database=" + url.QueryEscape(database)
	}
	status, _, err := probeHTTPPost(ctx, queryURL, username, password, "SELECT 1")
	if err != nil {
		return failResult(sanitize(err))
	}
	if status >= 200 && status < 300 {
		return okResult("Database query succeeded")
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return failResult("authentication failed")
	}
	return failResult(httpStatusMessage(status))
}

func clickhouseBaseURL(settings Settings) (string, error) {
	if host := settings.get("host"); host != "" {
		scheme := settings.get("scheme")
		if scheme == "" {
			scheme = "http"
		}
		port := settings.get("port")
		if port == "" {
			port = "8123"
		}
		parsed, err := parseHTTPURL(scheme+"://"+withDefaultPort(host, port), scheme)
		if err != nil {
			return "", err
		}
		return strings.TrimRight(parsed.String(), "/"), nil
	}
	if raw := settings.get("url"); raw != "" {
		parsed, err := parseHTTPURL(raw, "http")
		if err != nil {
			return "", err
		}
		return strings.TrimRight(parsed.String(), "/"), nil
	}
	return "", fmt.Errorf("host is required")
}

func minioProbe(ctx context.Context, settings Settings) Result {
	endpoint := settings.get("endpoint")
	if endpoint == "" {
		return incomplete("endpoint is required")
	}
	host, secure, err := normalizeMinIOEndpoint(endpoint, settings.get("useSSL"))
	if err != nil {
		return failResult(err.Error())
	}
	accessKey := settings.get("accessKey")
	secretKey := settings.get("secretKey")
	if accessKey == "" || secretKey == "" {
		status, _, err := probeHTTP(ctx, minioHealthURL(host, secure), "", "", false)
		if err != nil {
			return failResult(sanitize(err))
		}
		if status >= 200 && status < 300 {
			return okResult("Live health check succeeded (credentials not configured)")
		}
		return failResult(httpStatusMessage(status))
	}

	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return failResult(sanitize(err))
	}
	bucket := settings.get("bucket")
	if bucket == "" {
		_, err := client.ListBuckets(ctx)
		if err != nil {
			return failResult(sanitize(err))
		}
		return okResult("Credentials are valid")
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return failResult(sanitize(err))
	}
	if !exists {
		return failResult("bucket was not found")
	}
	return okResult("Bucket is reachable")
}

func normalizeMinIOEndpoint(endpoint, useSSL string) (string, bool, error) {
	secure := useSSL == "true" || useSSL == "1" || strings.EqualFold(useSSL, "on")
	endpoint = strings.TrimSpace(endpoint)
	if strings.Contains(endpoint, "://") {
		parsed, err := parseHTTPURL(endpoint, "http")
		if err != nil {
			return "", false, err
		}
		return parsed.Host, parsed.Scheme == "https" || secure, nil
	}
	return endpoint, secure, nil
}

func minioHealthURL(host string, secure bool) string {
	scheme := "http"
	if secure {
		scheme = "https"
	}
	return scheme + "://" + host + "/minio/health/live"
}

func ldapProbe(ctx context.Context, settings Settings) Result {
	raw := settings.get("url")
	if raw == "" {
		return incomplete("directory server URL is required")
	}
	ldapURL, err := normalizeLDAPURL(raw)
	if err != nil {
		return failResult(err.Error())
	}
	bindDN := settings.get("bindDN")
	if bindDN == "" {
		return incomplete("bind DN is required")
	}

	conn, err := ldap.DialURL(ldapURL, ldap.DialWithDialer(&net.Dialer{Timeout: timeout}))
	if err != nil {
		return failResult(sanitize(err))
	}
	defer conn.Close()
	conn.SetTimeout(remainingTimeout(ctx))
	if err := conn.Bind(bindDN, settings.get("bindPassword")); err != nil {
		return failResult("directory bind failed")
	}
	baseDN := settings.get("userBaseDN")
	if baseDN == "" {
		return okResult("Directory bind succeeded")
	}
	filter := settings.get("userFilter")
	if filter == "" || strings.Contains(filter, "{login}") {
		filter = "(objectClass=*)"
	}
	request := ldap.NewSearchRequest(
		baseDN,
		ldap.ScopeBaseObject, ldap.NeverDerefAliases, 1, 5, false,
		filter,
		[]string{"dn"},
		nil,
	)
	if _, err := conn.Search(request); err != nil {
		// Bind worked; search base may still be wrong — report it without failing bind.
		return okResult("Directory bind succeeded (user search base was not verified)")
	}
	return okResult("Directory bind succeeded")
}

func remainingTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return timeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Millisecond
	}
	return remaining
}

func normalizeLDAPURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("directory server URL is required")
	}
	if !strings.Contains(raw, "://") {
		return "ldap://" + withDefaultPort(raw, "389"), nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("directory server URL is invalid")
	}
	if parsed.Scheme != "ldap" && parsed.Scheme != "ldaps" {
		return "", fmt.Errorf("directory server URL must use ldap or ldaps")
	}
	return raw, nil
}

func prometheusProbe(ctx context.Context, settings Settings) Result {
	base := settings.get("url")
	if base == "" {
		return incomplete("API URL is required")
	}
	parsed, err := parseHTTPURL(base, "http")
	if err != nil {
		return failResult(err.Error())
	}
	root := strings.TrimRight(parsed.String(), "/")
	for _, path := range []string{"/-/healthy", "/-/ready"} {
		status, _, err := probeHTTP(ctx, root+path, "", "", false)
		if err != nil {
			return failResult(sanitize(err))
		}
		if status >= 200 && status < 300 {
			return okResult("Prometheus is healthy")
		}
		if status != 404 {
			return failResult(httpStatusMessage(status))
		}
	}
	return failResult("Prometheus health endpoint was not found")
}

func elasticsearchProbe(ctx context.Context, settings Settings) Result {
	raw := firstCSV(settings.get("url"))
	if raw == "" {
		return incomplete("node URL is required")
	}
	parsed, err := parseHTTPURL(raw, "http")
	if err != nil {
		return failResult(err.Error())
	}
	insecure := settings.get("tlsVerify") != "true"
	status, body, err := probeHTTP(ctx, strings.TrimRight(parsed.String(), "/")+"/_cluster/health", settings.get("username"), settings.get("password"), insecure)
	if err != nil {
		return failResult(sanitize(err))
	}
	if status >= 200 && status < 300 {
		return okResult("Cluster health check succeeded")
	}
	if status == 401 || status == 403 {
		return failResult("authentication failed")
	}
	if len(body) > 0 && status >= 400 {
		return failResult(httpStatusMessage(status))
	}
	return failResult(httpStatusMessage(status))
}
