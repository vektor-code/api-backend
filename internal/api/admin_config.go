package api

import (
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/infra/probe"
)

var errInvalidClickHouseURL = errors.New("clickhouse URL is invalid")

// POST /api/admin/config/:tool/test
func (h *Handler) TestAdminToolConnection(c *fiber.Ctx) error {
	if err := h.requireAdmin(c); err != nil {
		return err
	}
	tool := strings.ToLower(strings.TrimSpace(c.Params("tool")))
	if !probe.Supported(tool) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "unsupported tool"})
	}

	incoming := probe.Settings{}
	if len(c.Body()) > 0 {
		var raw map[string]any
		if err := c.BodyParser(&raw); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
		}
		incoming = settingsFromJSON(raw)
	}

	effective := probe.Merge(incoming, h.storedToolSettings(tool), probe.SecretKeys[tool])
	result := probe.Run(c.Context(), tool, effective)
	return c.JSON(result)
}

func settingsFromJSON(raw map[string]any) probe.Settings {
	out := probe.Settings{}
	for key, value := range raw {
		switch typed := value.(type) {
		case string:
			out[key] = typed
		case bool:
			if typed {
				out[key] = "true"
			} else {
				out[key] = "false"
			}
		}
	}
	return out
}

func (h *Handler) storedToolSettings(tool string) probe.Settings {
	if h.store == nil {
		return probe.Settings{}
	}
	env := func(key string) string {
		return h.store.GetInfraConfig(key, os.Getenv(key))
	}
	switch tool {
	case "kafka":
		return probe.Settings{
			"brokers": env("KAFKA_BROKERS"),
			"topic":   env("KAFKA_TOPIC"),
			"group":   env("KAFKA_GROUP"),
		}
	case "clickhouse":
		clickhouseURL := env("CLICKHOUSE_URL")
		settings := probe.Settings{"url": clickhouseURL, "database": "kubetrace"}
		if parsed, err := parseClickHouseURL(clickhouseURL); err == nil {
			settings["scheme"] = parsed.scheme
			settings["host"] = parsed.host
			settings["port"] = parsed.port
			settings["username"] = parsed.username
			settings["password"] = parsed.password
		}
		return settings
	case "minio":
		return probe.Settings{
			"endpoint":  env("MINIO_ENDPOINT"),
			"useSSL":    env("MINIO_USE_SSL"),
			"accessKey": env("MINIO_ACCESS_KEY"),
			"secretKey": env("MINIO_SECRET_KEY"),
			"bucket":    env("MINIO_BUCKET"),
		}
	case "ldap":
		enabled := env("LDAP_ENABLED")
		if enabled == "" {
			enabled = "false"
		}
		return probe.Settings{
			"enabled":      enabled,
			"url":          env("LDAP_URL"),
			"bindDN":       env("LDAP_BIND_DN"),
			"bindPassword": env("LDAP_BIND_PASSWORD"),
			"userBaseDN":   env("LDAP_USER_BASE_DN"),
			"userFilter":   env("LDAP_USER_FILTER"),
		}
	case "prometheus":
		return probe.Settings{
			"url":            env("PROMETHEUS_URL"),
			"scrapeInterval": env("PROMETHEUS_SCRAPE_INTERVAL"),
			"discoveryMode":  env("PROMETHEUS_DISCOVERY_MODE"),
		}
	case "elasticsearch":
		return probe.Settings{
			"url":         env("ELASTICSEARCH_URL"),
			"indexPrefix": env("ELASTICSEARCH_INDEX_PREFIX"),
			"tlsVerify":   env("ELASTICSEARCH_TLS_VERIFY"),
			"username":    env("ELASTICSEARCH_USERNAME"),
			"password":    env("ELASTICSEARCH_PASSWORD"),
		}
	default:
		return probe.Settings{}
	}
}

type clickhouseParts struct {
	scheme   string
	host     string
	port     string
	username string
	password string
}

func parseClickHouseURL(raw string) (clickhouseParts, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		if err == nil {
			err = errInvalidClickHouseURL
		}
		return clickhouseParts{}, err
	}
	parts := clickhouseParts{
		scheme: parsed.Scheme,
		host:   parsed.Hostname(),
		port:   parsed.Port(),
	}
	if parts.scheme == "" {
		parts.scheme = "http"
	}
	if parsed.User != nil {
		parts.username = parsed.User.Username()
		parts.password, _ = parsed.User.Password()
	}
	return parts, nil
}
