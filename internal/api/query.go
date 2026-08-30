package api

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/models"
)

func parseSearchQuery(c *fiber.Ctx, defaultLimit int) *models.SearchQuery {
	q := &models.SearchQuery{
		Namespace:      c.Query("namespace"),
		Cluster:        c.Query("cluster"),
		ServiceName:    c.Query("service"),
		Operation:      c.Query("operation"),
		TraceID:        c.Query("traceId"),
		HttpMethod:     strings.ToUpper(strings.TrimSpace(c.Query("httpMethod"))),
		Limit:          c.QueryInt("limit", defaultLimit),
		Offset:         c.QueryInt("offset", 0),
		ExcludeProbes:  c.Query("excludeProbes") != "false",
		ExcludeStreams: c.Query("excludeStreams") != "false",
	}

	switch c.Query("hasError") {
	case "true":
		t := true
		q.HasError = &t
	case "false":
		f := false
		q.HasError = &f
	}

	switch c.Query("hasBody") {
	case "true":
		t := true
		q.HasBody = &t
	case "false":
		f := false
		q.HasBody = &f
	}

	if minSpans := c.QueryInt("minSpans", 0); minSpans > 0 {
		q.MinSpans = minSpans
	}
	if minMs := c.QueryFloat("minDuration", 0); minMs > 0 {
		q.MinDurationMs = minMs
	}
	if maxMs := c.QueryFloat("maxDuration", 0); maxMs > 0 {
		q.MaxDurationMs = maxMs
	}
	if startStr := c.Query("startTime"); startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			q.StartTime = t
		}
	}
	if endStr := c.Query("endTime"); endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			q.EndTime = t
		}
	}

	return q
}
