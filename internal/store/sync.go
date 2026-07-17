package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/minio/minio-go/v7"
)

// runGC cleans up old in-memory local traces to prevent OOM
func (s *Store) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.localMu.Lock()
		cutoff := time.Now().Add(-1 * time.Hour)
		for id, trace := range s.localTraces {
			if trace.StartTime.Before(cutoff) {
				delete(s.localTraces, id)
			}
		}
		for key, stat := range s.localStats {
			if stat.LastSeen.Before(cutoff) {
				delete(s.localStats, key)
			}
		}
		// Enforce maxTraces cap: if still over limit, evict oldest traces
		if len(s.localTraces) > s.maxTraces {
			// Collect trace IDs sorted by start time
			type traceEntry struct {
				id    string
				start time.Time
			}
			entries := make([]traceEntry, 0, len(s.localTraces))
			for id, t := range s.localTraces {
				entries = append(entries, traceEntry{id, t.StartTime})
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].start.Before(entries[j].start)
			})
			// Evict oldest entries until we're under the cap
			evictCount := len(entries) - s.maxTraces
			for i := 0; i < evictCount; i++ {
				delete(s.localTraces, entries[i].id)
			}
		}
		s.localMu.Unlock()
	}
}

// runSync runs the background replication loop
func (s *Store) runSync() {
	// Sync immediately on startup
	s.syncState()
	_ = s.LoadDisabledNamespaces()
	_ = s.LoadConfiguredNamespaces()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncState()
		_ = s.LoadDisabledNamespaces()
		_ = s.LoadConfiguredNamespaces()
	}
}

// syncState handles serialization of local state and merging from other active replicas
func (s *Store) syncState() {
	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Write this pod's local state to MinIO
	s.localMu.RLock()
	state := PodState{
		Stats:        make(map[string]*models.ServiceStats),
		RecentTraces: make(map[string]*models.Trace),
		UpdatedAt:    time.Now(),
	}
	for k, v := range s.localStats {
		state.Stats[k] = v
	}
	for k, v := range s.localTraces {
		state.RecentTraces[k] = v
	}
	s.localMu.RUnlock()

	data, err := json.Marshal(state)
	if err == nil {
		objectName := fmt.Sprintf("state/pod-%s.json", hn)
		_, _ = s.client.PutObject(ctx, s.bucketName, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
	}

	// 2. Scan and merge states from all active replicas
	newStatsCache := make(map[string]*models.ServiceStats)
	newRecentTraces := make(map[string]*models.Trace)

	ourKey := fmt.Sprintf("state/pod-%s.json", hn)
	var objects []minio.ObjectInfo
	var refTime time.Time

	// List objects once and locate our own object's LastModified time to use as reference clock
	for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    "state/",
		Recursive: false,
	}) {
		if obj.Err != nil {
			continue
		}
		objects = append(objects, obj)
		if obj.Key == ourKey {
			refTime = obj.LastModified
		}
	}

	if refTime.IsZero() {
		refTime = time.Now().UTC()
	}

	for _, obj := range objects {
		// Delete state files that haven't been updated for over an hour relative to the server reference clock
		if obj.LastModified.Before(refTime.Add(-1 * time.Hour)) {
			if obj.Key != ourKey {
				_ = s.client.RemoveObject(ctx, s.bucketName, obj.Key, minio.RemoveObjectOptions{})
			}
			continue
		}

		// Download and parse replica state
		objReader, err := s.client.GetObject(ctx, s.bucketName, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}

		var pState PodState
		dec := json.NewDecoder(objReader)
		err = dec.Decode(&pState)
		objReader.Close()
		if err != nil {
			continue
		}

		// Merge stats
		for k, svc := range pState.Stats {
			existing, ok := newStatsCache[k]
			if !ok {
				existing = &models.ServiceStats{
					ServiceName:      svc.ServiceName,
					Namespace:        svc.Namespace,
					IsInfrastructure: svc.IsInfrastructure,
					Language:         svc.Language,
				}
				newStatsCache[k] = existing
			}
			totalReq := existing.RequestCount + svc.RequestCount
			if totalReq > 0 {
				existing.P50Ms = (existing.P50Ms*float64(existing.RequestCount) + svc.P50Ms*float64(svc.RequestCount)) / float64(totalReq)
				existing.P95Ms = (existing.P95Ms*float64(existing.RequestCount) + svc.P95Ms*float64(svc.RequestCount)) / float64(totalReq)
				existing.P99Ms = (existing.P99Ms*float64(existing.RequestCount) + svc.P99Ms*float64(svc.RequestCount)) / float64(totalReq)
			}
			existing.RequestCount = totalReq
			existing.ErrorCount += svc.ErrorCount
			if svc.LastSeen.After(existing.LastSeen) {
				existing.LastSeen = svc.LastSeen
			}
			finalizeServiceStats(existing)
		}

		// Merge traces
		for k, t := range pState.RecentTraces {
			existing, ok := newRecentTraces[k]
			if !ok {
				newRecentTraces[k] = t
				continue
			}
			spanMap := make(map[string]*models.Span)
			for _, sp := range existing.Spans {
				spanMap[sp.SpanID] = sp
			}
			for _, sp := range t.Spans {
				spanMap[sp.SpanID] = sp
			}
			var combinedSpans []*models.Span
			for _, sp := range spanMap {
				combinedSpans = append(combinedSpans, sp)
			}
			newRecentTraces[k] = buildTrace(t.TraceID, combinedSpans)
		}
	}

	// Hot-swap global caches only if we successfully retrieved and parsed state files
	if len(objects) > 0 {
		// Enforce maxTraces cap on merged traces before swapping
		if len(newRecentTraces) > s.maxTraces {
			type traceEntry struct {
				id    string
				start time.Time
			}
			entries := make([]traceEntry, 0, len(newRecentTraces))
			for id, t := range newRecentTraces {
				entries = append(entries, traceEntry{id, t.StartTime})
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].start.Before(entries[j].start)
			})
			evictCount := len(entries) - s.maxTraces
			for i := 0; i < evictCount; i++ {
				delete(newRecentTraces, entries[i].id)
			}
		}

		s.statsMu.Lock()
		s.statsCache = newStatsCache
		s.statsMu.Unlock()

		s.tracesMu.Lock()
		s.recentTraces = newRecentTraces
		s.tracesMu.Unlock()

		// Invalidate pre-computed service maps so the next API call rebuilds them
		// from the fresh statsCache + recentTraces data.
		s.InvalidateServiceMapCache()

		// Rebuild pre-computed namespace stats so /api/stats returns instantly
		s.rebuildPrecomputedStats()
	}
}

// runMinioGC periodically deletes traces from MinIO that are older than 12 hours
func (s *Store) runMinioGC() {
	// Wait a bit after startup
	time.Sleep(15 * time.Second)
	s.cleanOldMinioTraces()

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.cleanOldMinioTraces()
	}
}

func (s *Store) cleanOldMinioTraces() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	hours := s.GetRetentionHours()
	if hours <= 0 {
		// 0 = keep forever: never GC legacy MinIO trace objects.
		return
	}
	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)
	log.Printf("[GC] Starting MinIO log retention cleanup (traces older than %dh: %v)...", hours, cutoff)

	objectsCh := make(chan minio.ObjectInfo, 100)
	var count int64

	// Send objects to delete to objectsCh
	go func() {
		defer close(objectsCh)
		for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
			Prefix:    "traces/",
			Recursive: true,
		}) {
			if obj.Err != nil {
				continue
			}
			if obj.LastModified.Before(cutoff) {
				objectsCh <- obj
				count++
			}
		}
	}()

	// RemoveObjects returns a channel of errors. We must consume it to execute the deletion.
	errorCh := s.client.RemoveObjects(ctx, s.bucketName, objectsCh, minio.RemoveObjectsOptions{})
	for err := range errorCh {
		if err.Err != nil {
			log.Printf("[GC] Error removing object %s: %v", err.ObjectName, err.Err)
		}
	}

	log.Printf("[GC] MinIO log retention cleanup finished. Removed %d objects.", count)
}

// DeleteAllMinioTraces removes all traces under traces/ prefix from MinIO bucket
// and clears in-memory caches.
func (s *Store) DeleteAllMinioTraces() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	objectsCh := make(chan minio.ObjectInfo, 100)
	var count int64

	go func() {
		defer close(objectsCh)
		for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
			Prefix:    "traces/",
			Recursive: true,
		}) {
			if obj.Err != nil {
				continue
			}
			objectsCh <- obj
			count++
		}
	}()

	errorCh := s.client.RemoveObjects(ctx, s.bucketName, objectsCh, minio.RemoveObjectsOptions{})
	for err := range errorCh {
		if err.Err != nil {
			log.Printf("[GC] Error removing object %s during manual delete: %v", err.ObjectName, err.Err)
		}
	}

	// Also clear memory caches so dashboard updates instantly
	s.statsMu.Lock()
	s.statsCache = make(map[string]*models.ServiceStats)
	s.statsMu.Unlock()

	s.tracesMu.Lock()
	s.recentTraces = make(map[string]*models.Trace)
	s.tracesMu.Unlock()

	s.localMu.Lock()
	s.localStats = make(map[string]*models.ServiceStats)
	s.localTraces = make(map[string]*models.Trace)
	s.localMu.Unlock()

	s.InvalidateServiceMapCache()

	// Clear ClickHouse spans
	clickhouseURL := s.GetInfraConfig("CLICKHOUSE_URL", os.Getenv("CLICKHOUSE_URL"))
	if clickhouseURL != "" {
		chQuery := "TRUNCATE TABLE kubetrace.spans;"
		chReq, err := http.NewRequestWithContext(ctx, "POST", clickhouseURL, strings.NewReader(chQuery))
		if err == nil {
			if chReq.URL.User != nil {
				pass, _ := chReq.URL.User.Password()
				chReq.SetBasicAuth(chReq.URL.User.Username(), pass)
			}
			chClient := &http.Client{Timeout: 10 * time.Second}
			chResp, err := chClient.Do(chReq)
			if err == nil {
				if chResp.StatusCode == http.StatusOK {
					log.Printf("[Sync] Successfully truncated ClickHouse spans table.")
				} else {
					body, _ := io.ReadAll(chResp.Body)
					log.Printf("[Sync] ClickHouse truncation failed (status %d): %s", chResp.StatusCode, string(body))
				}
				chResp.Body.Close()
			} else {
				log.Printf("[Sync] Error executing ClickHouse truncation request: %v", err)
			}
		}
	}

	return count, nil
}
