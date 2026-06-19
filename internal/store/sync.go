package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
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
		s.localMu.Unlock()
	}
}

// runSync runs the background replication loop
func (s *Store) runSync() {
	// Sync immediately on startup
	s.syncState()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncState()
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

	for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    "state/",
		Recursive: false,
	}) {
		if obj.Err != nil {
			continue
		}

		// Delete state files that haven't been updated for over an hour (dead pods)
		if obj.LastModified.Before(time.Now().Add(-1 * time.Hour)) {
			_ = s.client.RemoveObject(ctx, s.bucketName, obj.Key, minio.RemoveObjectOptions{})
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
			if totalReq > 0 {
				existing.ErrorRate = float64(existing.ErrorCount) / float64(totalReq) * 100
			}
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

	// Hot-swap global caches
	s.statsMu.Lock()
	s.statsCache = newStatsCache
	s.statsMu.Unlock()

	s.tracesMu.Lock()
	s.recentTraces = newRecentTraces
	s.tracesMu.Unlock()
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

	cutoff := time.Now().Add(-12 * time.Hour)
	log.Printf("[GC] Starting MinIO log retention cleanup (traces older than 12h: %v)...", cutoff)

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
