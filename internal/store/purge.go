package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/minio/minio-go/v7"
)

// PurgeAllTraces deletes live trace storage: ClickHouse spans first, then any
// leftover MinIO objects under traces/. The count is ClickHouse rows (plus
// MinIO objects if any remain). Control-plane tables are not touched.
func (s *Store) PurgeAllTraces() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	var deleted int64
	if s.chURL != "" {
		n, err := s.purgeClickHouseSpans(ctx)
		if err != nil {
			return 0, err
		}
		deleted += n
	}

	objects, err := s.deleteMinioTraceObjects(ctx)
	if err != nil {
		if deleted > 0 {
			log.Printf("[purge] removed %d ClickHouse spans but MinIO cleanup failed: %v", deleted, err)
		} else {
			return 0, fmt.Errorf("remove minio traces: %w", err)
		}
	} else {
		deleted += objects
	}

	s.clearTraceCaches()
	log.Printf("[purge] deleted %d spans", deleted)
	return deleted, nil
}

// DeleteAllMinioTraces is the historical name for PurgeAllTraces.
func (s *Store) DeleteAllMinioTraces() (int64, error) {
	return s.PurgeAllTraces()
}

func (s *Store) purgeClickHouseSpans(ctx context.Context) (int64, error) {
	rows, err := s.chQuery(ctx, "SELECT count() AS c FROM kubetrace.spans")
	if err != nil {
		return 0, fmt.Errorf("count spans: %w", err)
	}
	var n int64
	if len(rows) > 0 {
		n = chInt(rows[0]["c"])
	}
	if err := s.chExec(ctx, "TRUNCATE TABLE IF EXISTS kubetrace.spans"); err != nil {
		return 0, fmt.Errorf("truncate spans: %w", err)
	}
	return n, nil
}

func (s *Store) deleteMinioTraceObjects(ctx context.Context) (int64, error) {
	if s.client == nil {
		return 0, nil
	}

	objectsCh := make(chan minio.ObjectInfo, 100)
	var count int64
	var listErr error
	done := make(chan struct{})
	go func() {
		defer close(objectsCh)
		defer close(done)
		for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
			Prefix:    "traces/",
			Recursive: true,
		}) {
			if obj.Err != nil {
				listErr = obj.Err
				continue
			}
			objectsCh <- obj
			count++
		}
	}()

	errorCh := s.client.RemoveObjects(ctx, s.bucketName, objectsCh, minio.RemoveObjectsOptions{})
	for err := range errorCh {
		if err.Err != nil {
			log.Printf("[purge] error removing object %s: %v", err.ObjectName, err.Err)
		}
	}
	<-done
	return count, listErr
}

func (s *Store) clearTraceCaches() {
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
	s.rebuildPrecomputedStats()
}
