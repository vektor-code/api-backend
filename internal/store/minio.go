package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is the MinIO-backed trace storage engine
type Store struct {
	client     *minio.Client
	bucketName string

	// In-memory stats cache and recent trace index for fast dashboard rendering
	statsCache map[string]*models.ServiceStats
	statsMu    sync.RWMutex

	recentTraces map[string]*models.Trace
	tracesMu     sync.RWMutex

	// Pre-computed service map cache (rebuilt on syncState, served directly on API calls)
	serviceMapCache    map[string]*models.ServiceMapData // key = namespace ("" = all)
	serviceMapMu       sync.RWMutex
	serviceMapVersion  int64 // incremented when cache is invalidated

	// Local caches (collected by this pod instance only)
	localStats   map[string]*models.ServiceStats
	localTraces  map[string]*models.Trace
	localMu      sync.RWMutex
}

// New creates a new MinIO store
func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio init: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("make bucket: %w", err)
		}
	}

	s := &Store{
		client:           client,
		bucketName:       bucket,
		statsCache:       make(map[string]*models.ServiceStats),
		recentTraces:     make(map[string]*models.Trace),
		serviceMapCache:  make(map[string]*models.ServiceMapData),
		localStats:       make(map[string]*models.ServiceStats),
		localTraces:      make(map[string]*models.Trace),
	}

	go s.runGC()
	go s.runSync()
	go s.runMinioGC()
	return s, nil
}

// Close shuts down the store
func (s *Store) Close() error {
	return nil // MinIO client doesn't need explicit close
}

// getSpanObject retrieves a raw JSON span from MinIO and unmarshals it
func (s *Store) getSpanObject(ctx context.Context, key string) (*models.Span, error) {
	obj, err := s.client.GetObject(ctx, s.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}

	var span models.Span
	if err := json.Unmarshal(data, &span); err != nil {
		return nil, err
	}
	s.enrichSpanMetadata(&span)
	return &span, nil
}

// PodState represents the cache snapshot of a single replica pod
type PodState struct {
	Stats        map[string]*models.ServiceStats `json:"stats"`
	RecentTraces map[string]*models.Trace        `json:"recentTraces"`
	UpdatedAt    time.Time                       `json:"updatedAt"`
}

// TracePodInfo represents pod metadata extracted from ingested trace spans.
type TracePodInfo struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	NodeName    string            `json:"nodeName"`
	ServiceName string            `json:"serviceName"`
	Labels      map[string]string `json:"labels,omitempty"`
	LastSeen    time.Time         `json:"lastSeen"`
}
