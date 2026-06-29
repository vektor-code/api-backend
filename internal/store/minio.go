package store

import (
	"bytes"
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

	// Background upload queue to throttle MinIO writes and prevent CPU iowait
	uploadChan   chan uploadTask

	detectedClusters map[string]bool
	clustersMu       sync.RWMutex

	disabledNamespaces map[string]bool
	disabledNamespacesMu sync.RWMutex
}

type uploadTask struct {
	objectName  string
	data        []byte
	contentType string
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
		localStats:         make(map[string]*models.ServiceStats),
		localTraces:        make(map[string]*models.Trace),
		uploadChan:         make(chan uploadTask, 5000),
		detectedClusters:   make(map[string]bool),
		disabledNamespaces: make(map[string]bool),
	}

	_ = s.LoadDisabledNamespaces()

	// Start rate-limiting upload workers to throttle disk writes and lower CPU iowait
	for i := 0; i < 3; i++ {
		go s.uploadWorker()
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

// uploadWorker processes background S3 uploads at a throttled rate
func (s *Store) uploadWorker() {
	for task := range s.uploadChan {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = s.client.PutObject(ctx, s.bucketName, task.objectName, bytes.NewReader(task.data), int64(len(task.data)), minio.PutObjectOptions{
			ContentType: task.contentType,
		})
		cancel()
		// Sleep 15ms per object write to avoid local disk I/O bottlenecks and high CPU iowait
		time.Sleep(15 * time.Millisecond)
	}
}

func (s *Store) LoadDisabledNamespaces() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.bucketName, "config/disabled_namespaces.json", minio.GetObjectOptions{})
	if err != nil {
		s.disabledNamespacesMu.Lock()
		s.disabledNamespaces = make(map[string]bool)
		s.disabledNamespacesMu.Unlock()
		return nil
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}

	s.disabledNamespacesMu.Lock()
	s.disabledNamespaces = make(map[string]bool)
	for _, ns := range list {
		s.disabledNamespaces[ns] = true
	}
	s.disabledNamespacesMu.Unlock()
	return nil
}

func (s *Store) SaveDisabledNamespaces() error {
	s.disabledNamespacesMu.RLock()
	list := make([]string, 0, len(s.disabledNamespaces))
	for ns, disabled := range s.disabledNamespaces {
		if disabled {
			list = append(list, ns)
		}
	}
	s.disabledNamespacesMu.RUnlock()

	data, err := json.Marshal(list)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s.client.PutObject(ctx, s.bucketName, "config/disabled_namespaces.json", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func (s *Store) IsNamespaceDisabled(ns string) bool {
	s.disabledNamespacesMu.RLock()
	defer s.disabledNamespacesMu.RUnlock()
	return s.disabledNamespaces[ns]
}

func (s *Store) ToggleNamespace(ns string, disabled bool) error {
	s.disabledNamespacesMu.Lock()
	if s.disabledNamespaces == nil {
		s.disabledNamespaces = make(map[string]bool)
	}
	s.disabledNamespaces[ns] = disabled
	s.disabledNamespacesMu.Unlock()
	return s.SaveDisabledNamespaces()
}

func (s *Store) GetDisabledNamespaces() []string {
	s.disabledNamespacesMu.RLock()
	defer s.disabledNamespacesMu.RUnlock()
	list := make([]string, 0, len(s.disabledNamespaces))
	for ns, disabled := range s.disabledNamespaces {
		if disabled {
			list = append(list, ns)
		}
	}
	return list
}

func (s *Store) GetClusters() []string {
	s.clustersMu.RLock()
	defer s.clustersMu.RUnlock()
	clusters := make([]string, 0, len(s.detectedClusters))
	for c := range s.detectedClusters {
		clusters = append(clusters, c)
	}
	if len(clusters) == 0 {
		clusters = append(clusters, "default")
	}
	return clusters
}

