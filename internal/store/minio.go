package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
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

	// Postgres opt-in cache: namespace -> explicitly_enabled, refreshed by
	// LoadDisabledNamespaces. Guarded by disabledNamespacesMu.
	explicitlyEnabledNs map[string]bool

	configuredNamespaces map[string]bool
	configuredNamespacesMu sync.RWMutex

	reportedPods     map[string][]ReportedPod
	reportedPodsMu   sync.RWMutex

	db              *sql.DB
	postgresEnabled bool

	retentionHours  int
	retentionMu     sync.RWMutex

	maxTraces int // max number of traces to keep in memory

	precomputedStats   []*models.NamespaceStats
	precomputedStatsMu sync.RWMutex

	agentClusters *agentClusterRegistry

	// ClickHouse mode: reads served by ClickHouse, writes streamed to Kafka.
	// Replaces the per-span MinIO uploads and the MinIO state-file gossip.
	chMode     bool
	chURL      string
	chHotHours int // hours spans stay on the fast disk before moving to MinIO cold tier
	producer   *spanProducer
}

const (
	// defaultHotHours is how long spans stay on ClickHouse's fast disk before
	// their parts are moved to the MinIO-backed cold volume.
	defaultHotHours = 48
	// defaultRetentionHours is the delete horizon. 0 means keep forever: aged
	// parts live on MinIO indefinitely and stay queryable through ClickHouse.
	defaultRetentionHours = 0
)

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
		uploadChan:         make(chan uploadTask, 50000),
		detectedClusters:   make(map[string]bool),
		disabledNamespaces: make(map[string]bool),
		configuredNamespaces: make(map[string]bool),
		retentionHours:       defaultRetentionHours,
		maxTraces:            5000,
	}

	_ = s.LoadDisabledNamespaces()
	_ = s.LoadConfiguredNamespaces()
	_ = s.LoadRetentionConfig()

	s.chURL = os.Getenv("CLICKHOUSE_URL")
	s.chMode = s.chURL != ""
	s.chHotHours = getEnvInt("CLICKHOUSE_HOT_HOURS", defaultHotHours)

	if s.chMode {
		// ClickHouse mode: spans go to Kafka -> ingestor -> ClickHouse; reads
		// query ClickHouse. No per-span MinIO uploads, no state gossip, no
		// unbounded in-memory trace maps. Old parts tier down to MinIO via the
		// ClickHouse 'tiered' storage policy, so nothing disappears on reload.
		s.producer = newSpanProducer(os.Getenv("KAFKA_BROKERS"), getEnvDefault("KAFKA_TOPIC", "kubetrace-spans"))
		if s.producer == nil {
			log.Printf("[store] WARNING: ClickHouse mode is on but KAFKA_BROKERS is not set — spans will not be persisted")
		}
		log.Printf("[store] ClickHouse mode enabled (reads: ClickHouse, writes: Kafka)")
		// Reconcile the spans TTL with the admin-configured retention so the
		// dashboard setting is the source of truth (overrides the ingestor's
		// startup default). Best-effort: logs and continues on failure.
		if err := s.ApplyClickHouseTTL(s.GetRetentionHours()); err != nil {
			log.Printf("[store] WARNING: could not reconcile ClickHouse retention TTL: %v", err)
		}
		go s.runClickHouseRefresh()
		go s.runMinioGC() // still cleans up legacy traces/ objects
		return s, nil
	}

	// Legacy MinIO mode: per-span uploads + state-file replication.
	for i := 0; i < 16; i++ {
		go s.uploadWorker()
	}

	go s.runGC()
	go s.runSync()
	go s.runMinioGC()
	return s, nil
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return def
}

// Close shuts down the store
func (s *Store) Close() error {
	if s.postgresEnabled && s.db != nil {
		return s.db.Close()
	}
	return nil
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
		
		// Adaptive throttling: if queue is backed up, skip sleep to avoid drops
		if len(s.uploadChan) < 1000 {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (s *Store) LoadDisabledNamespaces() error {
	// Postgres mode: refresh the in-memory opt-in cache so hot paths
	// (per-span loops, OTLP ingest) never query the database directly.
	if s.postgresEnabled {
		rows, err := s.db.Query("SELECT namespace, explicitly_enabled FROM disabled_namespaces")
		if err != nil {
			return err
		}
		defer rows.Close()

		enabled := make(map[string]bool)
		for rows.Next() {
			var ns string
			var explicitlyEnabled bool
			if err := rows.Scan(&ns, &explicitlyEnabled); err == nil {
				enabled[ns] = explicitlyEnabled
			}
		}

		s.disabledNamespacesMu.Lock()
		s.explicitlyEnabledNs = enabled
		s.disabledNamespacesMu.Unlock()
		return nil
	}

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
	return !s.IsNamespaceExplicitlyEnabled(ns)
}

// IsNamespaceExplicitlyEnabled returns true only when an admin has enabled a namespace (opt-in model).
func (s *Store) IsNamespaceExplicitlyEnabled(ns string) bool {
	if s.postgresEnabled {
		s.disabledNamespacesMu.RLock()
		cache := s.explicitlyEnabledNs
		s.disabledNamespacesMu.RUnlock()

		// Cache not warmed yet (startup window before the first refresh):
		// fall back to a direct lookup once.
		if cache == nil {
			var explicitlyEnabled bool
			err := s.db.QueryRow("SELECT explicitly_enabled FROM disabled_namespaces WHERE namespace = $1", ns).Scan(&explicitlyEnabled)
			if err != nil {
				return false
			}
			return explicitlyEnabled
		}
		return cache[ns]
	}

	s.disabledNamespacesMu.RLock()
	defer s.disabledNamespacesMu.RUnlock()
	if s.disabledNamespaces == nil {
		return false
	}
	disabled, exists := s.disabledNamespaces[ns]
	return exists && !disabled
}

// GetExplicitlyEnabledNamespaces returns namespaces that an admin has explicitly enabled.
func (s *Store) GetExplicitlyEnabledNamespaces() []string {
	if s.postgresEnabled {
		rows, err := s.db.Query("SELECT namespace FROM disabled_namespaces WHERE explicitly_enabled = true")
		if err != nil {
			return []string{}
		}
		defer rows.Close()
		var list []string
		for rows.Next() {
			var ns string
			if err := rows.Scan(&ns); err == nil {
				list = append(list, ns)
			}
		}
		return list
	}

	s.disabledNamespacesMu.RLock()
	defer s.disabledNamespacesMu.RUnlock()
	var list []string
	for ns, disabled := range s.disabledNamespaces {
		if !disabled {
			list = append(list, ns)
		}
	}
	return list
}

func (s *Store) ToggleNamespace(ns string, disabled bool) error {
	if s.postgresEnabled {
		explicitlyEnabled := !disabled
		_, err := s.db.Exec(`
			INSERT INTO disabled_namespaces (namespace, disabled, explicitly_enabled, updated_at)
			VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
			ON CONFLICT (namespace) DO UPDATE SET disabled = $2, explicitly_enabled = $3, updated_at = CURRENT_TIMESTAMP
		`, ns, disabled, explicitlyEnabled)
		if err == nil {
			// Keep the opt-in cache immediately consistent with the toggle.
			s.disabledNamespacesMu.Lock()
			if s.explicitlyEnabledNs == nil {
				s.explicitlyEnabledNs = make(map[string]bool)
			}
			s.explicitlyEnabledNs[ns] = explicitlyEnabled
			s.disabledNamespacesMu.Unlock()
		}
		return err
	}

	s.disabledNamespacesMu.Lock()
	if s.disabledNamespaces == nil {
		s.disabledNamespaces = make(map[string]bool)
	}
	s.disabledNamespaces[ns] = disabled
	s.disabledNamespacesMu.Unlock()
	return s.SaveDisabledNamespaces()
}

func (s *Store) GetDisabledNamespaces() []string {
	if s.postgresEnabled {
		rows, err := s.db.Query("SELECT namespace FROM disabled_namespaces WHERE disabled = true")
		if err != nil {
			return []string{}
		}
		defer rows.Close()

		var list []string
		for rows.Next() {
			var ns string
			if err := rows.Scan(&ns); err == nil {
				list = append(list, ns)
			}
		}
		return list
	}

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

type ClusterInventoryItem struct {
	ID             string `json:"id"`
	DisplayName    string `json:"displayName"`
	Token          string `json:"token"`
	Status         string `json:"status"`
	CredentialType string `json:"credentialType"` // kubeconfig | bearer
	APIServer      string `json:"apiServer"`
	AgentNamespace string `json:"agentNamespace"`
	ManagedByAgent bool   `json:"managedByAgent"`
}

func (s *Store) GetClusterInventory() ([]ClusterInventoryItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.postgresEnabled {
		_, _ = s.db.Exec(`
			CREATE TABLE IF NOT EXISTS cluster_inventory (
				id VARCHAR(255) PRIMARY KEY,
				display_name VARCHAR(255) NOT NULL,
				token TEXT NOT NULL,
				status VARCHAR(50) NOT NULL,
				credential_type VARCHAR(50) DEFAULT 'kubeconfig',
				api_server VARCHAR(512) DEFAULT '',
				agent_namespace VARCHAR(255) DEFAULT 'trace-prod',
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		`)
		_, _ = s.db.Exec(`ALTER TABLE cluster_inventory ADD COLUMN IF NOT EXISTS credential_type VARCHAR(50) DEFAULT 'kubeconfig'`)
		_, _ = s.db.Exec(`ALTER TABLE cluster_inventory ADD COLUMN IF NOT EXISTS api_server VARCHAR(512) DEFAULT ''`)
		_, _ = s.db.Exec(`ALTER TABLE cluster_inventory ADD COLUMN IF NOT EXISTS agent_namespace VARCHAR(255) DEFAULT 'trace-prod'`)

		rows, err := s.db.Query("SELECT id, display_name, token, status, COALESCE(credential_type,'kubeconfig'), COALESCE(api_server,''), COALESCE(agent_namespace,'trace-prod') FROM cluster_inventory")
		if err == nil {
			defer rows.Close()
			var list []ClusterInventoryItem
			for rows.Next() {
				var item ClusterInventoryItem
				if err := rows.Scan(&item.ID, &item.DisplayName, &item.Token, &item.Status, &item.CredentialType, &item.APIServer, &item.AgentNamespace); err == nil {
					list = append(list, item)
				}
			}
			return list, nil
		}
	}

	obj, err := s.client.GetObject(ctx, s.bucketName, "config/clusters_inventory.json", minio.GetObjectOptions{})
	if err != nil {
		return []ClusterInventoryItem{}, nil
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return []ClusterInventoryItem{}, nil
	}

	var list []ClusterInventoryItem
	if err := json.Unmarshal(data, &list); err != nil {
		return []ClusterInventoryItem{}, nil
	}
	return list, nil
}

func (s *Store) SaveClusterInventory(list []ClusterInventoryItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.postgresEnabled {
		// First delete any items not in the list to support cluster deletion
		var ids []string
		for _, item := range list {
			ids = append(ids, item.ID)
		}
		if len(ids) > 0 {
			placeholders := make([]string, len(ids))
			args := make([]interface{}, len(ids))
			for i, id := range ids {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
				args[i] = id
			}
			query := fmt.Sprintf("DELETE FROM cluster_inventory WHERE id NOT IN (%s)", strings.Join(placeholders, ", "))
			_, _ = s.db.Exec(query, args...)
		} else {
			_, _ = s.db.Exec("DELETE FROM cluster_inventory")
		}

		for _, item := range list {
			_, err := s.db.Exec(`
				INSERT INTO cluster_inventory (id, display_name, token, status, credential_type, api_server, agent_namespace, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
				ON CONFLICT (id) DO UPDATE SET display_name = $2, token = $3, status = $4, credential_type = $5, api_server = $6, agent_namespace = $7, updated_at = CURRENT_TIMESTAMP
			`, item.ID, item.DisplayName, item.Token, item.Status, defaultCredentialType(item.CredentialType), item.APIServer, defaultAgentNamespace(item.AgentNamespace))
			if err != nil {
				return err
			}
		}
		return nil
	}

	data, err := json.Marshal(list)
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(ctx, s.bucketName, "config/clusters_inventory.json", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func (s *Store) GetClusters() []string {
	s.clustersMu.RLock()
	defer s.clustersMu.RUnlock()
	clustersMap := make(map[string]bool)
	for c := range s.detectedClusters {
		clustersMap[c] = true
	}
	if inv, err := s.GetClusterInventory(); err == nil {
		for _, item := range inv {
			clustersMap[item.ID] = true
		}
	}
	clusters := make([]string, 0, len(clustersMap))
	for c := range clustersMap {
		clusters = append(clusters, c)
	}
	if len(clusters) == 0 {
		for _, agent := range s.GetAgentManagedClusters() {
			clusters = append(clusters, agent.ClusterID)
		}
	}
	return clusters
}

func (s *Store) LoadConfiguredNamespaces() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.bucketName, "config/namespaces_list.json", minio.GetObjectOptions{})
	if err != nil {
		s.configuredNamespacesMu.Lock()
		s.configuredNamespaces = make(map[string]bool)
		s.configuredNamespacesMu.Unlock()
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

	s.configuredNamespacesMu.Lock()
	s.configuredNamespaces = make(map[string]bool)
	for _, ns := range list {
		s.configuredNamespaces[ns] = true
	}
	s.configuredNamespacesMu.Unlock()
	return nil
}

func (s *Store) SaveConfiguredNamespaces() error {
	s.configuredNamespacesMu.RLock()
	list := make([]string, 0, len(s.configuredNamespaces))
	for ns := range s.configuredNamespaces {
		list = append(list, ns)
	}
	s.configuredNamespacesMu.RUnlock()

	data, err := json.Marshal(list)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s.client.PutObject(ctx, s.bucketName, "config/namespaces_list.json", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func (s *Store) AddConfiguredNamespace(ns string) error {
	if s.postgresEnabled {
		_, err := s.db.Exec(`
			INSERT INTO configured_namespaces (namespace, configured, updated_at)
			VALUES ($1, true, CURRENT_TIMESTAMP)
			ON CONFLICT (namespace) DO NOTHING
		`, ns)
		return err
	}

	s.configuredNamespacesMu.Lock()
	if s.configuredNamespaces == nil {
		s.configuredNamespaces = make(map[string]bool)
	}
	s.configuredNamespaces[ns] = true
	s.configuredNamespacesMu.Unlock()
	return s.SaveConfiguredNamespaces()
}

func (s *Store) DeleteConfiguredNamespace(ns string) error {
	if s.postgresEnabled {
		_, err := s.db.Exec("DELETE FROM configured_namespaces WHERE namespace = $1", ns)
		if err != nil {
			return err
		}
		_, _ = s.db.Exec("DELETE FROM disabled_namespaces WHERE namespace = $1", ns)
		return nil
	}

	s.configuredNamespacesMu.Lock()
	delete(s.configuredNamespaces, ns)
	s.configuredNamespacesMu.Unlock()
	
	// Also remove it from disabled list to clean up
	s.disabledNamespacesMu.Lock()
	delete(s.disabledNamespaces, ns)
	s.disabledNamespacesMu.Unlock()
	_ = s.SaveDisabledNamespaces()

	return s.SaveConfiguredNamespaces()
}

func (s *Store) GetConfiguredNamespaces() []string {
	if s.postgresEnabled {
		rows, err := s.db.Query("SELECT namespace FROM configured_namespaces")
		if err != nil {
			return []string{}
		}
		defer rows.Close()

		var list []string
		for rows.Next() {
			var ns string
			if err := rows.Scan(&ns); err == nil {
				list = append(list, ns)
			}
		}
		return list
	}

	s.configuredNamespacesMu.RLock()
	defer s.configuredNamespacesMu.RUnlock()
	list := make([]string, 0, len(s.configuredNamespaces))
	for ns := range s.configuredNamespaces {
		list = append(list, ns)
	}
	return list
}

// Retention is the delete horizon in hours. 0 means "keep forever" — aged parts
// are tiered to MinIO by ClickHouse and never deleted.

func (s *Store) LoadRetentionConfig() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.bucketName, "config/retention_hours.json", minio.GetObjectOptions{})
	if err != nil {
		s.retentionMu.Lock()
		s.retentionHours = defaultRetentionHours
		s.retentionMu.Unlock()
		return nil
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	var hours int
	if err := json.Unmarshal(data, &hours); err != nil {
		return err
	}

	if hours < 0 {
		hours = defaultRetentionHours
	}

	s.retentionMu.Lock()
	s.retentionHours = hours
	s.retentionMu.Unlock()
	return nil
}

func (s *Store) SaveRetentionConfig(hours int) error {
	if hours < 0 {
		hours = defaultRetentionHours
	}

	data, err := json.Marshal(hours)
	if err != nil {
		return err
	}

	s.retentionMu.Lock()
	s.retentionHours = hours
	s.retentionMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.client.PutObject(ctx, s.bucketName, "config/retention_hours.json", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	}); err != nil {
		return err
	}

	// In ClickHouse mode this is the real retention control: reconcile the
	// spans-table TTL so the change takes effect immediately.
	if s.chMode {
		if err := s.ApplyClickHouseTTL(hours); err != nil {
			return fmt.Errorf("apply retention to clickhouse: %w", err)
		}
	}
	return nil
}

func (s *Store) GetRetentionHours() int {
	s.retentionMu.RLock()
	defer s.retentionMu.RUnlock()
	if s.retentionHours < 0 {
		return defaultRetentionHours
	}
	return s.retentionHours
}

func defaultCredentialType(t string) string {
	if t == "" {
		return "kubeconfig"
	}
	return t
}

func defaultAgentNamespace(ns string) string {
	if ns == "" {
		return "trace-prod"
	}
	return ns
}

// GetClusterByID returns a single cluster inventory item by ID.
func (s *Store) GetClusterByID(id string) (*ClusterInventoryItem, error) {
	list, err := s.GetClusterInventory()
	if err != nil {
		return nil, err
	}
	for _, item := range list {
		if item.ID == id {
			copy := item
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("cluster not found: %s", id)
}

// EnsureAgentClusterInInventory auto-registers a cluster detected via agent-backend heartbeat.
func (s *Store) EnsureAgentClusterInInventory(clusterID, agentNamespace string) error {
	if clusterID == "" {
		clusterID = "default"
	}
	list, err := s.GetClusterInventory()
	if err != nil {
		return err
	}
	for i, item := range list {
		if item.ID == clusterID {
			changed := false
			if item.AgentNamespace != agentNamespace && agentNamespace != "" {
				list[i].AgentNamespace = agentNamespace
				changed = true
			}
			if !item.ManagedByAgent {
				list[i].ManagedByAgent = true
				changed = true
			}
			if item.Status == "" {
				list[i].Status = "Active"
				changed = true
			}
			if changed {
				return s.pruneStaleClusterInventory(list)
			}
			return s.pruneStaleClusterInventory(list)
		}
	}
	list = append(list, ClusterInventoryItem{
		ID:             clusterID,
		DisplayName:    clusterID,
		Token:          "",
		Status:         "Active",
		CredentialType: "agent",
		AgentNamespace: agentNamespace,
		ManagedByAgent: true,
	})
	return s.pruneStaleClusterInventory(list)
}

// pruneStaleClusterInventory removes orphan placeholder clusters when a real agent cluster exists.
func (s *Store) pruneStaleClusterInventory(list []ClusterInventoryItem) error {
	hasAgent := false
	for _, item := range list {
		if item.ManagedByAgent || item.CredentialType == "agent" {
			hasAgent = true
			break
		}
	}
	if !hasAgent {
		return s.SaveClusterInventory(list)
	}
	filtered := make([]ClusterInventoryItem, 0, len(list))
	for _, item := range list {
		if item.ID == "default" && !item.ManagedByAgent && item.Token == "" {
			continue
		}
		filtered = append(filtered, item)
	}
	return s.SaveClusterInventory(filtered)
}

