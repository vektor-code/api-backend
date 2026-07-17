package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// WorkloadInstrumentation tracks per-application instrumentation preferences.
type WorkloadInstrumentation struct {
	ClusterID      string `json:"clusterId"`
	Namespace      string `json:"namespace"`
	WorkloadName   string `json:"workloadName"`
	WorkloadKind   string `json:"workloadKind"`
	Enabled        bool   `json:"enabled"`
	Language       string `json:"language"`
	ManualOverride bool   `json:"manualOverride"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
}

func workloadKey(clusterID, namespace, kind, name string) string {
	return clusterID + "/" + namespace + "/" + kind + "/" + name
}

func (s *Store) ensureWorkloadInstrumentationTable() {
	if !s.postgresEnabled || s.db == nil {
		return
	}
	_, _ = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS workload_instrumentation (
			cluster_id VARCHAR(255) NOT NULL,
			namespace VARCHAR(255) NOT NULL,
			workload_name VARCHAR(255) NOT NULL,
			workload_kind VARCHAR(50) NOT NULL,
			enabled BOOLEAN DEFAULT false,
			language VARCHAR(50),
			manual_override BOOLEAN DEFAULT false,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (cluster_id, namespace, workload_kind, workload_name)
		)
	`)
}

// GetWorkloadInstrumentations returns all stored workload instrumentation configs, optionally filtered by cluster.
func (s *Store) GetWorkloadInstrumentations(clusterID string) ([]WorkloadInstrumentation, error) {
	s.ensureWorkloadInstrumentationTable()
	if s.postgresEnabled && s.db != nil {
		query := `SELECT cluster_id, namespace, workload_name, workload_kind, enabled, language, manual_override, updated_at FROM workload_instrumentation`
		args := []interface{}{}
		if clusterID != "" {
			query += " WHERE cluster_id = $1"
			args = append(args, clusterID)
		}
		rows, err := s.db.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var list []WorkloadInstrumentation
		for rows.Next() {
			var item WorkloadInstrumentation
			var updatedAt time.Time
			if err := rows.Scan(&item.ClusterID, &item.Namespace, &item.WorkloadName, &item.WorkloadKind, &item.Enabled, &item.Language, &item.ManualOverride, &updatedAt); err == nil {
				item.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
				list = append(list, item)
			}
		}
		return list, nil
	}
	return s.getWorkloadInstrumentationsFromMinIO(clusterID)
}

func (s *Store) getWorkloadInstrumentationsFromMinIO(clusterID string) ([]WorkloadInstrumentation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obj, err := s.client.GetObject(ctx, s.bucketName, "config/workload_instrumentation.json", minio.GetObjectOptions{})
	if err != nil {
		return []WorkloadInstrumentation{}, nil
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return []WorkloadInstrumentation{}, nil
	}
	var list []WorkloadInstrumentation
	if err := json.Unmarshal(data, &list); err != nil {
		return []WorkloadInstrumentation{}, nil
	}
	if clusterID == "" {
		return list, nil
	}
	filtered := make([]WorkloadInstrumentation, 0)
	for _, item := range list {
		if item.ClusterID == clusterID {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

// SaveWorkloadInstrumentation upserts a workload instrumentation config.
func (s *Store) SaveWorkloadInstrumentation(item WorkloadInstrumentation) error {
	s.ensureWorkloadInstrumentationTable()
	if s.postgresEnabled && s.db != nil {
		_, err := s.db.Exec(`
			INSERT INTO workload_instrumentation (cluster_id, namespace, workload_name, workload_kind, enabled, language, manual_override, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
			ON CONFLICT (cluster_id, namespace, workload_kind, workload_name) DO UPDATE SET
				enabled = EXCLUDED.enabled,
				language = EXCLUDED.language,
				manual_override = EXCLUDED.manual_override,
				updated_at = CURRENT_TIMESTAMP
		`, item.ClusterID, item.Namespace, item.WorkloadName, item.WorkloadKind, item.Enabled, item.Language, item.ManualOverride)
		return err
	}
	list, _ := s.getWorkloadInstrumentationsFromMinIO("")
	key := workloadKey(item.ClusterID, item.Namespace, item.WorkloadKind, item.WorkloadName)
	found := false
	for i, existing := range list {
		if workloadKey(existing.ClusterID, existing.Namespace, existing.WorkloadKind, existing.WorkloadName) == key {
			list[i] = item
			found = true
			break
		}
	}
	if !found {
		list = append(list, item)
	}
	return s.saveWorkloadInstrumentationsToMinIO(list)
}

func (s *Store) saveWorkloadInstrumentationsToMinIO(list []WorkloadInstrumentation) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := json.Marshal(list)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, s.bucketName, "config/workload_instrumentation.json", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

// IsWorkloadInstrumentationEnabled checks if instrumentation is enabled for a workload.
func (s *Store) IsWorkloadInstrumentationEnabled(clusterID, namespace, kind, name string) (bool, bool) {
	list, err := s.GetWorkloadInstrumentations(clusterID)
	if err != nil {
		return false, false
	}
	for _, item := range list {
		if item.Namespace == namespace && item.WorkloadKind == kind && item.WorkloadName == name {
			return item.Enabled, true
		}
	}
	return false, false
}

// Cluster-scoped reported pods helpers.

func (s *Store) SetReportedPodsForCluster(clusterID, ns string, pods []ReportedPod) {
	s.reportedPodsMu.Lock()
	defer s.reportedPodsMu.Unlock()
	if s.reportedPods == nil {
		s.reportedPods = make(map[string][]ReportedPod)
	}
	key := clusterID + "/" + ns
	if clusterID == "" {
		key = ns
	}
	s.reportedPods[key] = pods
}

func (s *Store) GetReportedPodsForCluster(clusterID, ns string) []ReportedPod {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return nil
	}
	key := clusterID + "/" + ns
	if clusterID == "" {
		key = ns
	}
	if ns == "" && clusterID != "" {
		var all []ReportedPod
		prefix := clusterID + "/"
		for k, pods := range s.reportedPods {
			if len(k) > len(prefix) && k[:len(prefix)] == prefix {
				all = append(all, pods...)
			}
		}
		return all
	}
	return s.reportedPods[key]
}

func (s *Store) GetReportedNamespacesForCluster(clusterID string) []string {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return nil
	}
	prefix := clusterID + "/"
	seen := make(map[string]bool)
	for key, pods := range s.reportedPods {
		if len(pods) == 0 {
			continue
		}
		if clusterID == "" {
			seen[key] = true
			continue
		}
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			ns := key[len(prefix):]
			seen[ns] = true
		}
	}
	result := make([]string, 0, len(seen))
	for ns := range seen {
		result = append(result, ns)
	}
	return result
}

func (s *Store) IsNamespaceDisabledForCluster(clusterID, namespace string) bool {
	return s.IsNamespaceDisabled(namespace)
}

func (s *Store) GetDisabledNamespacesForCluster(clusterID string) []string {
	all := s.GetDisabledNamespaces()
	if clusterID == "" {
		return all
	}
	prefix := clusterID + ":"
	var result []string
	for _, ns := range all {
		if len(ns) > len(prefix) && ns[:len(prefix)] == prefix {
			result = append(result, ns[len(prefix):])
		} else if !containsColon(ns) {
			result = append(result, ns)
		}
	}
	return result
}

func containsColon(s string) bool {
	return strings.Contains(s, ":")
}

// GetApplicationConfigMap returns a map of namespace:serviceName -> WorkloadInstrumentation config.
func (s *Store) GetApplicationConfigMap() map[string]WorkloadInstrumentation {
	m := make(map[string]WorkloadInstrumentation)
	list, err := s.GetWorkloadInstrumentations("")
	if err == nil {
		for _, item := range list {
			m[item.Namespace+":"+item.WorkloadName] = item
		}
	}
	return m
}

// GetApplicationActivationMap returns a preloaded map of namespace:serviceName -> enabled.
func (s *Store) GetApplicationActivationMap() map[string]bool {
	m := make(map[string]bool)

	// Baseline from reported pods
	s.reportedPodsMu.RLock()
	for _, pods := range s.reportedPods {
		for _, p := range pods {
			svcName := p.ServiceName()
			if svcName != "" {
				m[p.Namespace+":"+svcName] = p.Instrumented
			}
		}
	}
	s.reportedPodsMu.RUnlock()

	// Override with database configurations
	list, err := s.GetWorkloadInstrumentations("")
	if err == nil {
		for _, item := range list {
			m[item.Namespace+":"+item.WorkloadName] = item.Enabled
		}
	}

	return m
}

// IsApplicationActivated checks if an application/service is activated/enabled for tracing.
func (s *Store) IsApplicationActivated(clusterID, namespace, serviceName string) bool {
	list, err := s.GetWorkloadInstrumentations("")
	if err == nil {
		for _, item := range list {
			if item.Namespace == namespace && item.WorkloadName == serviceName {
				return item.Enabled
			}
		}
	}

	// Default to checking if the agent reported it as instrumented if no override config exists
	var pods []ReportedPod
	if clusterID != "" {
		pods = s.GetReportedPodsForCluster(clusterID, namespace)
	}
	if len(pods) == 0 {
		pods = s.GetReportedPodsForCluster("default", namespace)
	}
	if len(pods) == 0 {
		pods = s.GetReportedPodsForCluster("rmm-app-cluster", namespace)
	}
	if len(pods) == 0 {
		pods = s.GetReportedPods(namespace)
	}

	for _, p := range pods {
		if p.ServiceName() == serviceName {
			return p.Instrumented
		}
	}
	return true
}
