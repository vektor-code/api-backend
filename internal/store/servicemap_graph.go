package store

import (
	"context"
	"fmt"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
)

func (s *Store) buildServiceMapFromGraph(namespace string) (*models.ServiceMapData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := s.chQuery(ctx, fmt.Sprintf(`SELECT
		src_namespace,
		src_service,
		dst_namespace,
		dst_service,
		dst_kind,
		is_infra,
		toInt64(round(sum(call_count))) AS call_count,
		toInt64(round(sum(error_count))) AS error_count,
		sum(duration_ns_sum) / nullIf(sum(call_count), 0) / 1e6 AS avg_ms
	FROM kubetrace.service_graph
	WHERE bucket > now() - INTERVAL %d SECOND
	GROUP BY src_namespace, src_service, dst_namespace, dst_service, dst_kind, is_infra`, int(chRefreshWindow.Seconds())))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	activeMap := s.GetApplicationActivationMap()
	configMap := s.GetApplicationConfigMap()
	disabledMap := make(map[string]bool)
	for _, ns := range s.GetDisabledNamespaces() {
		disabledMap[ns] = true
	}

	s.statsMu.RLock()
	statsCacheSnapshot := s.statsCache
	s.statsMu.RUnlock()

	data := &models.ServiceMapData{Namespace: namespace}
	edgeMap := make(map[string]*models.ServiceEdge)
	infraNodes := make(map[string]*models.ServiceStats)

	for _, row := range rows {
		srcNS := chString(row["src_namespace"])
		dstNS := chString(row["dst_namespace"])
		src := chString(row["src_service"])
		dst := chString(row["dst_service"])
		if src == "" || dst == "" {
			continue
		}
		if disabledMap[srcNS] || disabledMap[dstNS] {
			continue
		}
		if namespace != "" && srcNS != namespace && dstNS != namespace && src != "Internet" {
			continue
		}
		calls := chInt(row["call_count"])
		errs := chInt(row["error_count"])
		avg := chFloat(row["avg_ms"])
		isInfra := chInt(row["is_infra"]) == 1

		edgeKey := srcNS + ":" + src + "->" + dstNS + ":" + dst
		e, ok := edgeMap[edgeKey]
		if !ok {
			e = &models.ServiceEdge{
				Source:          src,
				Target:          dst,
				SourceNamespace: srcNS,
				TargetNamespace: dstNS,
			}
			edgeMap[edgeKey] = e
		}
		e.CallCount += calls
		if e.CallCount > 0 {
			e.AvgDurationMs = (e.AvgDurationMs*float64(e.CallCount-calls) + avg*float64(calls)) / float64(e.CallCount)
		}
		e.ErrorCount += errs

		if isInfra {
			infraKey := dstNS + ":" + dst
			infra, ok := infraNodes[infraKey]
			if !ok {
				infra = &models.ServiceStats{
					ServiceName:      dst,
					Namespace:        dstNS,
					IsInfrastructure: true,
				}
				infraNodes[infraKey] = infra
			}
			infra.RequestCount += calls
			infra.ErrorCount += errs
			infra.P50Ms = avg
			infra.P95Ms = avg
			infra.P99Ms = avg
			finalizeServiceStats(infra)
		}
	}

	for key, svc := range statsCacheSnapshot {
		parts := splitNSService(key)
		if len(parts) != 2 {
			continue
		}
		if disabledMap[parts[0]] {
			continue
		}
		if !serviceListed(parts[0], svc.ServiceName, activeMap, configMap) {
			continue
		}
		if namespace == "" || parts[0] == namespace {
			node := *svc
			data.Nodes = append(data.Nodes, node)
		}
	}

	for _, infra := range infraNodes {
		if namespace == "" || infra.Namespace == namespace {
			data.Nodes = append(data.Nodes, *infra)
		}
	}

	var internetCalls, internetErrors int64
	hasInternet := false
	for _, edge := range edgeMap {
		if edge.Source == "Internet" {
			hasInternet = true
			internetCalls += edge.CallCount
			internetErrors += edge.ErrorCount
		}
		data.Edges = append(data.Edges, *edge)
	}
	if hasInternet {
		internet := models.ServiceStats{
			ServiceName:  "Internet",
			Namespace:    namespace,
			RequestCount: internetCalls,
			ErrorCount:   internetErrors,
		}
		finalizeServiceStats(&internet)
		data.Nodes = append(data.Nodes, internet)
	}
	return data, nil
}

func splitNSService(key string) []string {
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return nil
}

func serviceListed(ns, name string, activeMap map[string]bool, configMap map[string]WorkloadInstrumentation) bool {
	cfg, exists := configMap[ns+":"+name]
	if exists {
		return cfg.Enabled
	}
	if enabled, ok := activeMap[ns+":"+name]; ok {
		return enabled
	}
	return true
}
