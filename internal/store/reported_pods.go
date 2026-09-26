package store

import (
	"strings"
)

type ReportedPod struct {
	Name                string            `json:"name"`
	Namespace           string            `json:"namespace"`
	NodeName            string            `json:"nodeName"`
	Labels              map[string]string `json:"labels"`
	Phase               string            `json:"phase"`
	CpuUsage            float64           `json:"cpuUsage"`
	CpuLimit            float64           `json:"cpuLimit"`
	MemoryUsage         float64           `json:"memoryUsage"`
	MemoryLimit         float64           `json:"memoryLimit"`
	RestartCount        int               `json:"restartCount"`
	Ready               bool              `json:"ready"`
	StatusReason        string            `json:"statusReason,omitempty"`
	StatusMessage       string            `json:"statusMessage,omitempty"`
	Language            string            `json:"language"`
	Instrumented        bool              `json:"instrumented"`
	InstrumentationType string            `json:"instrumentationType"`
	Details             string            `json:"details"`
	DatabaseName        string            `json:"databaseName"`
	DatabaseHost        string            `json:"databaseHost"`
	DatabasePort        string            `json:"databasePort"`
	IsFrontend          bool              `json:"isFrontend"`
	ContainerImages     []string          `json:"containerImages,omitempty"`
}

var reportedPodServiceLabels = [...]string{"app.kubernetes.io/name", "app", "service", "k8s-app"}

func (p ReportedPod) ServiceName() string {
	if p.Labels != nil {
		for _, label := range reportedPodServiceLabels {
			if value := strings.TrimSpace(p.Labels[label]); value != "" {
				return value
			}
		}
	}
	return serviceNameFromPodName(p.Name)
}

func (p ReportedPod) MatchesService(serviceName string) bool {
	if serviceName == "" {
		return false
	}
	return p.ServiceName() == serviceName || strings.HasPrefix(p.Name, serviceName+"-")
}

func serviceNameFromPodName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, "-")
	if len(parts) >= 3 {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return name
}

// mergePreservePodLanguage keeps a previously reported language when a fresh
// sync omits it for the same pod name in the namespace.
func mergePreservePodLanguage(prev, incoming []ReportedPod) []ReportedPod {
	if len(prev) == 0 {
		return incoming
	}
	prevLang := make(map[string]string, len(prev))
	for _, p := range prev {
		if p.Language != "" {
			prevLang[p.Namespace+"/"+p.Name] = p.Language
		}
	}
	for i := range incoming {
		if incoming[i].Language == "" {
			if lang, ok := prevLang[incoming[i].Namespace+"/"+incoming[i].Name]; ok {
				incoming[i].Language = lang
			}
		}
	}
	return incoming
}

func (s *Store) SetReportedPods(ns string, pods []ReportedPod) {
	s.reportedPodsMu.Lock()
	defer s.reportedPodsMu.Unlock()
	if s.reportedPods == nil {
		s.reportedPods = make(map[string][]ReportedPod)
	}
	s.reportedPods[ns] = pods
}

func (s *Store) GetReportedPods(ns string) []ReportedPod {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return nil
	}
	if ns == "" {
		var all []ReportedPod
		for _, pods := range s.reportedPods {
			all = append(all, pods...)
		}
		return all
	}
	return s.reportedPods[ns]
}

func (s *Store) GetReportedLanguageForService(namespace, serviceName string) string {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return ""
	}
	pods := s.reportedPods[namespace]
	var fallback string
	for _, p := range pods {
		if !p.MatchesService(serviceName) {
			continue
		}
		if IsAssignedStack(p.Language) {
			return p.Language
		}
		if p.Language != "" && fallback == "" {
			fallback = p.Language
		}
	}
	return fallback
}

func (s *Store) GetReportedDatabaseForService(namespace, serviceName string) string {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return ""
	}
	pods := s.reportedPods[namespace]
	for _, p := range pods {
		if p.MatchesService(serviceName) {
			if p.DatabaseName != "" {
				return p.DatabaseName
			}
		}
	}
	return ""
}

// GetReportedNamespaces returns all namespaces that have reported pods.
func (s *Store) GetReportedNamespaces() []string {
	s.reportedPodsMu.RLock()
	defer s.reportedPodsMu.RUnlock()
	if s.reportedPods == nil {
		return nil
	}
	namespaces := make([]string, 0, len(s.reportedPods))
	for ns, pods := range s.reportedPods {
		if len(pods) > 0 {
			namespaces = append(namespaces, ns)
		}
	}
	return namespaces
}

// GetServicesForNamespace extracts unique service names from reported pods
// in the given namespace using Kubernetes labels (app.kubernetes.io/name, app, service).
func (s *Store) GetServicesForNamespace(ns string) []string {
	pods := s.GetReportedPods(ns)
	if len(pods) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var services []string
	for _, p := range pods {
		svcName := p.ServiceName()
		if svcName != "" && !seen[svcName] {
			seen[svcName] = true
			services = append(services, svcName)
		}
	}
	return services
}
