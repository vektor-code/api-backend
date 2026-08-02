// Package dependency classifies the remote system a span talks to.
//
// It replaces four divergent copies of this logic that previously lived in the
// ingestor, the API read path, the API write path and the frontend, each with
// slightly different rules — so the same span could be labelled differently
// depending on which path it travelled.
//
// Two properties matter more than breadth of coverage:
//
//   - Determinism. Every tier walks an ordered slice, never a Go map, so a
//     given set of attributes always yields the same answer on every replica.
//   - Honest evidence. The result carries what the decision was based on and
//     how strong it was, so a guess from a substring is distinguishable from an
//     explicit db.system.name.
//
// All matching data lives in a ruleset (see rules.json), not in this file.
package dependency

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"strings"
)

//go:embed rules.json
var defaultRulesJSON []byte

// RulesPathEnv points at a ruleset file that replaces the embedded defaults,
// normally a ConfigMap mounted into the pod.
const RulesPathEnv = "KUBETRACE_DEPENDENCY_RULES"

// Kind is the category of remote system. Keeping caches, gateways and secret
// stores out of "database" is what stops nginx and Vault appearing on the
// database dashboard.
type Kind string

const (
	KindDatabase      Kind = "database"
	KindCache         Kind = "cache"
	KindMessaging     Kind = "messaging"
	KindStorage       Kind = "storage"
	KindGateway       Kind = "gateway"
	KindSecrets       Kind = "secrets"
	KindDiscovery     Kind = "discovery"
	KindObservability Kind = "observability"
)

// Confidence levels, ordered by how much the evidence is worth.
const (
	ConfidenceExplicit  = 1.0  // the instrumentation told us
	ConfidencePort      = 0.85 // a well-known port
	ConfidenceHostRule  = 0.8  // an operator-configured host or CIDR
	ConfidenceHost      = 0.65 // a peer host name matched a known product
	ConfidenceSpanName  = 0.45 // the span name matched a known product
	ConfidenceAttribute = 0.35 // db.* attributes exist but nothing identifies the engine
)

// Result describes the remote system a span talks to.
type Result struct {
	System     string  `json:"system"`
	Kind       Kind    `json:"kind"`
	Evidence   string  `json:"evidence"`
	Confidence float64 `json:"confidence"`
}

// SystemRule matches a product by name fragments.
type SystemRule struct {
	ID         string   `json:"id"`
	Kind       Kind     `json:"kind"`
	Substrings []string `json:"substrings,omitempty"`
	Tokens     []string `json:"tokens,omitempty"`
	// Scope "host" restricts the rule to peer host names. Generic tokens such
	// as "db" are meaningful in a hostname and misleading in a span name.
	Scope string `json:"scope,omitempty"`
}

// PortRule maps a port onto a system, optionally requiring a host hint so that
// ports shared by unrelated software do not produce confident nonsense.
type PortRule struct {
	Port         string   `json:"port"`
	System       string   `json:"system"`
	HostContains []string `json:"hostContains,omitempty"`
}

// HostRule pins a specific address or network to a system. This is how a site
// declares "10.254.5.30 is our Oracle box" without anyone editing Go code.
type HostRule struct {
	Match  string `json:"match"` // exact | prefix | suffix | contains | cidr
	Value  string `json:"value"`
	System string `json:"system"`
}

// ExternalRule gives a friendly display name to a recognized third-party API.
type ExternalRule struct {
	Contains string `json:"contains"`
	Name     string `json:"name"`
}

// Ruleset is the complete matching configuration.
type Ruleset struct {
	Systems []SystemRule `json:"systems"`
	Ports   []PortRule   `json:"ports"`
	Hosts   []HostRule   `json:"hosts"`

	// External identifies third-party SaaS dependencies by hostname.
	External []ExternalRule `json:"external"`
	// InternalSuffixes are DNS suffixes that mark a host as cluster-internal.
	InternalSuffixes []string `json:"internalSuffixes"`
	// PrivateCIDRs are the networks treated as internal. Real CIDR matching
	// matters here: a "172." prefix test wrongly captures 172.0-172.15 and
	// 172.32-172.255, which are public address space.
	PrivateCIDRs []string `json:"privateCIDRs"`
}

// Classifier answers "what is on the other end of this span".
type Classifier struct {
	rules       Ruleset
	kindByID    map[string]Kind
	networks    []hostNetwork
	privateNets []netip.Prefix
}

type hostNetwork struct {
	prefix netip.Prefix
	system string
}

// Default builds a classifier from the file named by KUBETRACE_DEPENDENCY_RULES,
// falling back to the embedded ruleset. A malformed override is reported rather
// than silently ignored, so a bad ConfigMap cannot quietly degrade detection.
func Default() (*Classifier, error) {
	if path := strings.TrimSpace(os.Getenv(RulesPathEnv)); path != "" {
		return Load(path)
	}
	return NewFromJSON(defaultRulesJSON)
}

// MustDefault is Default but falls back to the embedded ruleset on error. It
// returns the error too so the caller can log it.
func MustDefault() (*Classifier, error) {
	c, err := Default()
	if err == nil {
		return c, nil
	}
	fallback, ferr := NewFromJSON(defaultRulesJSON)
	if ferr != nil {
		panic("dependency: embedded ruleset is invalid: " + ferr.Error())
	}
	return fallback, err
}

// Load reads a ruleset from disk.
func Load(path string) (*Classifier, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dependency ruleset %q: %w", path, err)
	}
	return NewFromJSON(data)
}

// NewFromJSON parses and validates a ruleset.
func NewFromJSON(data []byte) (*Classifier, error) {
	var rs Ruleset
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("parse dependency ruleset: %w", err)
	}
	return New(rs)
}

// New validates a ruleset and builds the lookup structures.
func New(rs Ruleset) (*Classifier, error) {
	c := &Classifier{rules: rs, kindByID: make(map[string]Kind, len(rs.Systems))}

	for _, s := range rs.Systems {
		if s.ID == "" {
			return nil, fmt.Errorf("dependency ruleset: system with empty id")
		}
		if s.Kind == "" {
			return nil, fmt.Errorf("dependency ruleset: system %q has no kind", s.ID)
		}
		if _, dup := c.kindByID[s.ID]; !dup {
			c.kindByID[s.ID] = s.Kind
		}
	}
	for _, p := range rs.Ports {
		if _, ok := c.kindByID[p.System]; !ok {
			return nil, fmt.Errorf("dependency ruleset: port %s references unknown system %q", p.Port, p.System)
		}
	}
	for _, h := range rs.Hosts {
		if _, ok := c.kindByID[h.System]; !ok {
			return nil, fmt.Errorf("dependency ruleset: host rule %q references unknown system %q", h.Value, h.System)
		}
		if h.Match != "cidr" {
			continue
		}
		prefix, err := netip.ParsePrefix(h.Value)
		if err != nil {
			return nil, fmt.Errorf("dependency ruleset: host rule %q is not a valid CIDR: %w", h.Value, err)
		}
		c.networks = append(c.networks, hostNetwork{prefix: prefix, system: h.System})
	}
	for _, cidr := range rs.PrivateCIDRs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("dependency ruleset: privateCIDRs entry %q is not a valid CIDR: %w", cidr, err)
		}
		c.privateNets = append(c.privateNets, prefix)
	}
	return c, nil
}

// ClassifyExternal reports whether a host is a third-party service outside the
// cluster, and the name to display for it. Cluster-internal DNS suffixes,
// loopback and private networks are excluded.
//
// Callers still need to exclude hosts that match their own service names; only
// the caller knows what those are.
func (c *Classifier) ClassifyExternal(host string) (string, bool) {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return "", false
	}
	host, _ = splitHostPort(host, "")
	if host == "" || host == "localhost" {
		return "", false
	}

	for _, suffix := range c.rules.InternalSuffixes {
		if strings.HasSuffix(host, strings.ToLower(suffix)) {
			return "", false
		}
	}

	if addr, err := netip.ParseAddr(host); err == nil {
		if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() {
			return "", false
		}
		for _, prefix := range c.privateNets {
			if prefix.Contains(addr) {
				return "", false
			}
		}
	}

	for _, rule := range c.rules.External {
		if rule.Contains != "" && strings.Contains(host, strings.ToLower(rule.Contains)) {
			return rule.Name, true
		}
	}

	// An unrecognized host is third-party only if it is a public FQDN. A bare
	// single-label name is a cluster service, not the internet.
	if strings.Contains(host, ".") {
		return host, true
	}
	return "", false
}

// KindOf returns the category registered for a system id. Systems that the
// ruleset does not know about are reported as databases only when the caller
// already established that from an explicit db.* attribute.
func (c *Classifier) KindOf(system string) (Kind, bool) {
	k, ok := c.kindByID[system]
	return k, ok
}

// CrossesProcessBoundary reports whether a span kind represents a call out to
// another system.
//
// Only outbound spans can have a remote dependency. An ORM emits an INTERNAL
// span carrying db.statement for the same query the driver reports as a CLIENT
// span, so classifying both counts every query twice and invents a second
// "database" system alongside the real engine.
func CrossesProcessBoundary(spanKind string) bool {
	switch strings.ToUpper(strings.TrimSpace(spanKind)) {
	case "CLIENT", "PRODUCER", "CONSUMER":
		return true
	default:
		return false
	}
}

// Classify walks the evidence tiers in descending order of reliability and
// returns on the first hit. tags must already have been through
// semconv.Normalize, so db.system holds a canonical id.
//
// spanKind gates the whole thing: a span that does not leave the process has no
// remote dependency to identify, whatever attributes it carries.
func (c *Classifier) Classify(tags map[string]string, spanName, spanKind string) (Result, bool) {
	if !CrossesProcessBoundary(spanKind) {
		return Result{}, false
	}
	if tags == nil {
		tags = map[string]string{}
	}

	host := strings.ToLower(firstNonEmpty(tags,
		"server.address", "net.peer.name", "network.peer.address", "net.peer.ip", "peer.service"))
	port := firstNonEmpty(tags, "server.port", "net.peer.port", "network.peer.port", "peer.port")
	host, port = splitHostPort(host, port)

	// Tier 1 — the instrumentation stated the system outright.
	//
	// On a PRODUCER or CONSUMER span, messaging.system outranks db.system. A
	// job queue backed by Redis reports both, and it is the queue that matters:
	// reading db.system first filed those spans as a cache, so queue traffic
	// never appeared under messaging at all.
	messagingFirst := isMessagingSpan(spanKind)

	if !messagingFirst {
		if sys := strings.ToLower(strings.TrimSpace(tags["db.system"])); sys != "" && sys != "unknown" {
			return c.explicit(sys, KindDatabase, "db.system"), true
		}
	}
	if sys := strings.ToLower(strings.TrimSpace(tags["messaging.system"])); sys != "" && sys != "unknown" {
		// Force the messaging kind rather than looking the id up: Redis is
		// registered as a cache, but Redis carrying a queue is messaging.
		return Result{
			System: sys, Kind: KindMessaging,
			Evidence: "messaging.system", Confidence: ConfidenceExplicit,
		}, true
	}
	if messagingFirst {
		if sys := strings.ToLower(strings.TrimSpace(tags["db.system"])); sys != "" && sys != "unknown" {
			return c.explicit(sys, KindDatabase, "db.system"), true
		}
	}

	// Tier 2 — operator-declared hosts and networks.
	if r, ok := c.matchHostRules(host); ok {
		return r, true
	}

	// Tier 3 — well-known ports.
	if r, ok := c.matchPort(port, host); ok {
		return r, true
	}

	// Tiers 4 and 5 are name guesses. For a span that carries HTTP attributes
	// and no database attributes, a name guess must not be allowed to invent a
	// database call: "GET /api/db-health" is an HTTP request, not a query.
	weakDBAllowed := !hasHTTPSignal(tags) || hasDBSignal(tags)

	if host != "" {
		if r, ok := c.matchName(host, true, weakDBAllowed, "host", ConfidenceHost); ok {
			return r, true
		}
	}
	if spanName != "" {
		if r, ok := c.matchName(strings.ToLower(spanName), false, weakDBAllowed, "span.name", ConfidenceSpanName); ok {
			return r, true
		}
	}

	// Tier 6 — attributes establish what sort of dependency it is, even though
	// nothing names the product.
	//
	// Messaging is checked before database because a PRODUCER/CONSUMER span
	// carrying messaging.* attributes is a queue operation whatever the
	// transport. Laravel's queue instrumentation, for one, emits a full set of
	// messaging.destination / messaging.message.* attributes and no
	// messaging.system, which previously left those spans unclassified.
	if hasMessagingSignal(tags) && isMessagingSpan(spanKind) {
		return Result{
			System:     "queue",
			Kind:       KindMessaging,
			Evidence:   "messaging.attributes",
			Confidence: ConfidenceAttribute,
		}, true
	}
	if hasDBSignal(tags) {
		return Result{
			System:     "database",
			Kind:       KindDatabase,
			Evidence:   "db.attributes",
			Confidence: ConfidenceAttribute,
		}, true
	}

	return Result{}, false
}

func (c *Classifier) explicit(system string, fallback Kind, evidence string) Result {
	kind := fallback
	if k, ok := c.kindByID[system]; ok {
		kind = k
	}
	return Result{System: system, Kind: kind, Evidence: evidence, Confidence: ConfidenceExplicit}
}

func (c *Classifier) matchHostRules(host string) (Result, bool) {
	if host == "" {
		return Result{}, false
	}
	for _, h := range c.rules.Hosts {
		v := strings.ToLower(h.Value)
		matched := false
		switch h.Match {
		case "exact":
			matched = host == v
		case "prefix":
			matched = strings.HasPrefix(host, v)
		case "suffix":
			matched = strings.HasSuffix(host, v)
		case "contains":
			matched = strings.Contains(host, v)
		case "cidr":
			continue // handled below against the parsed prefixes
		}
		if matched {
			return Result{
				System: h.System, Kind: c.kindByID[h.System],
				Evidence: "host.rule", Confidence: ConfidenceHostRule,
			}, true
		}
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		for _, n := range c.networks {
			if n.prefix.Contains(addr) {
				return Result{
					System: n.system, Kind: c.kindByID[n.system],
					Evidence: "host.cidr", Confidence: ConfidenceHostRule,
				}, true
			}
		}
	}
	return Result{}, false
}

func (c *Classifier) matchPort(port, host string) (Result, bool) {
	if port == "" {
		return Result{}, false
	}
	for _, p := range c.rules.Ports {
		if p.Port != port {
			continue
		}
		if len(p.HostContains) > 0 {
			if host == "" || !containsAny(host, p.HostContains) {
				continue
			}
		}
		return Result{
			System: p.System, Kind: c.kindByID[p.System],
			Evidence: "port", Confidence: ConfidencePort,
		}, true
	}
	return Result{}, false
}

// matchName tests the system rules in ruleset order. Substrings match anywhere;
// tokens must line up with a word boundary, which is what keeps "pg" from
// matching "upgrade" and "s3" from matching "s3cret".
func (c *Classifier) matchName(value string, isHost, weakDBAllowed bool, evidence string, confidence float64) (Result, bool) {
	tokens := tokenize(value)
	for _, s := range c.rules.Systems {
		if s.Scope == "host" && !isHost {
			continue
		}
		if !weakDBAllowed && (s.Kind == KindDatabase || s.Kind == KindCache) {
			continue
		}
		for _, sub := range s.Substrings {
			if strings.Contains(value, sub) {
				return Result{System: s.ID, Kind: s.Kind, Evidence: evidence, Confidence: confidence}, true
			}
		}
		for _, tok := range s.Tokens {
			if tokens[tok] {
				return Result{System: s.ID, Kind: s.Kind, Evidence: evidence, Confidence: confidence}, true
			}
		}
	}
	return Result{}, false
}

// tokenize splits on everything that is not a letter or digit, so
// "rmis-pg-primary.db.svc" yields rmis, pg, primary, db, svc.
func tokenize(value string) map[string]bool {
	out := make(map[string]bool, 8)
	start := -1
	for i := 0; i <= len(value); i++ {
		isAlnum := i < len(value) && (isLetter(value[i]) || isDigit(value[i]))
		if isAlnum && start < 0 {
			start = i
		} else if !isAlnum && start >= 0 {
			out[value[start:i]] = true
			start = -1
		}
	}
	return out
}

func hasDBSignal(tags map[string]string) bool {
	for _, k := range []string{
		"db.statement", "db.query.text", "db.name", "db.namespace",
		"db.operation", "db.operation.name", "db.collection.name",
	} {
		if tags[k] != "" {
			return true
		}
	}
	return false
}

// isMessagingSpan reports whether the span kind is a queue operation.
func isMessagingSpan(spanKind string) bool {
	switch strings.ToUpper(strings.TrimSpace(spanKind)) {
	case "PRODUCER", "CONSUMER":
		return true
	default:
		return false
	}
}

// hasMessagingSignal reports whether the span carries queue attributes, even
// when no broker product is named.
func hasMessagingSignal(tags map[string]string) bool {
	for _, k := range []string{
		"messaging.destination", "messaging.destination.name", "messaging.destination_name",
		"messaging.operation", "messaging.message.id", "messaging.message.body.size",
	} {
		if tags[k] != "" {
			return true
		}
	}
	return false
}

func hasHTTPSignal(tags map[string]string) bool {
	for _, k := range []string{"http.method", "http.request.method", "http.url", "url.full", "http.route"} {
		if tags[k] != "" {
			return true
		}
	}
	return false
}

// splitHostPort pulls a trailing :port off a host when the port was not
// supplied as its own attribute, and strips IPv6 brackets.
func splitHostPort(host, port string) (string, string) {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") {
		if end := strings.Index(host, "]"); end > 0 {
			inner := host[1:end]
			rest := host[end+1:]
			if port == "" && strings.HasPrefix(rest, ":") {
				port = rest[1:]
			}
			return inner, port
		}
	}
	if idx := strings.LastIndex(host, ":"); idx > 0 && !strings.Contains(host[idx+1:], ":") {
		candidate := host[idx+1:]
		if isAllDigits(candidate) {
			if port == "" {
				port = candidate
			}
			host = host[:idx]
		}
	}
	return host, strings.TrimSpace(port)
}

func firstNonEmpty(tags map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(tags[k]); v != "" {
			return v
		}
	}
	return ""
}

func containsAny(value string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(value, strings.ToLower(n)) {
			return true
		}
	}
	return false
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isLetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
