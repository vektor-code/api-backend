// Package spanenrich is the one place a span gets interpreted.
//
// Ingest used to enrich spans differently depending on how they arrived: OTLP
// protobuf went through one rule set, the API backend's enriched JSON through
// another, and the API read path guessed a third time. Both ingest paths now
// call Enrich, so a span's classification depends on the span, not on its
// route.
//
// The pipeline is: reconcile semantic-convention spellings, identify the remote
// system, then sanitize and fingerprint the query.
package spanenrich

import (
	"os"
	"strconv"
	"strings"

	"github.com/kubetrace/shared/dependency"
	"github.com/kubetrace/shared/httproute"
	"github.com/kubetrace/shared/semconv"
	"github.com/kubetrace/shared/sqlnorm"
)

// Tag keys this package writes. They are namespaced so they cannot collide with
// anything an SDK emits.
const (
	TagSystem     = "crnet.apm.dependency.system"
	TagKind       = "crnet.apm.dependency.kind"
	TagEvidence   = "crnet.apm.dependency.evidence"
	TagConfidence = "crnet.apm.dependency.confidence"
	// TagFingerprint carries the query fingerprint so that consumers reading
	// spans as attribute maps can group by query shape without re-normalizing.
	TagFingerprint = "crnet-apm.db.fingerprint"
	// TagHTTPIdentity marks SERVER spans whose HTTP method is not a token.
	// Diagnostic only — original method attributes and span.name are left intact.
	TagHTTPIdentity = "crnet.apm.http_identity"
	// HTTPIdentityMalformed is the TagHTTPIdentity value for ineligible HTTP SERVER spans.
	HTTPIdentityMalformed = "malformed"
	// AttrAdjustedCount mirrors the collector's sampling attribute.
	AttrAdjustedCount = "crnet.apm.sampling.adjusted_count"
)

// Environment overrides. Defaults are chosen so that an operator who sets
// nothing still gets sanitized query text.
const (
	// RawQueryEnv set to "true" stores query text verbatim, literals included.
	// Off by default: the database conventions require sanitization before
	// query text is collected, because literals routinely carry personal data.
	RawQueryEnv = "KUBETRACE_QUERY_TEXT_RAW"
	// MaxQueryLenEnv caps stored query text.
	MaxQueryLenEnv = "KUBETRACE_QUERY_TEXT_MAX_LEN"
)

// Result carries the structured fields extracted from a span, for storage in
// dedicated columns rather than inside a tag map.
type Result struct {
	System      string
	Kind        string
	Evidence    string
	Confidence  float64
	Operation   string
	Collection  string
	Namespace   string
	Summary     string
	Fingerprint string

	// Transaction is the low-cardinality name to aggregate this span under in
	// endpoint views. It is derived, not stored by the SDK: some services name
	// every server span after the bare HTTP method, which would otherwise
	// merge all of their endpoints into a single row.
	Transaction string
	// SampleWeight is how many spans this one represents. 1 unless the
	// collector kept it probabilistically.
	SampleWeight float64
}

// IsDatabase reports whether this span represents a call to a stateful data
// store — the set the database dashboard should count. Gateways, secret stores
// and observability endpoints are deliberately excluded.
func (r Result) IsDatabase() bool {
	return r.Kind == string(dependency.KindDatabase) || r.Kind == string(dependency.KindCache)
}

// Enricher applies the pipeline. It is safe for concurrent use.
type Enricher struct {
	classifier *dependency.Classifier
	redact     bool
	maxLen     int
}

// New builds an Enricher from the process environment. The returned error is
// non-nil when a configured ruleset could not be used; the Enricher is still
// usable and falls back to the embedded defaults, so callers should log the
// error rather than abort.
func New() (*Enricher, error) {
	classifier, err := dependency.MustDefault()

	e := &Enricher{
		classifier: classifier,
		redact:     !strings.EqualFold(strings.TrimSpace(os.Getenv(RawQueryEnv)), "true"),
		maxLen:     sqlnorm.DefaultMaxLength,
	}
	if v := strings.TrimSpace(os.Getenv(MaxQueryLenEnv)); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n > 0 {
			e.maxLen = n
		}
	}
	return e, err
}

// NewWith builds an Enricher explicitly, for tests and callers that do not want
// environment lookups.
func NewWith(classifier *dependency.Classifier, redact bool, maxLen int) *Enricher {
	if maxLen <= 0 {
		maxLen = sqlnorm.DefaultMaxLength
	}
	return &Enricher{classifier: classifier, redact: redact, maxLen: maxLen}
}

// Enrich mutates tags in place and returns the structured view of the span.
// It is idempotent: running it twice produces the same tags and Result.
//
// Query text is sanitized for every span that carries it, including spans that
// are not classified as dependencies — an ORM's INTERNAL span holds the same
// literals the driver span does, and redaction must not depend on kind.
func (e *Enricher) Enrich(tags map[string]string, spanName, spanKind string) Result {
	if tags == nil {
		return Result{}
	}

	semconv.Normalize(tags)

	var out Result
	if res, ok := e.classifier.Classify(tags, spanName, spanKind); ok {
		out.System = res.System
		out.Kind = string(res.Kind)
		out.Evidence = res.Evidence
		out.Confidence = res.Confidence
		e.applyClassification(tags, res)
	}

	e.applyQuery(tags, out.System, &out)
	out.Namespace = tags["db.name"]
	if httproute.HTTPServerIdentityEligible(spanKind, tags) {
		out.Transaction = httproute.TransactionName(tags, spanName)
	} else {
		tags[TagHTTPIdentity] = HTTPIdentityMalformed
	}
	out.SampleWeight = sampleWeight(tags)

	return out
}

// applyClassification writes the system back onto the span under both the
// legacy and stable spellings, plus the provenance tags.
func (e *Enricher) applyClassification(tags map[string]string, res dependency.Result) {
	switch res.Kind {
	case dependency.KindMessaging:
		if isBlank(tags["messaging.system"]) {
			tags["messaging.system"] = res.System
			tags["messaging.system.name"] = semconv.StableSystemName(res.System)
		}
	case dependency.KindDatabase, dependency.KindCache:
		if isBlank(tags["db.system"]) {
			tags["db.system"] = res.System
			tags["db.system.name"] = semconv.StableSystemName(res.System)
		}
	}

	// Gateways, storage, secret stores and observability endpoints are recorded
	// only under the crnet-apm.* tags. Writing them to db.system is what used to
	// put nginx and Vault on the database dashboard.
	tags[TagSystem] = res.System
	tags[TagKind] = string(res.Kind)
	tags[TagEvidence] = res.Evidence
	tags[TagConfidence] = strconv.FormatFloat(res.Confidence, 'f', 2, 64)
}

// applyQuery sanitizes the statement and derives the grouping keys. Query text
// is rewritten in place so raw literals never reach storage.
func (e *Enricher) applyQuery(tags map[string]string, system string, out *Result) {
	raw := tags["db.statement"]
	if raw == "" {
		raw = tags["db.query.text"]
	}

	// Operation and collection are preferred from the instrumentation, which
	// knows more than we can recover from the text. The conventions ask that
	// application-provided values be kept as-is, without case normalization.
	out.Operation = tags["db.operation"]
	out.Collection = tags["db.collection.name"]

	if raw == "" {
		if out.Operation != "" {
			out.Summary = strings.TrimSpace(out.Operation + " " + out.Collection)
			if isBlank(tags["db.query.summary"]) {
				tags["db.query.summary"] = out.Summary
			}
		}
		return
	}

	normalized := sqlnorm.NormalizeWithLimit(system, raw, e.maxLen)

	if out.Operation == "" {
		out.Operation = sqlnorm.Operation(system, normalized)
		if out.Operation != "" && isBlank(tags["db.operation"]) {
			tags["db.operation"] = out.Operation
			tags["db.operation.name"] = out.Operation
		}
	}
	if out.Collection == "" {
		out.Collection = sqlnorm.Collection(system, normalized)
		if out.Collection != "" && isBlank(tags["db.collection.name"]) {
			tags["db.collection.name"] = out.Collection
		}
	}

	out.Summary = strings.TrimSpace(strings.TrimSpace(out.Operation) + " " + strings.TrimSpace(out.Collection))
	if out.Summary == "" {
		out.Summary = sqlnorm.Summary(system, normalized)
	}
	if out.Summary != "" && isBlank(tags["db.query.summary"]) {
		tags["db.query.summary"] = out.Summary
	}

	out.Fingerprint = sqlnorm.Fingerprint(normalized)
	tags[TagFingerprint] = out.Fingerprint

	if e.redact {
		tags["db.statement"] = normalized
		tags["db.query.text"] = normalized
	}
}

// sampleWeight reads the adjusted count the collector recorded when a span
// survived sampling probabilistically. Aggregations multiply by this so counts
// describe traffic rather than what happened to be stored.
func sampleWeight(tags map[string]string) float64 {
	raw := strings.TrimSpace(tags[AttrAdjustedCount])
	if raw == "" {
		return 1
	}
	weight, err := strconv.ParseFloat(raw, 64)
	if err != nil || weight < 1 {
		return 1
	}
	return weight
}

func isBlank(v string) bool {
	v = strings.TrimSpace(v)
	return v == "" || strings.EqualFold(v, "unknown")
}

// ClassifyExternal reports whether a host is a third-party service outside the
// cluster, and the display name for it. It delegates to the same ruleset the
// dependency classification uses.
func (e *Enricher) ClassifyExternal(host string) (string, bool) {
	return e.classifier.ClassifyExternal(host)
}
