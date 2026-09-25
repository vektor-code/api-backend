// Package w3c renders W3C Trace Context values from the identifiers we store.
//
// Ingest, API and UI each used to build the traceparent string themselves, and
// the UI simply concatenated `00-<traceID>-<spanID>-01`. That produces a value
// no W3C parser accepts whenever an exporter sends a short or upper-cased ID,
// so the "copy traceparent" affordance handed people a broken header. One
// formatter keeps every layer on the same representation.
package w3c

import "strings"

const (
	traceIDHexLen = 32
	spanIDHexLen  = 16

	// OTLP packs the 8-bit W3C trace flags into the low byte of Span.flags.
	otlpTraceFlagsMask = 0x000000FF
	sampledFlag        = 0x01
)

// FormatTraceparent builds a version-00 traceparent header value. IDs are
// lowercased and left-padded to their W3C widths; values that cannot be a valid
// trace context (non-hex, over-long, or all-zero IDs) yield "" so callers can
// omit the field instead of displaying something unusable.
func FormatTraceparent(traceID, spanID string, sampled bool) string {
	trace, ok := normalizeID(traceID, traceIDHexLen)
	if !ok {
		return ""
	}
	span, ok := normalizeID(spanID, spanIDHexLen)
	if !ok {
		return ""
	}
	return "00-" + trace + "-" + span + "-" + TraceFlags(sampled)
}

// TraceFlags returns the two-hex-digit trace-flags field of a traceparent.
func TraceFlags(sampled bool) string {
	if sampled {
		return "01"
	}
	return "00"
}

// SampledFromOTLPFlags reads the sampled bit out of an OTLP Span.flags value.
//
// Most SDKs still leave the field at zero, and OTLP says readers must not read
// an unset field as "not sampled". A span that reached us was sampled by
// definition, so treat a fully unset value as sampled and only trust bit 0 once
// the exporter has populated the field.
func SampledFromOTLPFlags(flags uint32) bool {
	if flags == 0 {
		return true
	}
	return flags&otlpTraceFlagsMask&sampledFlag != 0
}

// normalizeID lowercases, validates and left-pads a hex ID to width.
func normalizeID(id string, width int) (string, bool) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" || len(id) > width {
		return "", false
	}
	allZero := true
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= '1' && c <= '9', c >= 'a' && c <= 'f':
			allZero = false
		case c == '0':
		default:
			return "", false
		}
	}
	if allZero {
		return "", false
	}
	if len(id) < width {
		id = strings.Repeat("0", width-len(id)) + id
	}
	return id, true
}
