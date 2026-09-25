package w3c

import "testing"

func TestFormatTraceparent(t *testing.T) {
	cases := []struct {
		name    string
		traceID string
		spanID  string
		sampled bool
		want    string
	}{
		{
			name:    "full ids sampled",
			traceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			spanID:  "00f067aa0ba902b7",
			sampled: true,
			want:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		{
			name:    "unsampled flags",
			traceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			spanID:  "00f067aa0ba902b7",
			want:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
		},
		{
			name:    "uppercase is lowercased",
			traceID: "4BF92F3577B34DA6A3CE929D0E0E4736",
			spanID:  "00F067AA0BA902B7",
			sampled: true,
			want:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		{
			name:    "short ids are left padded",
			traceID: "abc",
			spanID:  "1f",
			sampled: true,
			want:    "00-00000000000000000000000000000abc-000000000000001f-01",
		},
		{
			name:    "surrounding whitespace ignored",
			traceID: "  4bf92f3577b34da6a3ce929d0e0e4736 ",
			spanID:  " 00f067aa0ba902b7",
			sampled: true,
			want:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		{name: "empty trace id", spanID: "00f067aa0ba902b7", sampled: true},
		{name: "empty span id", traceID: "4bf92f3577b34da6a3ce929d0e0e4736", sampled: true},
		{
			name:    "all zero trace id is invalid",
			traceID: "00000000000000000000000000000000",
			spanID:  "00f067aa0ba902b7",
			sampled: true,
		},
		{
			name:    "all zero span id is invalid",
			traceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			spanID:  "0000000000000000",
			sampled: true,
		},
		{
			name:    "non hex is rejected",
			traceID: "4bf92f3577b34da6a3ce929d0e0e473z",
			spanID:  "00f067aa0ba902b7",
			sampled: true,
		},
		{
			name:    "over long trace id is rejected",
			traceID: "4bf92f3577b34da6a3ce929d0e0e47360",
			spanID:  "00f067aa0ba902b7",
			sampled: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatTraceparent(tc.traceID, tc.spanID, tc.sampled); got != tc.want {
				t.Fatalf("FormatTraceparent(%q, %q, %v) = %q, want %q", tc.traceID, tc.spanID, tc.sampled, got, tc.want)
			}
		})
	}
}

func TestTraceFlags(t *testing.T) {
	if got := TraceFlags(true); got != "01" {
		t.Fatalf("TraceFlags(true) = %q, want 01", got)
	}
	if got := TraceFlags(false); got != "00" {
		t.Fatalf("TraceFlags(false) = %q, want 00", got)
	}
}

func TestSampledFromOTLPFlags(t *testing.T) {
	cases := []struct {
		name  string
		flags uint32
		want  bool
	}{
		// An exporter that never populates flags must not make every span look
		// unsampled — it reached us, so it was sampled.
		{name: "unset field", flags: 0, want: true},
		{name: "sampled bit", flags: 0x01, want: true},
		{name: "sampled with is_remote bits", flags: 0x301, want: true},
		{name: "flags populated without sampled bit", flags: 0x100, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SampledFromOTLPFlags(tc.flags); got != tc.want {
				t.Fatalf("SampledFromOTLPFlags(%#x) = %v, want %v", tc.flags, got, tc.want)
			}
		})
	}
}
