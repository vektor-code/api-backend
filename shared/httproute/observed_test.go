package httproute

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// testdata/observed_paths.txt holds the distinct request paths seen in 24h of
// production traffic from the services that emit no http.route. Normalization
// has to hit a narrow target on this corpus: collapse the record ids without
// merging endpoints that are genuinely different, and the only way to know it
// still does is to run it against the real shapes.
func TestObservedPathsCollapseCorrectly(t *testing.T) {
	paths := loadObservedPaths(t)
	if len(paths) == 0 {
		t.Skip("no observed paths recorded")
	}

	normalized := make(map[string]int, len(paths))
	for _, p := range paths {
		normalized[NormalizePath(p)]++
	}

	// Cardinality must fall — that is the entire point.
	if len(normalized) >= len(paths) {
		t.Errorf("normalization did not reduce cardinality: %d paths -> %d", len(paths), len(normalized))
	}

	// Endpoints that differ only by record id must land together.
	for _, want := range []string{
		"/api/v2/inter-service/users/{id}",
		"/api/v2/inter-service/organizations/{id}",
		"/api/v2/dictionary/inter-service/chart-of-accounts/{id}",
	} {
		if normalized[want] < 2 {
			t.Errorf("expected several paths to collapse into %q, got %d", want, normalized[want])
		}
	}

	// Endpoints that are genuinely distinct must stay distinct. These read like
	// identifiers to a naive normalizer but are route names.
	for _, want := range []string{
		"/api/v2/inter-service/users/authorization/me",
		"/api/health/readiness",
		"/api/health/liveness",
	} {
		if normalized[want] == 0 {
			t.Errorf("endpoint %q was normalized away", want)
		}
	}

	// Nothing should collapse into a bare prefix — that would mean the
	// placeholder swallowed a meaningful segment.
	for name := range normalized {
		if strings.HasSuffix(name, "/{id}/{id}") {
			t.Errorf("suspicious over-normalization: %q", name)
		}
	}
}

// Every observed path must survive a round trip unchanged once normalized,
// or the output is not a stable grouping key.
func TestNormalizeIsIdempotentOnObservedPaths(t *testing.T) {
	for _, p := range loadObservedPaths(t) {
		once := NormalizePath(p)
		twice := NormalizePath(once)
		if once != twice {
			t.Errorf("not idempotent for %q: %q then %q", p, once, twice)
		}
	}
}

func loadObservedPaths(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/observed_paths.txt")
	if err != nil {
		return nil
	}
	defer f.Close()

	var paths []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			paths = append(paths, line)
		}
	}
	return paths
}
