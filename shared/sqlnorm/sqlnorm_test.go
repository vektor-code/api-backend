package sqlnorm

import "testing"

func TestNormalizeReplacesLiterals(t *testing.T) {
	tests := []struct {
		name   string
		system string
		in     string
		want   string
	}{
		{
			name:   "string and numeric literals",
			system: "postgresql",
			in:     "SELECT * FROM users WHERE name = 'alice' AND age = 30",
			want:   "SELECT * FROM users WHERE name = ? AND age = ?",
		},
		{
			name:   "escaped quote inside literal",
			system: "postgresql",
			in:     "SELECT * FROM t WHERE s = 'O''Brien'",
			want:   "SELECT * FROM t WHERE s = ?",
		},
		{
			name:   "backslash escaped quote",
			system: "mysql",
			in:     `SELECT * FROM t WHERE s = 'a\'b'`,
			want:   "SELECT * FROM t WHERE s = ?",
		},
		{
			name:   "identifiers with digits survive",
			system: "mysql",
			in:     "SELECT col1, utf8mb4_col FROM table2",
			want:   "SELECT col1, utf8mb4_col FROM table2",
		},
		{
			name:   "quoted identifiers are preserved",
			system: "postgresql",
			in:     `SELECT "userId" FROM "Users" WHERE "id" = 5`,
			want:   `SELECT "userId" FROM "Users" WHERE "id" = ?`,
		},
		{
			name:   "backtick identifiers are preserved",
			system: "mysql",
			in:     "SELECT `id` FROM `orders` WHERE `total` = 12.5",
			want:   "SELECT `id` FROM `orders` WHERE `total` = ?",
		},
		{
			name:   "positional parameters normalize",
			system: "postgresql",
			in:     "SELECT * FROM t WHERE a = $1 AND b = $2",
			want:   "SELECT * FROM t WHERE a = ? AND b = ?",
		},
		{
			name:   "line comment removed",
			system: "postgresql",
			in:     "SELECT 1 -- traceparent='00-abc'\nFROM t",
			want:   "SELECT ? FROM t",
		},
		{
			name:   "block comment removed",
			system: "postgresql",
			in:     "SELECT /* sqlcommenter route='/x' */ id FROM t",
			want:   "SELECT id FROM t",
		},
		{
			name:   "hex literal",
			system: "mysql",
			in:     "SELECT * FROM t WHERE b = 0xDEADBEEF",
			want:   "SELECT * FROM t WHERE b = ?",
		},
		{
			name:   "dollar quoted body",
			system: "postgresql",
			in:     "SELECT $$secret payload$$ FROM t",
			want:   "SELECT ? FROM t",
		},
		{
			name:   "booleans and null are literals",
			system: "postgresql",
			in:     "SELECT * FROM t WHERE active = true AND deleted IS NULL",
			want:   "SELECT * FROM t WHERE active = ? AND deleted IS ?",
		},
		{
			name:   "whitespace collapses",
			system: "postgresql",
			in:     "SELECT\n\tid,\n\tname\nFROM    users",
			want:   "SELECT id, name FROM users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Normalize(tc.system, tc.in); got != tc.want {
				t.Errorf("Normalize()\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// Collapsing variable-length value groups is what keeps a single query shape
// from producing a different fingerprint for every batch size.
func TestNormalizeCollapsesGroups(t *testing.T) {
	tests := []struct{ in, want string }{
		{"SELECT * FROM t WHERE id IN (1, 2, 3)", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE id IN (1)", "SELECT * FROM t WHERE id IN (?)"},
		{"INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", "INSERT INTO t VALUES (?)"},
	}
	for _, tc := range tests {
		if got := Normalize("postgresql", tc.in); got != tc.want {
			t.Errorf("Normalize(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
		}
	}
}

func TestFingerprintGroupsByShape(t *testing.T) {
	a := Fingerprint(Normalize("postgresql", "SELECT * FROM users WHERE id = 1"))
	b := Fingerprint(Normalize("postgresql", "SELECT * FROM users WHERE id = 99999"))
	c := Fingerprint(Normalize("postgresql", "SELECT * FROM orders WHERE id = 1"))

	if a != b {
		t.Errorf("same query shape produced different fingerprints: %s vs %s", a, b)
	}
	if a == c {
		t.Error("different tables produced the same fingerprint")
	}
	if a == "" {
		t.Error("fingerprint should not be empty")
	}
}

func TestFingerprintIsCaseInsensitive(t *testing.T) {
	a := Fingerprint(Normalize("postgresql", "SELECT id FROM users"))
	b := Fingerprint(Normalize("postgresql", "select id from users"))
	if a != b {
		t.Errorf("case difference changed the fingerprint: %s vs %s", a, b)
	}
}

func TestOperationAndCollection(t *testing.T) {
	tests := []struct {
		system, query  string
		wantOp, wantCo string
	}{
		{"postgresql", "SELECT * FROM users WHERE id = ?", "SELECT", "users"},
		{"postgresql", "INSERT INTO orders (id) VALUES (?)", "INSERT", "orders"},
		{"postgresql", "UPDATE accounts SET x = ?", "UPDATE", "accounts"},
		{"postgresql", "DELETE FROM sessions WHERE id = ?", "DELETE", "sessions"},
		{"postgresql", `SELECT * FROM "Users"`, "SELECT", "Users"},
		{"postgresql", "WITH recent AS (SELECT ?) SELECT * FROM recent", "SELECT", "recent"},
		{"postgresql", "SELECT * FROM (SELECT ?) x", "SELECT", ""},
		{"redis", "HGET session ?", "HGET", ""},
	}
	for _, tc := range tests {
		if got := Operation(tc.system, tc.query); got != tc.wantOp {
			t.Errorf("Operation(%q) = %q, want %q", tc.query, got, tc.wantOp)
		}
		if got := Collection(tc.system, tc.query); got != tc.wantCo {
			t.Errorf("Collection(%q) = %q, want %q", tc.query, got, tc.wantCo)
		}
	}
}

func TestSummaryIsLowCardinality(t *testing.T) {
	a := Summary("postgresql", Normalize("postgresql", "SELECT * FROM users WHERE id = 1"))
	b := Summary("postgresql", Normalize("postgresql", "SELECT * FROM users WHERE id = 2"))
	if a != "SELECT users" || a != b {
		t.Errorf("Summary should collapse to %q, got %q and %q", "SELECT users", a, b)
	}
}

func TestNormalizeKeyValueCommands(t *testing.T) {
	tests := []struct{ in, want string }{
		{"GET user:1234", "GET ?"},
		{"HSET myhash field1 value1", "HSET ?"},
		{"PING", "PING"},
	}
	for _, tc := range tests {
		if got := Normalize("redis", tc.in); got != tc.want {
			t.Errorf("Normalize(redis, %q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeDocumentKeepsKeysDropsValues(t *testing.T) {
	got := Normalize("mongodb", `{"find": "users", "filter": {"email": "a@b.com"}}`)
	want := `{"find": ?, "filter": {"email": ?}}`
	if got != want {
		t.Errorf("Normalize(mongodb)\n got: %q\nwant: %q", got, want)
	}
}

func TestNormalizeTruncates(t *testing.T) {
	long := "SELECT " + string(make([]byte, 0))
	for i := 0; i < 500; i++ {
		long += "col_a, "
	}
	long += "col_z FROM t"

	got := NormalizeWithLimit("postgresql", long, 64)
	if len(got) > 64+4 {
		t.Errorf("expected truncation to ~64 chars, got %d", len(got))
	}
}

func TestNormalizeEmpty(t *testing.T) {
	if got := Normalize("postgresql", "   "); got != "" {
		t.Errorf("expected empty result, got %q", got)
	}
}

func BenchmarkNormalizeSQL(b *testing.B) {
	q := "SELECT u.id, u.name FROM users u JOIN orders o ON o.user_id = u.id " +
		"WHERE u.email = 'someone@example.com' AND o.total > 100.50 AND o.id IN (1,2,3,4,5) ORDER BY o.created_at DESC LIMIT 20"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Fingerprint(Normalize("postgresql", q))
	}
}
