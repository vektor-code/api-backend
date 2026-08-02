// Package sqlnorm sanitizes database query text and derives low-cardinality
// grouping keys from it.
//
// It serves two purposes at once:
//
//   - Redaction. The OpenTelemetry database conventions state that query text
//     should only be collected when literals have been replaced by a
//     placeholder, because literals routinely carry personal data. Normalize
//     performs that replacement.
//   - Aggregation. Grouping telemetry by raw query text is unbounded — every
//     distinct literal becomes its own series. Fingerprint and Summary give a
//     stable key and a human-readable label for the same query shape.
//
// The scanner is single-pass and allocation-light because it runs on every
// database span at ingest.
package sqlnorm

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// DefaultMaxLength bounds the normalized output. The conventions explicitly
// allow truncation, since sanitizing is not free and enormous generated
// statements carry no extra diagnostic value.
const DefaultMaxLength = 4096

// Placeholder is what every literal collapses to.
const Placeholder = "?"

// Normalize returns query with literals replaced by Placeholder, comments
// stripped, whitespace collapsed, and repeated value groups folded. The
// dialect is selected from the canonical system id; anything unrecognized is
// treated as SQL, which is the safe default.
func Normalize(system, query string) string {
	return NormalizeWithLimit(system, query, DefaultMaxLength)
}

// NormalizeWithLimit is Normalize with an explicit output cap. A limit <= 0
// disables truncation.
func NormalizeWithLimit(system, query string, maxLen int) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	var out string
	switch family(system) {
	case familyKeyValue:
		out = normalizeCommand(query)
	case familyDocument:
		out = normalizeDocument(query)
	default:
		out = normalizeSQL(query)
	}

	if maxLen > 0 && len(out) > maxLen {
		out = strings.TrimSpace(out[:maxLen]) + " ..."
	}
	return out
}

type dialectFamily int

const (
	familySQL dialectFamily = iota
	familyKeyValue
	familyDocument
)

// keyValueSystems and documentSystems are keyed by the canonical ids produced
// by the semconv package.
var keyValueSystems = map[string]bool{
	"redis": true, "valkey": true, "memcached": true, "hazelcast": true,
	"aerospike": true, "etcd": true,
}

var documentSystems = map[string]bool{
	"mongodb": true, "couchbase": true, "couchdb": true,
	"elasticsearch": true, "opensearch": true,
}

func family(system string) dialectFamily {
	s := strings.ToLower(strings.TrimSpace(system))
	switch {
	case keyValueSystems[s]:
		return familyKeyValue
	case documentSystems[s]:
		return familyDocument
	default:
		return familySQL
	}
}

// normalizeSQL scans the statement once, emitting identifiers and operators
// verbatim while collapsing every literal to a placeholder.
//
// Quoted identifiers keep their quoting characters. That is deliberate: the
// quote style ("x" vs `x` vs [x]) is the strongest dialect signal available
// when db.system is absent, and dropping it would destroy that signal.
func normalizeSQL(q string) string {
	var b strings.Builder
	b.Grow(len(q))

	prevIdentChar := false
	for i := 0; i < len(q); {
		c := q[i]

		switch {
		// Line comment: -- ... EOL
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' {
				i++
			}
			writeSpace(&b)
			prevIdentChar = false

		// Block comment: /* ... */  (also covers sqlcommenter metadata)
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			i += 2
			for i+1 < len(q) && !(q[i] == '*' && q[i+1] == '/') {
				i++
			}
			if i+1 < len(q) {
				i += 2
			} else {
				i = len(q)
			}
			writeSpace(&b)
			prevIdentChar = false

		// Single-quoted string literal, with '' and \' escapes.
		case c == '\'':
			i++
			for i < len(q) {
				if q[i] == '\\' && i+1 < len(q) {
					i += 2
					continue
				}
				if q[i] == '\'' {
					if i+1 < len(q) && q[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			b.WriteString(Placeholder)
			prevIdentChar = false

		// Dollar-quoted string ($$...$$ / $tag$...$tag$), PostgreSQL.
		// Distinguished from $1 positional parameters by the closing '$'.
		case c == '$' && isDollarQuoteStart(q, i):
			tagEnd := strings.IndexByte(q[i+1:], '$')
			tag := q[i : i+1+tagEnd+1]
			rest := q[i+len(tag):]
			if end := strings.Index(rest, tag); end >= 0 {
				i += len(tag) + end + len(tag)
			} else {
				i = len(q)
			}
			b.WriteString(Placeholder)
			prevIdentChar = false

		// Positional / named bind parameters are already placeholders.
		case c == '$' && i+1 < len(q) && isDigit(q[i+1]):
			i++
			for i < len(q) && isDigit(q[i]) {
				i++
			}
			b.WriteString(Placeholder)
			prevIdentChar = false

		case (c == ':' || c == '@') && i+1 < len(q) && isIdentStart(q[i+1]) && !prevIdentChar:
			i++
			for i < len(q) && isIdentChar(q[i]) {
				i++
			}
			b.WriteString(Placeholder)
			prevIdentChar = false

		case c == '?':
			i++
			b.WriteString(Placeholder)
			prevIdentChar = false

		// Quoted identifiers are preserved verbatim.
		case c == '"' || c == '`':
			quote := c
			b.WriteByte(c)
			i++
			for i < len(q) {
				b.WriteByte(q[i])
				if q[i] == quote {
					i++
					break
				}
				i++
			}
			prevIdentChar = true

		case c == '[':
			b.WriteByte(c)
			i++
			for i < len(q) {
				b.WriteByte(q[i])
				if q[i] == ']' {
					i++
					break
				}
				i++
			}
			prevIdentChar = true

		// Hexadecimal literal.
		case c == '0' && i+1 < len(q) && (q[i+1] == 'x' || q[i+1] == 'X') && !prevIdentChar:
			i += 2
			for i < len(q) && isHexDigit(q[i]) {
				i++
			}
			b.WriteString(Placeholder)
			prevIdentChar = false

		// Numeric literal — only when it does not continue an identifier, so
		// utf8mb4 and column1 survive intact.
		case isDigit(c) && !prevIdentChar:
			i = skipNumber(q, i)
			b.WriteString(Placeholder)
			prevIdentChar = false

		case c == '.' && i+1 < len(q) && isDigit(q[i+1]) && !prevIdentChar:
			i = skipNumber(q, i)
			b.WriteString(Placeholder)
			prevIdentChar = false

		case isSpace(c):
			for i < len(q) && isSpace(q[i]) {
				i++
			}
			writeSpace(&b)
			prevIdentChar = false

		case isIdentChar(c):
			start := i
			for i < len(q) && isIdentChar(q[i]) {
				i++
			}
			word := q[start:i]
			if isBoolLiteral(word) {
				b.WriteString(Placeholder)
				prevIdentChar = false
			} else {
				b.WriteString(word)
				prevIdentChar = true
			}

		default:
			b.WriteByte(c)
			i++
			prevIdentChar = false
		}
	}

	return collapseGroups(strings.TrimSpace(b.String()))
}

// collapseGroups folds repeated placeholder groups so that an IN clause or a
// multi-row INSERT does not produce a different fingerprint for every batch
// size: IN (?, ?, ?) becomes IN (?), and VALUES (?, ?), (?, ?) becomes
// VALUES (?).
func collapseGroups(q string) string {
	var b strings.Builder
	b.Grow(len(q))

	for i := 0; i < len(q); {
		if q[i] != '(' {
			b.WriteByte(q[i])
			i++
			continue
		}
		end, ok := placeholderGroupEnd(q, i)
		if !ok {
			b.WriteByte(q[i])
			i++
			continue
		}
		b.WriteString("(" + Placeholder + ")")
		i = end + 1
		// Fold any immediately following ", (?, ?)" groups into the same one.
		for {
			j := i
			for j < len(q) && isSpace(q[j]) {
				j++
			}
			if j >= len(q) || q[j] != ',' {
				break
			}
			j++
			for j < len(q) && isSpace(q[j]) {
				j++
			}
			if j >= len(q) || q[j] != '(' {
				break
			}
			nextEnd, ok := placeholderGroupEnd(q, j)
			if !ok {
				break
			}
			i = nextEnd + 1
		}
	}
	return b.String()
}

// placeholderGroupEnd reports the index of the ')' closing a parenthesised list
// that contains nothing but placeholders, commas and whitespace.
func placeholderGroupEnd(q string, open int) (int, bool) {
	seenPlaceholder := false
	for i := open + 1; i < len(q); i++ {
		switch {
		case q[i] == ')':
			return i, seenPlaceholder
		case q[i] == '?':
			seenPlaceholder = true
		case q[i] == ',' || isSpace(q[i]):
		default:
			return 0, false
		}
	}
	return 0, false
}

// normalizeCommand handles key-value protocols, where the first token is the
// command and everything after it is data: "GET user:42" -> "GET ?".
func normalizeCommand(q string) string {
	fields := strings.Fields(q)
	if len(fields) == 0 {
		return ""
	}
	cmd := strings.ToUpper(fields[0])
	if len(fields) == 1 {
		return cmd
	}
	return cmd + " " + Placeholder
}

// normalizeDocument scrubs values out of JSON-shaped query bodies while keeping
// the key structure, which is what identifies the query shape.
func normalizeDocument(q string) string {
	var b strings.Builder
	b.Grow(len(q))

	expectingValue := false
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == '"':
			start := i
			i++
			for i < len(q) {
				if q[i] == '\\' && i+1 < len(q) {
					i += 2
					continue
				}
				if q[i] == '"' {
					i++
					break
				}
				i++
			}
			if expectingValue {
				b.WriteString(Placeholder)
				expectingValue = false
			} else {
				b.WriteString(q[start:i])
			}

		case c == ':':
			b.WriteByte(c)
			i++
			expectingValue = true

		case c == ',':
			b.WriteByte(c)
			i++
			expectingValue = false

		case c == '{' || c == '[':
			b.WriteByte(c)
			i++
			expectingValue = false

		case c == '}' || c == ']':
			b.WriteByte(c)
			i++
			expectingValue = false

		case isSpace(c):
			for i < len(q) && isSpace(q[i]) {
				i++
			}
			writeSpace(&b)

		case isDigit(c) || c == '-':
			if expectingValue {
				i = skipNumber(q, i)
				b.WriteString(Placeholder)
				expectingValue = false
			} else {
				b.WriteByte(c)
				i++
			}

		default:
			start := i
			for i < len(q) && isIdentChar(q[i]) {
				i++
			}
			if i == start {
				b.WriteByte(c)
				i++
				continue
			}
			if expectingValue {
				b.WriteString(Placeholder)
				expectingValue = false
			} else {
				b.WriteString(q[start:i])
			}
		}
	}
	return strings.TrimSpace(b.String())
}

// Fingerprint returns a stable 64-bit hash of a normalized query, suitable as a
// GROUP BY key. It is case-insensitive so that SELECT and select fold together.
func Fingerprint(normalized string) string {
	if normalized == "" {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(normalized)))
	return strconv.FormatUint(h.Sum64(), 16)
}

// Operation returns the leading verb of a statement — SELECT, INSERT, HGET —
// uppercased. For CTEs it looks past the WITH clause for the statement the CTE
// feeds, since that is what actually executes.
func Operation(system, query string) string {
	q := strings.TrimSpace(query)
	if q == "" {
		return ""
	}
	if family(system) == familyKeyValue {
		if fields := strings.Fields(q); len(fields) > 0 {
			return strings.ToUpper(fields[0])
		}
		return ""
	}

	fields := strings.Fields(q)
	if len(fields) == 0 {
		return ""
	}
	first := strings.ToUpper(strings.Trim(fields[0], "(;"))
	if first != "WITH" {
		if sqlVerbs[first] {
			return first
		}
		return ""
	}
	// A CTE body is parenthesised; the driving statement is the first verb that
	// appears at paren depth zero after it.
	depth := 0
	for _, f := range fields[1:] {
		depth += strings.Count(f, "(") - strings.Count(f, ")")
		if depth > 0 {
			continue
		}
		if v := strings.ToUpper(strings.Trim(f, "(),;")); sqlVerbs[v] {
			return v
		}
	}
	return "SELECT"
}

var sqlVerbs = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true,
	"UPSERT": true, "REPLACE": true, "CREATE": true, "DROP": true, "ALTER": true,
	"TRUNCATE": true, "CALL": true, "EXEC": true, "EXECUTE": true, "GRANT": true,
	"REVOKE": true, "BEGIN": true, "COMMIT": true, "ROLLBACK": true, "SET": true,
	"SHOW": true, "EXPLAIN": true, "ANALYZE": true, "VACUUM": true, "COPY": true,
	"USE": true, "DESCRIBE": true, "WITH": true,
}

// collectionKeywords are the tokens that introduce a table name, mapped to
// whether the name follows immediately.
var collectionKeywords = map[string]bool{
	"FROM": true, "INTO": true, "UPDATE": true, "JOIN": true, "TABLE": true,
}

// Collection returns the primary table or collection a statement targets.
// It returns "" when the target is a subquery or otherwise not a plain name,
// rather than guessing.
func Collection(system, query string) string {
	if family(system) != familySQL {
		return ""
	}
	fields := strings.Fields(query)
	for i, f := range fields {
		key := strings.ToUpper(strings.Trim(f, "(),;"))
		if !collectionKeywords[key] || i+1 >= len(fields) {
			continue
		}
		candidate := strings.Trim(fields[i+1], "(),;")
		candidate = strings.Trim(candidate, "\"`[]")
		if candidate == "" || !isIdentStart(candidate[0]) {
			continue
		}
		if sqlVerbs[strings.ToUpper(candidate)] {
			continue
		}
		return candidate
	}
	return ""
}

// Summary builds db.query.summary: a short, low-cardinality label for the query
// shape, such as "SELECT orders" or "HGET". It never contains literals.
func Summary(system, query string) string {
	op := Operation(system, query)
	if op == "" {
		return ""
	}
	if coll := Collection(system, query); coll != "" {
		return op + " " + coll
	}
	return op
}

func writeSpace(b *strings.Builder) {
	if b.Len() == 0 {
		return
	}
	if b.String()[b.Len()-1] == ' ' {
		return
	}
	b.WriteByte(' ')
}

func skipNumber(q string, i int) int {
	for i < len(q) && (isDigit(q[i]) || q[i] == '.') {
		i++
	}
	// Scientific notation: 1.5e-10
	if i < len(q) && (q[i] == 'e' || q[i] == 'E') {
		j := i + 1
		if j < len(q) && (q[j] == '+' || q[j] == '-') {
			j++
		}
		if j < len(q) && isDigit(q[j]) {
			for j < len(q) && isDigit(q[j]) {
				j++
			}
			i = j
		}
	}
	return i
}

func isDollarQuoteStart(q string, i int) bool {
	rest := q[i+1:]
	end := strings.IndexByte(rest, '$')
	if end < 0 {
		return false
	}
	for k := 0; k < end; k++ {
		if !isIdentChar(rest[k]) {
			return false
		}
	}
	return true
}

func isBoolLiteral(word string) bool {
	switch strings.ToUpper(word) {
	case "TRUE", "FALSE", "NULL":
		return true
	}
	return false
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentChar(c byte) bool { return isIdentStart(c) || isDigit(c) }

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
