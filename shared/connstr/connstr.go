// Package connstr parses database connection strings.
//
// Two incompatible parsers used to exist for this: one in the agent (URL and
// JDBC forms) and one in the API backend (URL and DSN key=value forms), so the
// same connection string yielded a database name in one service and nothing in
// the other. This handles all the forms either of them did.
//
// Credentials are never returned. Callers want the host, port and database
// name; the user and password in a DSN are dropped on the floor.
package connstr

import "strings"

// Info is what a connection string tells us about its target.
type Info struct {
	Host     string
	Port     string
	Database string
}

// Parse accepts the connection-string shapes seen in the wild:
//
//	postgresql://user:pass@host:5432/dbname?sslmode=require
//	jdbc:postgresql://host:5432/dbname
//	jdbc:oracle:thin:@host:1521:SID
//	mysql:host=10.0.0.1;dbname=app;port=3306
//	Server=host,1433;Initial Catalog=app;User Id=sa
//	host=10.0.0.1 port=5432 dbname=app user=postgres
//
// Fields that cannot be determined are returned empty rather than guessed.
func Parse(connStr string) Info {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return Info{}
	}

	// JDBC wraps another form; unwrap it and re-parse.
	if rest, ok := cutPrefixFold(connStr, "jdbc:"); ok {
		return parseJDBC(rest)
	}
	if strings.Contains(connStr, "://") {
		return parseURL(connStr)
	}
	return parseKeyValue(connStr)
}

// parseURL handles scheme://[user[:pass]@]host[:port][/database][?params]
func parseURL(connStr string) Info {
	var info Info

	rest := connStr[strings.Index(connStr, "://")+3:]

	// Strip credentials. LastIndex, because a password may itself contain '@'.
	if at := strings.LastIndex(rest, "@"); at != -1 {
		rest = rest[at+1:]
	}

	authority := rest
	if slash := strings.Index(rest, "/"); slash != -1 {
		authority = rest[:slash]
		info.Database = trimQuery(rest[slash+1:])
	}
	// SQL Server appends its properties straight onto the authority with a
	// semicolon rather than a path separator.
	authority = trimQuery(authority)

	info.Host, info.Port = splitHostPort(authority)
	return info
}

// parseJDBC handles the jdbc: subforms, which differ per vendor.
func parseJDBC(rest string) Info {
	if strings.Contains(rest, "://") {
		info := parseURL(rest)
		// SQL Server puts its database in a semicolon property, not the path.
		if info.Database == "" {
			if db := lookupKey(rest, databaseKeys); db != "" {
				info.Database = db
			}
		}
		return info
	}

	// Oracle thin: jdbc:oracle:thin:@host:1521:SID or .../SERVICE
	if at := strings.Index(rest, "@"); at != -1 {
		target := strings.TrimPrefix(rest[at+1:], "//")
		if slash := strings.Index(target, "/"); slash != -1 {
			host, port := splitHostPort(target[:slash])
			return Info{Host: host, Port: port, Database: trimQuery(target[slash+1:])}
		}
		parts := strings.Split(target, ":")
		switch len(parts) {
		case 3:
			return Info{Host: parts[0], Port: parts[1], Database: trimQuery(parts[2])}
		case 2:
			return Info{Host: parts[0], Port: parts[1]}
		case 1:
			return Info{Host: parts[0]}
		}
	}

	return parseKeyValue(rest)
}

// parseKeyValue handles DSN and ADO-style property lists, where separators may
// be semicolons, commas or spaces.
func parseKeyValue(connStr string) Info {
	// A vendor prefix such as "mysql:" precedes the properties.
	if colon := strings.Index(connStr, ":"); colon != -1 {
		if eq := strings.Index(connStr, "="); eq == -1 || colon < eq {
			connStr = connStr[colon+1:]
		}
	}

	info := Info{
		Host:     lookupKey(connStr, hostKeys),
		Port:     lookupKey(connStr, portKeys),
		Database: lookupKey(connStr, databaseKeys),
	}

	// "Server=host,1433" is the SQL Server spelling of host:port.
	if info.Port == "" && info.Host != "" {
		if comma := strings.LastIndex(info.Host, ","); comma != -1 {
			if port := info.Host[comma+1:]; isAllDigits(port) {
				info.Port = port
				info.Host = info.Host[:comma]
			}
		}
	}
	if info.Port == "" && info.Host != "" {
		info.Host, info.Port = splitHostPort(info.Host)
	}
	return info
}

var (
	hostKeys     = []string{"host", "server", "data source", "hostname", "addr", "address"}
	portKeys     = []string{"port"}
	databaseKeys = []string{"dbname", "database", "databasename", "initial catalog", "db"}
)

// lookupKey finds the value of the first matching property name.
func lookupKey(s string, keys []string) string {
	normalized := strings.ReplaceAll(s, ";", "\n")

	for _, want := range keys {
		for _, field := range splitProperties(normalized) {
			eq := strings.Index(field, "=")
			if eq == -1 {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(field[:eq]))
			key = strings.ReplaceAll(key, "_", " ")
			if key != want {
				continue
			}
			value := strings.TrimSpace(field[eq+1:])
			value = strings.Trim(value, `"'`)
			if value != "" {
				return trimQuery(value)
			}
		}
	}
	return ""
}

// splitProperties splits on newlines first, then whitespace, so both
// "a=1;b=2" and "a=1 b=2" work. Values containing spaces are preserved when
// they are semicolon-delimited, which is the common case for them.
func splitProperties(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Count(line, "=") <= 1 {
			out = append(out, line)
			continue
		}
		out = append(out, strings.Fields(line)...)
	}
	return out
}

// splitHostPort separates a trailing :port and unwraps IPv6 brackets.
func splitHostPort(authority string) (string, string) {
	authority = strings.TrimSpace(authority)
	if authority == "" {
		return "", ""
	}
	if strings.HasPrefix(authority, "[") {
		if end := strings.Index(authority, "]"); end > 0 {
			host := authority[1:end]
			if rest := authority[end+1:]; strings.HasPrefix(rest, ":") {
				return host, rest[1:]
			}
			return host, ""
		}
	}
	if colon := strings.LastIndex(authority, ":"); colon != -1 {
		if port := authority[colon+1:]; isAllDigits(port) {
			return authority[:colon], port
		}
	}
	return authority, ""
}

// trimQuery drops query strings, fragments and trailing property lists.
func trimQuery(s string) string {
	for _, sep := range []string{"?", "#", ";", " ", "/"} {
		if idx := strings.Index(s, sep); idx != -1 {
			s = s[:idx]
		}
	}
	return strings.TrimSpace(s)
}

func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
