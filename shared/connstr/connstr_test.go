package connstr

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Info
	}{
		{
			name: "postgres url with credentials",
			in:   "postgresql://appuser:s3cret@pg.prod.svc:5432/orders?sslmode=require",
			want: Info{Host: "pg.prod.svc", Port: "5432", Database: "orders"},
		},
		{
			name: "password containing an at sign",
			in:   "postgres://user:p@ss@db.internal:5432/app",
			want: Info{Host: "db.internal", Port: "5432", Database: "app"},
		},
		{
			name: "mysql url without port",
			in:   "mysql://user:pw@mysql.default.svc/inventory",
			want: Info{Host: "mysql.default.svc", Database: "inventory"},
		},
		{
			name: "url without database",
			in:   "redis://cache.prod.svc:6379",
			want: Info{Host: "cache.prod.svc", Port: "6379"},
		},
		{
			name: "ipv6 host",
			in:   "postgresql://[2001:db8::1]:5432/app",
			want: Info{Host: "2001:db8::1", Port: "5432", Database: "app"},
		},
		{
			name: "jdbc postgres",
			in:   "jdbc:postgresql://pg-host:5432/billing",
			want: Info{Host: "pg-host", Port: "5432", Database: "billing"},
		},
		{
			name: "jdbc sqlserver with property database",
			in:   "jdbc:sqlserver://sql-host:1433;databaseName=Reporting;encrypt=true",
			want: Info{Host: "sql-host", Port: "1433", Database: "Reporting"},
		},
		{
			name: "jdbc oracle thin with SID",
			in:   "jdbc:oracle:thin:@ora-host:1521:ORCL",
			want: Info{Host: "ora-host", Port: "1521", Database: "ORCL"},
		},
		{
			name: "jdbc oracle thin with service name",
			in:   "jdbc:oracle:thin:@//ora-host:1521/PRODSVC",
			want: Info{Host: "ora-host", Port: "1521", Database: "PRODSVC"},
		},
		{
			name: "pdo dsn",
			in:   "mysql:host=10.254.5.30;dbname=emuhasibatliq_dev;charset=utf8",
			want: Info{Host: "10.254.5.30", Database: "emuhasibatliq_dev"},
		},
		{
			name: "pdo dsn with port",
			in:   "pgsql:host=pg;port=5432;dbname=app",
			want: Info{Host: "pg", Port: "5432", Database: "app"},
		},
		{
			name: "libpq space separated",
			in:   "host=10.0.0.1 port=5432 dbname=app user=postgres password=x",
			want: Info{Host: "10.0.0.1", Port: "5432", Database: "app"},
		},
		{
			name: "ado style with comma port",
			in:   "Server=sql-host,1433;Initial Catalog=Warehouse;User Id=sa;Password=x",
			want: Info{Host: "sql-host", Port: "1433", Database: "Warehouse"},
		},
		{
			name: "empty",
			in:   "",
			want: Info{},
		},
		{
			name: "garbage does not panic",
			in:   "://///",
			want: Info{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Parse(tc.in)
			if got != tc.want {
				t.Errorf("Parse(%q)\n got: %+v\nwant: %+v", tc.in, got, tc.want)
			}
		})
	}
}

// Credentials must never survive parsing.
func TestParseDropsCredentials(t *testing.T) {
	got := Parse("postgresql://admin:hunter2@pg:5432/app")
	if got.Host != "pg" || got.Database != "app" {
		t.Fatalf("unexpected parse: %+v", got)
	}
	for _, field := range []string{got.Host, got.Port, got.Database} {
		if field == "admin" || field == "hunter2" {
			t.Errorf("credential leaked into result: %+v", got)
		}
	}
}
