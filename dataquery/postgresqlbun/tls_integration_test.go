package postgresqlbun

import (
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"net/url"
	"os"
	"testing"
)

func TestPostgreSQLNativeTLS(t *testing.T) {
	dsn := os.Getenv("SA_QUERY_TLS_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("disposable PostgreSQL TLS fixture not configured")
	}
	badCA := os.Getenv("SA_QUERY_TLS_BAD_CA")
	if badCA == "" {
		t.Fatal("missing unrelated test CA")
	}
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "reject-untrusted-ca", true: "verified-tls"}[trusted], func(t *testing.T) {
			address, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			args := address.Query()
			args.Set("sslmode", "verify-full")
			if !trusted {
				args.Set("sslrootcert", badCA)
			}
			address.RawQuery = args.Encode()
			env := &testEnvironment{connectors: map[int]runtime.ManagedDataConnector{}}
			c, err := MakeConnector(env, &config.PostgreSQLBunDataConnectorConfig{ID: 1, ConnectionString: address.String(), ConnectionsCount: 1, ConnectTimeoutMs: 2000, OperationTimeoutMs: 3000})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Stop(t.Context())
			err = c.Start(t.Context())
			if !trusted {
				if err == nil {
					t.Fatal("untrusted CA accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var encrypted bool
			if err = c.DB().QueryRowContext(t.Context(), "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&encrypted); err != nil {
				t.Fatal(err)
			}
			if !encrypted {
				t.Fatal("connection silently downgraded to plaintext")
			}
		})
	}
}
