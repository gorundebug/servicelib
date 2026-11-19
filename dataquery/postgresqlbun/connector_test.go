package postgresqlbun

import (
	"context"
	"database/sql"
	"errors"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"strings"
	"testing"
	"time"
)

type testEnvironment struct {
	runtime.RuntimeEnvironment
	connectors map[int]runtime.ManagedDataConnector
}

func TestMakerSuppliedPoolOwnership(t *testing.T) {
	cfg := &config.PostgreSQLBunDataConnectorConfig{ID: 7, Name: "custom", OperationTimeoutMs: 123}
	driver, err := driverConnector(&config.PostgreSQLBunDataConnectorConfig{ConnectionString: "postgres://test:test@127.0.0.1:1/test?sslmode=disable"})
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(driver)
	pool.SetMaxOpenConns(11)
	db := bun.NewDB(pool, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })
	if _, err := NewConnector(nil, db); err == nil {
		t.Fatal("invalid config accepted")
	}
	if _, err := NewConnector(cfg, nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	connector, err := NewConnector(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	if connector.DB() != db || db.Stats().MaxOpenConnections != 11 || connector.OperationTimeout() != 123*time.Millisecond {
		t.Fatal("maker's pool or configuration replaced")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := connector.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup lost context: %v", err)
	}
	connector.Stop(t.Context())
	if err := pool.PingContext(t.Context()); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("managed stop did not close supplied pool: %v", err)
	}
}

func (e *testEnvironment) GetManagedDataConnector(id int) runtime.ManagedDataConnector {
	return e.connectors[id]
}
func (e *testEnvironment) AddManagedDataConnector(c runtime.ManagedDataConnector) {
	e.connectors[c.GetID()] = c
}

func TestConnectorPoolAndLifecycle(t *testing.T) {
	env := &testEnvironment{connectors: map[int]runtime.ManagedDataConnector{}}
	cfg := &config.PostgreSQLBunDataConnectorConfig{ID: 1, Name: "catalog", ConnectionString: "postgres://test:test@127.0.0.1:1/test?sslmode=disable", ConnectionsCount: 3}
	connector, err := MakeConnector(env, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Stop(t.Context())
	reused, err := MakeConnector(env, cfg)
	if err != nil || reused != connector {
		t.Fatalf("pool not reused: %v", err)
	}
	if connector.DB().Stats().MaxOpenConnections != 3 {
		t.Fatal("pool limit lost")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := connector.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	if err := connector.StartAdmission(t.Context()); err != nil {
		t.Fatal(err)
	}
	connector.StopAdmission(t.Context())
	document := config.DataConnectorConfigToAPI(cfg)
	if document.ConnectionString != nil || document.Implementations != nil {
		t.Fatal("credentials or redundant implementation selection exported")
	}
}

func TestInvalidDSNDoesNotPanicOrDiscloseCredentials(t *testing.T) {
	cfg := &config.PostgreSQLBunDataConnectorConfig{ConnectionString: "postgres://private-secret%zz@host/db"}
	driver, err := driverConnector(cfg)
	if driver != nil || err == nil {
		t.Fatal("invalid DSN accepted")
	}
	if strings.Contains(err.Error(), "private-secret") {
		t.Fatal("credentials leaked")
	}
}
