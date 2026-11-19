package postgresqlbun

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// Connector owns one pool shared by all Query endpoints in this service.
// It uses the existing managed-connector lifecycle; it has no source admission.
type Connector struct {
	id               int
	name             string
	db               *bun.DB
	operationTimeout time.Duration
}

var _ runtime.ManagedDataConnector = (*Connector)(nil)

// NewConnector transfers ownership of db to the managed connector on success.
// A service infrastructure maker can configure Bun hooks and the native pool
// before passing it here. Registration remains the generated service's job.
// On failure the caller retains ownership. No connection is opened here;
// Start checks connectivity and Stop closes the pool after graph shutdown.
func NewConnector(cfg *config.PostgreSQLBunDataConnectorConfig, db *bun.DB) (*Connector, error) {
	if cfg == nil || cfg.ID <= 0 || cfg.OperationTimeoutMs < 0 || db == nil {
		return nil, fmt.Errorf("PostgreSQL connector requires a positive id, nonnegative operation timeout and a pool")
	}
	return &Connector{id: cfg.ID, name: cfg.Name, db: db, operationTimeout: time.Duration(cfg.OperationTimeoutMs) * time.Millisecond}, nil
}

func (c *Connector) GetID() int                           { return c.id }
func (c *Connector) GetName() string                      { return c.name }
func (c *Connector) OperationTimeout() time.Duration      { return c.operationTimeout }
func (c *Connector) DB() *bun.DB                          { return c.db }
func (c *Connector) Start(ctx context.Context) error      { return c.db.PingContext(ctx) }
func (c *Connector) StartAdmission(context.Context) error { return nil }
func (c *Connector) StopAdmission(context.Context)        {}
func (c *Connector) Stop(context.Context)                 { _ = c.db.Close() }

// driverConnector uses pgx's native context cancellation with Bun's SQL builder.
// Do not expose parse errors: they may include credentials from the DSN.
func driverConnector(cfg *config.PostgreSQLBunDataConnectorConfig) (driver.Connector, error) {
	parsed, err := pgx.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL connection configuration")
	}
	if cfg.ConnectTimeoutMs > 0 {
		parsed.ConnectTimeout = time.Duration(cfg.ConnectTimeoutMs) * time.Millisecond
	}
	return stdlib.GetConnector(*parsed), nil
}

// MakeConnector runs during graph construction, before lifecycle startup.
// Repeated endpoints reuse the registered pool. This does not create tables.
func MakeConnector(env runtime.RuntimeEnvironment, cfg *config.PostgreSQLBunDataConnectorConfig) (*Connector, error) {
	if cfg == nil || cfg.ID <= 0 || cfg.ConnectionString == "" || cfg.ConnectionsCount <= 0 || cfg.ConnectTimeoutMs < 0 || cfg.OperationTimeoutMs < 0 {
		return nil, fmt.Errorf("PostgreSQL connector requires id, connectionString, positive connectionsCount and nonnegative timeouts")
	}
	if existing := env.GetManagedDataConnector(cfg.ID); existing != nil {
		connector, ok := existing.(*Connector)
		if !ok {
			return nil, fmt.Errorf("connector %d already has another runtime type", cfg.ID)
		}
		return connector, nil
	}
	driver, err := driverConnector(cfg)
	if err != nil {
		return nil, err
	}
	pool := sql.OpenDB(driver)
	pool.SetMaxOpenConns(cfg.ConnectionsCount)
	pool.SetMaxIdleConns(cfg.ConnectionsCount)
	db := bun.NewDB(pool, pgdialect.New())
	connector, err := NewConnector(cfg, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	env.AddManagedDataConnector(connector)
	return connector, nil
}
