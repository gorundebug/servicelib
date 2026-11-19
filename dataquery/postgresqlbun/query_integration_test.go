package postgresqlbun

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type queryTestErrors struct {
	runtime.TypedConsumedStream[error]
	values []error
}

func (s *queryTestErrors) Consume(_ context.Context, err error) { s.values = append(s.values, err) }

type queryTestStream struct {
	runtime.TypedQueryStream[int, int, error]
	values   []int
	failures queryTestErrors
}

func (s *queryTestStream) SetQueryConsumer(runtime.Consumer[int]) {}
func (s *queryTestStream) ConsumeResult(_ context.Context, value int) {
	s.values = append(s.values, value)
}
func (s *queryTestStream) GetErrorStream() runtime.TypedConsumedStream[error] { return &s.failures }

type queryTestLifecycle struct {
	ends     int
	err      error
	db       *bun.DB
	released bool
}

func (h *queryTestLifecycle) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, int, error) {
	return ctx, 1, nil
}
func (h *queryTestLifecycle) EndRequest(ctx context.Context, _ runtime.Stream, state int, err error, out runtime.Collect[int], failures runtime.Collect[error]) {
	h.ends++
	h.err = err
	h.released = h.db.Stats().InUse == 0
	if err != nil {
		failures.Out(ctx, err)
	}
	out.Out(ctx, -1) // Application completion uses the ordinary result collector.
}

type queryTestRows struct {
	queryTestLifecycle
	cancel   context.CancelFunc
	mapError error
	invalid  bool
}

func (h *queryTestRows) BuildQuery(_ context.Context, _ int, count int, db *bun.DB) (RowsQuery, error) {
	if h.invalid {
		return RawRows(db, "SELECT missing_column FROM generate_series(1, 1)"), nil
	}
	return RawRows(db, "SELECT generate_series(1, ?)", count), nil
}
func (h *queryTestRows) HandleRow(ctx context.Context, _ int, rows *sql.Rows, db *bun.DB, out runtime.Collect[int]) error {
	var value int
	if err := rows.Scan(&value); err != nil {
		return err
	}
	out.Out(ctx, value)
	if h.cancel != nil {
		h.cancel()
	}
	return h.mapError
}

type queryTestExec struct {
	queryTestLifecycle
	statement string
}

func (h *queryTestExec) BuildQuery(_ context.Context, _ int, value int, db *bun.DB) (ExecQuery, error) {
	return db.NewRaw(h.statement, value), nil
}
func (h *queryTestExec) HandleResult(ctx context.Context, _ int, result sql.Result, out runtime.Collect[int]) error {
	count, err := result.RowsAffected()
	if err == nil {
		out.Out(ctx, int(count))
	}
	return err
}

// Opt in with a dedicated disposable PostgreSQL database, never a working DB.
func TestPostgreSQLQueryIntegration(t *testing.T) {
	dsn := os.Getenv("SA_QUERY_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SA_QUERY_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	driver, err := driverConnector(&config.PostgreSQLBunDataConnectorConfig{ConnectionString: dsn})
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(driver)
	pool.SetMaxOpenConns(1)
	db := bun.NewDB(pool, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	mappingFailure := errors.New("mapping failed")
	for _, name := range []string{"empty", "many", "partial mapping failure", "cancel", "sql error"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			h := &queryTestRows{queryTestLifecycle: queryTestLifecycle{db: db}}
			count, expected := 3, 3
			switch name {
			case "empty":
				count, expected = 0, 0
			case "partial mapping failure":
				h.mapError, expected = mappingFailure, 1
			case "cancel":
				h.cancel, expected = cancel, 1
			case "sql error":
				h.invalid, expected = true, 0
			}
			stream := &queryTestStream{}
			consumer, err := MakeRowsEndpointConsumer[int, int, int, error](stream, db, h)
			if err != nil {
				t.Fatal(err)
			}
			consumer.Consume(ctx, count)
			if h.ends != 1 || !h.released {
				t.Fatalf("ends=%d pool released=%v", h.ends, h.released)
			}
			if len(stream.values) != expected+1 || stream.values[len(stream.values)-1] != -1 {
				t.Fatalf("results %v", stream.values)
			}
			if name == "partial mapping failure" && !errors.Is(h.err, mappingFailure) {
				t.Fatalf("lost mapping error: %v", h.err)
			}
			if name == "cancel" && !errors.Is(h.err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", h.err)
			}
			if name == "sql error" && h.err == nil {
				t.Fatal("lost SQL error")
			}
			if (name == "empty" || name == "many") && h.err != nil {
				t.Fatal(h.err)
			}
		})
	}
	t.Run("execute", func(t *testing.T) {
		if _, err := db.ExecContext(t.Context(), "CREATE TEMP TABLE query_values (value integer)"); err != nil {
			t.Fatal(err)
		}
		h := &queryTestExec{queryTestLifecycle: queryTestLifecycle{db: db}, statement: "INSERT INTO query_values VALUES (?)"}
		stream := &queryTestStream{}
		consumer, err := MakeExecEndpointConsumer[int, int, int, error](stream, db, h)
		if err != nil {
			t.Fatal(err)
		}
		consumer.Consume(t.Context(), 42)
		if h.ends != 1 || h.err != nil || !h.released {
			t.Fatalf("end=%d error=%v released=%v", h.ends, h.err, h.released)
		}
		if len(stream.values) != 2 || stream.values[0] != 1 || stream.values[1] != -1 {
			t.Fatalf("results %v", stream.values)
		}
		var value int
		if err := db.QueryRowContext(t.Context(), "SELECT value FROM query_values").Scan(&value); err != nil || value != 42 {
			t.Fatalf("value=%d error=%v", value, err)
		}
	})
}

type noopQueryEnvironment struct{ runtime.RuntimeEnvironment }

func (noopQueryEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*queryTestStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
	return noopQueryEnvironment{}
}
