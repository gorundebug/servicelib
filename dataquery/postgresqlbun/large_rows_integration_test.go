package postgresqlbun

import (
	"context"
	"database/sql"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type gatedRowsStream struct {
	queryTestStream
	entered chan struct{}
	release chan struct{}
	count   atomic.Int64
	wrong   atomic.Bool
}

func (s *gatedRowsStream) ConsumeResult(ctx context.Context, value int) {
	if value == -1 {
		return
	}
	n := s.count.Add(1)
	if int64(value) != n {
		s.wrong.Store(true)
	}
	if n == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	}
}
func TestLargeRowsWaitForResultConsumer(t *testing.T) {
	dsn := os.Getenv("SA_QUERY_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	driver, err := driverConnector(&config.PostgreSQLBunDataConnectorConfig{ConnectionString: dsn})
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(driver)
	pool.SetMaxOpenConns(1)
	db := bun.NewDB(pool, pgdialect.New())
	defer db.Close()
	stream := &gatedRowsStream{entered: make(chan struct{}), release: make(chan struct{})}
	handler := &queryTestRows{queryTestLifecycle: queryTestLifecycle{db: db}}
	consumer, err := MakeRowsEndpointConsumer[int, int, int, error](stream, db, handler)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); consumer.Consume(ctx, 10000) }()
	defer func() { cancel(); <-done }()
	select {
	case <-stream.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-done:
		t.Fatal("Query completed while result consumer was blocked")
	case <-time.After(20 * time.Millisecond):
	}
	if stream.count.Load() != 1 {
		t.Fatal("Query delivered beyond blocked result")
	}
	close(stream.release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if stream.count.Load() != 10000 || stream.wrong.Load() {
		t.Fatal("lost or reordered rows")
	}
	if handler.ends != 1 || handler.err != nil || !handler.released {
		t.Fatalf("finalization: %+v", handler.queryTestLifecycle)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}
