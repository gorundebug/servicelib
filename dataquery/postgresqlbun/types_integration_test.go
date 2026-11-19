package postgresqlbun

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type nativeRecord struct {
	ID       string
	Optional sql.NullString
	Amount   string
	Large    int64
	Instant  time.Time
	UUID     string
	Body     []byte
	Document []byte
}
type nativeRecordStream struct {
	runtime.TypedQueryStream[string, nativeRecord, error]
	values   []nativeRecord
	failures queryTestErrors
}

func (*nativeRecordStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment {
	return noopQueryEnvironment{}
}
func (s *nativeRecordStream) SetQueryConsumer(runtime.Consumer[string]) {}
func (s *nativeRecordStream) ConsumeResult(_ context.Context, value nativeRecord) {
	s.values = append(s.values, value)
}
func (s *nativeRecordStream) GetErrorStream() runtime.TypedConsumedStream[error] { return &s.failures }

type nativeRecordHandler struct {
	db                   *bun.DB
	raw, returning, fail bool
	ended                int
	outcome              error
	released             bool
}

func (*nativeRecordHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, int, error) {
	return ctx, 0, nil
}
func (h *nativeRecordHandler) BuildQuery(_ context.Context, _ int, id string, db *bun.DB) (RowsQuery, error) {
	if h.returning {
		return RawRows(db, "INSERT INTO query_type_values SELECT ?, optional, amount, large, instant, uuid, body, document FROM query_type_values WHERE id = ? RETURNING *", id, "bound-'key"), nil
	}
	if h.raw {
		return RawRows(db, "SELECT * FROM query_type_values WHERE id = ?", id), nil
	}
	return db.NewSelect().Table("query_type_values").Column("*").Where("id = ?", id), nil
}
func (h *nativeRecordHandler) HandleRow(ctx context.Context, _ int, rows *sql.Rows, _ *bun.DB, out runtime.Collect[nativeRecord]) error {
	var value nativeRecord
	if err := rows.Scan(&value.ID, &value.Optional, &value.Amount, &value.Large, &value.Instant, &value.UUID, &value.Body, &value.Document); err != nil {
		return err
	}
	out.Out(ctx, value)
	if h.fail {
		return errors.New("intentional mapper failure")
	}
	return nil
}
func (h *nativeRecordHandler) EndRequest(_ context.Context, _ runtime.Stream, _ int, err error, _ runtime.Collect[nativeRecord], _ runtime.Collect[error]) {
	h.ended++
	h.outcome = err
	h.released = h.db.Stats().InUse == 0
}

func TestPostgreSQLNativeTypesBuilderRawAndReturning(t *testing.T) {
	dsn := os.Getenv("SA_QUERY_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	driver, err := driverConnector(&config.PostgreSQLBunDataConnectorConfig{ConnectionString: dsn})
	require.NoError(t, err)
	pool := sql.OpenDB(driver)
	pool.SetMaxOpenConns(1)
	db := bun.NewDB(pool, pgdialect.New())
	defer db.Close()
	ctx := t.Context()
	_, err = db.ExecContext(ctx, "CREATE TEMP TABLE query_type_values (id text PRIMARY KEY, optional text, amount numeric(38,18), large bigint, instant timestamptz, uuid uuid, body bytea, document jsonb)")
	require.NoError(t, err)
	instant := time.Date(2026, 11, 19, 12, 34, 56, 123456000, time.UTC)
	amount := "12345678901234567890.123456789012345678"
	uuid := "12345678-1234-5678-1234-567812345678"
	document := `{"value":[1,null,"quoted\""]}`
	_, err = db.ExecContext(ctx, "INSERT INTO query_type_values VALUES (?,NULL,?,?,?,?,?,?)", "bound-'key", amount, int64(9223372036854775807), instant, uuid, []byte{97, 0, 255}, document)
	require.NoError(t, err)
	for _, mode := range []string{"builder", "raw", "returning", "returning-failure"} {
		t.Run(mode, func(t *testing.T) {
			h := &nativeRecordHandler{db: db, raw: mode == "raw", returning: mode == "returning" || mode == "returning-failure", fail: mode == "returning-failure"}
			stream := &nativeRecordStream{}
			consumer, err := MakeRowsEndpointConsumer[int, string, nativeRecord, error](stream, db, h)
			require.NoError(t, err)
			key := "bound-'key"
			if h.returning {
				key = mode
			}
			consumer.Consume(ctx, key)
			require.Equal(t, 1, h.ended)
			require.True(t, h.released)
			if h.fail {
				require.ErrorContains(t, h.outcome, "intentional mapper failure")
			} else {
				require.NoError(t, h.outcome)
			}
			require.Len(t, stream.values, 1)
			v := stream.values[0]
			require.Equal(t, key, v.ID)
			require.False(t, v.Optional.Valid)
			require.Equal(t, amount, v.Amount)
			require.Equal(t, int64(9223372036854775807), v.Large)
			require.True(t, instant.Equal(v.Instant))
			require.Equal(t, uuid, v.UUID)
			require.Equal(t, []byte{97, 0, 255}, v.Body)
			require.JSONEq(t, document, string(v.Document))
			var count int
			require.NoError(t, db.NewSelect().Table("query_type_values").ColumnExpr("count(*)").Where("id = ?", key).Scan(ctx, &count))
			require.Equal(t, 1, count, "autocommitted write must not be replayed")
		})
	}
}
