// Package postgresqlbun adapts Bun PostgreSQL operations to Query streams.
package postgresqlbun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/gorundebug/servicelib/dataquery"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/uptrace/bun"
)

// RowsQuery is satisfied by *bun.SelectQuery. RawRows provides the equivalent
// parameterized SQL path without assembling all rows in an intermediate slice.
type RowsQuery interface {
	Rows(context.Context) (*sql.Rows, error)
}

// ExecQuery is satisfied by Bun insert/update/delete/schema/raw query builders.
type ExecQuery interface {
	Exec(context.Context, ...any) (sql.Result, error)
}

type rawRows struct {
	db    *bun.DB
	query string
	args  []any
}

func (q rawRows) Rows(ctx context.Context) (*sql.Rows, error) {
	return q.db.QueryContext(ctx, q.query, q.args...)
}

// RawRows uses Bun's placeholders and escaping. SQL and arguments are separate;
// application code must never concatenate untrusted values into SQL text.
func RawRows(db *bun.DB, query string, args ...any) RowsQuery {
	return rawRows{db: db, query: query, args: append([]any(nil), args...)}
}

type RowsHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildQuery(ctx context.Context, state HandlerState, value T, db *bun.DB) (RowsQuery, error)
	// HandleRow can use db.ScanRow(ctx, rows, &model), then emit a mapped R.
	// It must scan only the current row; iteration and closing belong to SDK.
	HandleRow(ctx context.Context, state HandlerState, rows *sql.Rows, db *bun.DB, out runtime.Collect[R]) error
}

type ExecHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildQuery(ctx context.Context, state HandlerState, value T, db *bun.DB) (ExecQuery, error)
	HandleResult(ctx context.Context, state HandlerState, result sql.Result, out runtime.Collect[R]) error
}

// MakeRowsEndpointConsumer borrows a connector-owned pool. No new connection
// pool is created per request. Native rows close before EndRequest is called.
func MakeRowsEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], db *bun.DB, handler RowsHandler[HandlerState, T, R, E], options ...dataquery.Options) (runtime.Consumer[T], error) {
	if db == nil || handler == nil {
		return nil, fmt.Errorf("PostgreSQL Query requires database and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		query, err := handler.BuildQuery(ctx, state, value, db)
		if err != nil {
			return err
		}
		if query == nil {
			return fmt.Errorf("PostgreSQL Query builder returned no query")
		}
		rows, err := query.Rows(ctx)
		if err != nil {
			return err
		}
		if rows == nil {
			return fmt.Errorf("PostgreSQL Query returned no rows handle")
		}
		defer func() { err = errors.Join(err, rows.Close()) }()
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := handler.HandleRow(ctx, state, rows, db, out); err != nil {
				return err
			}
		}
		return rows.Err()
	}, options...)
}

// MakeExecEndpointConsumer executes once. Writes are never automatically
// replayed: a transport error may arrive after the database committed them.
func MakeExecEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], db *bun.DB, handler ExecHandler[HandlerState, T, R, E], options ...dataquery.Options) (runtime.Consumer[T], error) {
	if db == nil || handler == nil {
		return nil, fmt.Errorf("PostgreSQL Query requires database and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) error {
		query, err := handler.BuildQuery(ctx, state, value, db)
		if err != nil {
			return err
		}
		if query == nil {
			return fmt.Errorf("PostgreSQL Query builder returned no query")
		}
		result, err := query.Exec(ctx)
		if err != nil {
			return err
		}
		return handler.HandleResult(ctx, state, result, out)
	}, options...)
}
