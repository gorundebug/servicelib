package temporal

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/stretchr/testify/require"
)

type sdkMetricsLogRecord struct {
	ctx     context.Context
	message string
	fields  []log.Field
}

type sdkMetricsContextLogger struct {
	log.Logger
	records chan sdkMetricsLogRecord
}

func (l *sdkMetricsContextLogger) Error(ctx context.Context, message string, fields ...log.Field) {
	l.records <- sdkMetricsLogRecord{ctx, message, fields}
}

type sdkMetricsFailingListener struct {
	err    error
	closed bool
}

func (l *sdkMetricsFailingListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *sdkMetricsFailingListener) Close() error              { l.closed = true; return nil }
func (l *sdkMetricsFailingListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

func TestSDKMetricsServerFailurePreservesInitializationContext(t *testing.T) {
	type key struct{}
	parent, release := context.WithTimeout(context.WithValue(t.Context(), key{}, "metrics-owner"), time.Minute)
	defer release()
	ctx, cancel := context.WithCancelCause(parent)
	cause := errors.New("connector startup cancelled")
	cancel(cause)
	acceptErr := errors.New("metrics listener failed")
	listener := &sdkMetricsFailingListener{err: acceptErr}
	logger := &sdkMetricsContextLogger{records: make(chan sdkMetricsLogRecord, 1)}
	serveSDKMetrics(ctx, &http.Server{}, listener, logger, "metrics-address")
	record := <-logger.records
	require.Same(t, ctx, record.ctx)
	require.Equal(t, "metrics-owner", record.ctx.Value(key{}))
	require.ErrorIs(t, context.Cause(record.ctx), cause)
	wantDeadline, _ := parent.Deadline()
	gotDeadline, ok := record.ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, wantDeadline, gotDeadline)
	require.Equal(t, "Temporal SDK metrics server failed", record.message)
	require.Equal(t, "metrics-address", record.fields[0].StrVal())
	require.ErrorIs(t, record.fields[1].ErrorVal(), acceptErr)
	require.True(t, listener.closed)
}

func TestSDKMetricsSharedServerSurvivesConnectorCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})}
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	logger := &sdkMetricsContextLogger{records: make(chan sdkMetricsLogRecord, 1)}
	done := make(chan struct{})
	go func() { defer close(done); serveSDKMetrics(ctx, server, listener, logger, listener.Addr().String()) }()
	requestContext, finish := context.WithTimeout(t.Context(), 5*time.Second)
	defer finish()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, "http://"+listener.Addr().String(), nil)
	require.NoError(t, err)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, server.Close())
	select {
	case <-done:
	case <-requestContext.Done():
		t.Fatal("metrics server failed to stop")
	}
	require.Empty(t, logger.records)
}

func TestTemporalConnectorStartRejectsMissingOrCancelledContext(t *testing.T) {
	connector := &Connector{}
	require.ErrorContains(t, connector.Start(nil), "context is nil")
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("service owner stopped")
	cancel(cause)
	require.ErrorIs(t, connector.Start(ctx), cause)
}
