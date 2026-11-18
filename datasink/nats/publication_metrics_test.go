package nats

import (
	"context"
	"errors"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	native "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPublicationMetricsCompletionBoundary(t *testing.T) {
	engine := testmetrics.New()
	meter, err := brokermetrics.NewPublication(engine.Metrics(), "nats", "events", "publish", "client-accepted", "flushed", "acknowledged", "error")
	require.NoError(t, err)
	count := func(outcome string) int64 {
		return engine.Counter("broker_endpoint_publication_outcomes_total", metrics.Labels{"protocol": "nats", "connector": "events", "endpoint": "publish", "outcome": outcome}).Count()
	}
	var complete func(Delivery, error)
	results := 0
	msg := &SinkMessage[string]{publication: meter,
		result:    runtime.ConsumerFunc[string](func(context.Context, string) { results++ }),
		sendAsync: func(_ context.Context, _ *native.Msg, cb func(Delivery, error)) { complete = cb },
		publish:   func(context.Context, *native.Msg) (Delivery, error) { return Delivery{Accepted: true}, nil },
	}
	msg.Send(t.Context(), func(d Delivery, e error) string {
		require.EqualValues(t, 1, count("client-accepted"), "record before user callback")
		require.NoError(t, e)
		require.Equal(t, Delivery{Accepted: true}, d)
		return "done"
	})
	require.Zero(t, count("client-accepted"), "admission is not completion")
	require.Zero(t, results)
	complete(Delivery{Accepted: true}, nil)
	require.Equal(t, 1, results)
	d, err := msg.SendSync(t.Context())
	require.NoError(t, err)
	require.Equal(t, Delivery{Accepted: true}, d)
	require.EqualValues(t, 2, count("client-accepted"))
	expected := errors.New("ambiguous native result")
	msg.publish = func(context.Context, *native.Msg) (Delivery, error) { return Delivery{}, expected }
	_, err = msg.SendSync(t.Context())
	require.ErrorIs(t, err, expected)
	require.EqualValues(t, 1, count("error"))
	msg.Skip(t.Context(), "skip")
	require.EqualValues(t, 2, count("client-accepted"), "graph output is not publication")
}
