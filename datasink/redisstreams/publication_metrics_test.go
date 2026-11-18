package redisstreams

import (
	"context"
	"errors"
	"testing"

	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
)

func TestPublicationMetricsCompletionBoundary(t *testing.T) {
	engine := testmetrics.New()
	meter, err := brokermetrics.NewPublication(engine.Metrics(), "redisstreams", "events", "publish", "not-sent", "accepted", "rejected", "unknown")
	require.NoError(t, err)
	count := func(outcome string) int64 {
		return engine.Counter("broker_endpoint_publication_outcomes_total", metrics.Labels{"protocol": "redisstreams", "connector": "events", "endpoint": "publish", "outcome": outcome}).Count()
	}
	var complete func(Delivery, error)
	results := 0
	msg := &SinkMessage[string]{publication: meter, retain: func() bool { return true }, release: func() {},
		result:    runtime.ConsumerFunc[string](func(context.Context, string) { results++ }),
		sendAsync: func(_ context.Context, _ []Field, cb func(Delivery, error)) { complete = cb },
		publish:   func(context.Context, []Field) (Delivery, error) { return Delivery{Outcome: Accepted}, nil },
	}
	msg.Send(t.Context(), func(d Delivery, e error) string {
		require.EqualValues(t, 1, count("accepted"), "record before user callback")
		require.NoError(t, e)
		require.Equal(t, Delivery{Outcome: Accepted}, d)
		return "done"
	})
	require.Zero(t, count("accepted"), "admission is not completion")
	require.Zero(t, results)
	complete(Delivery{Outcome: Accepted}, nil)
	require.Equal(t, 1, results)
	d, err := msg.SendSync(t.Context())
	require.NoError(t, err)
	require.Equal(t, Delivery{Outcome: Accepted}, d)
	require.EqualValues(t, 2, count("accepted"))
	expected := errors.New("ambiguous native result")
	msg.publish = func(context.Context, []Field) (Delivery, error) { return Delivery{Outcome: Unknown}, expected }
	_, err = msg.SendSync(t.Context())
	require.ErrorIs(t, err, expected)
	require.EqualValues(t, 1, count("unknown"))
	msg.Skip(t.Context(), "skip")
	require.EqualValues(t, 2, count("accepted"), "graph output is not publication")
}
