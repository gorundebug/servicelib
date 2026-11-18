package brokermetrics

import (
	"sync"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
)

func TestPublicationCounterConcurrencyAndRecordingPolicy(t *testing.T) {
	engine := testmetrics.New()
	p, err := NewPublication(engine.Metrics(), "rabbitmq", "events", "publish", "accepted", "unknown")
	require.NoError(t, err)
	var workers sync.WaitGroup
	for range 10 {
		workers.Go(func() {
			for range 100 {
				p.Observe(t.Context(), "accepted")
			}
		})
	}
	workers.Wait()
	p.Observe(metrics.WithoutRecording(t.Context()), "accepted")
	require.EqualValues(t, 1000, engine.Counter("broker_endpoint_publication_outcomes_total", metrics.Labels{
		"protocol": "rabbitmq", "connector": "events", "endpoint": "publish", "outcome": "accepted",
	}).Count())
	noop, err := NewPublication(metrics.NewNoopMetricsEngine().Metrics(), "rabbitmq", "events", "publish", "accepted")
	require.NoError(t, err)
	require.Nil(t, noop, "disabled metrics retain no per-endpoint series")
	noop.Observe(t.Context(), "accepted")
}
