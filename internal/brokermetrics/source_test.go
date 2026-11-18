package brokermetrics

import (
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSourceConcurrentObservationsAndRecordingPolicy(t *testing.T) {
	engine := testmetrics.New()
	s, err := NewSource(engine.Metrics(), "redisstreams", "events", "read")
	require.NoError(t, err)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 100 {
				s.Observe(t.Context(), "reclaimed")
			}
		})
	}
	workers.Wait()
	s.Observe(metrics.WithoutRecording(t.Context()), "reclaimed")
	require.EqualValues(t, 800, engine.Counter("broker_endpoint_source_observations_total", metrics.Labels{"protocol": "redisstreams", "connector": "events", "endpoint": "read", "observation": "reclaimed"}).Count())
	require.Nil(t, s.observations["acknowledged"])
	noop, err := NewSource(metrics.NewNoopMetricsEngine().Metrics(), "nats", "events", "read")
	require.NoError(t, err)
	require.Nil(t, noop)
	noop.Observe(t.Context(), "received")
}
