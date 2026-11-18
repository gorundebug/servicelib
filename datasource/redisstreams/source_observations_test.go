package redisstreams

import (
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeSourceObservationMetadata(t *testing.T) {
	engine := testmetrics.New()
	observations, err := brokermetrics.NewSource(engine.Metrics(), "redisstreams", "events", "read")
	require.NoError(t, err)
	ep := &endpoint{observations: observations}
	for _, message := range []ConsumerMessage{{}, {}} {
		ep.observeDelivery(t.Context(), message)
	}
	labels := metrics.Labels{"protocol": "redisstreams", "connector": "events", "endpoint": "read", "observation": "received"}
	require.EqualValues(t, 2, engine.Counter("broker_endpoint_source_observations_total", labels).Count())

	ep.observations = nil
	ep.observeDelivery(t.Context(), ConsumerMessage{})
}
