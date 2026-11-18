package pulsar

import (
	native "github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
	"testing"
)

type observationMessage struct {
	native.Message
	count uint32
}

func (m observationMessage) RedeliveryCount() uint32 { return m.count }
func TestNativeSourceObservationMetadata(t *testing.T) {
	engine := testmetrics.New()
	observations, err := brokermetrics.NewSource(engine.Metrics(), "pulsar", "events", "read")
	require.NoError(t, err)
	ep := &endpoint{observations: observations}
	for _, message := range []ConsumerMessage{{Message: observationMessage{count: 0}}, {Message: observationMessage{count: 2}}} {
		ep.observeDelivery(t.Context(), message)
	}
	labels := metrics.Labels{"protocol": "pulsar", "connector": "events", "endpoint": "read", "observation": "received"}
	require.EqualValues(t, 2, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	labels["observation"] = "redelivered"
	require.EqualValues(t, 1, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	ep.observations = nil
	ep.observeDelivery(t.Context(), ConsumerMessage{})
}
