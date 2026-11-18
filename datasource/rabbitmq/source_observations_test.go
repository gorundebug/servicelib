package rabbitmq

import (
	"github.com/Azure/go-amqp"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeSourceObservationMetadata(t *testing.T) {
	engine := testmetrics.New()
	observations, err := brokermetrics.NewSource(engine.Metrics(), "rabbitmq", "events", "read")
	require.NoError(t, err)
	ep := &endpoint{observations: observations}
	for _, message := range []ConsumerMessage{{Message: &amqp.Message{}}, {Message: &amqp.Message{Header: &amqp.MessageHeader{DeliveryCount: 2}}}} {
		ep.observeDelivery(t.Context(), message)
	}
	labels := metrics.Labels{"protocol": "rabbitmq", "connector": "events", "endpoint": "read", "observation": "received"}
	require.EqualValues(t, 2, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	labels["observation"] = "redelivered"
	require.EqualValues(t, 1, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	ep.observations = nil
	ep.observeDelivery(t.Context(), ConsumerMessage{})
}
