package nats

import (
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"testing"
)

type observationMessage struct {
	jetstream.Msg
	count uint64
}

func (m observationMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.count}, nil
}
func TestNativeSourceObservationMetadata(t *testing.T) {
	engine := testmetrics.New()
	observations, err := brokermetrics.NewSource(engine.Metrics(), "jetstream", "events", "read")
	require.NoError(t, err)
	ep := &endpoint{observations: observations}
	for _, message := range []ConsumerMessage{{JetStream: observationMessage{count: 1}}, {JetStream: observationMessage{count: 3}}} {
		ep.observeDelivery(t.Context(), message)
	}
	labels := metrics.Labels{"protocol": "jetstream", "connector": "events", "endpoint": "read", "observation": "received"}
	require.EqualValues(t, 2, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	labels["observation"] = "redelivered"
	require.EqualValues(t, 1, engine.Counter("broker_endpoint_source_observations_total", labels).Count())
	ep.observations = nil
	ep.observeDelivery(t.Context(), ConsumerMessage{})
}
