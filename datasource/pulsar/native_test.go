package pulsar

import (
	"context"
	sinkpulsar "github.com/gorundebug/servicelib/datasink/pulsar"
	"github.com/gorundebug/servicelib/internal/pulsarclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"
)

type mapped struct {
	delivery sinkpulsar.Delivery
	err      error
}
type outputStream struct {
	runtime.TypedSinkStreamWithResult[int, mapped, error]
	env      *fixtureEnv
	results  chan mapped
	consumer runtime.Consumer[int]
}

func (s *outputStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (*outputStream) GetEndpointId() int                                  { return 3 }
func (*outputStream) GetName() string                                     { return "output" }
func (s *outputStream) SetSinkConsumer(c runtime.Consumer[int])           { s.consumer = c }
func (s *outputStream) ConsumeResult(_ context.Context, m mapped)         { s.results <- m }

type outputHandler struct {
	sync       bool
	onDelivery func()
	ended      chan error
	errors     chan error
}

func (h *outputHandler) OnError(_ context.Context, _ runtime.Stream, e error) {
	if h.errors != nil {
		h.errors <- e
	}
}
func (*outputHandler) GetStreamID(context.Context, int) string { return "publication" }
func (*outputHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}
func (h *outputHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, m *sinkpulsar.SinkMessage[mapped]) error {
	m.Message.Payload = []byte(strconv.Itoa(v))
	if h.sync {
		d, e := m.SendSync(ctx)
		m.Out(ctx, mapped{d, e})
		return e
	}
	m.Send(ctx, func(d sinkpulsar.Delivery, e error) mapped {
		if h.onDelivery != nil {
			h.onDelivery()
		}
		return mapped{d, e}
	})
	return nil
}
func (h *outputHandler) EndRequest(_ context.Context, _ runtime.Stream, e error, _ struct{}) {
	if h.ended != nil {
		h.ended <- e
	}
}

func TestNativeGraphCallbacksAndSettlement(t *testing.T) {
	address := os.Getenv("PULSAR_TEST_ADMIN_URL")
	if address == "" {
		t.Skip("requires isolated Pulsar")
	}
	for _, ack := range []bool{false, true} {
		t.Run(strconv.FormatBool(ack), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			s, ep := fixture(t, address, true)
			observed := testmetrics.New()
			s.env.observedMetrics = observed.Metrics()

			received := make(chan ConsumerMessage, 1)
			h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, sc StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
				received <- m
				r.SetResultCallback("id", func(ctx context.Context, _ StreamContext[int, int, error], _ struct{}, _ int) bool {
					if ack {
						require.NoError(t, m.Ack(ctx))
					}
					r.Done()
					return true
				})
				sc.Collect(ctx, 1)
				return nil
			}}
			_, err := MakePulsarEndpointConsumer[struct{}, int, int, error](s, h)
			require.NoError(t, err)
			s.emit = func(ctx context.Context, v int) { s.result.Consume(ctx, v) }
			out := &outputStream{env: s.env, results: make(chan mapped, 1)}
			publisher, err := sinkpulsar.MakePulsarEndpointConsumerWithResult[struct{}, int, mapped, error](out, &outputHandler{})
			require.NoError(t, err)
			require.NoError(t, s.env.source.Start(ctx))
			defer s.env.source.Stop(ctx)
			require.NoError(t, s.env.sink.Start(ctx))
			defer s.env.sink.Stop(ctx)
			publisher.Consume(ctx, 42)
			result := receive(t, out.results)
			require.NoError(t, result.err)
			require.Equal(t, sinkpulsar.Accepted, result.delivery.Outcome)
			m := receive(t, received)
			require.Equal(t, result.delivery.MessageID.Serialize(), m.Message.ID().Serialize())
			require.Equal(t, "42", string(m.Message.Payload()))
			require.NoError(t, receive(t, h.ended))
			require.EqualValues(t, 1, observed.Counter("broker_endpoint_source_observations_total", metrics.Labels{"protocol": "pulsar", "connector": "pulsar", "endpoint": ep.Name, "observation": "received"}).Count())
			s.env.source.Stop(ctx)
			// Reopen the same durable subscription: Done without ACK leaves a delivery.
			dc := s.env.cfg.GetDataConnectorByID(1).(*config.PulsarDataConnectorConfig)
			reader, err := pulsarclient.Open(ctx, dc, ep, true)
			require.NoError(t, err)
			defer reader.Close(ctx)
			short, cancelRead := context.WithTimeout(ctx, 150*time.Millisecond)
			defer cancelRead()
			replay, err := reader.Receive(short)
			if ack {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
				require.Equal(t, m.Message.ID().Serialize(), replay.ID().Serialize())
				require.NoError(t, reader.Ack(ctx, replay))
			}

			require.Error(t, m.Ack(ctx)) // completed request cannot settle a retained handle
			s.env.source.Stop(ctx)
			s.env.sink.Stop(ctx)
		})
	}
}
