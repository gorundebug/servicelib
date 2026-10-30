package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type resultConfig struct{ config.Config }

func (resultConfig) GetStreams() []config.StreamConfig    { return nil }
func (resultConfig) GetServices() []*config.ServiceConfig { return nil }
func (resultConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{&config.KafkaEndpointConfig{ID: 1, Name: "output", IdDataConnector: 2, Topic: "events"}}
}
func (resultConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{&config.KafkaDataConnectorConfig{ID: 2, Name: "kafka"}}
}
func (resultConfig) GetPools() []*config.PoolConfig     { return nil }
func (resultConfig) GetModules() []*config.ModuleConfig { return nil }
func (resultConfig) GetLinks() []*config.LinkConfig     { return nil }
func (resultConfig) GetTypes() []*config.TypeConfig     { return nil }

type resultEnvironment struct {
	runtime.RuntimeEnvironment
	cfg        *config.RuntimeConfig
	sink       runtime.DataSink
	registered int
}

func (e *resultEnvironment) RuntimeConfig() *config.RuntimeConfig { return e.cfg }
func (e *resultEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (e *resultEnvironment) Tracing() tracing.Tracing                                 { return nil }
func (e *resultEnvironment) GetDataSink(int) runtime.DataSink                         { return e.sink }
func (e *resultEnvironment) RegisterEndpointConsumer(runtime.RuntimeEndpointConsumer) { e.registered++ }

type resultDataSink struct {
	*runtime.OutputDataSink
	send func(context.Context, *sarama.ProducerMessage)
}

func (*resultDataSink) Start(context.Context) error { return nil }
func (*resultDataSink) Stop(context.Context)        {}

func (d *resultDataSink) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) {
	d.send(ctx, msg)
}

type delivery struct {
	value int
	err   error
}
type deliveryOutput struct {
	runtime.TypedConsumedStream[delivery]
	values   []delivery
	contexts []context.Context
}

func (s *deliveryOutput) Consume(ctx context.Context, v delivery) {
	s.values = append(s.values, v)
	s.contexts = append(s.contexts, ctx)
}

type resultTestStream struct {
	runtime.TypedSinkStreamWithResult[int, delivery, error]
	env      *resultEnvironment
	consumer runtime.Consumer[int]
	output   deliveryOutput
}

func (s *resultTestStream) GetName() string                                   { return "publish" }
func (s *resultTestStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (s *resultTestStream) GetEndpointId() int                                { return 1 }
func (s *resultTestStream) SetSinkConsumer(c runtime.Consumer[int])           { s.consumer = c }
func (s *resultTestStream) ConsumeResult(ctx context.Context, v delivery)     { s.output.Consume(ctx, v) }

type plainTestStream struct {
	*resultTestStream
	errors deliveryOutput
}

func (s *plainTestStream) GetErrorStream() runtime.TypedConsumedStream[delivery] { return &s.errors }

type deliveryHandler struct {
	mode   string
	ended  int
	endErr error
}

func (*deliveryHandler) GetStreamID(context.Context, int) string { return "request" }
func (*deliveryHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}
func (h *deliveryHandler) EndRequest(_ context.Context, _ runtime.Stream, err error, _ struct{}) {
	h.ended++
	h.endErr = err
}
func (h *deliveryHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, msg *SinkMessage[delivery]) error {
	msg.Key = []byte("key")
	msg.Value = []byte("value")
	switch h.mode {
	case "skip":
		msg.Skip(ctx, delivery{value: v})
	case "async":
		msg.Send(ctx, func(_ int32, _ int64, err error) delivery { return delivery{value: v, err: err} })
	case "sync":
		_, _, err := msg.SendSync(ctx)
		msg.Out(ctx, delivery{value: v, err: err})
		return err
	}
	return nil
}

func resultFixture(t *testing.T) (*resultTestStream, *resultDataSink) {
	t.Helper()
	cfg, err := config.NewRuntimeConfig(resultConfig{})
	if err != nil {
		t.Fatal(err)
	}
	env := &resultEnvironment{cfg: cfg}
	base, err := runtime.MakeOutputDataSink(cfg.GetDataConnectorByID(2), env)
	if err != nil {
		t.Fatal(err)
	}
	ds := &resultDataSink{OutputDataSink: base}
	env.sink = ds
	ep, err := getSaramaKafkaDataSinkEndpoint(1, env)
	if err != nil {
		t.Fatal(err)
	}
	ep.active.Store(true)
	return &resultTestStream{env: env}, ds
}

func TestKafkaSinkResultRouting(t *testing.T) {
	for _, mode := range []string{"skip", "async", "sync"} {
		for _, failed := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "-ok", true: "-failure"}[failed], func(t *testing.T) {
				stream, ds := resultFixture(t)
				h := &deliveryHandler{mode: mode}
				sentinel := errors.New("broker unavailable")
				var pending func()
				sends := 0
				ds.send = func(ctx context.Context, msg *sarama.ProducerMessage) {
					sends++
					if msg.Topic != "events" {
						t.Fatal(msg.Topic)
					}
					key, _ := msg.Key.Encode()
					value, _ := msg.Value.Encode()
					if string(key) != "key" || string(value) != "value" {
						t.Fatal("payload changed")
					}
					deliver := func() {
						var err error
						if failed {
							err = sentinel
						}
						msg.Metadata.(*messageMetadata).onDelivery(1, 42, err)
					}
					if mode == "async" {
						pending = deliver
					} else {
						deliver()
					}
				}
				consumer, err := MakeSaramaKafkaEndpointConsumerWithResult[struct{}, int, delivery, error](stream, h)
				if err != nil {
					t.Fatal(err)
				}
				if stream.consumer != consumer || stream.env.registered != 1 {
					t.Fatal("consumer registration lost")
				}
				consumer.Consume(t.Context(), 7)
				if mode == "async" {
					if len(stream.output.values) != 0 {
						t.Fatal("result emitted before acknowledgment")
					}
					pending()
				}
				if len(stream.output.values) != 1 || stream.output.values[0].value != 7 {
					t.Fatal(stream.output.values)
				}
				if failed && mode != "skip" && !errors.Is(stream.output.values[0].err, sentinel) {
					t.Fatal("delivery error lost")
				}
				if h.ended != 1 {
					t.Fatal("request lifecycle changed")
				}
				if mode == "skip" && sends != 0 {
					t.Fatal("Skip published a message")
				}
				if mode != "skip" && sends != 1 {
					t.Fatal(sends)
				}
			})
		}
	}
}

func TestKafkaPlainSinkRetainsErrorRoute(t *testing.T) {
	base, ds := resultFixture(t)
	ds.send = func(context.Context, *sarama.ProducerMessage) { t.Fatal("unexpected publication") }
	stream := &plainTestStream{resultTestStream: base}
	consumer, err := MakeSaramaKafkaEndpointConsumer[struct{}, int, delivery](stream, &deliveryHandler{mode: "skip"})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Consume(t.Context(), 9)
	if len(stream.errors.values) != 1 || stream.errors.values[0].value != 9 || len(base.output.values) != 0 {
		t.Fatal("ordinary sink result route changed")
	}
}

func TestKafkaSinkResultCancellation(t *testing.T) {
	stream, ds := resultFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	ds.send = func(context.Context, *sarama.ProducerMessage) { cancel() }
	h := &deliveryHandler{mode: "sync"}
	consumer, err := MakeSaramaKafkaEndpointConsumerWithResult[struct{}, int, delivery, error](stream, h)
	if err != nil {
		t.Fatal(err)
	}
	consumer.Consume(ctx, 1)
	if h.ended != 1 || !errors.Is(h.endErr, context.Canceled) || len(stream.output.values) != 1 || !errors.Is(stream.output.values[0].err, context.Canceled) {
		t.Fatal("caller cancellation lost")
	}
	if stream.output.contexts[0].Err() != context.Canceled {
		t.Fatal("result replaced caller context")
	}
}
