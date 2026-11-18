package rabbitmq

import (
	"context"
	"errors"
	"github.com/Azure/go-amqp"
	sinkrabbit "github.com/gorundebug/servicelib/datasink/rabbitmq"
	"github.com/gorundebug/servicelib/internal/rabbitmqclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func broker(t *testing.T) (string, *rmq.AmqpConnection) {
	t.Helper()
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("set RABBITMQ_TEST_URL for RabbitMQ 4.x native acceptance")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	c, e := rmq.Dial(ctx, url, &rmq.AmqpConnOptions{SASLType: amqp.SASLTypePlain("adapter", "adapter-test"), RecoveryConfiguration: &rmq.RecoveryConfiguration{ActiveRecovery: false}, TopologyRecoveryOptions: rmq.TopologyRecoveryDisabled})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c.Close(ctx)
	})
	return url, c
}
func setupQueue(t *testing.T, c *rmq.AmqpConnection, name string) {
	t.Helper()
	if _, e := c.Management().DeclareQueue(t.Context(), &rmq.QuorumQueueSpecification{Name: name}); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c.Management().DeleteQueue(ctx, name)
	})
}

type mapped struct {
	delivery sinkrabbit.Delivery
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
func (h *outputHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, m *sinkrabbit.SinkMessage[mapped]) error {
	m.Message = rmq.NewMessage([]byte(strconv.Itoa(v)))
	m.Message.ApplicationProperties = map[string]any{"application": "kept"}
	m.Message.Properties = &amqp.MessageProperties{MessageID: "native-id", CorrelationID: "native-correlation"}
	if h.sync {
		d, e := m.SendSync(ctx)
		m.Out(ctx, mapped{d, e})
		return e
	}
	m.Send(ctx, func(d sinkrabbit.Delivery, e error) mapped {
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
func publisher(t *testing.T, s *inputStream, h *outputHandler) (runtime.Consumer[int], *outputStream) {
	t.Helper()
	out := &outputStream{env: s.env, results: make(chan mapped, 16)}
	p, e := sinkrabbit.MakeRabbitMQEndpointConsumerWithResult[struct{}, int, mapped, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.env.sink.Stop(ctx)
	})
	return p, out
}

func TestNativeAsyncAcceptedMetadataAndDoneNotSettlement(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, true)
	setupQueue(t, c, ep.QueueName)
	received := make(chan ConsumerMessage, 1)
	h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, sc StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
		received <- m
		r.SetResultCallback("id", func(context.Context, StreamContext[int, int, error], struct{}, int) bool { r.Done(); return true })
		sc.Collect(ctx, 1)
		return nil
	}}
	_, e := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	s.emit = func(ctx context.Context, v int) { s.result.Consume(ctx, v) }
	if e = s.env.source.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	p, out := publisher(t, s, &outputHandler{})
	p.Consume(t.Context(), 42)
	result := receive(t, out.results)
	if result.err != nil || result.delivery.Outcome != sinkrabbit.Accepted {
		t.Fatalf("native publish outcome: %+v", result)
	}
	m := receive(t, received)
	if e = receive(t, h.ended); e != nil {
		t.Fatal(e)
	}
	if string(m.Message.GetData()) != "42" || m.Message.Properties.MessageID != "native-id" || m.Message.Properties.CorrelationID != "native-correlation" || m.Message.ApplicationProperties["application"] != "kept" || m.Message.ApplicationProperties["x-stream-id"] != "publication" {
		t.Fatalf("metadata changed: %+v", m.Message)
	}
	s.env.source.Stop(t.Context())
	if e = m.Accept(t.Context()); !errors.Is(e, rabbitmqclient.ErrStaleDelivery) {
		t.Fatalf("stale handle accepted: %v", e)
	}
	consumer, e := c.NewConsumer(t.Context(), ep.QueueName, &rmq.ConsumerOptions{InitialCredits: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.Close(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	delivery, e := consumer.Receive(ctx)
	if e != nil {
		t.Fatalf("Done acknowledged instead of redelivery: %v", e)
	}
	if e = delivery.Accept(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestNativeReleaseAndReject(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(strconv.FormatBool(reject), func(t *testing.T) {
			url, c := broker(t)
			s, ep := fixture(t, url, false)
			observed := testmetrics.New()
			s.env.observedMetrics = observed.Metrics()
			setupQueue(t, c, ep.QueueName)
			seen := make(chan error, 3)
			count := 0
			h := &handler{consume: func(ctx context.Context, _ StreamContext[int, int, error], m ConsumerMessage, _ ResultContext[struct{}, int, int, error]) error {
				count++
				var e error
				if reject {
					e = m.Reject(ctx, &amqp.Error{Condition: amqp.ErrCondDecodeError, Description: "invalid"})
				} else if count == 1 {
					e = m.Release(ctx)
				} else {
					e = m.Accept(ctx)
				}
				seen <- e
				return e
			}}
			if _, e := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
				t.Fatal(e)
			}
			if e := s.env.source.Start(t.Context()); e != nil {
				t.Fatal(e)
			}
			p, out := publisher(t, s, &outputHandler{})
			p.Consume(t.Context(), 1)
			if r := receive(t, out.results); r.err != nil {
				t.Fatal(r.err)
			}
			if e := receive(t, seen); e != nil {
				t.Fatal(e)
			}
			if !reject {
				if e := receive(t, seen); e != nil {
					t.Fatal(e)
				}
			}
			s.env.source.Stop(t.Context())
			labels := metrics.Labels{"protocol": "rabbitmq", "connector": "rabbit", "endpoint": ep.Name, "observation": "received"}
			expected := int64(2)
			if reject {
				expected = 1
			}
			if actual := observed.Counter("broker_endpoint_source_observations_total", labels).Count(); actual != expected {
				t.Fatalf("received=%d want=%d", actual, expected)
			}
			// AMQP released leaves native delivery-count unchanged.
			labels["observation"] = "redelivered"
			if actual := observed.Counter("broker_endpoint_source_observations_total", labels).Count(); actual != 0 {
				t.Fatalf("unexpected native redelivery count %d", actual)
			}
			info, e := c.Management().QueueInfo(t.Context(), ep.QueueName)
			if e != nil {
				t.Fatal(e)
			}
			if info.MessageCount() != 0 {
				t.Fatal("settled message retained")
			}
		})
	}
}
func TestNativeExchangeBindingAndUnroutableOutcome(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	setupQueue(t, c, ep.QueueName)
	outcfg := s.env.cfg.GetEndpointConfigByID(3).(*config.RabbitMQEndpointConfig)
	outcfg.ExchangeName = "exchange-" + runtime.NewStreamID()
	outcfg.RoutingKey = "bound"
	outcfg.CreateExchange = true
	outcfg.CreateBinding = true
	p, out := publisher(t, s, &outputHandler{})
	p.Consume(t.Context(), 1)
	if r := receive(t, out.results); r.err != nil || r.delivery.Outcome != sinkrabbit.Accepted {
		t.Fatalf("binding outcome: %+v", r)
	}
	raw, e := c.NewPublisher(t.Context(), &rmq.ExchangeAddress{Exchange: outcfg.ExchangeName, Key: "unbound"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close(t.Context())
	res, e := raw.Publish(t.Context(), rmq.NewMessage([]byte("unroutable")))
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := res.Outcome.(*rmq.StateReleased); !ok {
		t.Fatalf("expected native released: %T", res.Outcome)
	}
	s2, _ := fixture(t, url, false)
	cfg := s2.env.cfg.GetEndpointConfigByID(3).(*config.RabbitMQEndpointConfig)
	cfg.QueueName = ""
	cfg.ExchangeName = outcfg.ExchangeName
	cfg.RoutingKey = "unbound"
	p2, out2 := publisher(t, s2, &outputHandler{})
	p2.Consume(t.Context(), 2)
	r := receive(t, out2.results)
	if r.err != nil || r.delivery.Outcome != sinkrabbit.Unroutable {
		t.Fatalf("unroutable conflated with transport unknown: %+v", r)
	}
	c.Management().DeleteExchange(t.Context(), outcfg.ExchangeName)
}
func TestMissingResourcesFailWithoutCreation(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	if _, e := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, &handler{}); e != nil {
		t.Fatal(e)
	}
	if e := s.env.source.Start(t.Context()); e == nil {
		t.Fatal("missing queue accepted")
	}
	if _, e := c.Management().QueueInfo(t.Context(), ep.QueueName); e == nil {
		t.Fatal("queue was created implicitly")
	}
}
func control(t *testing.T, action string) {
	t.Helper()
	container := os.Getenv("RABBITMQ_TEST_CONTAINER")
	if container == "" {
		t.Fatal("RABBITMQ_TEST_CONTAINER required for controlled failure tests")
	}
	if out, e := exec.Command("docker", action, container).CombinedOutput(); e != nil {
		t.Fatalf("%s: %v %s", action, e, out)
	}
}

func TestNativeStopDrainsSettlement(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	setupQueue(t, c, ep.QueueName)
	admitted := make(chan struct{})
	proceed := make(chan struct{})
	settled := make(chan error, 1)
	h := &handler{consume: func(ctx context.Context, _ StreamContext[int, int, error], m ConsumerMessage, _ ResultContext[struct{}, int, int, error]) error {
		close(admitted)
		<-proceed
		err := m.Accept(ctx)
		settled <- err
		return err
	}}
	if _, err := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h); err != nil {
		t.Fatal(err)
	}
	if err := s.env.source.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, out := publisher(t, s, &outputHandler{})
	p.Consume(t.Context(), 1)
	receive(t, out.results)
	receive(t, admitted)
	stopped := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		s.env.source.Stop(ctx)
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop did not drain handler")
	case <-time.After(30 * time.Millisecond):
	}
	close(proceed)
	if err := receive(t, settled); err != nil {
		t.Fatal(err)
	}
	receive(t, stopped)
}

func TestNativeAsyncBackpressureAndStopDeadline(t *testing.T) {
	if os.Getenv("RABBITMQ_TEST_CONTAINER") == "" {
		t.Skip("controlled broker required")
	}
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	setupQueue(t, c, ep.QueueName)
	s.env.cfg.GetEndpointConfigByID(3).(*config.RabbitMQEndpointConfig).PublisherMaxInFlight = 1
	ended := make(chan error, 2)
	p, out := publisher(t, s, &outputHandler{ended: ended})
	control(t, "pause")
	defer control(t, "unpause")
	p.Consume(t.Context(), 1)
	receive(t, ended)
	select {
	case r := <-out.results:
		t.Fatalf("completed while broker paused: %+v", r)
	default:
	}
	p.Consume(t.Context(), 2)
	r := receive(t, out.results)
	if r.delivery.Outcome != sinkrabbit.NotSent || !errors.Is(r.err, sinkrabbit.ErrBackpressure) {
		t.Fatalf("admission: %+v", r)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.env.sink.Stop(ctx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Stop exceeded deadline")
	}
	r = receive(t, out.results)
	if r.delivery.Outcome != sinkrabbit.Unknown || r.err == nil {
		t.Fatalf("in-flight cancellation must be unknown: %+v", r)
	}
}

func TestNativeReconnectResumesAndFencesOldDelivery(t *testing.T) {
	if os.Getenv("RABBITMQ_TEST_CONTAINER") == "" {
		t.Skip("controlled broker required")
	}
	url, c := broker(t)
	s, ep := fixture(t, url, true)
	setupQueue(t, c, ep.QueueName)
	messages := make(chan ConsumerMessage, 4)
	requests := make(chan ResultContext[struct{}, int, int, error], 4)
	h := &handler{consume: func(ctx context.Context, _ StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
		messages <- m
		requests <- r
		return nil
	}}
	if _, err := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h); err != nil {
		t.Fatal(err)
	}
	if err := s.env.source.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.env.source.Stop(ctx)
	}()
	p, out := publisher(t, s, &outputHandler{})
	p.Consume(t.Context(), 1)
	receive(t, out.results)
	old := receive(t, messages)
	r := receive(t, requests)
	control(t, "restart")
	if err := old.Accept(t.Context()); !errors.Is(err, rabbitmqclient.ErrStaleDelivery) {
		t.Fatalf("old generation: %v", err)
	}
	r.Done()
	select {
	case m := <-messages:
		if err := m.Accept(t.Context()); err != nil {
			t.Fatal(err)
		}
		receive(t, requests).Done()
	case <-time.After(20 * time.Second):
		t.Fatal("source did not recover")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		p.Consume(t.Context(), 2)
		res := receive(t, out.results)
		if res.delivery.Outcome == sinkrabbit.Accepted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher did not recover: %+v", res)
		}
		time.Sleep(50 * time.Millisecond)
	}
	m := receive(t, messages)
	if err := m.Accept(t.Context()); err != nil {
		t.Fatal(err)
	}
	receive(t, requests).Done()
}

func TestZeroDeliveryCannotSettle(t *testing.T) {
	var m ConsumerMessage
	for _, err := range []error{m.Accept(t.Context()), m.Release(t.Context()), m.Reject(t.Context(), nil)} {
		if !errors.Is(err, rabbitmqclient.ErrStaleDelivery) {
			t.Fatal(err)
		}
	}
}

func TestNativeBrokerRejectedOutcome(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	_, err := c.Management().DeclareQueue(t.Context(), &rmq.QuorumQueueSpecification{Name: ep.QueueName, MaxLength: 1, OverflowStrategy: &rmq.RejectPublishOverflowStrategy{}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Management().DeleteQueue(t.Context(), ep.QueueName)
	p, out := publisher(t, s, &outputHandler{})
	for i := 0; i < 20; i++ {
		p.Consume(t.Context(), i)
		r := receive(t, out.results)
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.delivery.Outcome == sinkrabbit.Rejected {
			if r.delivery.Native == nil || r.delivery.Native.MessageRejectedError == nil {
				t.Fatal("native rejection details lost")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("bounded queue never rejected publication")
}

func TestNativeSourceStopDeadlineCancelsRequest(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	setupQueue(t, c, ep.QueueName)
	admitted := make(chan struct{})
	ended := make(chan error, 1)
	h := &handler{ended: ended, consume: func(ctx context.Context, _ StreamContext[int, int, error], _ ConsumerMessage, _ ResultContext[struct{}, int, int, error]) error {
		close(admitted)
		<-ctx.Done()
		return context.Cause(ctx)
	}}
	if _, err := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h); err != nil {
		t.Fatal(err)
	}
	if err := s.env.source.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, out := publisher(t, s, &outputHandler{sync: true})
	p.Consume(t.Context(), 1)
	receive(t, out.results)
	receive(t, admitted)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.env.source.Stop(ctx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Stop exceeded deadline")
	}
	if err := receive(t, ended); !errors.Is(err, context.Canceled) {
		t.Fatalf("request not cancelled: %v", err)
	}
}

func TestNativeStopBoundsBlockedDeliveryCallback(t *testing.T) {
	url, c := broker(t)
	s, ep := fixture(t, url, false)
	setupQueue(t, c, ep.QueueName)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	p, _ := publisher(t, s, &outputHandler{onDelivery: func() { close(entered); <-release }})
	p.Consume(t.Context(), 1)
	receive(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.env.sink.Stop(ctx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("SDK callback blocked Stop beyond deadline")
	}
}

func TestSourceHandlerFailuresPreserveLifecycle(t *testing.T) {
	for _, begin := range []bool{true, false} {
		t.Run(strconv.FormatBool(begin), func(t *testing.T) {
			s, _ := fixture(t, "amqp://localhost", false)
			failure := errors.New("handler failure")
			calls := 0
			h := &handler{ended: make(chan error, 1), consume: func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error {
				calls++
				return failure
			}}
			if begin {
				h.beginErr = failure
			}
			_, err := MakeRabbitMQEndpointConsumer[struct{}, int, int, error](s, h)
			if err != nil {
				t.Fatal(err)
			}
			s.env.source.GetEndpoint(2).(*endpoint).request(t.Context(), ConsumerMessage{})
			if begin {
				if calls != 0 || len(h.ended) != 0 {
					t.Fatal("BeginRequest failure invoked request/end")
				}
			} else {
				if calls != 1 || !errors.Is(receive(t, h.ended), failure) {
					t.Fatal("ConsumeMessage failure not passed to EndRequest")
				}
			}
		})
	}
}
