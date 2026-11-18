package nats

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/api"
	sinknats "github.com/gorundebug/servicelib/datasink/nats"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	native "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fixtureConfig struct {
	config.Config
	dc        *config.NATSDataConnectorConfig
	endpoints []config.EndpointConfig
}

func (c fixtureConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{c.dc}
}
func (c fixtureConfig) GetEndpoints() []config.EndpointConfig { return c.endpoints }
func (fixtureConfig) GetServices() []*config.ServiceConfig    { return nil }
func (fixtureConfig) GetStreams() []config.StreamConfig       { return nil }
func (fixtureConfig) GetPools() []*config.PoolConfig          { return nil }
func (fixtureConfig) GetModules() []*config.ModuleConfig      { return nil }
func (fixtureConfig) GetLinks() []*config.LinkConfig          { return nil }
func (fixtureConfig) GetTypes() []*config.TypeConfig          { return nil }

type fixtureEnv struct {
	observedMetrics metrics.Metrics
	runtime.RuntimeEnvironment
	cfg    *config.RuntimeConfig
	source runtime.DataSource
	sink   runtime.DataSink
}

func (e *fixtureEnv) RuntimeConfig() *config.RuntimeConfig                   { return e.cfg }
func (e *fixtureEnv) GetDataSource(int) runtime.DataSource                   { return e.source }
func (e *fixtureEnv) AddDataSource(s runtime.DataSource)                     { e.source = s }
func (e *fixtureEnv) GetDataSink(int) runtime.DataSink                       { return e.sink }
func (e *fixtureEnv) AddDataSink(s runtime.DataSink)                         { e.sink = s }
func (*fixtureEnv) RegisterEndpointConsumer(runtime.RuntimeEndpointConsumer) {}
func (e *fixtureEnv) Metrics() metrics.Metrics {
	if e.observedMetrics != nil {
		return e.observedMetrics
	}
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*fixtureEnv) Log() log.Logger          { return log.NoopLogger{} }
func (*fixtureEnv) Tracing() tracing.Tracing { return nil }

type errorStream struct {
	runtime.TypedConsumedStream[error]
}

func (errorStream) Consume(context.Context, error) {}

type inputStream struct {
	runtime.TypedInputStream[int, int, error]
	env       *fixtureEnv
	result    runtime.Consumer[int]
	emit      func(context.Context, int)
	hasResult bool
}

func (s *inputStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (s *inputStream) GetEnvironment() environment.ServiceEnvironment    { return s.env }
func (*inputStream) GetEndpointId() int                                  { return 2 }
func (*inputStream) GetName() string                                     { return "input" }
func (s *inputStream) GetResultStream() runtime.TypedSerializedStream[int] {
	if s.hasResult {
		return s
	}
	return nil
}
func (s *inputStream) SetResultConsumer(c runtime.Consumer[int])        { s.result = c }
func (*inputStream) GetErrorStream() runtime.TypedConsumedStream[error] { return errorStream{} }
func (s *inputStream) Consume(ctx context.Context, v int) {
	if s.emit != nil {
		s.emit(ctx, v)
	}
}

type handler struct {
	beginErr error
	consume  func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error
	ended    chan error
	failed   chan error
}

func (h *handler) OnError(_ context.Context, _ StreamContext[int, int, error], e error) {
	if h.failed != nil {
		h.failed <- e
	}
}
func (h *handler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, h.beginErr
}
func (h *handler) ConsumeMessage(ctx context.Context, sc StreamContext[int, int, error], _ struct{}, m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
	return h.consume(ctx, sc, m, r)
}
func (*handler) GetMessageID(context.Context, StreamContext[int, int, error], struct{}, int) string {
	return "id"
}
func (h *handler) EndRequest(_ context.Context, _ StreamContext[int, int, error], e error, _ struct{}) {
	if h.ended != nil {
		h.ended <- e
	}
}
func fixture(t *testing.T, url string, kind api.DataConnectorType, hasResult bool) (*inputStream, *config.NATSEndpointConfig) {
	t.Helper()
	ep := &config.NATSEndpointConfig{ID: 2, Name: "input", IdDataConnector: 1, Enabled: true, Subject: "events"}
	out := &config.NATSEndpointConfig{ID: 3, Name: "output", IdDataConnector: 1, Enabled: true, Subject: "events"}
	if kind == api.DataConnectorTypeJetStream {
		ep.StreamName = "EVENTS"
		ep.DurableName = "worker"
		out.StreamName = "EVENTS"
	}
	cfg, e := config.NewRuntimeConfig(fixtureConfig{dc: &config.NATSDataConnectorConfig{ID: 1, Name: "nats", Type: kind, Servers: []string{url}, OperationTimeoutMs: 500}, endpoints: []config.EndpointConfig{ep, out}})
	if e != nil {
		t.Fatal(e)
	}
	return &inputStream{env: &fixtureEnv{cfg: cfg}, hasResult: hasResult}, ep
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

type ackSpy struct {
	jetstream.Msg
	acks atomic.Int32
}

func (m *ackSpy) Ack() error { m.acks.Add(1); return nil }
func TestSynchronousResultRegisteredBeforeEmitAndDoneDoesNotAck(t *testing.T) {
	s, _ := fixture(t, "nats://127.0.0.1:1", api.DataConnectorTypeJetStream, true)
	h := &handler{ended: make(chan error, 1)}
	spy := &ackSpy{}
	h.consume = func(ctx context.Context, sc StreamContext[int, int, error], _ ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
		r.SetResultCallback("id", func(_ context.Context, _ StreamContext[int, int, error], _ struct{}, v int) bool {
			if v != 7 {
				t.Errorf("result %d", v)
			}
			r.Done()
			return true
		})
		sc.Collect(ctx, 7)
		return nil
	}
	c, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	s.emit = func(ctx context.Context, v int) { s.result.Consume(ctx, v) }
	c.(*consumer[struct{}, int, int, error]).process(t.Context(), ConsumerMessage{Message: &native.Msg{}, JetStream: spy})
	if e := receive(t, h.ended); e != nil {
		t.Fatal(e)
	}
	if spy.acks.Load() != 0 {
		t.Fatal("Done acknowledged")
	}
}
func TestRequestFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"begin", "consume", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := fixture(t, "nats://127.0.0.1:1", api.DataConnectorTypeNATS, true)
			sentinel := errors.New("failure")
			h := &handler{ended: make(chan error, 1)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			called := false
			h.consume = func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error {
				called = true
				if mode == "cancel" {
					cancel()
					return nil
				}
				return sentinel
			}
			if mode == "begin" {
				h.beginErr = sentinel
			}
			c, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
			if e != nil {
				t.Fatal(e)
			}
			c.(*consumer[struct{}, int, int, error]).process(ctx, ConsumerMessage{})
			if mode == "begin" {
				if called || len(h.ended) != 0 {
					t.Fatal("begin failure entered lifecycle")
				}
				return
			}
			want := sentinel
			if mode == "cancel" {
				want = context.Canceled
			}
			if e := receive(t, h.ended); !errors.Is(e, want) {
				t.Fatalf("got %v want %v", e, want)
			}
		})
	}
}

func server(t *testing.T) string {
	return serverOptions(t, nil, nil)
}

type testBroker struct {
	url    string
	cmd    *exec.Cmd
	args   []string
	binary string
}

func (b *testBroker) pause(t *testing.T) {
	t.Helper()
	if e := b.cmd.Process.Signal(syscall.SIGSTOP); e != nil {
		t.Fatal(e)
	}
}
func (b *testBroker) resume(t *testing.T) {
	t.Helper()
	if e := b.cmd.Process.Signal(syscall.SIGCONT); e != nil {
		t.Fatal(e)
	}
}
func (b *testBroker) restart(t *testing.T) {
	t.Helper()
	b.cmd.Process.Kill()
	b.cmd.Wait()
	b.cmd = exec.Command(b.binary, b.args...)
	if e := b.cmd.Start(); e != nil {
		t.Fatal(e)
	}
}
func serverOptions(t *testing.T, args []string, options []native.Option) string {
	return controlledServer(t, args, options).url
}
func controlledServer(t *testing.T, args []string, options []native.Option) *testBroker {
	t.Helper()
	binary := os.Getenv("NATS_SERVER_BINARY")
	if binary == "" {
		var e error
		binary, e = exec.LookPath("nats-server")
		if e != nil {
			t.Skip("set NATS_SERVER_BINARY to run real-server integration tests")
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	cmd := exec.Command(binary, append([]string{"-js", "-a", "127.0.0.1", "-p", strconv.Itoa(port), "-sd", t.TempDir()}, args...)...)
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	b := &testBroker{cmd: cmd, binary: binary, args: cmd.Args[1:]}
	t.Cleanup(func() { b.cmd.Process.Kill(); b.cmd.Wait() })
	url := "nats://127.0.0.1:" + strconv.Itoa(port)
	b.url = url
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, e := native.Connect(url, append(options, native.Timeout(50*time.Millisecond))...)
		if e == nil {
			c.Close()
			return b
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return nil
}

type outputStream struct {
	runtime.TypedSinkStreamWithResult[int, sinknats.Delivery, error]
	env      *fixtureEnv
	results  chan sinknats.Delivery
	consumer runtime.Consumer[int]
}

func (s *outputStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment    { return s.env }
func (*outputStream) GetEndpointId() int                                     { return 3 }
func (*outputStream) GetName() string                                        { return "output" }
func (s *outputStream) SetSinkConsumer(c runtime.Consumer[int])              { s.consumer = c }
func (s *outputStream) ConsumeResult(_ context.Context, d sinknats.Delivery) { s.results <- d }

type outputHandler struct {
	payload         []byte
	headers         native.Header
	err             error
	async           bool
	deliveries      chan error
	transportErrors chan error
}

func (h *outputHandler) OnError(_ context.Context, _ runtime.Stream, e error) {
	if h.transportErrors != nil {
		h.transportErrors <- e
	}
}
func (*outputHandler) GetStreamID(context.Context, int) string { return "publication" }
func (*outputHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}
func (h *outputHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, m *sinknats.SinkMessage[sinknats.Delivery]) error {
	m.Data = []byte(strconv.Itoa(v))
	m.Header = native.Header{"X-Test": []string{"preserved"}}
	if h.payload != nil {
		m.Data = h.payload
	}
	if h.headers != nil {
		m.Header = h.headers
	}
	if h.async {
		m.Send(ctx, func(d sinknats.Delivery, e error) sinknats.Delivery {
			if h.deliveries != nil {
				h.deliveries <- e
			}
			return d
		})
		return nil
	}
	d, e := m.SendSync(ctx)
	if e == nil {
		m.Out(ctx, d)
	}
	return e
}
func (h *outputHandler) EndRequest(_ context.Context, _ runtime.Stream, e error, _ struct{}) {
	h.err = e
}

func TestNativeCoreAndJetStreamEndToEnd(t *testing.T) {
	url := server(t)
	for _, kind := range []api.DataConnectorType{api.DataConnectorTypeNATS, api.DataConnectorTypeJetStream} {
		t.Run(strconv.Itoa(int(kind)), func(t *testing.T) {
			s, ep := fixture(t, url, kind, false)
			observed := testmetrics.New()
			s.env.observedMetrics = observed.Metrics()
			protocol := "nats"
			if kind == api.DataConnectorTypeJetStream {
				protocol = "jetstream"
			}
			count := func(observation string) int64 {
				return observed.Counter("broker_endpoint_source_observations_total", metrics.Labels{"protocol": protocol, "connector": "nats", "endpoint": "input", "observation": observation}).Count()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var js jetstream.JetStream
			if kind == api.DataConnectorTypeJetStream {
				conn, e := native.Connect(url)
				if e != nil {
					t.Fatal(e)
				}
				defer conn.Close()
				js, e = jetstream.New(conn)
				if e != nil {
					t.Fatal(e)
				}
				_, e = js.CreateStream(ctx, jetstream.StreamConfig{Name: ep.StreamName, Subjects: []string{ep.Subject}})
				if e != nil {
					t.Fatal(e)
				}
				_, e = js.CreateConsumer(ctx, ep.StreamName, jetstream.ConsumerConfig{Durable: ep.DurableName, FilterSubject: ep.Subject, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second})
				if e != nil {
					t.Fatal(e)
				}
			}
			got := make(chan ConsumerMessage, 4)
			h := &handler{ended: make(chan error, 4), failed: make(chan error, 1), consume: func(_ context.Context, _ StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
				got <- m
				r.Done()
				return nil
			}}
			_, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.env.source.Start(ctx); e != nil {
				t.Fatal(e)
			}
			defer s.env.source.Stop(ctx)
			out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}
			payload := []byte{0, 255, 128, 42, 0}
			oh := &outputHandler{payload: payload, headers: native.Header{"X-Test": []string{"preserved", "second"}}}
			pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, oh)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.env.sink.Start(ctx); e != nil {
				t.Fatal(e)
			}
			defer s.env.sink.Stop(ctx)
			pub.Consume(ctx, 42)
			if oh.err != nil {
				t.Fatal(oh.err)
			}
			delivery := receive(t, out.results)
			m := receive(t, got)
			receive(t, h.ended)
			if !bytes.Equal(m.Message.Data, payload) || !slices.Equal(m.Message.Header.Values("X-Test"), []string{"preserved", "second"}) {
				t.Fatal("payload/header lost")
			}
			if m.Message.Header.Get("x-stream-id") != "publication" {
				t.Fatal("sink correlation header missing")
			}
			if count("received") != 1 {
				t.Fatal("native delivery missing from source observations")
			}
			if kind == api.DataConnectorTypeNATS {
				if m.JetStream != nil || delivery.Ack != nil {
					t.Fatal("Core faked ACK")
				}
				return
			}
			if delivery.Ack == nil || delivery.Ack.Stream != "EVENTS" {
				t.Fatal("missing publish acknowledgement")
			}
			c, e := js.Consumer(ctx, ep.StreamName, ep.DurableName)
			if e != nil {
				t.Fatal(e)
			}
			info, e := c.Info(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if info.NumAckPending != 1 {
				t.Fatalf("Done ACKed delivery: %+v", info)
			}
			if e = m.JetStream.Nak(); e != nil {
				t.Fatal(e)
			}
			redelivery := receive(t, got)
			receive(t, h.ended)
			md, e := redelivery.JetStream.Metadata()
			if e != nil || md.NumDelivered < 2 {
				t.Fatalf("no NAK redelivery: %v %v", md, e)
			}
			if e = redelivery.JetStream.DoubleAck(ctx); e != nil {
				t.Fatal(e)
			}
			if count("received") != 2 || count("redelivered") != 1 {
				t.Fatal("NAK/ACK altered delivery observation counts")
			}
			info, e = c.Info(ctx)
			if e != nil || info.NumAckPending != 0 {
				t.Fatalf("explicit ACK not observed: %v %v", info, e)
			}
		})
	}
}

func TestMissingJetStreamResourcesAreNotCreated(t *testing.T) {
	url := server(t)
	s, ep := fixture(t, url, api.DataConnectorTypeJetStream, false)
	h := &handler{}
	_, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.source.Start(t.Context()); !errors.Is(e, jetstream.ErrStreamNotFound) {
		t.Fatalf("got %v", e)
	}
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	js, e := jetstream.New(conn)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = js.Stream(t.Context(), ep.StreamName); !errors.Is(e, jetstream.ErrStreamNotFound) {
		t.Fatalf("resource created without opt-in: %v", e)
	}
}

func TestStopBoundsBlockedHandler(t *testing.T) {
	url := server(t)
	s, _ := fixture(t, url, api.DataConnectorTypeNATS, false)
	entered := make(chan struct{})
	release := make(chan struct{})
	h := &handler{consume: func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error {
		close(entered)
		<-release
		return nil
	}}
	_, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.source.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.Publish("events", nil)
	conn.Flush()
	receive(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.env.source.Stop(ctx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Stop ignored deadline")
	}
	close(release)
	ep := s.env.source.GetEndpoint(2).(*endpoint)
	receive(t, ep.done)
}

func TestExplicitCreationAndExistingConsumerValidation(t *testing.T) {
	url := server(t)
	s, ep := fixture(t, url, api.DataConnectorTypeJetStream, false)
	ep.CreateStream = true
	ep.CreateConsumer = true
	h := &handler{consume: func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error {
		return nil
	}}
	if _, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
		t.Fatal(e)
	}
	if e := s.env.source.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	s.env.source.Stop(t.Context())
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	js, e := jetstream.New(conn)
	if e != nil {
		t.Fatal(e)
	}
	stream, e := js.Stream(t.Context(), "EVENTS")
	if e != nil {
		t.Fatal(e)
	}
	si, e := stream.Info(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if si.Config.Storage != jetstream.FileStorage || si.Config.Replicas != 1 {
		t.Fatal("creation defaults changed")
	}
	c, e := js.Consumer(t.Context(), "EVENTS", "worker")
	if e != nil {
		t.Fatal(e)
	}
	ci, e := c.Info(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if ci.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Fatal("creation not explicit ACK")
	}
	_, e = js.CreateConsumer(t.Context(), "EVENTS", jetstream.ConsumerConfig{Durable: "wrong", FilterSubject: "events", AckPolicy: jetstream.AckNonePolicy})
	if e != nil {
		t.Fatal(e)
	}
	s, ep = fixture(t, url, api.DataConnectorTypeJetStream, false)
	ep.DurableName = "wrong"
	ep.CreateConsumer = true
	if _, e = MakeNATSEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
		t.Fatal(e)
	}
	if e = s.env.source.Start(t.Context()); e == nil {
		t.Fatal("accepted non-explicit consumer")
	}
	c, e = js.Consumer(t.Context(), "EVENTS", "wrong")
	if e != nil {
		t.Fatal(e)
	}
	ci, e = c.Info(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if ci.Config.AckPolicy != jetstream.AckNonePolicy {
		t.Fatal("existing consumer mutated")
	}
}

func TestCorePermissionFailureIsNotPublishSuccess(t *testing.T) {
	path := t.TempDir() + "/server.conf"
	content := `authorization { users: [{ user: "test", password: "secret", permissions: { publish: { deny: ["events"] }, subscribe: { allow: [">"] } } }] }`
	if e := os.WriteFile(path, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	url := serverOptions(t, []string{"-c", path}, []native.Option{native.UserInfo("test", "secret")})
	s, _ := fixture(t, url, api.DataConnectorTypeNATS, false)
	dc := s.env.cfg.GetDataConnectorByID(1).(*config.NATSDataConnectorConfig)
	dc.Username = "test"
	dc.Password = "secret"
	out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}
	h := &outputHandler{}
	pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	defer s.env.sink.Stop(t.Context())
	pub.Consume(t.Context(), 1)
	if !errors.Is(h.err, native.ErrPermissionViolation) {
		t.Fatalf("permission rejection reported as %v", h.err)
	}
	if len(out.results) != 0 {
		t.Fatal("success result after rejection")
	}
}

func TestStopWaitsForAdmittedResultCallbackOnlyUntilDeadline(t *testing.T) {
	url := server(t)
	s, _ := fixture(t, url, api.DataConnectorTypeNATS, true)
	entered := make(chan struct{})
	release := make(chan struct{})
	callbackDone := make(chan struct{})
	h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, sc StreamContext[int, int, error], _ ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
		r.SetResultCallback("id", func(context.Context, StreamContext[int, int, error], struct{}, int) bool {
			r.Done()
			close(entered)
			<-release
			return true
		})
		sc.Collect(ctx, 1)
		return nil
	}}
	_, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	s.emit = func(ctx context.Context, v int) { go func() { defer close(callbackDone); s.result.Consume(ctx, v) }() }
	if e = s.env.source.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.Publish("events", nil)
	conn.Flush()
	receive(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.env.source.Stop(ctx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("callback drain exceeded stop bound")
	}
	if len(h.ended) != 0 {
		t.Fatal("EndRequest raced live callback")
	}
	close(release)
	receive(t, callbackDone)
	receive(t, h.ended)
	receive(t, s.env.source.GetEndpoint(2).(*endpoint).done)
}

func TestJetStreamSendReturnsBeforeAckAndBoundsPending(t *testing.T) {
	b := controlledServer(t, nil, nil)
	s, _ := fixture(t, b.url, api.DataConnectorTypeJetStream, false)
	ep := s.env.cfg.GetEndpointConfigByID(3).(*config.NATSEndpointConfig)
	ep.CreateStream = true
	dc := s.env.cfg.GetDataConnectorByID(1).(*config.NATSDataConnectorConfig)
	dc.OperationTimeoutMs = 2000
	out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 300)}
	h := &outputHandler{async: true, deliveries: make(chan error, 300)}
	pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	b.pause(t)
	defer b.resume(t)
	start := time.Now()
	for i := 0; i < 257; i++ {
		pub.Consume(t.Context(), i)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Send waited for publication ACK")
	}
	if e = receive(t, h.deliveries); !errors.Is(e, sinknats.ErrBackpressure) {
		t.Fatalf("expected bounded admission failure, got %v", e)
	}
	if len(h.deliveries) != 0 {
		t.Fatal("successful delivery reported before server ACK")
	}
	stopCtx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	start = time.Now()
	s.env.sink.Stop(stopCtx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("async drain exceeded deadline")
	}
	for i := 0; i < 256; i++ {
		if e = receive(t, h.deliveries); e == nil {
			t.Fatal("cancelled unconfirmed publication reported success")
		}
	}
}

func TestNativeAsyncAckPreservesRequestContext(t *testing.T) {
	b := controlledServer(t, nil, nil)
	s, _ := fixture(t, b.url, api.DataConnectorTypeJetStream, false)
	s.env.cfg.GetEndpointConfigByID(3).(*config.NATSEndpointConfig).CreateStream = true
	out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}
	h := &outputHandler{async: true, deliveries: make(chan error, 1)}
	pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	defer s.env.sink.Stop(t.Context())
	b.pause(t)
	pub.Consume(t.Context(), 1)
	if len(out.results) != 0 {
		t.Fatal("completion before ACK")
	}
	b.resume(t)
	if e = receive(t, h.deliveries); e != nil {
		t.Fatalf("ConsumeMessage return cancelled admitted async send: %v", e)
	}
	d := receive(t, out.results)
	if d.Ack == nil {
		t.Fatal("missing native PubAck")
	}
}

func TestSourceStopAllowsAdmittedJetStreamAck(t *testing.T) {
	url := server(t)
	s, ep := fixture(t, url, api.DataConnectorTypeJetStream, true)
	ep.CreateStream = true
	ep.CreateConsumer = true
	entered := make(chan struct{})
	release := make(chan struct{})
	ackErr := make(chan error, 1)
	h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, sc StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
		r.SetResultCallback("id", func(ctx context.Context, _ StreamContext[int, int, error], _ struct{}, _ int) bool {
			ackErr <- m.JetStream.DoubleAck(ctx)
			r.Done()
			return true
		})
		close(entered)
		<-release
		sc.Collect(ctx, 1)
		return nil
	}}
	_, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	s.emit = func(ctx context.Context, v int) { s.result.Consume(ctx, v) }
	if e = s.env.source.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	js, e := jetstream.New(conn)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = js.Publish(t.Context(), "events", nil); e != nil {
		t.Fatal(e)
	}
	receive(t, entered)
	stopped := make(chan struct{})
	stopCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { defer close(stopped); s.env.source.Stop(stopCtx) }()
	select {
	case <-stopped:
		t.Fatal("Stop did not drain admitted request")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if e = receive(t, ackErr); e != nil {
		t.Fatalf("Stop killed ACK transport/context before drain: %v", e)
	}
	receive(t, stopped)
	if e = receive(t, h.ended); e != nil {
		t.Fatal(e)
	}
}

func TestNativeReconnectResumesCoreAndJetStream(t *testing.T) {
	for _, kind := range []api.DataConnectorType{api.DataConnectorTypeNATS, api.DataConnectorTypeJetStream} {
		t.Run(strconv.Itoa(int(kind)), func(t *testing.T) {
			b := controlledServer(t, nil, nil)
			s, ep := fixture(t, b.url, kind, false)
			if kind == api.DataConnectorTypeJetStream {
				ep.CreateStream = true
				ep.CreateConsumer = true
			}
			received := make(chan ConsumerMessage, 4)
			h := &handler{consume: func(ctx context.Context, _ StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
				received <- m
				if m.JetStream != nil {
					return m.JetStream.DoubleAck(ctx)
				}
				return nil
			}}
			if _, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
				t.Fatal(e)
			}
			if e := s.env.source.Start(t.Context()); e != nil {
				t.Fatal(e)
			}
			defer s.env.source.Stop(t.Context())
			out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 4)}
			oh := &outputHandler{async: true, deliveries: make(chan error, 4)}
			pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, oh)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.env.sink.Start(t.Context()); e != nil {
				t.Fatal(e)
			}
			defer s.env.sink.Stop(t.Context())
			pub.Consume(t.Context(), 1)
			if e = receive(t, oh.deliveries); e != nil {
				t.Fatal(e)
			}
			receive(t, received)
			nativeSource := s.env.source.GetEndpoint(2).(*endpoint)
			before := nativeSource.conn.Stats().Reconnects
			// Initial connect's timeout has expired before reconnect is attempted: a
			// startup-only dial context accidentally retained by the client would fail.
			time.Sleep(2100 * time.Millisecond)
			b.restart(t)
			deadline := time.Now().Add(4 * time.Second)
			for nativeSource.conn.Stats().Reconnects == before && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if nativeSource.conn.Stats().Reconnects == before {
				t.Fatal("source did not reconnect")
			}
			// Sink reconnect may follow source by one native reconnect interval.
			time.Sleep(500 * time.Millisecond)
			pub.Consume(t.Context(), 2)
			if e = receive(t, oh.deliveries); e != nil {
				t.Fatal(e)
			}
			m := receive(t, received)
			if string(m.Message.Data) != "2" {
				t.Fatalf("unexpected resumed message %q", m.Message.Data)
			}
		})
	}
}

func TestSourceExtractsMixedCaseCorrelationBeforeBeginRequest(t *testing.T) {
	s, _ := fixture(t, "nats://127.0.0.1:1", api.DataConnectorTypeNATS, false)
	h := &handler{consume: func(ctx context.Context, _ StreamContext[int, int, error], _ ConsumerMessage, _ ResultContext[struct{}, int, int, error]) error {
		sid, ok := runtime.StreamIdFromContext(ctx)
		if !ok || sid.GetID() != "inherited" {
			t.Error("source replaced inherited stream ID")
		}
		if !tracing.SamplingEnabled(ctx) {
			t.Error("source lost sampling marker")
		}
		return nil
	}}
	c, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h)
	if e != nil {
		t.Fatal(e)
	}
	c.(*consumer[struct{}, int, int, error]).process(t.Context(), ConsumerMessage{Message: &native.Msg{Header: native.Header{"X-StReAm-ID": []string{"inherited"}, "X-TrAcE": []string{"1"}}}})
}

func TestAsyncCoreReportsPermissionErrorThroughOnError(t *testing.T) {
	path := t.TempDir() + "/server.conf"
	content := `authorization { users: [{ user: "test", password: "secret", permissions: { publish: { deny: ["events"] }, subscribe: { allow: [">"] } } }] }`
	if e := os.WriteFile(path, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	url := serverOptions(t, []string{"-c", path}, []native.Option{native.UserInfo("test", "secret")})
	s, _ := fixture(t, url, api.DataConnectorTypeNATS, false)
	dc := s.env.cfg.GetDataConnectorByID(1).(*config.NATSDataConnectorConfig)
	dc.Username = "test"
	dc.Password = "secret"
	out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}
	h := &outputHandler{async: true, deliveries: make(chan error, 1), transportErrors: make(chan error, 2)}
	pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	defer s.env.sink.Stop(t.Context())
	pub.Consume(t.Context(), 1)
	if e = receive(t, h.deliveries); e != nil {
		t.Fatal(e)
	}
	d := receive(t, out.results)
	if !d.Accepted || d.Flushed || d.Ack != nil {
		t.Fatalf("dishonest Core async result: %+v", d)
	}
	if e = receive(t, h.transportErrors); !errors.Is(e, native.ErrPermissionViolation) {
		t.Fatalf("server rejection not surfaced: %v", e)
	}
}

type deliveryStream struct {
	runtime.TypedConsumedStream[sinknats.Delivery]
	values []sinknats.Delivery
}

func (s *deliveryStream) Consume(_ context.Context, d sinknats.Delivery) {
	s.values = append(s.values, d)
}

type legacyOutput struct {
	*outputStream
	route deliveryStream
}

func (s *legacyOutput) GetErrorStream() runtime.TypedConsumedStream[sinknats.Delivery] {
	return &s.route
}
func TestOrdinarySinkPreservesDeclaredResultRoute(t *testing.T) {
	url := server(t)
	s, _ := fixture(t, url, api.DataConnectorTypeNATS, false)
	out := &legacyOutput{outputStream: &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}}
	h := &outputHandler{}
	pub, e := sinknats.MakeNATSEndpointConsumer[struct{}, int, sinknats.Delivery](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	defer s.env.sink.Stop(t.Context())
	pub.Consume(t.Context(), 1)
	if h.err != nil || len(out.route.values) != 1 || len(out.results) != 0 {
		t.Fatalf("ordinary Sink result route changed: %v %+v", h.err, out.route.values)
	}
}

func TestCoreQueueGroupDistributesWithoutDuplicateDelivery(t *testing.T) {
	url := server(t)
	got := make(chan int, 32)
	for i := 0; i < 2; i++ {
		s, ep := fixture(t, url, api.DataConnectorTypeNATS, false)
		ep.QueueGroup = "workers"
		worker := i
		h := &handler{consume: func(context.Context, StreamContext[int, int, error], ConsumerMessage, ResultContext[struct{}, int, int, error]) error {
			got <- worker
			return nil
		}}
		if _, e := MakeNATSEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
			t.Fatal(e)
		}
		if e := s.env.source.Start(t.Context()); e != nil {
			t.Fatal(e)
		}
		defer s.env.source.Stop(t.Context())
	}
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	for i := 0; i < 20; i++ {
		if e = conn.Publish("events", []byte(strconv.Itoa(i))); e != nil {
			t.Fatal(e)
		}
	}
	conn.Flush()
	counts := [2]int{}
	for i := 0; i < 20; i++ {
		counts[receive(t, got)]++
	}
	if counts[0] == 0 || counts[1] == 0 {
		t.Fatal("queue group did not distribute")
	}
	select {
	case <-got:
		t.Fatal("duplicate queue delivery")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestIncompatibleExistingStreamFailsStartup(t *testing.T) {
	url := server(t)
	conn, e := native.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	js, e := jetstream.New(conn)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"other.>"}}); e != nil {
		t.Fatal(e)
	}
	s, ep := fixture(t, url, api.DataConnectorTypeJetStream, false)
	ep.CreateStream = true
	ep.CreateConsumer = true
	h := &handler{}
	if _, e = MakeNATSEndpointConsumer[struct{}, int, int, error](s, h); e != nil {
		t.Fatal(e)
	}
	if e = s.env.source.Start(t.Context()); e == nil {
		t.Fatal("incompatible existing stream accepted by source")
	}
	out := &outputStream{env: s.env}
	if _, e = sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, &outputHandler{}); e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e == nil {
		t.Fatal("incompatible existing stream accepted by sink")
	}
}

type delayedOutputHandler struct {
	*outputHandler
	entered chan struct{}
	release chan struct{}
}

func (h *delayedOutputHandler) ConsumeMessage(ctx context.Context, stream runtime.Stream, state struct{}, v int, m *sinknats.SinkMessage[sinknats.Delivery]) error {
	close(h.entered)
	<-h.release
	return h.outputHandler.ConsumeMessage(ctx, stream, state, v, m)
}
func TestSinkStopDrainsPublicationFromAlreadyAdmittedHandler(t *testing.T) {
	url := server(t)
	s, _ := fixture(t, url, api.DataConnectorTypeJetStream, false)
	s.env.cfg.GetEndpointConfigByID(3).(*config.NATSEndpointConfig).CreateStream = true
	out := &outputStream{env: s.env, results: make(chan sinknats.Delivery, 1)}
	h := &delayedOutputHandler{outputHandler: &outputHandler{async: true, deliveries: make(chan error, 1)}, entered: make(chan struct{}), release: make(chan struct{})}
	pub, e := sinknats.MakeNATSEndpointConsumerWithResult[struct{}, int, sinknats.Delivery, error](out, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.env.sink.Start(t.Context()); e != nil {
		t.Fatal(e)
	}
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); pub.Consume(t.Context(), 1) }()
	receive(t, h.entered)
	stopCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() { defer close(stopped); s.env.sink.Stop(stopCtx) }()
	select {
	case <-stopped:
		t.Fatal("admitted handler not drained")
	case <-time.After(30 * time.Millisecond):
	}
	close(h.release)
	receive(t, handlerDone)
	if e = receive(t, h.deliveries); e != nil {
		t.Fatalf("shutdown rejected admitted handler's publication: %v", e)
	}
	if d := receive(t, out.results); d.Ack == nil {
		t.Fatal("drain did not wait for ACK")
	}
	receive(t, stopped)
}

func TestStopDuringStartupCancelsHandshakeImmediately(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := listener.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	s, _ := fixture(t, "nats://"+listener.Addr().String(), api.DataConnectorTypeNATS, false)
	if _, e = MakeNATSEndpointConsumer[struct{}, int, int, error](s, &handler{}); e != nil {
		t.Fatal(e)
	}
	started := make(chan error, 1)
	go func() { started <- s.env.source.Start(t.Context()) }()
	conn := receive(t, accepted)
	defer conn.Close()
	stopCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	start := time.Now()
	s.env.source.Stop(stopCtx)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Stop waited for connection timeout despite no admitted requests")
	}
	if e = receive(t, started); !errors.Is(e, context.Canceled) {
		t.Fatalf("startup cancellation lost: %v", e)
	}
}
