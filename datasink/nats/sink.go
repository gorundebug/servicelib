// Package nats implements native Core NATS and JetStream sinks.
package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/internal/natsclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	native "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// EndpointHandler uses the same request/result lifecycle as the Kafka sink.
// EndRequest follows ConsumeMessage return, independently of delivery. Async
// Send callbacks can outlive EndRequest; do not release state they still capture.
// Implementations must honor cancellation and must not retain SinkMessage.
type EndpointHandler[H, T, R any] interface {
	OnError(context.Context, runtime.Stream, error)
	GetStreamID(context.Context, T) string
	BeginRequest(context.Context, runtime.Stream) (context.Context, H)
	ConsumeMessage(context.Context, runtime.Stream, H, T, *SinkMessage[R]) error
	EndRequest(context.Context, runtime.Stream, error, H)
}

// Delivery distinguishes native client acceptance, Core protocol flush, and a
// durable JetStream ACK. No Core outcome confirms subscriber delivery.
type Delivery struct {
	Ack      *jetstream.PubAck
	Accepted bool // accepted by the native client, including its bounded reconnect buffer
	Flushed  bool // Core SendSync protocol flush only; never subscriber acknowledgement
}

var ErrBackpressure = errors.New("NATS asynchronous publication capacity exhausted")

type SinkMessage[R any] struct {
	publication *brokermetrics.Publication
	Data        []byte
	Header      native.Header
	subject     string
	publish     func(context.Context, *native.Msg) (Delivery, error)
	sendAsync   func(context.Context, *native.Msg, func(Delivery, error))
	result      runtime.Consumer[R]
}

func (m *SinkMessage[R]) Subject() string { return m.subject }

// SendSync waits for Core flush or JetStream publication ACK, bounded by both
// caller cancellation and connector operation timeout. It never retries a send.
func (m *SinkMessage[R]) SendSync(ctx context.Context) (delivery Delivery, err error) {
	defer func() { m.observe(ctx, delivery, err) }()
	return m.publish(ctx, &native.Msg{Subject: m.subject, Data: m.Data, Header: m.Header})
}

// Send returns after bounded native admission. Delivery conversion and result
// emission run on completion; admission errors may call onDelivery inline.
func (m *SinkMessage[R]) Send(ctx context.Context, onDelivery func(Delivery, error) R) {
	m.sendAsync(ctx, &native.Msg{Subject: m.subject, Data: m.Data, Header: m.Header}, func(d Delivery, e error) { m.observe(ctx, d, e); m.Out(ctx, onDelivery(d, e)) })
}
func (m *SinkMessage[R]) Out(ctx context.Context, result R)  { m.result.Consume(ctx, result) }
func (m *SinkMessage[R]) Skip(ctx context.Context, result R) { m.Out(ctx, result) }

func (m *SinkMessage[R]) observe(ctx context.Context, d Delivery, err error) {
	outcome := "error"
	if err == nil {
		switch {
		case d.Ack != nil:
			outcome = "acknowledged"
		case d.Flushed:
			outcome = "flushed"
		case d.Accepted:
			outcome = "client-accepted"
		}
	}
	m.publication.Observe(ctx, outcome)
}

type sink struct{ *runtime.OutputDataSink }
type endpoint struct {
	publication *brokermetrics.Publication
	*runtime.DataSinkEndpoint
	mu                  sync.Mutex
	started, stopped    bool
	ctx                 context.Context
	cancel              context.CancelFunc
	ready               chan struct{}
	conn                *native.Conn
	js                  jetstream.JetStream
	subject, streamName string
	timeout             time.Duration
	tracingEngine       tracing.Tracing
	gate                natsclient.Gate
	slots               chan struct{}
	bytes               int
	onErrors            []func(context.Context, error)
	errorsDone          chan struct{}
}

func (ds *sink) Start(ctx context.Context) error {
	eps := ds.GetEndpoints()
	for i := 0; i < eps.Len(); i++ {
		if err := eps.At(i).(*endpoint).start(ctx); err != nil {
			ds.Stop(ctx)
			return err
		}
	}
	return nil
}
func (ds *sink) Stop(ctx context.Context) {
	eps := ds.GetEndpoints()
	for i := 0; i < eps.Len(); i++ {
		ep := eps.At(i).(*endpoint)
		ep.mu.Lock()
		ep.stopped = true
		ep.gate.Close()
		ep.mu.Unlock()
	}
	for i := 0; i < eps.Len(); i++ {
		ep := eps.At(i).(*endpoint)
		ep.stop(ctx)
	}
}
func (ep *endpoint) stop(ctx context.Context) {
	ep.mu.Lock()
	ep.stopped = true
	done := ep.gate.Close()
	ready := ep.ready
	cancel := ep.cancel
	ep.mu.Unlock()
	// Let admitted handlers/delivery callbacks drain until the caller deadline.
	select {
	case <-done:
	case <-ctx.Done():
		ep.GetDataSink().(*sink).OnStopTimeout(ctx)
	}
	if cancel != nil {
		cancel()
	}
	ep.mu.Lock()
	conn := ep.conn
	ep.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	ep.mu.Lock()
	js, errorsDone := ep.js, ep.errorsDone
	ep.mu.Unlock()
	if js != nil {
		js.CleanupPublisher()
	}
	if errorsDone != nil {
		select {
		case <-errorsDone:
		case <-ctx.Done():
		}
	}
	if ready != nil {
		select {
		case <-ready:
		case <-ctx.Done():
		}
	}
}
func (ep *endpoint) start(parent context.Context) (err error) {
	ep.mu.Lock()
	if ep.started || ep.stopped {
		ep.mu.Unlock()
		return fmt.Errorf("NATS sink lifecycle already started or stopped")
	}
	ep.started = true
	ep.ctx, ep.cancel = context.WithCancel(parent)
	ctx := ep.ctx
	ep.ready = make(chan struct{})
	ready := ep.ready
	ep.mu.Unlock()
	defer close(ready)
	cfg, ok := ep.GetConfig().(*config.NATSEndpointConfig)
	if !ok {
		return fmt.Errorf("invalid NATS endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSink().GetConfig().(*config.NATSDataConnectorConfig)
	if !ok {
		return fmt.Errorf("invalid NATS connector config")
	}
	if err = natsclient.Subject(cfg.Subject, true); err != nil {
		return err
	}
	if cfg.QueueGroup != "" || cfg.DurableName != "" || cfg.CreateConsumer {
		return fmt.Errorf("NATS sink cannot configure a subscription or consumer")
	}
	transportErrors := make(chan error, 1)
	conn, err := natsclient.Connect(ctx, dc, func(e error) {
		select {
		case transportErrors <- e:
		default:
		}
	})
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	var js jetstream.JetStream
	if dc.GetType() == api.DataConnectorTypeJetStream {
		js, err = jetstream.New(conn, jetstream.WithPublishAsyncMaxPending(natsclient.PendingMessages), jetstream.WithPublishAsyncTimeout(dc.OperationTimeout()))
		if err != nil {
			return err
		}
		op, cancel := context.WithTimeout(ctx, dc.OperationTimeout())
		defer cancel()
		if _, err = natsclient.Stream(op, js, cfg); err != nil {
			return err
		}
	} else if cfg.StreamName != "" || cfg.CreateStream {
		return fmt.Errorf("Core NATS sink cannot configure a stream")
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.stopped {
		return errors.New("NATS sink stopped during startup")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	ep.conn = conn
	ep.js = js
	ep.subject = cfg.Subject
	ep.streamName = cfg.StreamName
	ep.timeout = dc.OperationTimeout()
	ep.tracingEngine = ep.GetRuntimeEnvironment().Tracing()
	ep.slots = make(chan struct{}, natsclient.PendingMessages)
	ep.errorsDone = make(chan struct{})
	go func() {
		defer close(ep.errorsDone)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-transportErrors:
				if ctx.Err() != nil {
					return
				}
				for _, report := range ep.onErrors {
					report(ctx, e)
				}
			}
		}
	}()
	ep.gate.Open()
	success = true
	context.AfterFunc(ctx, conn.Close)
	return nil
}
func (ep *endpoint) publish(ctx context.Context, msg *native.Msg) (Delivery, error) {
	msg.Header = natsclient.Inject(ctx, ep.tracingEngine, msg.Header)
	op, cancel := context.WithTimeout(ctx, ep.timeout)
	defer cancel()
	stop := context.AfterFunc(ep.ctx, cancel)
	defer stop()
	if err := op.Err(); err != nil {
		return Delivery{}, err
	}
	if ep.ctx.Err() != nil {
		return Delivery{}, ep.ctx.Err()
	}
	if ep.js != nil {
		ack, err := ep.js.PublishMsg(op, msg, jetstream.WithExpectStream(ep.streamName), jetstream.WithRetryAttempts(0))
		return Delivery{Ack: ack, Accepted: err == nil}, err
	}
	if err := ep.conn.PublishMsg(msg); err != nil {
		return Delivery{}, err
	}
	if err := ep.conn.FlushWithContext(op); err != nil {
		return Delivery{}, err
	}
	// A PONG also follows any permission rejection for preceding publications.
	// Core has no per-message ACK, so a connection error conservatively poisons
	// subsequent sends rather than reporting a rejected publication as success.
	err := ep.conn.LastError()
	return Delivery{Accepted: err == nil, Flushed: err == nil}, err
}

// sendAsync performs bounded, non-waiting admission. Each admitted native future
// has one bounded completion goroutine; neither a queue nor an executor is hidden.
func (ep *endpoint) sendAsync(ctx context.Context, msg *native.Msg, callback func(Delivery, error)) {
	msg.Header = natsclient.Inject(ctx, ep.tracingEngine, msg.Header)
	size := msg.Size()
	ep.mu.Lock()
	if !ep.gate.Retain() {
		ep.mu.Unlock()
		callback(Delivery{}, errors.New("NATS sink stopped"))
		return
	}
	if len(ep.slots) == cap(ep.slots) || size > natsclient.PendingBytes-ep.bytes {
		ep.mu.Unlock()
		ep.gate.Leave()
		callback(Delivery{}, ErrBackpressure)
		return
	}
	ep.slots <- struct{}{}
	ep.bytes += size
	ep.mu.Unlock()
	release := func() { ep.mu.Lock(); ep.bytes -= size; <-ep.slots; ep.mu.Unlock(); ep.gate.Leave() }
	op, cancel := context.WithTimeout(ctx, ep.timeout)
	stop := context.AfterFunc(ep.ctx, cancel)
	finish := func(d Delivery, e error) { defer release(); defer cancel(); defer stop(); callback(d, e) }
	if e := op.Err(); e != nil {
		finish(Delivery{}, e)
		return
	}
	if e := ep.ctx.Err(); e != nil {
		finish(Delivery{}, e)
		return
	}
	// Native async publication retains the message until acknowledgement.
	copyMsg := &native.Msg{Subject: msg.Subject, Data: append([]byte(nil), msg.Data...), Header: make(native.Header, len(msg.Header))}
	for k, v := range msg.Header {
		copyMsg.Header[k] = append([]string(nil), v...)
	}
	if ep.js == nil {
		e := ep.conn.PublishMsg(copyMsg)
		go finish(Delivery{Accepted: e == nil}, e)
		return
	}
	future, e := ep.js.PublishMsgAsync(copyMsg, jetstream.WithExpectStream(ep.streamName), jetstream.WithStallWait(time.Nanosecond), jetstream.WithRetryAttempts(0))
	if e != nil {
		finish(Delivery{}, e)
		return
	}
	go func() {
		select {
		case ack := <-future.Ok():
			finish(Delivery{Ack: ack, Accepted: true}, nil)
		case e := <-future.Err():
			finish(Delivery{Accepted: true}, e)
		case <-op.Done():
			// Report cancellation promptly, but retain the byte/slot budget until the
			// SDK releases its retained publication (ACK, timeout, or CleanupPublisher).
			callback(Delivery{Accepted: true}, op.Err())
			select {
			case <-future.Ok():
			case <-future.Err():
			}
			release()
			stop()
			cancel()
		}
	}()
}

type requestLifetime struct {
	refs    atomic.Int32
	cleanup func()
}

func (l *requestLifetime) release() {
	if l.refs.Add(-1) == 0 {
		l.cleanup()
	}
}

type sinkStream[T any] interface {
	runtime.RuntimeStream
	runtime.Stream
	GetEndpointId() int
	SetSinkConsumer(runtime.Consumer[T])
}
type consumer[H, T, R any] struct {
	endpoint *endpoint
	stream   sinkStream[T]
	result   runtime.Consumer[R]
	handler  EndpointHandler[H, T, R]
	mu       sync.RWMutex
	callback runtime.SinkCallback[T]
}

func (ec *consumer[H, T, R]) Endpoint() runtime.SinkEndpoint      { return ec.endpoint }
func (ec *consumer[H, T, R]) Stream() runtime.Stream              { return ec.stream }
func (ec *consumer[H, T, R]) GetID() int                          { return ec.endpoint.GetID() }
func (ec *consumer[H, T, R]) FunctionImplementation() interface{} { return ec.handler }
func (ec *consumer[H, T, R]) SetSinkCallback(cb runtime.SinkCallback[T]) {
	ec.mu.Lock()
	ec.callback = cb
	ec.mu.Unlock()
}
func (ec *consumer[H, T, R]) complete(ctx context.Context, value T, err error) {
	ec.mu.RLock()
	cb := ec.callback
	ec.mu.RUnlock()
	if cb != nil {
		cb.Done(ctx, value, err)
	}
}
func (ec *consumer[H, T, R]) Consume(ctx context.Context, value T) {
	ep := ec.endpoint
	if !ep.gate.Enter() {
		ec.complete(ctx, value, errors.New("NATS sink is not running"))
		return
	}
	defer ep.gate.Leave()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(ep.ctx, cancel)
	hc := runtime.WithStreamId(ctx, ec.handler.GetStreamID(ctx, value))
	hc, state := ec.handler.BeginRequest(hc, ec.stream)
	if hc == nil {
		hc = ctx
	}
	hc, hcancel := context.WithCancel(hc)
	hstop := context.AfterFunc(ctx, hcancel)
	life := &requestLifetime{cleanup: func() { hstop(); hcancel(); stop(); cancel() }}
	life.refs.Store(1)
	defer life.release()
	start := ep.OnRequestStart(hc)
	msg := &SinkMessage[R]{publication: ep.publication, subject: ep.subject, result: ec.result, sendAsync: func(sendCtx context.Context, m *native.Msg, callback func(Delivery, error)) {
		life.refs.Add(1)
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		ep.sendAsync(sendCtx, m, func(d Delivery, e error) { defer life.release(); defer sstop(); defer scancel(); callback(d, e) })
	}, publish: func(sendCtx context.Context, m *native.Msg) (Delivery, error) {
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		defer sstop()
		defer scancel()
		return ep.publish(sendCtx, m)
	}}
	err := ec.handler.ConsumeMessage(hc, ec.stream, state, value, msg)
	ec.handler.EndRequest(hc, ec.stream, err, state)
	ep.OnRequestEnd(hc, start, err)
	ec.complete(ctx, value, err)
}

func MakeNATSEndpointConsumer[H, T, R any](stream runtime.TypedSinkStream[T, R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, stream.GetErrorStream(), handler)
}

// MakeNATSEndpointConsumerWithResult routes R to ordinary continuation output,
// never to E. Error construction remains the handler's explicit typed contract.
func MakeNATSEndpointConsumerWithResult[H, T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, runtime.ConsumerFunc[R](stream.ConsumeResult), handler)
}
func makeConsumer[H, T, R any](stream sinkStream[T], result runtime.Consumer[R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("NATS sink handler is nil")
	}
	env := stream.GetRuntimeEnvironment()
	cfg, ok := env.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId()).(*config.NATSEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid NATS endpoint config")
	}
	dc, ok := env.RuntimeConfig().GetDataConnectorByID(cfg.IdDataConnector).(*config.NATSDataConnectorConfig)
	if !ok {
		return nil, fmt.Errorf("invalid NATS connector config")
	}
	var ds *sink
	if existing := env.GetDataSink(dc.ID); existing != nil {
		ds, ok = existing.(*sink)
		if !ok {
			return nil, fmt.Errorf("connector already registered with another sink kind")
		}
	} else {
		base, err := runtime.MakeOutputDataSink(dc, env)
		if err != nil {
			return nil, err
		}
		ds = &sink{base}
		env.AddDataSink(ds)
	}
	var ep *endpoint
	if existing := ds.GetEndpoint(cfg.ID); existing != nil {
		ep, ok = existing.(*endpoint)
		if !ok {
			return nil, fmt.Errorf("endpoint is not a NATS sink")
		}
	} else {
		base, err := runtime.MakeDataSinkEndpoint(ds, cfg.ID, env)
		if err != nil {
			return nil, err
		}
		protocol := "nats"
		if dc.Type == api.DataConnectorTypeJetStream {
			protocol = "jetstream"
		}
		publication, err := brokermetrics.NewPublication(env.Metrics(), protocol, dc.GetName(), cfg.GetName(), "client-accepted", "flushed", "acknowledged", "error")
		if err != nil {
			return nil, err
		}
		ep = &endpoint{DataSinkEndpoint: base, publication: publication}
		ds.AddEndpoint(ep)
	}
	ec := &consumer[H, T, R]{endpoint: ep, stream: stream, result: result, handler: handler}
	ep.onErrors = append(ep.onErrors, func(ctx context.Context, e error) { handler.OnError(ctx, stream, e) })
	stream.SetSinkConsumer(ec)
	ep.AddEndpointConsumer(ec)
	env.RegisterEndpointConsumer(ec)
	return ec, nil
}
