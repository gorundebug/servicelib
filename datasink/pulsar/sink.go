// Package pulsar implements native Pulsar sinks.
package pulsar

import (
	"context"
	"errors"
	"fmt"
	native "github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/internal/pulsarclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"sync"
	"sync/atomic"
)

// EndpointHandler separates handler completion from native delivery completion.
// Async callbacks may outlive EndRequest; captured state must remain valid.
type EndpointHandler[H, T, R any] interface {
	OnError(context.Context, runtime.Stream, error)
	GetStreamID(context.Context, T) string
	BeginRequest(context.Context, runtime.Stream) (context.Context, H)
	ConsumeMessage(context.Context, runtime.Stream, H, T, *SinkMessage[R]) error
	EndRequest(context.Context, runtime.Stream, error, H)
}
type Outcome string

const (
	NotSent  Outcome = "not-sent"
	Accepted Outcome = "accepted"
	Rejected Outcome = "rejected"
	Unknown  Outcome = "unknown"
)

type Delivery struct {
	Outcome   Outcome
	MessageID native.MessageID
}

var ErrBackpressure = pulsarclient.ErrCapacity
var ErrRetired = errors.New("Pulsar handler invocation has ended")

type SinkMessage[R any] struct {
	publication *brokermetrics.Publication
	Message     native.ProducerMessage
	publish     func(context.Context, *native.ProducerMessage) (Delivery, error)
	sendAsync   func(context.Context, *native.ProducerMessage, func(Delivery, error))
	result      runtime.Consumer[R]
	mu          sync.Mutex
	retired     bool
	retain      func() bool
	release     func()
}

func (m *SinkMessage[R]) begin() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired {
		return ErrRetired
	}
	if !m.retain() {
		return errors.New("Pulsar sink is stopped")
	}
	return nil
}

func (m *SinkMessage[R]) retire() {
	m.mu.Lock()
	m.retired = true
	m.mu.Unlock()
}

func (m *SinkMessage[R]) SendSync(ctx context.Context) (delivery Delivery, err error) {
	defer func() { m.observe(ctx, delivery, err) }()
	if err := m.begin(); err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	defer m.release()
	return m.publish(ctx, &m.Message)
}
func (m *SinkMessage[R]) Send(ctx context.Context, callback func(Delivery, error) R) {
	if err := m.begin(); err != nil {
		m.observe(ctx, Delivery{Outcome: NotSent}, err)
		m.Out(ctx, callback(Delivery{Outcome: NotSent}, err))
		return
	}
	defer m.release()
	m.sendAsync(ctx, &m.Message, func(d Delivery, e error) { m.observe(ctx, d, e); m.Out(ctx, callback(d, e)) })
}

func (m *SinkMessage[R]) Out(ctx context.Context, value R)  { m.result.Consume(ctx, value) }
func (m *SinkMessage[R]) Skip(ctx context.Context, value R) { m.Out(ctx, value) }

func (m *SinkMessage[R]) observe(ctx context.Context, d Delivery, _ error) {
	outcome := string(d.Outcome)
	if outcome == "" {
		outcome = "unknown"
	}
	m.publication.Observe(ctx, outcome)
}

type sink struct{ *runtime.OutputDataSink }
type endpoint struct {
	publication *brokermetrics.Publication
	*runtime.DataSinkEndpoint
	mu               sync.Mutex
	started, stopped bool
	ctx              context.Context
	cancel           context.CancelFunc
	ready            chan struct{}
	session          *pulsarclient.Session
	gate             pulsarclient.Gate
	onErrors         []func(context.Context, error)
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
		ep.mu.Lock()
		done, ready, cancel := ep.gate.Close(), ep.ready, ep.cancel
		ep.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			ds.OnStopTimeout(ctx)
		}
		if cancel != nil {
			cancel()
		}
		if ready != nil {
			select {
			case <-ready:
			case <-ctx.Done():
			}
		}
		ep.mu.Lock()
		session := ep.session
		ep.mu.Unlock()
		if session != nil {
			_ = session.Close(ctx)
		}
	}
}
func (ep *endpoint) start(parent context.Context) (err error) {
	ep.mu.Lock()
	if ep.started || ep.stopped {
		ep.mu.Unlock()
		return errors.New("Pulsar sink lifecycle already started or stopped")
	}
	ep.started = true
	ep.ctx, ep.cancel = context.WithCancel(parent)
	ep.ready = make(chan struct{})
	ready := ep.ready
	ep.mu.Unlock()
	defer close(ready)
	cfg, ok := ep.GetConfig().(*config.PulsarEndpointConfig)
	if !ok {
		return errors.New("invalid Pulsar endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSink().GetConfig().(*config.PulsarDataConnectorConfig)
	if !ok {
		return errors.New("invalid Pulsar connector config")
	}
	session, err := pulsarclient.Open(ep.ctx, dc, cfg, false)
	if err != nil {
		return err
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.stopped {
		_ = session.Close(ep.ctx)
		return errors.New("Pulsar sink stopped during startup")
	}
	ep.session = session
	ep.gate.Open()
	return nil
}
func nativeOutcome(id native.MessageID, err error) Delivery {
	if err == nil {
		return Delivery{Outcome: Accepted, MessageID: id}
	}
	if errors.Is(err, pulsarclient.ErrNotSent) {
		return Delivery{Outcome: NotSent}
	}
	var nativeErr *native.Error
	if errors.As(err, &nativeErr) {
		switch nativeErr.Result() {
		case native.ProducerQueueIsFull, native.MessageTooBig, native.InvalidMessage:
			return Delivery{Outcome: NotSent}
		case native.AuthorizationError, native.AuthenticationError, native.TopicTerminated:
			return Delivery{Outcome: Rejected}
		}
	}
	return Delivery{Outcome: Unknown}
}
func (ep *endpoint) publish(ctx context.Context, m *native.ProducerMessage) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	id, err := ep.session.Send(ctx, m)
	if errors.Is(err, pulsarclient.ErrCapacity) || errors.Is(err, pulsarclient.ErrClosed) {
		return Delivery{Outcome: NotSent}, err
	}
	return nativeOutcome(id, err), err
}
func (ep *endpoint) sendAsync(ctx context.Context, m *native.ProducerMessage, callback func(Delivery, error)) {
	if !ep.gate.Retain() {
		callback(Delivery{Outcome: NotSent}, errors.New("Pulsar sink is stopped"))
		return
	}
	err := ep.session.SendAsync(ctx, m, func(id native.MessageID, err error) { defer ep.gate.Leave(); callback(nativeOutcome(id, err), err) })
	if err != nil {
		ep.gate.Leave()
		callback(Delivery{Outcome: NotSent}, err)
	}
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
		ec.complete(ctx, value, errors.New("Pulsar sink is not running"))
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
	msg := &SinkMessage[R]{publication: ep.publication, result: ec.result, sendAsync: func(sendCtx context.Context, m *native.ProducerMessage, callback func(Delivery, error)) {
		life.refs.Add(1)
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		ep.sendAsync(sendCtx, m, func(d Delivery, e error) { defer life.release(); defer sstop(); defer scancel(); callback(d, e) })
	}, publish: func(sendCtx context.Context, m *native.ProducerMessage) (Delivery, error) {
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		defer sstop()
		defer scancel()
		return ep.publish(sendCtx, m)
	}}
	msg.retain = func() bool {
		if !ep.gate.Retain() {
			return false
		}
		life.refs.Add(1)
		return true
	}
	msg.release = func() { life.release(); ep.gate.Leave() }
	defer msg.retire()
	err := ec.handler.ConsumeMessage(hc, ec.stream, state, value, msg)
	msg.retire()
	ec.handler.EndRequest(hc, ec.stream, err, state)
	ep.OnRequestEnd(hc, start, err)
	ec.complete(ctx, value, err)
}

func MakePulsarEndpointConsumer[H, T, R any](stream runtime.TypedSinkStream[T, R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, stream.GetErrorStream(), handler)
}

// MakePulsarEndpointConsumerWithResult routes R to ordinary continuation output,
// never to E. Error construction remains the handler's explicit typed contract.
func MakePulsarEndpointConsumerWithResult[H, T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, runtime.ConsumerFunc[R](stream.ConsumeResult), handler)
}
func makeConsumer[H, T, R any](stream sinkStream[T], result runtime.Consumer[R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("Pulsar sink handler is nil")
	}
	env := stream.GetRuntimeEnvironment()
	cfg, ok := env.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId()).(*config.PulsarEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid Pulsar endpoint config")
	}
	dc, ok := env.RuntimeConfig().GetDataConnectorByID(cfg.IdDataConnector).(*config.PulsarDataConnectorConfig)
	if !ok {
		return nil, fmt.Errorf("invalid Pulsar connector config")
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
			return nil, fmt.Errorf("endpoint is not a Pulsar sink")
		}
	} else {
		base, err := runtime.MakeDataSinkEndpoint(ds, cfg.ID, env)
		if err != nil {
			return nil, err
		}
		protocol := "pulsar"
		publication, err := brokermetrics.NewPublication(env.Metrics(), protocol, dc.GetName(), cfg.GetName(), "not-sent", "accepted", "rejected", "unknown")
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
