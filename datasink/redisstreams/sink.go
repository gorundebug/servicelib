// Package redisstreams implements native Redis Streams sinks.
package redisstreams

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/internal/redisstreamsclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
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
	Outcome Outcome
	EntryID string
}

var ErrBackpressure = errors.New("Redis Streams asynchronous publication capacity exhausted")
var ErrRetired = errors.New("Redis Streams handler invocation has ended")

type Field = redisstreamsclient.Field

type SinkMessage[R any] struct {
	publication *brokermetrics.Publication
	// Fields is the lossless native representation. Do not also set Message.
	Fields    []Field
	Message   map[string]interface{}
	publish   func(context.Context, []Field) (Delivery, error)
	sendAsync func(context.Context, []Field, func(Delivery, error))
	result    runtime.Consumer[R]
	mu        sync.Mutex
	retired   bool
	retain    func() bool
	release   func()
}

func (m *SinkMessage[R]) begin() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired {
		return ErrRetired
	}
	if !m.retain() {
		return errors.New("Redis sink is stopped")
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
	fields, err := m.fields()
	if err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	return m.publish(ctx, fields)
}
func (m *SinkMessage[R]) Send(ctx context.Context, callback func(Delivery, error) R) {
	if err := m.begin(); err != nil {
		m.observe(ctx, Delivery{Outcome: NotSent}, err)
		m.Out(ctx, callback(Delivery{Outcome: NotSent}, err))
		return
	}
	defer m.release()
	fields, err := m.fields()
	if err != nil {
		m.observe(ctx, Delivery{Outcome: NotSent}, err)
		m.Out(ctx, callback(Delivery{Outcome: NotSent}, err))
		return
	}
	m.sendAsync(ctx, fields, func(d Delivery, e error) { m.observe(ctx, d, e); m.Out(ctx, callback(d, e)) })
}
func (m *SinkMessage[R]) fields() ([]Field, error) {
	if len(m.Fields) > 0 && len(m.Message) > 0 {
		return nil, errors.New("set Redis Fields or Message, not both")
	}
	if len(m.Fields) > 0 {
		return m.Fields, nil
	}
	fields := make([]Field, 0, len(m.Message))
	for name, value := range m.Message {
		switch v := value.(type) {
		case string:
			fields = append(fields, Field{name, v})
		case []byte:
			fields = append(fields, Field{name, string(v)})
		default:
			return nil, errors.New("Redis field values must be string or bytes")
		}
	}
	return fields, nil
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
	session          *redisstreamsclient.Session
	gate             redisstreamsclient.Gate
	slots            chan struct{}
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
			_ = session.Close()
		}
	}
}
func (ep *endpoint) start(parent context.Context) (err error) {
	ep.mu.Lock()
	if ep.started || ep.stopped {
		ep.mu.Unlock()
		return errors.New("Redis sink lifecycle already started or stopped")
	}
	ep.started = true
	ep.ctx, ep.cancel = context.WithCancel(parent)
	ep.ready = make(chan struct{})
	ready := ep.ready
	ep.mu.Unlock()
	defer close(ready)
	cfg, ok := ep.GetConfig().(*config.RedisStreamsEndpointConfig)
	if !ok {
		return errors.New("invalid Redis endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSink().GetConfig().(*config.RedisStreamsDataConnectorConfig)
	if !ok {
		return errors.New("invalid Redis connector config")
	}
	session, err := redisstreamsclient.Open(ep.ctx, dc, cfg, false)
	if err != nil {
		return err
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.stopped {
		_ = session.Close()
		return errors.New("Redis sink stopped during startup")
	}
	ep.session = session
	ep.slots = make(chan struct{}, cfg.MaxInFlight())
	ep.gate.Open()
	return nil
}
func (ep *endpoint) publish(ctx context.Context, m []Field) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	if len(m) == 0 {
		return Delivery{Outcome: NotSent}, errors.New("Redis publication requires field/value records")
	}
	id, err := ep.session.PublishFields(ctx, m)
	if err == nil {
		return Delivery{Outcome: Accepted, EntryID: id}, nil
	}
	var serverError redis.Error
	if errors.Is(err, redis.Nil) || errors.As(err, &serverError) {
		return Delivery{Outcome: Rejected}, err
	}
	return Delivery{Outcome: Unknown}, err
}
func (ep *endpoint) sendAsync(ctx context.Context, m []Field, callback func(Delivery, error)) {
	if !ep.gate.Retain() {
		callback(Delivery{Outcome: NotSent}, errors.New("Redis sink is stopped"))
		return
	}
	select {
	case ep.slots <- struct{}{}:
	default:
		ep.gate.Leave()
		callback(Delivery{Outcome: NotSent}, ErrBackpressure)
		return
	}
	// Strings are immutable; copy the ordered slice before returning to the handler.
	copy := append([]Field(nil), m...)
	go func() {
		defer ep.gate.Leave()
		defer func() { <-ep.slots }()
		d, e := ep.publish(ctx, copy)
		callback(d, e)
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
		ec.complete(ctx, value, errors.New("RedisStreams sink is not running"))
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
	msg := &SinkMessage[R]{publication: ep.publication, Message: make(map[string]interface{}), result: ec.result, sendAsync: func(sendCtx context.Context, m []Field, callback func(Delivery, error)) {
		life.refs.Add(1)
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		ep.sendAsync(sendCtx, m, func(d Delivery, e error) { defer life.release(); defer sstop(); defer scancel(); callback(d, e) })
	}, publish: func(sendCtx context.Context, m []Field) (Delivery, error) {
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

func MakeRedisStreamsEndpointConsumer[H, T, R any](stream runtime.TypedSinkStream[T, R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, stream.GetErrorStream(), handler)
}

// MakeRedisStreamsEndpointConsumerWithResult routes R to ordinary continuation output,
// never to E. Error construction remains the handler's explicit typed contract.
func MakeRedisStreamsEndpointConsumerWithResult[H, T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, runtime.ConsumerFunc[R](stream.ConsumeResult), handler)
}
func makeConsumer[H, T, R any](stream sinkStream[T], result runtime.Consumer[R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("RedisStreams sink handler is nil")
	}
	env := stream.GetRuntimeEnvironment()
	cfg, ok := env.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId()).(*config.RedisStreamsEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid RedisStreams endpoint config")
	}
	dc, ok := env.RuntimeConfig().GetDataConnectorByID(cfg.IdDataConnector).(*config.RedisStreamsDataConnectorConfig)
	if !ok {
		return nil, fmt.Errorf("invalid RedisStreams connector config")
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
			return nil, fmt.Errorf("endpoint is not a RedisStreams sink")
		}
	} else {
		base, err := runtime.MakeDataSinkEndpoint(ds, cfg.ID, env)
		if err != nil {
			return nil, err
		}
		protocol := "redisstreams"
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
