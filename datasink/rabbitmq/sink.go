// Package rabbitmq implements RabbitMQ AMQP 1.0 sinks using native publication outcomes.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/internal/rabbitmqclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"sync"
	"sync/atomic"
	"time"
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
	NotSent    Outcome = "not-sent"
	Accepted   Outcome = "accepted"
	Unroutable Outcome = "unroutable"
	Rejected   Outcome = "rejected"
	Modified   Outcome = "modified"
	Unknown    Outcome = "unknown"
)

// Delivery retains the actual AMQP outcome. A transport error after native send
// admission is Unknown, never Rejected or Unroutable and never synthetic success.
type Delivery struct {
	Outcome Outcome
	Native  *rmq.PublishResult
}

var ErrBackpressure = errors.New("RabbitMQ asynchronous publication capacity exhausted")

type SinkMessage[R any] struct {
	publication *brokermetrics.Publication
	Message     *amqp.Message
	publish     func(context.Context, *amqp.Message) (Delivery, error)
	sendAsync   func(context.Context, *amqp.Message, func(Delivery, error))
	result      runtime.Consumer[R]
}

func (m *SinkMessage[R]) SendSync(ctx context.Context) (delivery Delivery, err error) {
	defer func() { m.observe(ctx, delivery, err) }()
	return m.publish(ctx, m.Message)
}

// Send invokes the SDK's native PublishAsync; it does not wait for confirmation.
// Admission/send errors may invoke the result callback inline.
func (m *SinkMessage[R]) Send(ctx context.Context, onDelivery func(Delivery, error) R) {
	m.sendAsync(ctx, m.Message, func(d Delivery, e error) { m.observe(ctx, d, e); m.Out(ctx, onDelivery(d, e)) })
}
func (m *SinkMessage[R]) Out(ctx context.Context, value R)  { m.result.Consume(ctx, value) }
func (m *SinkMessage[R]) Skip(ctx context.Context, value R) { m.Out(ctx, value) }
func delivery(result *rmq.PublishResult, err error) (Delivery, error) {
	d := Delivery{Outcome: Unknown, Native: result}
	if err != nil {
		return d, err
	}
	if result == nil {
		return d, errors.New("RabbitMQ publication completed without an outcome")
	}
	switch result.Outcome.(type) {
	case *rmq.StateAccepted:
		d.Outcome = Accepted
	case *rmq.StateReleased:
		d.Outcome = Unroutable
	case *rmq.StateRejected:
		d.Outcome = Rejected
	case *rmq.StateModified:
		d.Outcome = Modified
	default:
		return d, errors.New("RabbitMQ publication returned an unknown outcome")
	}
	return d, nil
}

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
	mu                sync.Mutex
	started, stopped  bool
	ctx               context.Context
	cancel            context.CancelFunc
	ready, errorsDone chan struct{}
	session           *rabbitmqclient.Session
	timeout           time.Duration
	tracingEngine     tracing.Tracing
	gate              rabbitmqclient.Gate
	slots             chan struct{}
	bytes             int
	onErrors          []func(context.Context, error)
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
		done := ep.gate.Close()
		ready := ep.ready
		cancel := ep.cancel
		ep.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			ds.OnStopTimeout(ctx)
		}
		if cancel != nil {
			cancel()
		}
		ep.mu.Lock()
		session, errorsDone := ep.session, ep.errorsDone
		ep.mu.Unlock()
		if session != nil {
			if err := session.Close(ctx); err != nil {
				ds.OnStopTimeout(ctx)
			}
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
}
func (ep *endpoint) start(parent context.Context) (err error) {
	ep.mu.Lock()
	if ep.started || ep.stopped {
		ep.mu.Unlock()
		return errors.New("RabbitMQ sink lifecycle already started or stopped")
	}
	ep.started = true
	ep.ctx, ep.cancel = context.WithCancel(parent)
	ctx := ep.ctx
	ep.ready = make(chan struct{})
	ready := ep.ready
	ep.mu.Unlock()
	defer close(ready)
	cfg, ok := ep.GetConfig().(*config.RabbitMQEndpointConfig)
	if !ok {
		return errors.New("invalid RabbitMQ endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSink().GetConfig().(*config.RabbitMQDataConnectorConfig)
	if !ok {
		return errors.New("invalid RabbitMQ connector config")
	}
	transportErrors := make(chan error, 1)
	session, err := rabbitmqclient.Open(ctx, dc, cfg, false, func(e error) {
		select {
		case transportErrors <- e:
		default:
		}
	})
	if err != nil {
		return err
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	ep.session = session
	if ep.stopped || ctx.Err() != nil {
		ep.cancel()
		session.Close(ctx)
		return context.Canceled
	}
	ep.timeout = dc.OperationTimeout()
	ep.tracingEngine = ep.GetRuntimeEnvironment().Tracing()
	ep.slots = make(chan struct{}, cfg.MaxInFlight())
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
	return nil
}
func (ep *endpoint) snapshot(ctx context.Context, msg *amqp.Message) (*amqp.Message, int, error) {
	if msg == nil {
		return nil, 0, errors.New("RabbitMQ message is nil")
	}
	m := *msg
	m.ApplicationProperties = rabbitmqclient.Inject(ctx, ep.tracingEngine, msg.ApplicationProperties)
	total := 0
	for _, data := range m.Data {
		total += len(data)
		if total > rabbitmqclient.MaxPendingBytes {
			return nil, 0, ErrBackpressure
		}
	}
	bytes, err := m.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	if len(bytes) > rabbitmqclient.MaxPendingBytes {
		return nil, 0, ErrBackpressure
	}
	copy := &amqp.Message{}
	if err = copy.UnmarshalBinary(bytes); err != nil {
		return nil, 0, err
	}
	copy.Format = m.Format
	copy.DeliveryTag = append([]byte(nil), m.DeliveryTag...)
	return copy, len(bytes), nil
}
func (ep *endpoint) publish(ctx context.Context, msg *amqp.Message) (Delivery, error) {
	op, cancel := context.WithTimeout(ctx, ep.timeout)
	defer cancel()
	stop := context.AfterFunc(ep.ctx, cancel)
	defer stop()
	if err := op.Err(); err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	g, err := ep.session.Snapshot()
	if err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	copy, _, err := ep.snapshot(op, msg)
	if err != nil {
		return Delivery{Outcome: NotSent}, err
	}
	result, err := g.Publisher.Publish(op, copy)
	ep.recoverLink(g, err)
	return delivery(result, err)
}
func (ep *endpoint) sendAsync(ctx context.Context, msg *amqp.Message, callback func(Delivery, error)) {
	ep.mu.Lock()
	if !ep.gate.Retain() {
		ep.mu.Unlock()
		callback(Delivery{Outcome: NotSent}, errors.New("RabbitMQ sink stopped"))
		return
	}
	if len(ep.slots) == cap(ep.slots) {
		ep.mu.Unlock()
		ep.gate.Leave()
		callback(Delivery{Outcome: NotSent}, ErrBackpressure)
		return
	}
	ep.slots <- struct{}{}
	ep.mu.Unlock()
	size := 0
	release := func() { ep.mu.Lock(); ep.bytes -= size; <-ep.slots; ep.mu.Unlock(); ep.gate.Leave() }
	op, cancel := context.WithTimeout(ctx, ep.timeout)
	stop := context.AfterFunc(ep.ctx, cancel)
	var once sync.Once
	finish := func(d Delivery, e error) {
		once.Do(func() { defer release(); defer stop(); defer cancel(); callback(d, e) })
	}
	if err := op.Err(); err != nil {
		finish(Delivery{Outcome: NotSent}, err)
		return
	}
	g, err := ep.session.Snapshot()
	if err != nil {
		finish(Delivery{Outcome: NotSent}, err)
		return
	}
	copy, n, err := ep.snapshot(op, msg)
	if err != nil {
		finish(Delivery{Outcome: NotSent}, err)
		return
	}
	ep.mu.Lock()
	if n > rabbitmqclient.MaxPendingBytes-ep.bytes {
		ep.mu.Unlock()
		finish(Delivery{Outcome: NotSent}, ErrBackpressure)
		return
	}
	ep.bytes += n
	size = n
	ep.mu.Unlock()
	err = g.Publisher.PublishAsync(op, copy, func(result *rmq.PublishResult, e error) {
		ep.recoverLink(g, e)
		d, e := delivery(result, e)
		finish(d, e)
	})
	if err != nil {
		ep.recoverLink(g, err)
		finish(Delivery{Outcome: Unknown}, err)
	}
}

// Link detach may leave the connection open. Rebuild that generation without
// replaying an outcome-unknown publication.
func (ep *endpoint) recoverLink(g *rabbitmqclient.Generation, err error) {
	var linkError *amqp.LinkError
	if errors.As(err, &linkError) {
		ep.session.Invalidate(g)
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
		ec.complete(ctx, value, errors.New("RabbitMQ sink is not running"))
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
	msg := &SinkMessage[R]{publication: ep.publication, Message: rmq.NewMessage(nil), result: ec.result, sendAsync: func(sendCtx context.Context, m *amqp.Message, callback func(Delivery, error)) {
		life.refs.Add(1)
		sendCtx, scancel := context.WithCancel(sendCtx)
		sstop := context.AfterFunc(hc, scancel)
		ep.sendAsync(sendCtx, m, func(d Delivery, e error) { defer life.release(); defer sstop(); defer scancel(); callback(d, e) })
	}, publish: func(sendCtx context.Context, m *amqp.Message) (Delivery, error) {
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

func MakeRabbitMQEndpointConsumer[H, T, R any](stream runtime.TypedSinkStream[T, R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, stream.GetErrorStream(), handler)
}

// MakeRabbitMQEndpointConsumerWithResult routes R to ordinary continuation output,
// never to E. Error construction remains the handler's explicit typed contract.
func MakeRabbitMQEndpointConsumerWithResult[H, T, R, E any](stream runtime.TypedSinkStreamWithResult[T, R, E], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	return makeConsumer(stream, runtime.ConsumerFunc[R](stream.ConsumeResult), handler)
}
func makeConsumer[H, T, R any](stream sinkStream[T], result runtime.Consumer[R], handler EndpointHandler[H, T, R]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("RabbitMQ sink handler is nil")
	}
	env := stream.GetRuntimeEnvironment()
	cfg, ok := env.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId()).(*config.RabbitMQEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("invalid RabbitMQ endpoint config")
	}
	dc, ok := env.RuntimeConfig().GetDataConnectorByID(cfg.IdDataConnector).(*config.RabbitMQDataConnectorConfig)
	if !ok {
		return nil, fmt.Errorf("invalid RabbitMQ connector config")
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
			return nil, fmt.Errorf("endpoint is not a RabbitMQ sink")
		}
	} else {
		base, err := runtime.MakeDataSinkEndpoint(ds, cfg.ID, env)
		if err != nil {
			return nil, err
		}
		protocol := "rabbitmq"
		publication, err := brokermetrics.NewPublication(env.Metrics(), protocol, dc.GetName(), cfg.GetName(), "not-sent", "accepted", "rejected", "unknown", "unroutable", "modified")
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
