// Package redisstreams implements native Redis Streams consumer-group sources.
package redisstreams

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"github.com/gorundebug/servicelib/internal/redisstreamsclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"sync"
	"time"
)

type Field = redisstreamsclient.Field
type Entry = redisstreamsclient.Entry

type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// ConsumerMessage exposes the native entry and explicit settlement. Done never ACKs.
// Redis XACK does not fence an earlier consumer after reclaim; business operations must be idempotent.
type ConsumerMessage struct {
	Message redisstreamsclient.Entry
	session *redisstreamsclient.Session
	ctx     context.Context
}

func (m ConsumerMessage) Ack(ctx context.Context) error {
	if m.ctx == nil || m.session == nil {
		return errors.New("Redis delivery is not active")
	}
	if err := m.ctx.Err(); err != nil {
		return err
	}
	linked, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()
	return m.session.AckEntry(linked, m.Message)
}

type ResultCallback[H, T, R, E any] func(context.Context, StreamContext[T, R, E], H, R) bool
type ResultContext[H, T, R, E any] interface {
	SetResultCallback(string, ResultCallback[H, T, R, E])
	Done()
}

// EndpointHandler processes one request at a time per endpoint. Register result
// callbacks BEFORE sc.Collect: pipeline results may arrive synchronously.
// BeginRequest failure skips ConsumeMessage and EndRequest; ConsumeMessage errors
// reach EndRequest. OnError receives transport failures without invented E values.
// Result callbacks may run concurrently; handler state must be synchronized.
type EndpointHandler[H, T, R, E any] interface {
	OnError(context.Context, StreamContext[T, R, E], error)
	BeginRequest(context.Context, StreamContext[T, R, E]) (context.Context, H, error)
	ConsumeMessage(context.Context, StreamContext[T, R, E], H, ConsumerMessage, ResultContext[H, T, R, E]) error
	GetMessageID(context.Context, StreamContext[T, R, E], H, R) string
	EndRequest(context.Context, StreamContext[T, R, E], error, H)
}

type request[H, T, R, E any] struct {
	state     H
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	callbacks map[string]ResultCallback[H, T, R, E]
	gate      redisstreamsclient.Gate
}

func (r *request[H, T, R, E]) Done() { r.once.Do(func() { close(r.done) }) }
func (r *request[H, T, R, E]) SetResultCallback(id string, cb ResultCallback[H, T, R, E]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callbacks[id] = cb
}

type source struct{ *runtime.InputDataSource }
type endpoint struct {
	*runtime.DataSourceEndpoint
	observations          *brokermetrics.Source
	request               func(context.Context, ConsumerMessage)
	onError               func(context.Context, error)
	mu                    sync.Mutex
	started, stopped      bool
	cancel, receiveCancel context.CancelFunc
	session               *redisstreamsclient.Session
	done                  chan struct{}
}

func (ds *source) Start(ctx context.Context) error {
	eps := ds.GetEndpoints()
	for i := 0; i < eps.Len(); i++ {
		if err := eps.At(i).(*endpoint).start(ctx); err != nil {
			ds.Stop(ctx)
			return err
		}
	}
	return nil
}
func (ds *source) Stop(ctx context.Context) {
	eps := ds.GetEndpoints()
	for i := 0; i < eps.Len(); i++ {
		ep := eps.At(i).(*endpoint)
		ep.mu.Lock()
		ep.stopped = true
		if ep.receiveCancel != nil {
			ep.receiveCancel()
		}
		ep.mu.Unlock()
	}
	for i := 0; i < eps.Len(); i++ {
		ep := eps.At(i).(*endpoint)
		ep.mu.Lock()
		done, cancel := ep.done, ep.cancel
		ep.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				ds.OnStopTimeout(ctx)
			}
		}
		if cancel != nil {
			cancel()
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
		return errors.New("Redis source lifecycle already started or stopped")
	}
	ep.started = true
	ctx, cancel := context.WithCancel(parent)
	receiveCtx, receiveCancel := context.WithCancel(ctx)
	ep.cancel = cancel
	ep.receiveCancel = receiveCancel
	ep.done = make(chan struct{})
	done := ep.done
	ep.mu.Unlock()
	launched := false
	defer func() {
		if !launched {
			cancel()
			receiveCancel()
			close(done)
		}
	}()
	cfg, ok := ep.GetConfig().(*config.RedisStreamsEndpointConfig)
	if !ok {
		return errors.New("invalid Redis endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSource().GetConfig().(*config.RedisStreamsDataConnectorConfig)
	if !ok {
		return errors.New("invalid Redis connector config")
	}
	session, err := redisstreamsclient.Open(ctx, dc, cfg, true)
	if err != nil {
		return err
	}
	ep.mu.Lock()
	ep.session = session
	stopped := ep.stopped
	ep.mu.Unlock()
	if stopped {
		_ = session.Close()
		return errors.New("Redis source stopped during startup")
	}
	launched = true
	go func() {
		defer close(done)
		defer cancel()
		defer receiveCancel()
		nextClaim := time.Now()
		for receiveCtx.Err() == nil {
			var messages []redisstreamsclient.Entry
			var err error
			if !time.Now().Before(nextClaim) {
				var deleted []string
				messages, deleted, err = session.Claim(receiveCtx)
				if err == nil {
					for range messages {
						ep.observations.Observe(ctx, "reclaimed")
					}
				}
				nextClaim = time.Now().Add(session.ClaimInterval())
				if len(deleted) > 0 {
					ep.onError(ctx, fmt.Errorf("Redis pending entries were deleted before processing: %v", deleted))
				}
			} else {
				messages, err = session.Receive(receiveCtx)
			}
			if err != nil {
				if receiveCtx.Err() != nil {
					return
				}
				ep.onError(ctx, err)
				timer := time.NewTimer(session.ClaimInterval())
				select {
				case <-receiveCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
			for _, message := range messages {
				if receiveCtx.Err() != nil {
					return
				}
				ep.request(ctx, ConsumerMessage{Message: message, session: session})
			}
		}
	}()
	return nil
}

type consumer[H, T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	sc        StreamContext[T, R, E]
	handler   EndpointHandler[H, T, R, E]
	hasResult bool
	mu        sync.Mutex
	pending   map[string]*request[H, T, R, E]
}

func (ec *consumer[H, T, R, E]) GetID() int                          { return ec.Endpoint().GetID() }
func (ec *consumer[H, T, R, E]) FunctionImplementation() interface{} { return ec.handler }
func (ec *consumer[H, T, R, E]) process(ctx context.Context, msg ConsumerMessage) {
	ctx = runtime.ApplyDataSourceEndpointTracing(ctx, ec.Endpoint().GetRuntimeEnvironment(), ec.Endpoint().GetID())
	hc, state, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		ec.Endpoint().OnBeginRequestFailed(ctx, err)
		return
	}
	if hc == nil {
		hc = ctx
	}
	// Retain handler values but guarantee endpoint cancellation still reaches it.
	hc, cancel := context.WithCancel(hc)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	id := runtime.NewStreamID()
	if inherited, ok := runtime.StreamIdFromContext(hc); ok && inherited.GetID() != "" {
		id = inherited.GetID()
	}
	hc = runtime.WithStreamId(hc, id)
	start := ec.Endpoint().OnRequestStart(hc)
	r := &request[H, T, R, E]{state: state, done: make(chan struct{}), callbacks: make(map[string]ResultCallback[H, T, R, E])}
	r.gate.Open()
	if ec.hasResult {
		ec.mu.Lock()
		ec.pending[id] = r
		ec.mu.Unlock()
		ec.Endpoint().OnPendingAdd(hc, id)
	}
	msg.ctx = hc
	err = ec.handler.ConsumeMessage(hc, ec.sc, state, msg, r)
	if err == nil && ec.hasResult {
		select {
		case <-r.done:
		case <-hc.Done():
			err = context.Cause(hc)
			select {
			case <-r.done:
				err = nil
			default:
			}
		}
	}
	if ec.hasResult {
		ec.mu.Lock()
		delete(ec.pending, id)
		ec.mu.Unlock()
		ec.Endpoint().OnPendingRemove(hc, id)
	}
	// EndRequest never races callbacks. Stop can still return at its deadline if
	// application callback code ignores cancellation; no new callbacks can enter.
	<-r.gate.Close()
	ec.handler.EndRequest(hc, ec.sc, err, state)
	ec.Endpoint().OnRequestEnd(hc, start, err)
}
func (ec *consumer[H, T, R, E]) result(ctx context.Context, value R) {
	sid, ok := runtime.StreamIdFromContext(ctx)
	if !ok {
		ec.Endpoint().OnMissingStreamID(ctx)
		return
	}
	ec.mu.Lock()
	r := ec.pending[sid.GetID()]
	if r == nil || !r.gate.Enter() {
		ec.mu.Unlock()
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		return
	}
	ec.mu.Unlock()
	defer r.gate.Leave()
	id := ec.handler.GetMessageID(ctx, ec.sc, r.state, value)
	r.mu.Lock()
	cb, ok := r.callbacks[id]
	r.mu.Unlock()
	if !ok || cb == nil {
		ec.Endpoint().OnUnknownMessageID(ctx, sid.GetID(), id)
		return
	}
	if cb(ctx, ec.sc, r.state, value) {
		r.mu.Lock()
		_, exists := r.callbacks[id]
		delete(r.callbacks, id)
		r.mu.Unlock()
		if !exists {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), id)
		}
	}
}

// MakeRedisStreamsEndpointConsumer registers a native RedisStreams AMQP 1.0 source.
// It does not connect until DataSource.Start, and never uses CustomSource.
func MakeRedisStreamsEndpointConsumer[H, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler EndpointHandler[H, T, R, E]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("RedisStreams handler is nil")
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
	var ds *source
	if existing := env.GetDataSource(dc.ID); existing != nil {
		ds, ok = existing.(*source)
		if !ok {
			return nil, fmt.Errorf("connector already registered with another source kind")
		}
	} else {
		base, err := runtime.MakeInputDataSource(dc, env)
		if err != nil {
			return nil, err
		}
		ds = &source{base}
		env.AddDataSource(ds)
	}
	if ds.GetEndpoint(cfg.ID) != nil {
		return nil, fmt.Errorf("RedisStreams source endpoint already registered")
	}
	base, err := runtime.MakeDataSourceEndpoint(ds, cfg.ID, env)
	if err != nil {
		return nil, err
	}
	protocol := "redisstreams"
	observations, err := brokermetrics.NewSource(env.Metrics(), protocol, dc.GetName(), cfg.GetName())
	if err != nil {
		return nil, err
	}
	ep := &endpoint{DataSourceEndpoint: base, observations: observations}
	ec := &consumer[H, T, R, E]{DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](ep, stream), handler: handler, hasResult: stream.GetResultStream() != nil, pending: make(map[string]*request[H, T, R, E])}
	ec.sc = runtime.MakeStreamContext[T, R, E](stream, stream.GetResultStream(), runtime.CollectFunc[T](ec.Consume), runtime.CollectFunc[E](func(ctx context.Context, v E) { stream.GetErrorStream().Consume(ctx, v) }))
	ep.request = func(ctx context.Context, message ConsumerMessage) {
		ep.observeDelivery(ctx, message)
		ec.process(ctx, message)
	}
	ep.onError = func(ctx context.Context, err error) { handler.OnError(ctx, ec.sc, err) }
	if ec.hasResult {
		stream.SetResultConsumer(runtime.ConsumerFunc[R](ec.result))
	}
	ds.AddEndpoint(ep)
	env.RegisterEndpointConsumer(ec)
	return ec, nil
}

func (ep *endpoint) observeDelivery(ctx context.Context, message ConsumerMessage) {
	if ep.observations == nil {
		return
	}
	ep.observations.Observe(ctx, "received")

}
