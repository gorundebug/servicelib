// Package nats implements native Core NATS and durable JetStream pull sources.
package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorundebug/servicelib/internal/brokermetrics"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/internal/natsclient"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	native "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// ConsumerMessage exposes the actual native delivery. JetStream is nil for Core
// NATS: Core has no broker acknowledgement. For JetStream, the handler explicitly
// calls JetStream.Ack, DoubleAck(ctx), Nak, or NakWithDelay. Neither Done nor a
// successful handler return acknowledges a message. Unacked deliveries may recur.
type ConsumerMessage struct {
	Message   *native.Msg
	JetStream jetstream.Msg
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
	gate      natsclient.Gate
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
	observations  *brokermetrics.Source
	request       func(context.Context, ConsumerMessage)
	onError       func(context.Context, error)
	mu            sync.Mutex
	started       bool
	stopped       bool
	cancel        context.CancelFunc
	receiveCancel context.CancelFunc
	inputStop     func()
	errorsDone    chan struct{}
	done          chan struct{}
	conn          *native.Conn
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
		eps.At(i).(*endpoint).stopAdmission()
	}
	for i := 0; i < eps.Len(); i++ {
		ep := eps.At(i).(*endpoint)
		ep.mu.Lock()
		done := ep.done
		ep.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				ds.OnStopTimeout(ctx)
			}
		}
		ep.mu.Lock()
		cancel, conn, errorsDone := ep.cancel, ep.conn, ep.errorsDone
		ep.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if conn != nil {
			conn.Close()
		}
		if errorsDone != nil {
			select {
			case <-errorsDone:
			case <-ctx.Done():
			}
		}
	}
}
func (ep *endpoint) stopAdmission() {
	ep.mu.Lock()
	ep.stopped = true
	receiveCancel, inputStop := ep.receiveCancel, ep.inputStop
	// Before subscription readiness there is no admitted request to drain.
	// Cancel startup immediately instead of allowing resource creation to proceed.
	if inputStop == nil && ep.cancel != nil {
		ep.cancel()
	}
	ep.mu.Unlock()
	if receiveCancel != nil {
		receiveCancel()
	}
	if inputStop != nil {
		inputStop()
	}
}

func (ep *endpoint) start(parent context.Context) (err error) {
	ep.mu.Lock()
	if ep.started || ep.stopped {
		ep.mu.Unlock()
		return fmt.Errorf("NATS source lifecycle already started or stopped")
	}
	ep.started = true
	ctx, cancel := context.WithCancel(parent)
	ep.cancel = cancel
	receiveCtx, receiveCancel := context.WithCancel(ctx)
	ep.receiveCancel = receiveCancel
	ep.done = make(chan struct{})
	done := ep.done
	ep.mu.Unlock()
	launched := false
	defer func() {
		if !launched {
			cancel()
			close(done)
		}
	}()
	cfg, ok := ep.GetConfig().(*config.NATSEndpointConfig)
	if !ok {
		return fmt.Errorf("invalid NATS endpoint config")
	}
	if !cfg.Enabled {
		return nil
	}
	dc, ok := ep.GetDataSource().GetConfig().(*config.NATSDataConnectorConfig)
	if !ok {
		return fmt.Errorf("invalid NATS connector config")
	}
	if err = natsclient.Subject(cfg.Subject, false); err != nil {
		return err
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
	ownerStop := context.AfterFunc(ctx, conn.Close)
	success := false
	defer func() {
		if !success {
			ownerStop()
			conn.Close()
		}
	}()
	ep.mu.Lock()
	ep.conn = conn
	ep.mu.Unlock()
	op, finish := context.WithTimeout(ctx, dc.OperationTimeout())
	defer finish()
	var next func(context.Context) (ConsumerMessage, error)
	var cleanup func()
	if dc.GetType() == api.DataConnectorTypeJetStream {
		if cfg.QueueGroup != "" {
			return fmt.Errorf("JetStream pull sources cannot use queueGroup")
		}
		js, e := jetstream.New(conn)
		if e != nil {
			return e
		}
		s, e := natsclient.Stream(op, js, cfg)
		if e != nil {
			return e
		}
		c, e := natsclient.Consumer(op, s, cfg)
		if e != nil {
			return e
		}
		messages, e := c.Messages(jetstream.PullMaxMessagesWithBytesLimit(1, natsclient.PendingBytes))
		if e != nil {
			return e
		}
		next = func(ctx context.Context) (ConsumerMessage, error) {
			m, e := messages.Next(jetstream.NextContext(ctx))
			if e != nil {
				return ConsumerMessage{}, e
			}
			return ConsumerMessage{Message: &native.Msg{Subject: m.Subject(), Reply: m.Reply(), Header: m.Headers(), Data: m.Data()}, JetStream: m}, nil
		}
		cleanup = messages.Stop
	} else {
		if cfg.StreamName != "" || cfg.DurableName != "" || cfg.CreateStream || cfg.CreateConsumer {
			return fmt.Errorf("Core NATS cannot use JetStream resources")
		}
		var sub *native.Subscription
		if cfg.QueueGroup != "" {
			sub, err = conn.QueueSubscribeSync(cfg.Subject, cfg.QueueGroup)
		} else {
			sub, err = conn.SubscribeSync(cfg.Subject)
		}
		if err != nil {
			return err
		}
		if err = sub.SetPendingLimits(natsclient.PendingMessages, natsclient.PendingBytes); err != nil {
			sub.Unsubscribe()
			return err
		}
		if err = conn.FlushWithContext(op); err != nil {
			sub.Unsubscribe()
			return err
		}
		if err = conn.LastError(); err != nil {
			sub.Unsubscribe()
			return err
		}
		next = func(ctx context.Context) (ConsumerMessage, error) {
			m, e := sub.NextMsgWithContext(ctx)
			return ConsumerMessage{Message: m}, e
		}
		cleanup = func() { sub.Unsubscribe() }
	}
	if err = receiveCtx.Err(); err != nil {
		cleanup()
		return err
	}
	ep.mu.Lock()
	if ep.stopped {
		ep.mu.Unlock()
		cleanup()
		return context.Canceled
	}
	ep.inputStop = cleanup
	ep.errorsDone = make(chan struct{})
	errorsDone := ep.errorsDone
	ep.mu.Unlock()
	go func() {
		defer close(errorsDone)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-transportErrors:
				if ctx.Err() != nil {
					return
				}
				ep.onError(ctx, e)
			}
		}
	}()
	launched = true
	success = true
	go func() {
		defer close(done)
		defer ownerStop()
		defer receiveCancel()
		defer conn.Close()
		defer cancel()
		defer cleanup()
		for {
			msg, e := next(receiveCtx)
			if e != nil {
				if receiveCtx.Err() == nil && (conn.IsReconnecting() || errors.Is(e, jetstream.ErrNoHeartbeat)) {
					timer := time.NewTimer(50 * time.Millisecond)
					select {
					case <-timer.C:
					case <-receiveCtx.Done():
						timer.Stop()
						return
					}
					continue
				}
				if receiveCtx.Err() == nil {
					ep.onError(ctx, e)
				}
				return
			}
			if receiveCtx.Err() != nil {
				return
			}
			ep.mu.Lock()
			admit := !ep.stopped
			ep.mu.Unlock()
			if !admit {
				return
			}
			ep.request(ctx, msg)
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
	if msg.Message != nil {
		ctx = natsclient.Extract(ctx, ec.Endpoint().GetRuntimeEnvironment().Tracing(), msg.Message.Header)
	}
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
	err = ec.handler.ConsumeMessage(hc, ec.sc, state, msg, r)
	if err == nil && ec.hasResult {
		select {
		case <-r.done:
		case <-hc.Done():
			err = hc.Err()
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

// MakeNATSEndpointConsumer registers a native source for either connector kind.
// It does not connect until DataSource.Start, and never uses CustomSource.
func MakeNATSEndpointConsumer[H, T, R, E any](stream runtime.TypedInputStream[T, R, E], handler EndpointHandler[H, T, R, E]) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, errors.New("NATS handler is nil")
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
		return nil, fmt.Errorf("NATS source endpoint already registered")
	}
	base, err := runtime.MakeDataSourceEndpoint(ds, cfg.ID, env)
	if err != nil {
		return nil, err
	}
	protocol := "nats"
	if dc.Type == api.DataConnectorTypeJetStream {
		protocol = "jetstream"
	}
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
	if message.JetStream != nil {
		if metadata, err := message.JetStream.Metadata(); err == nil && metadata.NumDelivered > 1 {
			ep.observations.Observe(ctx, "redelivered")
		}
	}
}
