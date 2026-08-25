/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kafka

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	kafka "github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/store"
	saramatelemetry "github.com/gorundebug/servicelib/runtime/telemetry/sarama"
)

type scramClient struct {
	hashGenerator scram.HashGeneratorFcn
	client        *scram.Client
	conversation  *scram.ClientConversation
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGenerator.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.client = client
	c.conversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

func (c *scramClient) Done() bool { return c.conversation.Done() }

const pendingRotationInterval = 30 * time.Second

type handlerData struct {
	message *kafka.ConsumerMessage
	session kafka.ConsumerGroupSession
	claim   kafka.ConsumerGroupClaim
}

type ConsumerMessage struct {
	handlerData *handlerData
	Key, Value  []byte
	Topic       string
	Partition   int32
	Offset      int64
}

func contextFromKafkaHeaders(ctx context.Context, env runtime.RuntimeEnvironment, headers []*kafka.RecordHeader) context.Context {
	carrier := make(map[string]string, len(headers))
	for _, header := range headers {
		if header == nil {
			continue
		}
		key := strings.ToLower(string(header.Key))
		switch key {
		case "x-trace", "traceparent", "tracestate", "baggage", "x-stream-id":
			carrier[key] = string(header.Value)
		}
	}
	if streamID := carrier["x-stream-id"]; streamID != "" {
		ctx = runtime.WithStreamId(ctx, streamID)
	}
	if engine := env.Tracing(); engine != nil {
		ctx = engine.Extract(ctx, carrier)
	}
	if tracing.SamplingRequestedByCarrier(carrier) {
		ctx = tracing.EnableSampling(ctx)
	}
	return ctx
}

func (m *ConsumerMessage) Commit() {
	m.handlerData.session.Commit()
}

func (m *ConsumerMessage) MarkMessage(metadata string) {
	m.handlerData.session.MarkMessage(m.handlerData.message, metadata)
}

// StreamContext bundles the typed stream, result stream, output collector, and error collector
// that are passed to every EndpointHandler lifecycle method.
type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// ResultCallback is the callback type registered via ResultContext.SetResultCallback.
// It is called when a pipeline result R with a matching messageID arrives.
// Return true to deregister the callback after this invocation; false to keep it active.
type ResultCallback[HandlerState, T, R, E any] func(
	ctx context.Context,
	sc StreamContext[T, R, E],
	handlerState HandlerState,
	value R,
) bool

// ResultContext is given to EndpointHandler.ConsumeMessage.
// The handler registers result callbacks keyed by messageID.
// Done() signals that response processing is complete.
// Committing/marking the Kafka message is done explicitly via ConsumerMessage.Commit()/MarkMessage().
type ResultContext[HandlerState, T, R, E any] interface {
	SetResultCallback(messageID string, cb ResultCallback[HandlerState, T, R, E])
	Done()
}

type kafkaResult[HandlerState, T, R, E any] struct {
	once               sync.Once
	handlerState       HandlerState
	span               tracing.Span
	doneCh             chan struct{}
	mu                 sync.RWMutex
	cbMu               sync.Mutex
	messageCallbackMap map[string]ResultCallback[HandlerState, T, R, E]
}

func (r *kafkaResult[HandlerState, T, R, E]) SetResultCallback(messageID string, cb ResultCallback[HandlerState, T, R, E]) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.messageCallbackMap[messageID] = cb
}

func (r *kafkaResult[HandlerState, T, R, E]) Done() {
	r.once.Do(func() {
		tracing.SpanEvent(r.span, "done_called")
		close(r.doneCh)
	})
}

// EndpointHandler handles Kafka messages for a single endpoint.
//
// Pipeline lifecycle (pipeline result expected):
//
//	BeginRequest → ConsumeMessage → [await result] → EndRequest
//	                    │                  ↑
//	                    └─ SetResultCallback → cb(R)
//
// The framework blocks after ConsumeMessage until Done() is called on the
// ResultContext (either directly in ConsumeMessage, or via a registered callback
// when a matching pipeline result R arrives). EndRequest is called only after
// Done() is signalled or the context is cancelled.
//
// Pipeline lifecycle (no pipeline result):
//
//	BeginRequest → ConsumeMessage → EndRequest
//
// Concurrency returns the maximum number of Kafka messages processed in parallel.
// It is called on every incoming message so the limit can change dynamically.
// Returning 0 means unlimited concurrency.
//
// BeginRequest initialises per-message handler state. If it returns a non-nil
// error the pipeline will not be started: the framework will NOT call
// ConsumeMessage or EndRequest. BeginRequest is therefore responsible for
// releasing any resources it acquired before returning the error.
//
// ConsumeMessage is called once per Kafka message. It receives a ConsumerMessage
// (Key, Value, Topic, Partition, Offset, and Commit/MarkMessage helpers) and may
// push decoded values into the pipeline via sc.Collect, or register a callback via
// resultCtx.SetResultCallback to be notified when a matching pipeline result arrives
// asynchronously. If it returns a non-nil error the framework stops processing:
// EndRequest is called with that error.
//
// EndRequest finalises the message handling. It receives the error that caused the
// pipeline to stop (nil on the happy path). Unlike gRPC handlers, EndRequest does
// not return an error.
//
// GetMessageID correlates an inbound pipeline result value R with the originating
// message, enabling the framework to route the result back via the correct
// ResultContext callback.
//
// Thread safety:
// GetMessageID and the ResultContext callbacks registered via SetResultCallback
// may be called concurrently from multiple goroutines (one per pipeline result
// that arrives for the same message), and may also run concurrently with an
// in-progress ConsumeMessage call. Implementations must synchronise all access
// to HandlerState inside these methods.
// BeginRequest, ConsumeMessage, and EndRequest are called sequentially from a
// single goroutine per message, so no synchronisation is needed among them —
// but access to HandlerState from these methods still requires synchronisation
// if it is shared with GetMessageID or a registered callback.
type EndpointHandler[HandlerState, T, R, E any] interface {
	// Concurrency returns the maximum number of messages processed in parallel.
	// Called on every incoming message so the limit can change dynamically.
	// Returning 0 means unlimited.
	Concurrency(sc StreamContext[T, R, E]) int
	BeginRequest(ctx context.Context, sc StreamContext[T, R, E]) (context.Context, HandlerState, error)
	ConsumeMessage(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, msgData ConsumerMessage, resultCtx ResultContext[HandlerState, T, R, E]) error
	GetMessageID(ctx context.Context, sc StreamContext[T, R, E], handlerState HandlerState, value R) string
	EndRequest(ctx context.Context, sc StreamContext[T, R, E], err error, handlerState HandlerState)
}

type resultConsumer[R any] interface {
	consumeResult(ctx context.Context, value R)
}

type resultConsumerProxy[R any] struct {
	consumer resultConsumer[R]
}

func (c *resultConsumerProxy[R]) Consume(ctx context.Context, value R) {
	c.consumer.consumeResult(ctx, value)
}

type saramaKafkaInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context, kafka.ClusterAdmin) error
	Stop(context.Context)
}

type saramaKafkaEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
	EndpointRequest(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim, message *kafka.ConsumerMessage)
}

type saramaKafkaInputDataSource interface {
	runtime.DataSource
	waitGroup() *sync.WaitGroup
}

type saramaKafkaDataSource struct {
	*runtime.InputDataSource
	wg sync.WaitGroup
}

type saramaKafkaEndpoint struct {
	*runtime.DataSourceEndpoint
	consumer      saramaKafkaEndpointConsumer
	consumerGroup kafka.ConsumerGroup
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	started       bool
}

type saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	sc        StreamContext[T, R, E]
	hasResult bool
	handler   EndpointHandler[HandlerState, T, R, E]
	pending   *store.RotatingMap[string, *kafkaResult[HandlerState, T, R, E]]
	concMu    sync.Mutex
	concCond  *sync.Cond
	active    int
	stopped   bool
	tracer    tracing.Tracer
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) Out(ctx context.Context, value T) {
	ec.Consume(ctx, value)
}

func makeKafkaConfig(cfg *config.KafkaDataConnectorConfig) (*kafka.Config, error) {
	kafkaConfig := kafka.NewConfig()

	if cfg.Version == "" {
		kafkaConfig.Version = kafka.V2_6_0_0
	} else {
		var err error
		kafkaConfig.Version, err = kafka.ParseKafkaVersion(cfg.Version)
		if err != nil {
			return nil, fmt.Errorf("failed to parse kafka version for data connector %q", cfg.Name)
		}
	}
	if cfg.DialTimeout != 0 {
		kafkaConfig.Net.DialTimeout = time.Duration(cfg.DialTimeout) * time.Millisecond
	}
	if cfg.SecurityProtocol != api.PLAINTEXT {
		if cfg.Username == "" || cfg.Password == "" {
			return nil, fmt.Errorf("Kafka SASL username and password must both be configured for data connector %q", cfg.Name)
		}
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = cfg.Username
		kafkaConfig.Net.SASL.Password = cfg.Password
		switch cfg.SaslMechanism {
		case api.SCRAMSHA256:
			kafkaConfig.Net.SASL.Mechanism = kafka.SASLTypeSCRAMSHA256
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() kafka.SCRAMClient {
				return &scramClient{hashGenerator: scram.HashGeneratorFcn(sha256.New)}
			}
		case api.SCRAMSHA512:
			kafkaConfig.Net.SASL.Mechanism = kafka.SASLTypeSCRAMSHA512
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() kafka.SCRAMClient {
				return &scramClient{hashGenerator: scram.HashGeneratorFcn(sha512.New)}
			}
		default:
			kafkaConfig.Net.SASL.Mechanism = kafka.SASLTypePlaintext
		}
	}
	if cfg.SecurityProtocol == api.SASLSSL {
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return kafkaConfig, nil
}

func (ds *saramaKafkaDataSource) Start(ctx context.Context) error {
	cfg, ok := ds.GetConfig().(*config.KafkaDataConnectorConfig)
	if !ok || cfg.Implementation != api.DataConnectorImplementationIBMsarama {
		return fmt.Errorf("invalid data connector configuration for saramaKafkaDataSource")
	}

	endpoints := ds.GetEndpoints()
	enabled := false
	for i := 0; i < endpoints.Len(); i++ {
		endpointCfg, endpointOK := endpoints.At(i).GetConfig().(*config.KafkaEndpointConfig)
		if endpointOK && endpointCfg.Enabled {
			enabled = true
			break
		}
	}
	if !enabled {
		return nil
	}

	if cfg.Brokers == "" {
		return fmt.Errorf("no brokers specified for data connector %q", ds.GetName())
	}
	kafkaConfig, err := makeKafkaConfig(cfg)
	if err != nil {
		return err
	}

	brokers := strings.Split(cfg.Brokers, ",")

	admin, err := kafka.NewClusterAdmin(brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka admin failed for data connector %q: %v",
			ds.GetName(), err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			ds.GetEnvironment().Log().Warn(ctx, "close kafka admin failed",
				log.Str("connector", ds.GetName()), log.Err(err))
		}
	}()

	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(saramaKafkaInputEndpoint).Start(ctx, admin); err != nil {
			return err
		}
	}

	return nil
}

func (ds *saramaKafkaDataSource) waitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ds *saramaKafkaDataSource) Stop(ctx context.Context) {
	endpoints := ds.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint saramaKafkaInputEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(saramaKafkaInputEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.OnStopTimeout(ctx)
	}
}

func (ep *saramaKafkaEndpoint) Setup(_ kafka.ConsumerGroupSession) error {
	return nil
}

func (ep *saramaKafkaEndpoint) Cleanup(_ kafka.ConsumerGroupSession) error {
	return nil
}

func (ep *saramaKafkaEndpoint) ConsumeClaim(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		ep.consumer.EndpointRequest(session, claim, message)
	}
	return nil
}

func (ep *saramaKafkaEndpoint) Start(ctx context.Context, admin kafka.ClusterAdmin) error {
	cfg, ok := ep.GetConfig().(*config.KafkaEndpointConfig)
	if !ok {
		return fmt.Errorf("invalid kafka endpoint configuration for endpoint %q", ep.GetName())
	}
	if !cfg.Enabled {
		return nil
	}
	if cfg.Topic == "" {
		return fmt.Errorf("no topic specified for endpoint %q", ep.GetName())
	}
	if cfg.CreateTopic {
		numPartitions := 1
		if cfg.Partitions != 0 {
			numPartitions = cfg.Partitions
		}
		replicationFactor := 1
		if cfg.ReplicationFactor != 0 {
			replicationFactor = cfg.ReplicationFactor
		}
		topicDetail := &kafka.TopicDetail{
			NumPartitions:     int32(numPartitions),
			ReplicationFactor: int16(replicationFactor),
		}
		if err := admin.CreateTopic(cfg.Topic, topicDetail, false); err != nil {
			var kafkaErr *kafka.TopicError
			if !errors.As(err, &kafkaErr) || !errors.Is(kafkaErr.Err, kafka.ErrTopicAlreadyExists) {
				return fmt.Errorf("create topic failed for endpoint %q: %v",
					ep.GetName(), err)
			}
		}
	}

	if err := ep.consumer.Start(ctx); err != nil {
		return err
	}

	dsCfg, ok := ep.GetDataSource().GetConfig().(*config.KafkaDataConnectorConfig)
	if !ok || dsCfg.Implementation != api.DataConnectorImplementationIBMsarama {
		return fmt.Errorf("invalid data connector configuration for endpoint %q", ep.GetName())
	}
	kafkaConfig, err := makeKafkaConfig(dsCfg)
	if err != nil {
		return err
	}
	kafkaConfig.Consumer.Offsets.Initial = kafka.OffsetOldest

	if cfg.ConsumerGroup == "" {
		return fmt.Errorf("consumer group does not set for kafka source endpoint %q", ep.GetName())
	}
	if err := saramatelemetry.Register(
		ep.GetRuntimeEnvironment().Metrics(), kafkaConfig.MetricRegistry,
		saramatelemetry.RoleConsumer, cfg.ConsumerGroup,
	); err != nil {
		return err
	}

	ep.consumerGroup, err = kafka.NewConsumerGroup(
		strings.Split(dsCfg.Brokers, ","),
		cfg.ConsumerGroup,
		kafkaConfig,
	)
	if err != nil {
		return fmt.Errorf("new consumer group failed for kafka source endpoint %q: %v",
			ep.GetName(), err)
	}

	var cancelCtx context.Context
	cancelCtx, ep.cancel = context.WithCancel(ctx)
	ep.started = true
	ep.wg.Add(1)
	go func() {
		defer ep.wg.Done()
		topics := []string{cfg.Topic}
		for {
			if err := ep.consumerGroup.Consume(cancelCtx, topics, ep); err != nil {
				ep.GetRuntimeEnvironment().Log().Error(cancelCtx,
					"consumer group consume error",
					log.Str("endpoint", ep.GetName()), log.Err(err))
			}
			if cancelCtx.Err() != nil {
				break
			}
		}
	}()

	return nil
}

func (ep *saramaKafkaEndpoint) Stop(ctx context.Context) {
	if !ep.started {
		return
	}
	ep.consumer.Stop(ctx)
	ep.cancel()
	c := make(chan struct{})
	go func() {
		defer close(c)
		ep.wg.Wait()
	}()
	select {
	case <-c:
		if err := ep.consumerGroup.Close(); err != nil {
			ep.GetRuntimeEnvironment().Log().Warn(ctx,
				"close consumer group failed",
				log.Str("endpoint", ep.GetName()), log.Err(err))
		}
	case <-ctx.Done():
		ep.GetRuntimeEnvironment().Log().Warn(ctx,
			"kafka source endpoint stopped by timeout", log.Str("endpoint", ep.GetName()))
	}
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) Start(ctx context.Context) error {
	if ec.hasResult {
		ec.pending = store.MakeRotatingMap[string, *kafkaResult[HandlerState, T, R, E]](pendingRotationInterval)
		return ec.pending.Start(ctx)
	}
	return nil
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) Stop(ctx context.Context) {
	if ec.pending != nil {
		ec.pending.Stop(ctx)
	}
	ec.concMu.Lock()
	ec.stopped = true
	ec.concMu.Unlock()
	ec.concCond.Broadcast()
}

// acquireConcurrency blocks until a concurrency slot is available or the consumer is stopped.
// Note: during a Kafka rebalance, session.Context() is cancelled by sarama. In-flight requests
// will complete via handlerCtx.Done() and release their slots, which broadcasts to any goroutine
// waiting here. This may slightly delay ConsumeClaim returning during rebalance, but is not
// critical in practice since in-flight requests finish quickly on a cancelled context.
func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) acquireConcurrency() bool {
	for {
		limit := ec.handler.Concurrency(ec.sc)

		ec.concMu.Lock()
		if ec.stopped {
			ec.concMu.Unlock()
			return false
		}
		if limit == 0 || ec.active < limit {
			ec.active++
			ec.concMu.Unlock()
			return true
		}
		ec.concCond.Wait()
		ec.concMu.Unlock()
	}
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) releaseConcurrency() {
	ec.concMu.Lock()
	ec.active--
	ec.concCond.Broadcast()
	ec.concMu.Unlock()
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) EndpointRequest(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim, message *kafka.ConsumerMessage) {
	if !ec.acquireConcurrency() {
		return
	}
	defer ec.releaseConcurrency()

	environment := ec.Endpoint().GetRuntimeEnvironment()
	ctx := contextFromKafkaHeaders(session.Context(), environment, message.Headers)
	ctx = runtime.ApplyDataSourceEndpointTracing(
		ctx, environment, ec.Endpoint().GetID(),
	)
	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "kafka.input",
			tracing.StringAttr("stream", ec.Stream().GetName()),
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
		)
		defer span.End()
	}
	handlerCtx, handlerState, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "begin_request.error", tracing.StringAttr("error", err.Error()))
		}
		ec.Endpoint().OnBeginRequestFailed(ctx, err)
		return
	}
	tracing.SpanEvent(span, "begin_request")
	startTime := ec.Endpoint().OnRequestStart(handlerCtx)

	var streamID string
	if sid, ok := runtime.StreamIdFromContext(handlerCtx); ok {
		streamID = sid.GetID()
	} else {
		streamID = runtime.NewStreamID()
		handlerCtx = runtime.WithStreamId(handlerCtx, streamID)
	}
	if span != nil {
		tracing.SpanAttrs(span, tracing.StringAttr("stream_id", streamID), tracing.BoolAttr("has_result", ec.hasResult))
	}

	result := &kafkaResult[HandlerState, T, R, E]{
		handlerState:       handlerState,
		span:               span,
		doneCh:             make(chan struct{}),
		messageCallbackMap: make(map[string]ResultCallback[HandlerState, T, R, E]),
	}
	if ec.hasResult {
		if err = ec.pending.Set(streamID, result); err != nil {
			tracing.SpanError(span, err)
			ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
			return
		}
		ec.Endpoint().OnPendingAdd(handlerCtx, streamID)
	}

	if err = ec.handler.ConsumeMessage(
		handlerCtx,
		ec.sc,
		handlerState,
		ConsumerMessage{
			handlerData: &handlerData{
				message: message,
				session: session,
				claim:   claim,
			},
			Key:       message.Key,
			Value:     message.Value,
			Topic:     message.Topic,
			Partition: message.Partition,
			Offset:    message.Offset,
		},
		result,
	); err != nil {
		if ec.hasResult {
			result.mu.Lock()
			defer result.mu.Unlock()
			ec.pending.Pop(streamID)
			ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		}
		tracing.SpanError(span, err)
		if span != nil {
			tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
		}
		ec.handler.EndRequest(handlerCtx, ec.sc, err, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
		return
	}
	tracing.SpanEvent(span, "consume_message")

	if !ec.hasResult {
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
		return
	}

	select {
	case <-result.doneCh:
		tracing.SpanEvent(span, "done_received")
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)
		ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
		ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
	case <-handlerCtx.Done():
		result.mu.Lock()
		defer result.mu.Unlock()
		ec.pending.Pop(streamID)
		ec.Endpoint().OnPendingRemove(handlerCtx, streamID)

		// A result callback holds result.mu for the complete callback invocation.
		// If cancellation raced with a callback that called Done, the write lock
		// above waits for that callback and this recheck observes its completion.
		// Match the HTTP source boundary: a fully produced result wins over a
		// simultaneous deadline/cancellation notification.
		select {
		case <-result.doneCh:
			tracing.SpanEvent(span, "done_received")
			ec.handler.EndRequest(handlerCtx, ec.sc, nil, handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, nil)
		default:
			tracing.SpanError(span, handlerCtx.Err())
			if span != nil {
				tracing.SpanEvent(span, "context_cancelled", tracing.StringAttr("error", handlerCtx.Err().Error()))
			}
			ec.handler.EndRequest(handlerCtx, ec.sc, handlerCtx.Err(), handlerState)
			ec.Endpoint().OnRequestEnd(handlerCtx, startTime, handlerCtx.Err())
		}
	}
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) consumeResult(ctx context.Context, value R) {
	sid, ok := runtime.StreamIdFromContext(ctx)
	if !ok {
		ec.Endpoint().OnMissingStreamID(ctx)
		return
	}
	result, loaded := ec.pending.Get(sid.GetID())
	if !loaded {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		return
	}

	result.mu.RLock()
	defer result.mu.RUnlock()

	if res, ld := ec.pending.Get(sid.GetID()); !ld || res != result {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		tracing.SpanEvent(result.span, "late_result")
		return
	}

	messageID := ec.handler.GetMessageID(ctx, ec.sc, result.handlerState, value)

	result.cbMu.Lock()
	cb, ok := result.messageCallbackMap[messageID]
	result.cbMu.Unlock()
	if !ok || cb == nil {
		ec.Endpoint().OnUnknownMessageID(ctx, sid.GetID(), messageID)
		if result.span != nil {
			tracing.SpanEvent(result.span, "unknown_message_id", tracing.StringAttr("message_id", messageID))
		}
		return
	}
	if cb(ctx, ec.sc, result.handlerState, value) {
		var duplicate bool
		result.cbMu.Lock()
		if _, exists := result.messageCallbackMap[messageID]; exists {
			delete(result.messageCallbackMap, messageID)
		} else {
			duplicate = true
		}
		result.cbMu.Unlock()
		if duplicate {
			ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), messageID)
			if result.span != nil {
				tracing.SpanEvent(result.span, "duplicate_message_id", tracing.StringAttr("message_id", messageID))
			}
		}
	}
	if result.span != nil {
		tracing.SpanEvent(result.span, "result_consumed", tracing.StringAttr("message_id", messageID))
	}
}

func getOrCreateSaramaKafkaDataSource(id int, env runtime.RuntimeEnvironment) (runtime.DataSource, error) {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for datasource with id=%d not found", id)
	}
	inputDS, err := runtime.MakeInputDataSource(cfg, env)
	if err != nil {
		return nil, err
	}
	kafkaDataSource := &saramaKafkaDataSource{
		InputDataSource: inputDS,
	}
	var inputDataSource saramaKafkaInputDataSource = kafkaDataSource
	env.AddDataSource(inputDataSource)
	return inputDataSource, nil
}

func createSaramaKafkaDataSourceEndpoint(id int, env runtime.RuntimeEnvironment) (*saramaKafkaEndpoint, error) {
	cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
	if cfg == nil {
		return nil, fmt.Errorf("config for endpoint with id=%d not found", id)
	}
	dataSource, err := getOrCreateSaramaKafkaDataSource(cfg.GetIdDataConnector(), env)
	if err != nil {
		return nil, err
	}
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return nil, fmt.Errorf("endpoint %q already exists", cfg.GetName())
	}
	sourceEndpoint, err := runtime.MakeDataSourceEndpoint(dataSource, id, env)
	if err != nil {
		return nil, err
	}
	kafkaEndpoint := &saramaKafkaEndpoint{
		DataSourceEndpoint: sourceEndpoint,
	}
	var inputEndpoint saramaKafkaInputEndpoint = kafkaEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return kafkaEndpoint, nil
}

func MakeSaramaKafkaEndpointConsumer[HandlerState, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, T, R, E],
) (runtime.Consumer[T], error) {
	env := stream.GetRuntimeEnvironment()
	endpoint, err := createSaramaKafkaDataSourceEndpoint(stream.GetEndpointId(), env)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for kafka endpoint consumer for the stream %q", stream.GetName())
	}
	var tr tracing.Tracer
	if t := env.Tracing(); t != nil {
		tr = t.Tracer(env.ServiceConfig().Name)
	}
	endpointConsumer := &saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](endpoint, stream),
		hasResult:                  stream.GetResultStream() != nil,
		handler:                    handler,
		tracer:                     tr,
	}
	endpointConsumer.concCond = sync.NewCond(&endpointConsumer.concMu)
	endpointConsumer.sc = runtime.MakeStreamContext[T, R, E](
		endpointConsumer.Stream(),
		endpointConsumer.Stream().GetResultStream(),
		runtime.CollectFunc[T](endpointConsumer.Out),
		runtime.CollectFunc[E](endpointConsumer.Stream().GetErrorStream().Consume),
	)
	if endpointConsumer.hasResult {
		stream.SetResultConsumer(&resultConsumerProxy[R]{consumer: endpointConsumer})
	}
	endpoint.consumer = endpointConsumer
	env.RegisterEndpointConsumer(endpointConsumer)
	return endpointConsumer, nil
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) GetID() int {
	return ec.Endpoint().GetID()
}

func (ec *saramaKafkaTypedEndpointConsumer[HandlerState, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}
