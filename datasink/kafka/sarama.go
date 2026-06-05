/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kafka

import (
    "context"
    "errors"
    "fmt"
    "math/rand"
    "strings"
    "sync"
    "time"

    kafka "github.com/IBM/sarama"

    "github.com/gorundebug/servicelib/api"
    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
    "github.com/gorundebug/servicelib/runtime/environment/log"
    "github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// Partitioner allows the handler to control which Kafka partition a message lands on.
type Partitioner[T any] interface {
    Partition(value T, numPartitions int32) (int32, error)
}

// EndpointHandler handles Kafka sink calls for a single endpoint.
//
// Pipeline lifecycle (one Kafka message per Consume):
//
//	GetStreamID → BeginRequest → ConsumeMessage → EndRequest
//
// GetStreamID returns the logical stream identifier for the incoming value.
// It is used to correlate related messages.
//
// BeginRequest initialises per-message handler state. Unlike gRPC sink handlers,
// BeginRequest does not return an error; the framework always proceeds to
// ConsumeMessage. Use the returned context to carry per-request values.
//
// ConsumeMessage fills msg.Key and msg.Value, then calls msg.Send(onDelivery) to
// publish asynchronously. The onDelivery callback converts the Kafka delivery
// result (partition, offset, error) into R, which is then pushed back into the
// pipeline. Alternatively, call msg.Skip(r) to push a result without sending.
// Returning a non-nil error passes it to EndRequest.
//
// EndRequest finalises the request. Its error argument is the non-nil error
// returned by ConsumeMessage, or nil on success. EndRequest does not return an error.
type EndpointHandler[HandlerState, T, R any] interface {
    GetStreamID(ctx context.Context, value T) string
    BeginRequest(ctx context.Context, stream runtime.Stream) (context.Context, HandlerState)
    ConsumeMessage(ctx context.Context, stream runtime.Stream, handlerState HandlerState, value T, msg *SinkMessage[R]) error
    EndRequest(ctx context.Context, stream runtime.Stream, err error, handlerState HandlerState)
}

// SinkMessage is passed to EndpointHandler.ConsumeMessage.
// The handler sets Key and Value, then calls Send or Skip.
// Topic is read-only.
type SinkMessage[R any] struct {
    Key   []byte
    Value []byte
    // internal
    topic        string
    sendFn       func(key, value []byte, onDelivery func(partition int32, offset int64, err error))
    resultStream runtime.Collect[R]
}

func (m *SinkMessage[R]) Topic() string { return m.topic }

// Send publishes Key/Value to Kafka. onDelivery converts the delivery result to R,
// which is then forwarded to the pipeline result stream asynchronously.
func (m *SinkMessage[R]) Send(ctx context.Context, onDelivery func(partition int32, offset int64, err error) R) {
    m.sendFn(m.Key, m.Value, func(p int32, o int64, err error) {
        m.resultStream.Out(ctx, onDelivery(p, o, err))
    })
}

// SendSync publishes Key/Value to Kafka and blocks until delivery is confirmed.
// Returns partition, offset and delivery error. Use for guaranteed delivery semantics.
// After a successful send, push the result manually via Out.
func (m *SinkMessage[R]) SendSync(ctx context.Context) (int32, int64, error) {
    type result struct {
        partition int32
        offset    int64
        err       error
    }
    done := make(chan result, 1)
    m.sendFn(m.Key, m.Value, func(p int32, o int64, err error) {
        done <- result{p, o, err}
    })
    select {
    case res := <-done:
        return res.partition, res.offset, res.err
    case <-ctx.Done():
        return 0, 0, ctx.Err()
    }
}

// Out pushes result directly into the pipeline result stream.
// Use together with SendSync for guaranteed delivery.
func (m *SinkMessage[R]) Out(ctx context.Context, result R) {
    m.resultStream.Out(ctx, result)
}

// Skip pushes result r directly into the pipeline result stream without sending to Kafka.
func (m *SinkMessage[R]) Skip(ctx context.Context, result R) {
    m.resultStream.Out(ctx, result)
}

type saramaKafkaOutputDataSink interface {
    runtime.DataSink
    SendMessage(ctx context.Context, msg *kafka.ProducerMessage)
}

// messageMetadata is attached to every ProducerMessage sent to Kafka.
type messageMetadata struct {
    onDelivery  func(partition int32, offset int64, err error)
    partitionFn func(msg *kafka.ProducerMessage, numPartitions int32) (int32, error)
}

type kafkaSinkEndpoint interface {
    runtime.SinkEndpoint
    Start(context.Context, kafka.ClusterAdmin) error
    Stop(context.Context)
    SendMessage(ctx context.Context, key []byte, value []byte, metadata *messageMetadata)
}

type kafkaEndpointConsumer interface {
    runtime.OutputEndpointConsumer
    Start(context.Context) error
    Stop(context.Context)
}

type saramaKafkaDataSink struct {
    *runtime.OutputDataSink
    producer kafka.AsyncProducer
    wg       sync.WaitGroup
    sendWG   sync.WaitGroup
    mu       sync.Mutex
    stopped  bool
}

type saramaKafkaEndpoint struct {
    *runtime.DataSinkEndpoint
    topic    string
    consumer kafkaEndpointConsumer
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
    return kafkaConfig, nil
}

func (ds *saramaKafkaDataSink) Partition(msg *kafka.ProducerMessage, numPartitions int32) (int32, error) {
    if msg.Metadata == nil {
        return 0, fmt.Errorf("metadata is nil inside ProducerMessage for partition method in data sink %q",
            ds.GetName())
    }
    return msg.Metadata.(*messageMetadata).partitionFn(msg, numPartitions)
}

func (ds *saramaKafkaDataSink) RequiresConsistency() bool {
    return false
}

func (ds *saramaKafkaDataSink) Start(ctx context.Context) error {
    cfg, ok := ds.GetConfig().(*config.KafkaDataConnectorConfig)
    if !ok || cfg.Implementation != api.DataConnectorImplementationIBMsarama {
        return fmt.Errorf("invalid saramaKafkaDataSink configuration")
    }

    if cfg.Brokers == "" {
        return fmt.Errorf("no brokers specified for data connector %q", ds.GetName())
    }
    kafkaConfig, err := makeKafkaConfig(cfg)
    if err != nil {
        return err
    }

    kafkaConfig.Producer.Return.Successes = true
    kafkaConfig.Producer.RequiredAcks = kafka.WaitForLocal
    kafkaConfig.Producer.Partitioner = func(topic string) kafka.Partitioner {
        return ds
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

    endpoints := ds.GetEndpoints()
    length := endpoints.Len()
    for i := 0; i < length; i++ {
        if err := endpoints.At(i).(kafkaSinkEndpoint).Start(ctx, admin); err != nil {
            return err
        }
    }

    ds.producer, err = kafka.NewAsyncProducer(brokers, kafkaConfig)
    if err != nil {
        return fmt.Errorf("create kafka producer failed for data connector %q: %v",
            ds.GetName(), err)
    }

    ds.wg.Add(1)
    go func() {
        defer ds.wg.Done()
        for msg := range ds.producer.Successes() {
            func() {
                defer ds.sendWG.Done()
                if msg.Metadata == nil {
                    ds.GetEnvironment().Log().Error(ctx,
                        "metadata is nil inside ProducerMessage for success channel",
                        log.Str("connector", ds.GetName()))
                    return
                }
                meta := msg.Metadata.(*messageMetadata)
                if meta.onDelivery != nil {
                    meta.onDelivery(msg.Partition, msg.Offset, nil)
                }
            }()
        }
    }()

    ds.wg.Add(1)
    go func() {
        defer ds.wg.Done()
        for errMsg := range ds.producer.Errors() {
            func() {
                defer ds.sendWG.Done()
                if errMsg.Msg.Metadata == nil {
                    ds.GetEnvironment().Log().Error(ctx,
                        "metadata is nil inside ProducerMessage for errors channel",
                        log.Str("connector", ds.GetName()))
                    return
                }
                meta := errMsg.Msg.Metadata.(*messageMetadata)
                if meta.onDelivery != nil {
                    meta.onDelivery(0, 0, errMsg.Err)
                }
            }()
        }
    }()
    return nil
}

func (ds *saramaKafkaDataSink) Stop(ctx context.Context) {
    ds.mu.Lock()
    ds.stopped = true
    ds.mu.Unlock()

    ds.producer.AsyncClose()

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

    endpoints := ds.GetEndpoints()
    length := endpoints.Len()
    for i := 0; i < length; i++ {
        endpoints.At(i).(kafkaSinkEndpoint).Stop(ctx)
    }
}

func (ds *saramaKafkaDataSink) SendMessage(ctx context.Context, msg *kafka.ProducerMessage) {
    ds.mu.Lock()
    if ds.stopped {
        ds.mu.Unlock()
        meta := msg.Metadata.(*messageMetadata)
        if meta.onDelivery != nil {
            meta.onDelivery(0, 0, fmt.Errorf("kafka producer is stopped"))
        }
        return
    }
    ds.sendWG.Add(1)
    ds.mu.Unlock()
    select {
    case ds.producer.Input() <- msg:
    case <-ctx.Done():
        ds.sendWG.Done()
        meta := msg.Metadata.(*messageMetadata)
        if meta.onDelivery != nil {
            meta.onDelivery(0, 0, ctx.Err())
        }
    }
}

func (ep *saramaKafkaEndpoint) Start(ctx context.Context, admin kafka.ClusterAdmin) error {
    cfg, ok := ep.GetConfig().(*config.KafkaEndpointConfig)
    if !ok {
        return fmt.Errorf("invalid saramaKafkaDataSink configuration")
    }
    if cfg.Topic == "" {
        return fmt.Errorf("no topic specified for sink endpoint %q", ep.GetName())
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
                return fmt.Errorf("create topic failed for sink endpoint %q: %v",
                    ep.GetName(), err)
            }
        }
    }

    return ep.consumer.Start(ctx)
}

func (ep *saramaKafkaEndpoint) getDataSink() saramaKafkaOutputDataSink {
    return ep.GetDataSink().(saramaKafkaOutputDataSink)
}

func (ep *saramaKafkaEndpoint) SendMessage(ctx context.Context, key []byte, value []byte, metadata *messageMetadata) {
    var keyEncoder kafka.Encoder
    if len(key) > 0 {
        keyEncoder = kafka.ByteEncoder(key)
    }
    ep.getDataSink().SendMessage(ctx, &kafka.ProducerMessage{
        Topic:    ep.topic,
        Value:    kafka.ByteEncoder(value),
        Key:      keyEncoder,
        Metadata: metadata,
    })
}

func (ep *saramaKafkaEndpoint) Stop(ctx context.Context) {
    ep.consumer.Stop(ctx)
}

type collector[R any] struct {
    consumer runtime.Consumer[R]
}

func (rs *collector[R]) Out(ctx context.Context, value R) {
    rs.consumer.Consume(ctx, value)
}

type saramaKafkaEndpointConsumer[HandlerState, T, R any] struct {
    *runtime.DataSinkEndpointConsumer[T, R]
    handler     EndpointHandler[HandlerState, T, R]
    partitioner Partitioner[T]
    generator   *rand.Rand
    tracer      tracing.Tracer
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) getEndpoint() *saramaKafkaEndpoint {
    return ec.Endpoint().(*saramaKafkaEndpoint)
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) Consume(ctx context.Context, item T) {
    var span tracing.Span
    if ec.tracer != nil {
        ctx, span = ec.tracer.Start(ctx, "kafka.output",
            tracing.StringAttr("stream", ec.Stream().GetName()),
            tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
        )
        defer span.End()
    }
    stream := ec.Stream()
    streamID := ec.handler.GetStreamID(ctx, item)
    handlerCtx := runtime.WithStreamId(ctx, streamID)
    if span != nil {
        tracing.SpanAttrs(span, tracing.StringAttr("stream_id", streamID))
    }
    handlerCtx, handlerState := ec.handler.BeginRequest(handlerCtx, stream)
    tracing.SpanEvent(span, "begin_request")
    startTime := ec.Endpoint().OnRequestStart(handlerCtx)

    ep := ec.getEndpoint()
    rs := &collector[R]{consumer: stream.GetErrorStream()}

    msg := &SinkMessage[R]{
        topic:        ep.topic,
        resultStream: rs,
        sendFn: func(keyBytes, valueBytes []byte, onDelivery func(int32, int64, error)) {
            meta := &messageMetadata{
                onDelivery: onDelivery,
                partitionFn: func(pm *kafka.ProducerMessage, numPartitions int32) (int32, error) {
                    if ec.partitioner == nil {
                        return int32(ec.generator.Intn(int(numPartitions))), nil
                    }
                    return ec.partitioner.Partition(item, numPartitions)
                },
            }
            ep.SendMessage(handlerCtx, keyBytes, valueBytes, meta)
        },
    }

    err := ec.handler.ConsumeMessage(handlerCtx, stream, handlerState, item, msg)
    if err != nil {
        tracing.SpanError(span, err)
        if span != nil {
            tracing.SpanEvent(span, "consume_message.error", tracing.StringAttr("error", err.Error()))
        }
    } else {
        tracing.SpanEvent(span, "consume_message")
    }
    ec.handler.EndRequest(handlerCtx, stream, err, handlerState)
    ec.Endpoint().OnRequestEnd(handlerCtx, startTime, err)
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) Start(_ context.Context) error {
    if ec.partitioner == nil {
        ec.generator = rand.New(rand.NewSource(time.Now().UnixNano()))
    }
    return nil
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) Stop(_ context.Context) {}

func getSaramaKafkaDataSink(id int, env runtime.RuntimeEnvironment) (runtime.DataSink, error) {
    dataSink := env.GetDataSink(id)
    if dataSink != nil {
        return dataSink, nil
    }
    cfg := env.RuntimeConfig().GetDataConnectorByID(id)
    if cfg == nil {
        return nil, fmt.Errorf("config for datasink with id=%d not found", id)
    }
    outputDS, err := runtime.MakeOutputDataSink(cfg, env)
    if err != nil {
        return nil, err
    }
    kafkaDataSink := &saramaKafkaDataSink{
        OutputDataSink: outputDS,
    }
    var outputDataSink saramaKafkaOutputDataSink = kafkaDataSink
    env.AddDataSink(outputDataSink)
    return outputDataSink, nil
}

func getSaramaKafkaDataSinkEndpoint(id int, env runtime.RuntimeEnvironment) (*saramaKafkaEndpoint, error) {
    cfg := env.RuntimeConfig().GetEndpointConfigByID(id)
    if cfg == nil {
        return nil, fmt.Errorf("config for endpoint with id=%d not found", id)
    }
    endpointCfg, ok := cfg.(*config.KafkaEndpointConfig)
    if !ok {
        return nil, fmt.Errorf("config for endpoint %q has invalid type", cfg.GetName())
    }
    dataSink, err := getSaramaKafkaDataSink(endpointCfg.IdDataConnector, env)
    if err != nil {
        return nil, err
    }
    endpoint := dataSink.GetEndpoint(id)
    if endpoint != nil {
        return nil, fmt.Errorf("endpoint %q already exists", endpointCfg.GetName())
    }
    sinkEndpoint, err := runtime.MakeDataSinkEndpoint(dataSink, id, env)
    if err != nil {
        return nil, err
    }
    kafkaEndpoint := &saramaKafkaEndpoint{
        DataSinkEndpoint: sinkEndpoint,
        topic:            endpointCfg.Topic,
    }
    dataSink.AddEndpoint(kafkaEndpoint)
    return kafkaEndpoint, nil
}

type SaramaKafkaSinkOption[HandlerState, T, R any] func(*saramaKafkaEndpointConsumer[HandlerState, T, R])

func WithPartitioner[HandlerState, T, R any](partitioner Partitioner[T]) SaramaKafkaSinkOption[HandlerState, T, R] {
    return func(ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) {
        ec.partitioner = partitioner
    }
}

func MakeSaramaKafkaEndpointConsumer[HandlerState, T, R any](
    stream runtime.TypedSinkStream[T, R],
    handler EndpointHandler[HandlerState, T, R],
    opts ...SaramaKafkaSinkOption[HandlerState, T, R],
) (runtime.Consumer[T], error) {
    if handler == nil {
        return nil, fmt.Errorf("handler is nil for kafka endpoint sink for the stream %q", stream.GetName())
    }
    env := stream.GetRuntimeEnvironment()
    endpoint, err := getSaramaKafkaDataSinkEndpoint(stream.GetEndpointId(), env)
    if err != nil {
        return nil, err
    }
    var tr tracing.Tracer
    if t := env.Tracing(); t != nil {
        tr = t.Tracer(env.ServiceConfig().Name)
    }
    ec := &saramaKafkaEndpointConsumer[HandlerState, T, R]{
        DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T, R](endpoint, stream),
        handler:                  handler,
        tracer:                   tr,
    }
    for _, opt := range opts {
        opt(ec)
    }
    stream.SetSinkConsumer(ec)
    endpoint.consumer = ec
    env.RegisterEndpointConsumer(ec)
    return ec, nil
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) GetID() int {
    return ec.Endpoint().GetID()
}

func (ec *saramaKafkaEndpointConsumer[HandlerState, T, R]) FunctionImplementation() interface{} {
    return ec.handler
}
