/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	kafka "github.com/Shopify/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type Partitioner[T any] interface {
	Partition(value T, numPartitions int32) (int32, error)
}

type SaramaKafkaOutputDataSink interface {
	runtime.DataSink
	SendMessage(*kafka.ProducerMessage)
}

type MessageMetadata struct {
	value            interface{}
	endpointConsumer SaramaKafkaEndpointConsumer
}

type SaramaKafkaSinkEndpoint interface {
	runtime.SinkEndpoint
	Start(context.Context, kafka.ClusterAdmin) error
	Stop(context.Context)
	SendMessage(key []byte, value []byte, metadata *MessageMetadata)
}

type SaramaKafkaEndpointConsumer interface {
	runtime.OutputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
	SendSuccess(msg *kafka.ProducerMessage)
	SendError(errMsg *kafka.ProducerError)
	Partition(message *kafka.ProducerMessage, numPartitions int32) (int32, error)
}

type SaramaKafkaDataSink struct {
	*runtime.OutputDataSink
	producer kafka.AsyncProducer
	wg       sync.WaitGroup
	sendWG   sync.WaitGroup
}

type SaramaKafkaEndpoint struct {
	*runtime.DataSinkEndpoint
}

func makeKafkaConfig(cfg *config.DataConnectorConfig) (*kafka.Config, error) {
	kafkaConfig := kafka.NewConfig()

	if cfg.Version == nil {
		kafkaConfig.Version = kafka.V2_6_0_0
	} else {
		var err error
		kafkaConfig.Version, err = kafka.ParseKafkaVersion(*cfg.Version)
		if err != nil {
			return nil, fmt.Errorf("failed to parse kafka version for data connector with id %d", cfg.Id)
		}
	}
	if cfg.DialTimeout != nil {
		kafkaConfig.Net.DialTimeout = time.Duration(*cfg.DialTimeout) * time.Millisecond
	}
	return kafkaConfig, nil
}

func (ds *SaramaKafkaDataSink) Partition(msg *kafka.ProducerMessage, numPartitions int32) (int32, error) {
	if msg.Metadata == nil {
		err := fmt.Errorf("metadata is nil inside ProducerMessage for partition method in data sink with id=%d",
			ds.GetId())
		ds.GetEnvironment().Log().Error(err)
		return 0, err
	} else {
		return msg.Metadata.(MessageMetadata).endpointConsumer.Partition(msg, numPartitions)
	}
}

func (ds *SaramaKafkaDataSink) RequiresConsistency() bool {
	return false
}

func (ds *SaramaKafkaDataSink) Start(ctx context.Context) error {
	cfg := ds.GetDataConnector()
	if cfg.Brokers == nil {
		return fmt.Errorf("no brokers specified for data connector with id %d", ds.GetId())
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

	brokers := strings.Split(*cfg.Brokers, ",")

	admin, err := kafka.NewClusterAdmin(brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka admin failed for sink endpoint with id=%d: %v",
			ds.GetId(), err)
	}

	endpoints := ds.OutputDataSink.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(SaramaKafkaSinkEndpoint).Start(ctx, admin); err != nil {
			return err
		}
	}

	ds.producer, err = kafka.NewAsyncProducer(brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka producer failed for data connector with id=%d: %v",
			ds.GetId(), err)
	}

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		for msg := range ds.producer.Successes() {
			if msg.Metadata == nil {
				ds.GetEnvironment().Log().Errorf(
					"metadata is nil inside ProducerMessage for suceess channel in data sink with id=%d",
					ds.GetId())
			} else {
				func() {
					defer ds.sendWG.Done()
					msg.Metadata.(*MessageMetadata).endpointConsumer.SendSuccess(msg)
				}()
			}
		}
	}()

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		for msg := range ds.producer.Errors() {
			if msg.Msg.Metadata == nil {
				ds.GetEnvironment().Log().Errorf(
					"metadata is nil inside ProducerMessage for errors channel in data sink with id=%d",
					ds.GetId())
			} else {
				func() {
					defer ds.sendWG.Done()
					msg.Msg.Metadata.(*MessageMetadata).endpointConsumer.SendError(msg)
				}()
			}
		}
	}()
	return nil
}

func (ds *SaramaKafkaDataSink) Stop(ctx context.Context) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.sendWG.Wait()
	}()

	select {
	case <-c:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf(
			"Kafka data sink %d stopped by timeout and lost messages.",
			ds.GetId())
	}

	if err := ds.producer.Close(); err != nil {
		ds.GetEnvironment().Log().Warnf("close kafka producer failed for data connector with id=%d: %v",
			ds.GetId(), err)
	}

	c = make(chan struct{})

	go func() {
		defer close(c)
		ds.wg.Wait()
	}()

	select {
	case <-c:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf(
			"Kafka data sink %d stopped by timeout. Producer close timeout.",
			ds.GetId())
	}

	endpoints := ds.OutputDataSink.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		endpoints.At(i).(SaramaKafkaSinkEndpoint).Stop(ctx)
	}
}

func (ds *SaramaKafkaDataSink) SendMessage(msg *kafka.ProducerMessage) {
	ds.sendWG.Add(1)
	ds.producer.Input() <- msg
}

func (ep *SaramaKafkaEndpoint) Start(ctx context.Context, admin kafka.ClusterAdmin) error {
	cfg := ep.GetConfig()
	if cfg.Topic == nil {
		return fmt.Errorf("no topic specified for sink endpoint with id %d", ep.GetId())
	}
	if cfg.CreateTopic != nil && *cfg.CreateTopic {
		numPartitions := 1
		if cfg.Partitions != nil {
			numPartitions = *cfg.Partitions
		}

		replicationFactor := 1
		if cfg.ReplicationFactor != nil {
			replicationFactor = *cfg.ReplicationFactor
		}

		topicDetail := &kafka.TopicDetail{
			NumPartitions:     int32(numPartitions),
			ReplicationFactor: int16(replicationFactor),
		}

		if err := admin.CreateTopic(*cfg.Topic, topicDetail, false); err != nil {
			var kafkaErr *kafka.TopicError
			if !errors.As(err, &kafkaErr) || !errors.Is(kafkaErr.Err, kafka.ErrTopicAlreadyExists) {
				return fmt.Errorf("create topic failed for sink endpoint with id=%d: %v",
					ep.GetId(), err)
			}
		}
	}

	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		if err := endpointConsumers.At(i).(SaramaKafkaEndpointConsumer).Start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (ep *SaramaKafkaEndpoint) getDataSink() SaramaKafkaOutputDataSink {
	return ep.GetDataSink().(SaramaKafkaOutputDataSink)
}

func (ep *SaramaKafkaEndpoint) SendMessage(key []byte, value []byte, metadata *MessageMetadata) {
	var keyEncoder kafka.Encoder
	valueEncoder := kafka.ByteEncoder(value)

	if len(key) > 0 {
		keyEncoder = kafka.ByteEncoder(key)
	}
	msg := &kafka.ProducerMessage{
		Topic:    *ep.GetConfig().Topic,
		Value:    valueEncoder,
		Key:      keyEncoder,
		Metadata: metadata,
	}
	ep.getDataSink().SendMessage(msg)
}

func (ep *SaramaKafkaEndpoint) Stop(ctx context.Context) {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		endpointConsumers.At(i).(SaramaKafkaEndpointConsumer).Stop(ctx)
	}
}

type TypedSaramaKafkaEndpointConsumer[T, R any] struct {
	*runtime.DataSinkEndpointConsumer[T, R]
	sinkCallback runtime.SinkCallback[T]
	partitioner  Partitioner[T]
	writer       runtime.TypedEndpointWriter[T]
	generator    *rand.Rand
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) SetSinkCallback(callback runtime.SinkCallback[T]) {
	ec.sinkCallback = callback
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) getEndpoint() SaramaKafkaSinkEndpoint {
	return ec.Endpoint().(SaramaKafkaSinkEndpoint)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) Partition(message *kafka.ProducerMessage,
	numPartitions int32) (int32, error) {
	if ec.partitioner == nil {
		return int32(ec.generator.Intn(int(numPartitions))), nil
	}
	return ec.partitioner.Partition(message.Metadata.(MessageMetadata).value.(T), numPartitions)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) SendSuccess(msg *kafka.ProducerMessage) {
	ec.sinkCallback.Done(msg.Metadata.(MessageMetadata).value.(T), nil)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) SendError(errMsg *kafka.ProducerError) {
	ec.sinkCallback.Done(errMsg.Msg.Metadata.(MessageMetadata).value.(T), errMsg.Err)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) Consume(value T) {
	var buf bytes.Buffer
	if err := ec.writer.Write(value, &buf); err != nil && ec.sinkCallback != nil {
		ec.sinkCallback.Done(value, err)
	} else {
		metadata := &MessageMetadata{
			value:            value,
			endpointConsumer: ec,
		}
		ec.getEndpoint().SendMessage(nil, buf.Bytes(), metadata)
	}
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) Start(ctx context.Context) error {
	writer := ec.Endpoint().GetEnvironment().GetEndpointWriter(ec.Endpoint(), ec.Stream(), serde.GetSerdeType[T]())
	if writer == nil {
		return fmt.Errorf("writer does not defined for endpoint with id=%d", ec.Endpoint().GetId())
	}
	var ok bool
	ec.writer, ok = writer.(runtime.TypedEndpointWriter[T])
	if !ok {
		return fmt.Errorf("writer has invalid type for endpoint with id=%d", ec.Endpoint().GetId())
	}
	if ec.partitioner == nil {
		ec.generator = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return nil
}

func (ec *TypedSaramaKafkaEndpointConsumer[T, R]) Stop(ctx context.Context) {
}

func getSaramaKafkaDataSink(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSink {
	dataSink := env.GetDataSink(id)
	if dataSink != nil {
		return dataSink
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
	if cfg == nil {
		env.Log().Fatalf("config for datasource with id=%d not found", id)
	}
	kafkaDataSink := &SaramaKafkaDataSink{
		OutputDataSink: runtime.MakeOutputDataSink(cfg, env),
	}
	var outputDataSource SaramaKafkaOutputDataSink = kafkaDataSink
	env.AddDataSink(outputDataSource)
	return outputDataSource
}

func getSaramaKafkaDataSinkEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.SinkEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	if cfg == nil {
		env.Log().Fatalf("config for endpoint with id=%d not found", id)
	}
	dataSink := getSaramaKafkaDataSink(cfg.IdDataConnector, env)
	endpoint := dataSink.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	kafkaEndpoint := &SaramaKafkaEndpoint{
		DataSinkEndpoint: runtime.MakeDataSinkEndpoint(dataSink, id, env),
	}
	var outputEndpoint SaramaKafkaSinkEndpoint = kafkaEndpoint
	dataSink.AddEndpoint(outputEndpoint)
	return outputEndpoint
}

func MakeSaramaKafkaEndpointSink[T, R any](stream runtime.TypedSinkStream[T, R], partitioner Partitioner[T]) runtime.SinkConsumer[T] {
	env := stream.GetEnvironment()
	endpoint := getSaramaKafkaDataSinkEndpoint(stream.GetEndpointId(), env)
	typedEndpointConsumer := &TypedSaramaKafkaEndpointConsumer[T, R]{
		DataSinkEndpointConsumer: runtime.MakeDataSinkEndpointConsumer[T, R](endpoint, stream),
		partitioner:              partitioner,
	}
	var endpointConsumer SaramaKafkaEndpointConsumer = typedEndpointConsumer
	var consumer runtime.SinkConsumer[T] = typedEndpointConsumer
	stream.SetSinkConsumer(typedEndpointConsumer)
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
