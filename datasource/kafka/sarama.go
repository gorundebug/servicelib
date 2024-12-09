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
	"strings"
	"sync"
	"time"

	kafka "github.com/Shopify/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
)

type HandlerData struct {
	Message *kafka.ConsumerMessage
	Session kafka.ConsumerGroupSession
	Claim   kafka.ConsumerGroupClaim
}

type SaramaKafkaEndpointHandler[T any] interface {
	Handler(*HandlerData, runtime.Collect[T])
}

type SaramaKafkaInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context, kafka.ClusterAdmin) error
	Stop(context.Context)
}

type SaramaKafkaEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type SaramaKafkaInputDataSource interface {
	runtime.DataSource
	WaitGroup() *sync.WaitGroup
}

type SaramaKafkaDataSource struct {
	*runtime.InputDataSource
	wg sync.WaitGroup
}

type SaramaKafkaEndpoint struct {
	*runtime.DataSourceEndpoint
}

type TypedSaramaKafkaEndpointConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	reader        runtime.TypedEndpointReader[T]
	consumerGroup kafka.ConsumerGroup
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	handler       SaramaKafkaEndpointHandler[T]
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

func (ds *SaramaKafkaDataSource) Start(ctx context.Context) error {
	cfg := ds.GetDataConnector()
	if cfg.Brokers == nil {
		return fmt.Errorf("no brokers specified for data connector with id %d", ds.GetId())
	}
	kafkaConfig, err := makeKafkaConfig(cfg)
	if err != nil {
		return err
	}

	brokers := strings.Split(*cfg.Brokers, ",")

	admin, err := kafka.NewClusterAdmin(brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka admin failed for sink endpoint with id=%d: %v",
			ds.GetId(), err)
	}

	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(SaramaKafkaInputEndpoint).Start(ctx, admin); err != nil {
			return err
		}
	}

	return nil
}

func (ds *SaramaKafkaDataSource) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ds *SaramaKafkaDataSource) Stop(ctx context.Context) {
	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint SaramaKafkaInputEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(SaramaKafkaInputEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf("Stop kafka data source id=%d after timeout.", ds.GetId())
	}
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

func (ep *SaramaKafkaEndpoint) Stop(ctx context.Context) {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		endpointConsumers.At(i).(SaramaKafkaEndpointConsumer).Stop(ctx)
	}
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) getDataSource() SaramaKafkaInputDataSource {
	return ec.Endpoint().GetDataSource().(SaramaKafkaInputDataSource)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) Start(ctx context.Context) error {
	reader := ec.Endpoint().GetEnvironment().GetEndpointReader(ec.Endpoint(), ec.Stream(), serde.GetSerdeType[T]())
	if reader == nil {
		return fmt.Errorf("writer does not defined for endpoint with id=%d", ec.Endpoint().GetId())
	}
	var ok bool
	ec.reader, ok = reader.(runtime.TypedEndpointReader[T])
	if !ok {
		return fmt.Errorf("writer has invalid type for endpoint with id=%d", ec.Endpoint().GetId())
	}
	dataSourceCfg := ec.Endpoint().GetDataSource().GetDataConnector()
	kafkaConfig, err := makeKafkaConfig(dataSourceCfg)
	if err != nil {
		return err
	}
	kafkaConfig.Consumer.Offsets.Initial = kafka.OffsetOldest
	brokers := strings.Split(*dataSourceCfg.Brokers, ",")

	if ec.Endpoint().GetConfig().ConsumerGroup == nil {
		return fmt.Errorf("consumer group does not set for kafka source endpoint with id=%d",
			ec.Endpoint().GetId())
	}

	ec.consumerGroup, err = kafka.NewConsumerGroup(brokers, *ec.Endpoint().GetConfig().ConsumerGroup, kafkaConfig)
	if err != nil {
		return fmt.Errorf("new consumer group failed for kafka source endpoint with id=%d: %v",
			ec.Endpoint().GetId(), err)
	}

	var cancelCtx context.Context
	cancelCtx, ec.cancel = context.WithCancel(ctx)
	ec.getDataSource().WaitGroup().Add(1)
	ec.wg.Add(1)
	go func() {
		defer func() {
			ec.wg.Done()
			ec.getDataSource().WaitGroup().Done()
		}()
		topics := []string{*ec.Endpoint().GetConfig().Topic}
		for {
			if err := ec.consumerGroup.Consume(cancelCtx, topics, ec); err != nil {
				ec.Endpoint().GetEnvironment().Log().Errorf(
					"consumer group consume error for kafka source endpoint with id=%d: %v",
					ec.Endpoint().GetId(), err)
			}
			if cancelCtx.Err() != nil {
				break
			}
		}
	}()

	return nil
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) Stop(ctx context.Context) {
	ec.wg.Add(1)
	go func() {
		defer func() {
			ec.wg.Done()
		}()
		ec.cancel()
	}()
	go func() {
		defer ec.getDataSource().WaitGroup().Done()

		c := make(chan struct{})
		go func() {
			defer close(c)
			ec.wg.Wait()
		}()
		select {
		case <-c:
			if err := ec.consumerGroup.Close(); err != nil {
				ec.Endpoint().GetEnvironment().Log().Warnf("close consumer group failed for kafka source endpoint with id=%d: %v",
					ec.Endpoint().GetId(), err)
			}
		case <-ctx.Done():
			ec.Endpoint().GetEnvironment().Log().Warnf(
				"Kafka source endpoint %d stopped by timeout.",
				ec.Endpoint().GetId())
		}
	}()
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) Setup(session kafka.ConsumerGroupSession) error {
	return nil
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) Cleanup(session kafka.ConsumerGroupSession) error {
	return nil
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) Out(value T) {
	ec.Stream().Consume(value)
}

func (ec *TypedSaramaKafkaEndpointConsumer[T]) ConsumeClaim(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if ec.reader != nil {
			value, err := ec.reader.Read(bytes.NewReader(message.Value))
			if err != nil {
				ec.Endpoint().GetEnvironment().Log().Warnf("read message error in endpoint with id=%d: %v",
					ec.Endpoint().GetId(), err)
			} else {
				ec.Stream().Consume(value)
				session.MarkMessage(message, "")
			}
		} else {
			ec.handler.Handler(&HandlerData{
				Message: message,
				Session: session,
				Claim:   claim,
			}, ec)
		}
	}
	return nil
}

func getSaramaKafkaDataSource(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSource {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
	if cfg == nil {
		env.Log().Fatalf("config for datasource with id=%d not found", id)
		return nil
	}
	kafkaDataSource := &SaramaKafkaDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, env),
	}
	var inputDataSource SaramaKafkaInputDataSource = kafkaDataSource
	env.AddDataSource(inputDataSource)
	return inputDataSource
}

func getSaramaKafkaDataSourceEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.InputEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	if cfg == nil {
		env.Log().Fatalf("config for endpoint with id=%d not found", id)
		return nil
	}
	dataSource := getSaramaKafkaDataSource(cfg.IdDataConnector, env)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	kafkaEndpoint := &SaramaKafkaEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, id, env),
	}
	var inputEndpoint SaramaKafkaInputEndpoint = kafkaEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return kafkaEndpoint
}

func MakeSaramaKafkaEndpointConsumer[T any](stream runtime.TypedInputStream[T], handler SaramaKafkaEndpointHandler[T]) runtime.Consumer[T] {
	env := stream.GetEnvironment()
	endpoint := getSaramaKafkaDataSourceEndpoint(stream.GetEndpointId(), env)
	typedEndpointConsumer := &TypedSaramaKafkaEndpointConsumer[T]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
		handler:                    handler,
	}
	var endpointConsumer SaramaKafkaEndpointConsumer = typedEndpointConsumer
	var consumer runtime.Consumer[T] = typedEndpointConsumer
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
