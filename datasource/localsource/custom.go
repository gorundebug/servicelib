/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package localsource

import (
	"context"
	"github.com/gorundebug/servicelib/runtime"
	"sync"
	"time"
)

type DataProducer[T any] interface {
	Start(ctx context.Context, consumer runtime.Consumer[T]) error
	Stop(context.Context)
}

type CustomInputDataSource interface {
	runtime.DataSource
	WaitGroup() *sync.WaitGroup
}

type CustomInputEndpoint interface {
	runtime.InputEndpoint
	Start(context.Context) error
	Stop(context.Context)
	NextMessage()
}

type CustomEndpointConsumer interface {
	runtime.InputEndpointConsumer
	Start(context.Context) error
	Stop(context.Context)
}

type CustomDataSource struct {
	*runtime.InputDataSource
	wg sync.WaitGroup
}

type CustomEndpoint struct {
	*runtime.DataSourceEndpoint
	delay time.Duration
}

func (ep *CustomEndpoint) Start(ctx context.Context) error {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		if err := endpointConsumers.At(i).(CustomEndpointConsumer).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ep *CustomEndpoint) Stop(ctx context.Context) {
	endpointConsumers := ep.GetEndpointConsumers()
	length := endpointConsumers.Len()
	for i := 0; i < length; i++ {
		endpointConsumers.At(i).(CustomEndpointConsumer).Stop(ctx)
	}
}

func (ep *CustomEndpoint) NextMessage() {
	if ep.delay > 0 {
		time.Sleep(ep.delay)
	}
}

type TypedCustomEndpointConsumer[T any] struct {
	*runtime.DataSourceEndpointConsumer[T]
	dataProducer DataProducer[T]
	wg           sync.WaitGroup
}

func (ec *TypedCustomEndpointConsumer[T]) Consume(value T) {
	ec.Endpoint().(CustomInputEndpoint).NextMessage()
	ec.DataSourceEndpointConsumer.Consume(value)
}

func (ec *TypedCustomEndpointConsumer[T]) Start(ctx context.Context) error {
	endpoint := ec.Endpoint()
	dataSource := endpoint.GetDataSource().(CustomInputDataSource)
	dataSource.WaitGroup().Add(1)
	ec.wg.Add(1)
	go func() {
		defer func() {
			ec.wg.Done()
			dataSource.WaitGroup().Done()
		}()
		if err := ec.dataProducer.Start(ctx, ec); err != nil {
			dataSource.GetEnvironment().Log().Fatalln(err)
		}
	}()
	return nil
}

func (ec *TypedCustomEndpointConsumer[T]) Stop(ctx context.Context) {
	endpoint := ec.Endpoint()
	dataSource := endpoint.GetDataSource().(CustomInputDataSource)
	dataSource.WaitGroup().Add(1)
	ec.wg.Add(1)
	go func() {
		defer func() {
			ec.wg.Done()
		}()
		ec.dataProducer.Stop(ctx)
	}()
	go func() {
		defer dataSource.WaitGroup().Done()
		c := make(chan struct{})
		go func() {
			defer close(c)
			ec.wg.Wait()
		}()
		select {
		case <-c:
		case <-ctx.Done():
			dataSource.GetEnvironment().Log().Warnf(
				"Custom data source endpoint %q for the stream %q stopped by timeout.",
				endpoint.GetName(),
				ec.Stream().GetName())
		}
	}()
}

func (ds *CustomDataSource) Start(ctx context.Context) error {
	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		if err := endpoints.At(i).(CustomInputEndpoint).Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CustomDataSource) WaitGroup() *sync.WaitGroup {
	return &ds.wg
}

func (ds *CustomDataSource) Stop(ctx context.Context) {
	endpoints := ds.InputDataSource.GetEndpoints()
	length := endpoints.Len()
	for i := 0; i < length; i++ {
		ds.wg.Add(1)
		go func(endpoint CustomInputEndpoint) {
			defer ds.wg.Done()
			endpoint.Stop(ctx)
		}(endpoints.At(i).(CustomInputEndpoint))
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		ds.wg.Wait()
	}()
	select {
	case <-c:
	case <-ctx.Done():
		ds.GetEnvironment().Log().Warnf("Stop custom data source %q after timeout.", ds.GetName())
	}
}

func getCustomDataSource(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSource {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := env.AppConfig().GetDataConnectorById(id)
	if cfg == nil {
		env.Log().Fatalf("config for data source with id=%d not found", id)
	}
	customDataSource := &CustomDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, env),
	}
	var inputDataSource CustomInputDataSource = customDataSource
	env.AddDataSource(inputDataSource)
	return customDataSource
}

func getCustomDataSourceEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.InputEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	if cfg == nil {
		env.Log().Fatalf("config for source endpoint with id=%d not found", id)
	}
	dataSource := getCustomDataSource(cfg.IdDataConnector, env)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	customEndpoint := &CustomEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, id, env),
		delay:              time.Duration(*cfg.Delay) * time.Microsecond,
	}
	var inputEndpoint CustomInputEndpoint = customEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return customEndpoint
}

func MakeCustomEndpointConsumer[T any](stream runtime.TypedInputStream[T], dataProducer DataProducer[T]) runtime.Consumer[T] {
	env := stream.GetEnvironment()
	endpoint := getCustomDataSourceEndpoint(stream.GetEndpointId(), env)
	typedEndpointConsumer := &TypedCustomEndpointConsumer[T]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T](endpoint, stream),
		dataProducer:               dataProducer,
	}
	var endpointConsumer CustomEndpointConsumer = typedEndpointConsumer
	var consumer runtime.Consumer[T] = typedEndpointConsumer
	endpoint.AddEndpointConsumer(endpointConsumer)
	return consumer
}
