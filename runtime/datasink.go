/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/exp/maps"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type DataSink interface {
	DataConnector
	Start(context.Context) error
	Stop(context.Context)
	GetConfig() config.DataConnectorConfig
	GetRuntimeEnvironment() RuntimeEnvironment
	AddEndpoint(SinkEndpoint)
	GetEndpoint(id int) SinkEndpoint
	GetEndpoints() Collection[SinkEndpoint]
}

type SinkEndpoint interface {
	Endpoint
	GetConfig() config.EndpointConfig
	GetRuntimeEnvironment() RuntimeEnvironment
	GetDataSink() DataSink
	OnBeginRequestFailed(ctx context.Context, err error)
	OnLateResult(ctx context.Context, streamID string)
	OnRequestStart(ctx context.Context) time.Time
	OnRequestEnd(ctx context.Context, startTime time.Time, err error)
}

type OutputEndpointConsumer interface {
	Endpoint() SinkEndpoint
}

type OutputDataSink struct {
	id                 int
	name               string
	environment        RuntimeEnvironment
	endpoints          map[int]SinkEndpoint
	stopTimeoutCounter metrics.Int64Counter
}

func MakeOutputDataSink(dataConnector config.DataConnectorConfig, environment RuntimeEnvironment) (*OutputDataSink, error) {
	ds := &OutputDataSink{
		id:          dataConnector.GetID(),
		name:        dataConnector.GetName(),
		environment: environment,
		endpoints:   make(map[int]SinkEndpoint),
	}
	scope := environment.Metrics().Scope("datasink_connector", metrics.Labels{
		"connector": dataConnector.GetName(),
	})
	var err error
	if ds.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in data sink connector", metrics.Labels{"event": "stop_timeout"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink connector %q: %w", dataConnector.GetName(), err)
	}
	return ds, nil
}

func (ds *OutputDataSink) GetEnvironment() environment.ServiceEnvironment {
	return ds.environment
}

func (ds *OutputDataSink) OnStopTimeout(ctx context.Context) {
	ds.environment.Log().Warn(ctx, "data sink stopped by timeout", log.Str("name", ds.GetName()))
	ds.stopTimeoutCounter.Inc(ctx)
}

func (ds *OutputDataSink) GetConfig() config.DataConnectorConfig {
	return ds.environment.RuntimeConfig().GetDataConnectorByID(ds.id)
}

func (ds *OutputDataSink) GetName() string {
	return ds.name
}

func (ds *OutputDataSink) GetID() int {
	return ds.id
}

func (ds *OutputDataSink) GetRuntimeEnvironment() RuntimeEnvironment {
	return ds.environment
}

func (ds *OutputDataSink) GetEndpoint(id int) SinkEndpoint {
	return ds.endpoints[id]
}

func (ds *OutputDataSink) GetEndpoints() Collection[SinkEndpoint] {
	return MakeCollection(maps.Values(ds.endpoints))
}

func (ds *OutputDataSink) AddEndpoint(endpoint SinkEndpoint) {
	ds.endpoints[endpoint.GetID()] = endpoint
}

type DataSinkEndpoint struct {
	id                        int
	name                      string
	environment               RuntimeEnvironment
	dataSink                  DataSink
	endpointConsumers         []OutputEndpointConsumer
	beginRequestFailedCounter metrics.Int64Counter
	lateResultCounter         metrics.Int64Counter
	requestErrorsCounter      metrics.Int64Counter
	messagesTotal             metrics.Int64Counter
	requestDuration           metrics.Float64Histogram
	activeRequests            metrics.Int64Gauge
}

func MakeDataSinkEndpoint(dataSink DataSink, id int, environment RuntimeEnvironment) (*DataSinkEndpoint, error) {
	endpointName := environment.RuntimeConfig().GetEndpointConfigByID(id).GetName()
	ep := &DataSinkEndpoint{
		dataSink:          dataSink,
		id:                id,
		name:              endpointName,
		environment:       environment,
		endpointConsumers: make([]OutputEndpointConsumer, 0),
	}
	connectorName := dataSink.GetName()

	labels := metrics.Labels{
		"connector": connectorName,
		"endpoint":  endpointName,
	}
	if protocol := dataConnectorProtocol(dataSink.GetConfig().GetType()); protocol != "" {
		labels["protocol"] = protocol
	}
	scope := environment.Metrics().Scope("datasink_endpoint", labels)
	var err error
	if ep.beginRequestFailedCounter, err = scope.Counter("events_total", "Total number of events in data sink endpoint", metrics.Labels{"event": "begin_request_failed"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	if ep.lateResultCounter, err = scope.Counter("events_total", "Total number of events in data sink endpoint", metrics.Labels{"event": "late_result"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	if ep.requestErrorsCounter, err = scope.Counter("events_total", "Total number of events in data sink endpoint", metrics.Labels{"event": "request_error"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	if ep.messagesTotal, err = scope.Counter("messages_total", "Total number of successfully processed messages in data sink endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	if ep.requestDuration, err = scope.Histogram("request_duration_seconds", "Request duration in seconds for data sink endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	if ep.activeRequests, err = scope.Gauge("active_requests", "Number of active requests in data sink endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for sink endpoint %q: %w", endpointName, err)
	}
	return ep, nil
}

func (ep *DataSinkEndpoint) GetConfig() config.EndpointConfig {
	return ep.environment.RuntimeConfig().GetEndpointConfigByID(ep.id)
}

func (ep *DataSinkEndpoint) GetName() string {
	return ep.name
}

func (ep *DataSinkEndpoint) GetID() int {
	return ep.id
}

func (ep *DataSinkEndpoint) GetRuntimeEnvironment() RuntimeEnvironment {
	return ep.environment
}

func (ep *DataSinkEndpoint) GetDataSink() DataSink {
	return ep.dataSink
}

func (ep *DataSinkEndpoint) GetDataConnector() DataConnector {
	return ep.dataSink
}

func (ep *DataSinkEndpoint) OnBeginRequestFailed(ctx context.Context, err error) {
	ep.environment.Log().Error(ctx, "BeginRequest failed", log.Str("endpoint", ep.GetName()), log.Err(err))
	ep.beginRequestFailedCounter.Inc(ctx)
}

func (ep *DataSinkEndpoint) OnLateResult(ctx context.Context, streamID string) {
	ep.environment.Log().Warn(ctx, "late result for sink endpoint", log.Str("endpoint", ep.GetName()), log.Str("stream_id", streamID))
	ep.lateResultCounter.Inc(ctx)
}

func (ep *DataSinkEndpoint) OnRequestStart(_ context.Context) time.Time {
	ep.activeRequests.Inc()
	return time.Now()
}

func (ep *DataSinkEndpoint) OnRequestEnd(ctx context.Context, startTime time.Time, err error) {
	ep.activeRequests.Dec()
	ep.requestDuration.Observe(ctx, time.Since(startTime).Seconds())
	if err == nil {
		ep.messagesTotal.Inc(ctx)
	} else {
		ep.requestErrorsCounter.Inc(ctx)
	}
}

type DataSinkEndpointConsumer[T, R any] struct {
	endpoint SinkEndpoint
	stream   TypedSinkStream[T, R]
}

func (ec *DataSinkEndpointConsumer[T, R]) Endpoint() SinkEndpoint {
	return ec.endpoint
}

func (ec *DataSinkEndpointConsumer[T, R]) Stream() TypedSinkStream[T, R] {
	return ec.stream
}

func MakeDataSinkEndpointConsumer[T, R any](endpoint SinkEndpoint, stream TypedSinkStream[T, R]) *DataSinkEndpointConsumer[T, R] {
	ec := &DataSinkEndpointConsumer[T, R]{
		endpoint: endpoint,
		stream:   stream,
	}
	return ec
}
