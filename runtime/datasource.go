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
	"hash/maphash"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

const pendingRequestShardCount = 64

type pendingRequestShard struct {
	mu      sync.RWMutex
	started map[string]time.Time
}

type DataSource interface {
	DataConnector
	Start(context.Context) error
	Stop(context.Context)
	GetConfig() config.DataConnectorConfig
	GetRuntimeEnvironment() RuntimeEnvironment
	AddEndpoint(InputEndpoint)
	GetEndpoint(id int) InputEndpoint
	GetEndpoints() Collection[InputEndpoint]
}

type InputEndpoint interface {
	Endpoint
	GetConfig() config.EndpointConfig
	GetRuntimeEnvironment() RuntimeEnvironment
	GetDataSource() DataSource
	OnMissingStreamID(ctx context.Context)
	OnLateResult(ctx context.Context, sessionID string)
	OnUnknownMessageID(ctx context.Context, sessionID string, messageID string)
	OnDuplicateMessageID(ctx context.Context, sessionID string, messageID string)
	OnPendingAdd(ctx context.Context, streamID string)
	OnPendingRemove(ctx context.Context, streamID string)
	OnInvalidHTTPMethod(ctx context.Context, method string)
	OnBeginRequestFailed(ctx context.Context, err error)
	OnRequestStart(ctx context.Context) time.Time
	OnRequestEnd(ctx context.Context, startTime time.Time, err error)
}

type InputEndpointConsumer interface {
	Endpoint() InputEndpoint
}

type InputDataSource struct {
	id                 int
	environment        RuntimeEnvironment
	endpoints          map[int]InputEndpoint
	stopTimeoutCounter metrics.Int64Counter
}

func MakeInputDataSource(dataConnector config.DataConnectorConfig, environment RuntimeEnvironment) (*InputDataSource, error) {
	ds := &InputDataSource{
		id:          dataConnector.GetID(),
		environment: environment,
		endpoints:   make(map[int]InputEndpoint),
	}
	scope := environment.Metrics().Scope("datasource_connector", metrics.Labels{
		"connector": dataConnector.GetName(),
	})
	var err error
	if ds.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in data source connector", metrics.Labels{"event": "stop_timeout"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source connector %q: %w", dataConnector.GetName(), err)
	}
	return ds, nil
}

func (ds *InputDataSource) GetEnvironment() environment.ServiceEnvironment {
	return ds.environment
}

func (ds *InputDataSource) OnStopTimeout(ctx context.Context) {
	ds.environment.Log().Warn(ctx, "data source stopped by timeout", log.Str("name", ds.GetName()))
	ds.stopTimeoutCounter.Inc(ctx)
}

func (ds *InputDataSource) GetConfig() config.DataConnectorConfig {
	return ds.environment.RuntimeConfig().GetDataConnectorByID(ds.id)
}

func (ds *InputDataSource) GetName() string {
	return ds.GetConfig().GetName()
}

func (ds *InputDataSource) GetID() int {
	return ds.id
}

func (ds *InputDataSource) GetRuntimeEnvironment() RuntimeEnvironment {
	return ds.environment
}

func (ds *InputDataSource) GetEndpoint(id int) InputEndpoint {
	return ds.endpoints[id]
}

func (ds *InputDataSource) GetEndpoints() Collection[InputEndpoint] {
	return MakeCollection(maps.Values(ds.endpoints))
}

func (ds *InputDataSource) AddEndpoint(endpoint InputEndpoint) {
	ds.endpoints[endpoint.GetID()] = endpoint
}

type DataSourceEndpoint struct {
	id                        int
	environment               RuntimeEnvironment
	dataSource                DataSource
	endpointConsumers         []InputEndpointConsumer
	missingStreamIDCounter    metrics.Int64Counter
	lateResultCounter         metrics.Int64Counter
	unknownMessageIDCounter   metrics.Int64Counter
	duplicateMessageIDCounter metrics.Int64Counter
	invalidHTTPMethodCounter  metrics.Int64Counter
	beginRequestFailedCounter metrics.Int64Counter
	requestErrorsCounter      metrics.Int64Counter
	messagesTotal             metrics.Int64Counter
	requestDuration           metrics.Float64Histogram
	activeRequests            metrics.Int64Gauge
	pendingRequests           metrics.Int64Gauge
	pendingShards             [pendingRequestShardCount]pendingRequestShard
	pendingHashSeed           maphash.Seed
}

func MakeDataSourceEndpoint(dataSource DataSource, id int, environment RuntimeEnvironment) (*DataSourceEndpoint, error) {
	ep := &DataSourceEndpoint{
		dataSource:        dataSource,
		id:                id,
		environment:       environment,
		endpointConsumers: make([]InputEndpointConsumer, 0),
	}
	endpointName := environment.RuntimeConfig().GetEndpointConfigByID(id).GetName()
	connectorName := dataSource.GetName()

	labels := metrics.Labels{
		"connector": connectorName,
		"endpoint":  endpointName,
	}
	if protocol := dataConnectorProtocol(dataSource.GetConfig().GetType()); protocol != "" {
		labels["protocol"] = protocol
	}
	scope := environment.Metrics().Scope("datasource_endpoint", labels)
	var err error
	if ep.missingStreamIDCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "missing_stream_id"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.lateResultCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "late_result"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.unknownMessageIDCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "unknown_message_id"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.duplicateMessageIDCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "duplicate_message_id"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.invalidHTTPMethodCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "invalid_http_method"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.beginRequestFailedCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "begin_request_failed"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.requestErrorsCounter, err = scope.Counter("events_total", "Total number of events in data source endpoint", metrics.Labels{"event": "request_error"}); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.messagesTotal, err = scope.Counter("messages_total", "Total number of successfully processed messages in data source endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.requestDuration, err = scope.Histogram("request_duration_seconds", "Request duration in seconds for data source endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.activeRequests, err = scope.Gauge("active_requests", "Number of active requests in data source endpoint", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	if ep.pendingRequests, err = scope.Gauge("pending_requests", "Number of requests awaiting a pipeline result", nil); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	ep.pendingHashSeed = maphash.MakeSeed()
	for i := range ep.pendingShards {
		ep.pendingShards[i].started = make(map[string]time.Time)
	}
	if err = scope.ObservableFloat64Gauge(
		"pending_oldest_age_seconds",
		"Age in seconds of the oldest pending request awaiting a pipeline result",
		ep.oldestPendingAge,
	); err != nil {
		return nil, fmt.Errorf("failed to create metrics for source endpoint %q: %w", endpointName, err)
	}
	return ep, nil
}

func (ep *DataSourceEndpoint) GetConfig() config.EndpointConfig {
	return ep.environment.RuntimeConfig().GetEndpointConfigByID(ep.id)
}

func (ep *DataSourceEndpoint) GetName() string {
	return ep.GetConfig().GetName()
}

func (ep *DataSourceEndpoint) GetID() int {
	return ep.GetConfig().GetID()
}

func (ep *DataSourceEndpoint) GetRuntimeEnvironment() RuntimeEnvironment {
	return ep.environment
}

func (ep *DataSourceEndpoint) GetDataSource() DataSource {
	return ep.dataSource
}

func (ep *DataSourceEndpoint) GetDataConnector() DataConnector {
	return ep.dataSource
}

func (ep *DataSourceEndpoint) OnMissingStreamID(ctx context.Context) {
	ep.environment.Log().Error(ctx, "consumeResult called without streamID", log.Str("endpoint", ep.GetName()))
	ep.missingStreamIDCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) OnLateResult(ctx context.Context, sessionID string) {
	ep.environment.Log().Warn(ctx, "consumeResult: session not found in pending", log.Str("endpoint", ep.GetName()), log.Str("session_id", sessionID))
	ep.lateResultCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) OnUnknownMessageID(ctx context.Context, sessionID string, messageID string) {
	ep.environment.Log().Warn(ctx, "consumeResult: unknown message ID", log.Str("endpoint", ep.GetName()), log.Str("message_id", messageID), log.Str("session_id", sessionID))
	ep.unknownMessageIDCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) OnDuplicateMessageID(ctx context.Context, sessionID string, messageID string) {
	ep.environment.Log().Warn(ctx, "consumeResult: duplicate message ID", log.Str("endpoint", ep.GetName()), log.Str("message_id", messageID), log.Str("session_id", sessionID))
	ep.duplicateMessageIDCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) oldestPendingAge() float64 {
	var oldest time.Time
	for i := range ep.pendingShards {
		shard := &ep.pendingShards[i]
		shard.mu.RLock()
		for _, started := range shard.started {
			if oldest.IsZero() || started.Before(oldest) {
				oldest = started
			}
		}
		shard.mu.RUnlock()
	}
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest).Seconds()
}

func (ep *DataSourceEndpoint) OnPendingAdd(_ context.Context, streamID string) {
	ep.pendingRequests.Inc()
	shard := ep.pendingShard(streamID)
	shard.mu.Lock()
	shard.started[streamID] = time.Now()
	shard.mu.Unlock()
}

func (ep *DataSourceEndpoint) OnPendingRemove(_ context.Context, streamID string) {
	ep.pendingRequests.Dec()
	shard := ep.pendingShard(streamID)
	shard.mu.Lock()
	delete(shard.started, streamID)
	shard.mu.Unlock()
}

func (ep *DataSourceEndpoint) pendingShard(streamID string) *pendingRequestShard {
	index := maphash.String(ep.pendingHashSeed, streamID) % pendingRequestShardCount
	return &ep.pendingShards[index]
}

func (ep *DataSourceEndpoint) OnInvalidHTTPMethod(ctx context.Context, method string) {
	path := ""
	if cfg, ok := ep.GetConfig().(*config.HttpEndpointConfig); ok {
		path = cfg.Path
	}
	ep.environment.Log().Warn(ctx, "invalid HTTP method", log.Str("method", method), log.Str("endpoint", ep.GetName()), log.Str("path", path))
	ep.invalidHTTPMethodCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) OnBeginRequestFailed(ctx context.Context, err error) {
	ep.environment.Log().Error(ctx, "BeginRequest failed", log.Str("endpoint", ep.GetName()), log.Err(err))
	ep.beginRequestFailedCounter.Inc(ctx)
}

func (ep *DataSourceEndpoint) OnRequestStart(_ context.Context) time.Time {
	ep.activeRequests.Inc()
	return time.Now()
}

func (ep *DataSourceEndpoint) OnRequestEnd(ctx context.Context, startTime time.Time, err error) {
	ep.activeRequests.Dec()
	ep.requestDuration.Observe(ctx, time.Since(startTime).Seconds())
	if err == nil {
		ep.messagesTotal.Inc(ctx)
	} else {
		ep.requestErrorsCounter.Inc(ctx)
	}
}

type DataSourceEndpointConsumer[T, R, E any] struct {
	inputStream TypedInputStream[T, R, E]
	endpoint    InputEndpoint
}

func (ec *DataSourceEndpointConsumer[T, R, E]) Consume(ctx context.Context, value T) {
	ec.inputStream.Consume(ctx, value)
}

func (ec *DataSourceEndpointConsumer[T, R, E]) Endpoint() InputEndpoint {
	return ec.endpoint
}

func (ec *DataSourceEndpointConsumer[T, R, E]) Stream() TypedInputStream[T, R, E] {
	return ec.inputStream
}

func MakeDataSourceEndpointConsumer[T, R, E any](endpoint InputEndpoint, inputStream TypedInputStream[T, R, E]) *DataSourceEndpointConsumer[T, R, E] {
	ec := &DataSourceEndpointConsumer[T, R, E]{
		inputStream: inputStream,
		endpoint:    endpoint,
	}
	return ec
}
