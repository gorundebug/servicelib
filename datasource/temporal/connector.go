/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

// Package temporal implements the official Temporal SDK boundary used by
// symmetric Temporal Sink/Source endpoints. Business nodes never import it.
package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"go.opentelemetry.io/otel"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdkotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

const endpointWorkflowType = "servicelib.temporal-endpoint.v1"

var scheduleWorkflowIDSuffix = regexp.MustCompile(`-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)$`)

const (
	durableMemoManagedBy = "servicelib.managedBy"
	durableMemoOwner     = "servicelib.owner"
	durableMemoCallID    = "servicelib.callId"
)

func temporalOpaqueIdentityComponent(value string) string {
	var result strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '.' || b == '_' || b == '~' {
			result.WriteByte(b)
		} else {
			fmt.Fprintf(&result, "%%%02X", b)
		}
	}
	return result.String()
}

// temporalIdentityName is intentionally identical to servicegen.ToSnakeCase.
func temporalIdentityName(value string) string {
	var words []string
	var current []rune
	runes := []rune(value)
	for i, ch := range runes {
		if unicode.IsSpace(ch) || ch == '_' || ch == '-' || ch == '/' || ch == '.' {
			if len(current) > 0 {
				words = append(words, string(current))
				current = current[:0]
			}
			continue
		}
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) {
			continue
		}
		if len(current) > 0 && unicode.IsUpper(ch) {
			previous := current[len(current)-1]
			if !unicode.IsUpper(previous) || (i+1 < len(runes) && unicode.IsLower(runes[i+1])) {
				words = append(words, string(current))
				current = current[:0]
			}
		}
		current = append(current, ch)
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	for i := range words {
		words[i] = strings.ToLower(words[i])
	}
	return strings.Join(words, "_")
}

func temporalEndpointActivityType(connectorName, endpointName string) string {
	return fmt.Sprintf("%s.endpoint.%s.v1",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

func temporalDirectWorkflowType(connectorName, endpointName string) string {
	return fmt.Sprintf("%s.endpoint.%s.workflow.v1",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

func temporalEndpointWorkflowID(connectorName, endpointName, messageID string) string {
	return fmt.Sprintf("%s/endpoint/%s/%s",
		temporalIdentityName(connectorName),
		temporalIdentityName(endpointName),
		temporalOpaqueIdentityComponent(messageID),
	)
}

func temporalEndpointOwner(connectorName, endpointName string) string {
	return fmt.Sprintf("%s/endpoint/%s/v1",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

func temporalScheduleWorkflowID(connectorName, endpointName string) string {
	return fmt.Sprintf("%s/schedule/%s",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

// EndpointEnvelope is the transport envelope used by a symmetric Temporal
// endpoint. Payload is the endpoint's declared input type. ScheduledAt and
// FiredAt are populated only for an execution started by a Temporal Schedule.
type EndpointEnvelope struct {
	Version          int    `json:"version"`
	EndpointID       int    `json:"endpointId"`
	MessageID        string `json:"messageId"`
	StreamID         string `json:"streamId"`
	Priority         int    `json:"priority"`
	DeadlineUnixNano int64  `json:"deadlineUnixNano,omitempty"`
	Scheduled        bool   `json:"scheduled,omitempty"`
	ScheduleID       string `json:"scheduleId,omitempty"`
	ScheduledAtNano  int64  `json:"scheduledAtUnixNano,omitempty"`
	FiredAtNano      int64  `json:"firedAtUnixNano,omitempty"`
	Payload          []byte `json:"payload,omitempty"`
}

// EndpointResult carries the endpoint's declared result type back through the
// Workflow. Transport failures are returned as Activity errors instead.
type EndpointResult struct {
	Payload []byte `json:"payload,omitempty"`
}

type WorkflowGraphHandler func(context.Context, EndpointEnvelope) (EndpointResult, error)
type WorkflowInputEncoder func(any) ([]byte, error)
type connectorEndpointHandler = WorkflowGraphHandler
type connectorEndpointEncoder = WorkflowInputEncoder

type endpointWorkflowRequest struct {
	ActivityType               string           `json:"activityType"`
	ActivityStartToCloseMillis int              `json:"activityStartToCloseMillis"`
	ActivityHeartbeatMillis    int              `json:"activityHeartbeatMillis,omitempty"`
	MaximumAttempts            int32            `json:"maximumAttempts"`
	Priority                   int              `json:"priority"`
	Envelope                   EndpointEnvelope `json:"envelope"`
}

type WorkflowEndpointConfig struct {
	ID                         int                       `json:"id"`
	Name                       string                    `json:"name"`
	TaskQueue                  string                    `json:"taskQueue"`
	ExecutionType              api.TemporalExecutionType `json:"executionType"`
	ActivityType               string                    `json:"activityType"`
	WorkflowType               string                    `json:"workflowType"`
	WorkflowExecutionMillis    int                       `json:"workflowExecutionMillis"`
	ActivityStartToCloseMillis int                       `json:"activityStartToCloseMillis"`
	ActivityHeartbeatMillis    int                       `json:"activityHeartbeatMillis"`
	MaximumAttempts            int                       `json:"maximumAttempts"`
}

type DirectEndpointWorkflowRequest struct {
	ConnectorName string                   `json:"connectorName"`
	Envelope      EndpointEnvelope         `json:"envelope"`
	Endpoints     []WorkflowEndpointConfig `json:"endpoints"`
	RuntimeConfig []byte                   `json:"runtimeConfig"`
}

type workflowEndpointConfig = WorkflowEndpointConfig
type directEndpointWorkflowRequest = DirectEndpointWorkflowRequest

type WorkflowEndpointHandler func(workflow.Context, DirectEndpointWorkflowRequest) (EndpointResult, error)

type workflowSubmissionContext struct {
	workflowCtx   workflow.Context
	connector     string
	endpoints     map[int]workflowEndpointConfig
	runtimeConfig []byte
}

type workflowSubmissionContextKey struct{}

type endpointRegistration struct {
	id           int
	activityType string
	workflowType string
	handler      connectorEndpointHandler
	encodeInput  connectorEndpointEncoder
	workflow     WorkflowEndpointHandler
}

// Connector owns exactly one Temporal client and the Workers registered for
// one configured Temporal DataConnector.
type Connector struct {
	id          int
	name        string
	environment runtime.RuntimeEnvironment

	mu                    sync.Mutex
	client                client.Client
	workers               []worker.Worker
	endpointRegistrations map[int]endpointRegistration
	durableEvents         metrics.Int64CounterVec
	started               bool
}

// MakeConnector creates and registers one durable transport. Registration is
// explicit so a project with no Temporal connector starts no SDK runtime.
func MakeConnector(connectorID int, environment runtime.RuntimeEnvironment) (*Connector, error) {
	configured := environment.RuntimeConfig().GetDataConnectorByID(connectorID)
	cfg, ok := configured.(*config.TemporalDataConnectorConfig)
	if !ok || cfg.Implementation != api.DataConnectorImplementationTemporalGo {
		return nil, fmt.Errorf("data connector id=%d is not a temporal/go connector", connectorID)
	}
	if existing := environment.GetManagedDataConnector(connectorID); existing != nil {
		connector, ok := existing.(*Connector)
		if !ok {
			return nil, fmt.Errorf("managed data connector id=%d is not a Go Temporal connector", connectorID)
		}
		return connector, nil
	}
	durableEvents, err := environment.Metrics().Scope(
		"temporal_activity",
		metrics.Labels{"connector": cfg.Name},
	).CounterVec("events_total", "Total number of Temporal Activity lifecycle events")
	if err != nil {
		return nil, fmt.Errorf("create Activity metrics for Temporal connector %q: %w", cfg.Name, err)
	}
	connector := &Connector{
		id: connectorID, name: cfg.Name, environment: environment,
		endpointRegistrations: make(map[int]endpointRegistration),
		durableEvents:         durableEvents,
	}
	environment.AddManagedDataConnector(connector)
	return connector, nil
}

func (c *Connector) GetID() int      { return c.id }
func (c *Connector) GetName() string { return c.name }

func (c *Connector) activityDiagnostics(boundary, target string) runtime.DurableCallDiagnostics {
	return func(ctx context.Context, event runtime.DurableCallEvent, err error) {
		if c.durableEvents != nil {
			c.durableEvents.With(metrics.Labels{
				"connector": c.name,
				"boundary":  boundary,
				"target":    target,
				"event":     string(event),
			}).Inc(ctx)
		}
		if err == nil {
			return
		}
		fields := []log.Field{
			log.Str("connector", c.name),
			log.Str("boundary", boundary),
			log.Str("target", target),
			log.Str("event", string(event)),
			log.Err(err),
		}
		if event == runtime.DurableCallEventLateHeartbeat {
			c.environment.Log().Warn(ctx, "Temporal Activity lifecycle misuse", fields...)
			return
		}
		c.environment.Log().Error(ctx, "Temporal Activity failed", fields...)
	}
}

func (c *Connector) temporalConfig() (*config.TemporalDataConnectorConfig, error) {
	configured := c.environment.RuntimeConfig().GetDataConnectorByID(c.id)
	cfg, ok := configured.(*config.TemporalDataConnectorConfig)
	if !ok {
		return nil, fmt.Errorf("data connector %q is not Temporal", c.name)
	}
	return cfg, nil
}

// RegisterEndpoint binds one configured endpoint Activity to its existing
// input graph adapter. The Activity is infrastructure; handler invokes the
// ordinary endpoint consumer and never replaces a business node.
func (c *Connector) RegisterEndpoint(
	endpointID int,
	handler connectorEndpointHandler,
	encodeInput connectorEndpointEncoder,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return fmt.Errorf("cannot register Temporal endpoint %d after connector start", endpointID)
	}
	if handler == nil {
		return fmt.Errorf("Temporal endpoint %d handler is nil", endpointID)
	}
	if encodeInput == nil {
		return fmt.Errorf("Temporal endpoint %d input encoder is nil", endpointID)
	}
	if _, exists := c.endpointRegistrations[endpointID]; exists {
		return fmt.Errorf("Temporal endpoint %d is already registered", endpointID)
	}
	cfg, err := c.endpointConfig(endpointID)
	if err != nil {
		return err
	}
	c.endpointRegistrations[endpointID] = endpointRegistration{
		id:           endpointID,
		activityType: temporalEndpointActivityType(c.name, cfg.Name),
		workflowType: temporalDirectWorkflowType(c.name, cfg.Name),
		handler:      handler,
		encodeInput:  encodeInput,
	}
	return nil
}

// RegisterWorkflowEndpoint binds the statically generated Workflow function
// for one Workflow source. The function is registered directly with the SDK;
// it must not capture the process-owned service graph.
func (c *Connector) RegisterWorkflowEndpoint(
	endpointID int,
	handler WorkflowEndpointHandler,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return fmt.Errorf("cannot register Temporal endpoint %d after connector start", endpointID)
	}
	if handler == nil {
		return fmt.Errorf("Temporal Workflow endpoint %d handler is nil", endpointID)
	}
	registration, ok := c.endpointRegistrations[endpointID]
	if !ok {
		return fmt.Errorf("Temporal endpoint %d must be registered before its Workflow", endpointID)
	}
	if registration.workflow != nil {
		return fmt.Errorf("Temporal Workflow endpoint %d is already registered", endpointID)
	}
	registration.workflow = handler
	c.endpointRegistrations[endpointID] = registration
	return nil
}

func executeEndpointWorkflow(
	workflowCtx workflow.Context,
	request directEndpointWorkflowRequest,
	registration endpointRegistration,
	propagator temporalContextPropagator,
) (EndpointResult, error) {
	return executeWorkflowEndpoint(
		workflowCtx, request, registration.id, registration.workflowType,
		registration.handler, registration.encodeInput, propagator,
	)
}

// ExecuteWorkflowEndpoint runs one generated Workflow graph in the current
// Workflow isolate and owns its durable context and Continue-As-New boundary.
func ExecuteWorkflowEndpoint(
	workflowCtx workflow.Context,
	request DirectEndpointWorkflowRequest,
	endpointID int,
	workflowType string,
	handler WorkflowGraphHandler,
	encodeInput WorkflowInputEncoder,
) (EndpointResult, error) {
	return executeWorkflowEndpoint(
		workflowCtx, request, endpointID, workflowType,
		handler, encodeInput, temporalContextPropagator{},
	)
}

func executeWorkflowEndpoint(
	workflowCtx workflow.Context,
	request DirectEndpointWorkflowRequest,
	endpointID int,
	workflowType string,
	handler WorkflowGraphHandler,
	encodeInput WorkflowInputEncoder,
	propagator temporalContextPropagator,
) (EndpointResult, error) {
	envelope := request.Envelope
	if envelope.Version != 1 || envelope.EndpointID != endpointID {
		return EndpointResult{}, fmt.Errorf("invalid endpoint envelope for Temporal endpoint %d", endpointID)
	}
	if envelope.Scheduled {
		info := workflow.GetInfo(workflowCtx)
		envelope.MessageID = info.WorkflowExecution.ID
		envelope.StreamID = info.WorkflowExecution.ID
		envelope.ScheduledAtNano = scheduledTimeFromWorkflowID(
			info.WorkflowExecution.ID, info.WorkflowStartTime,
		).UnixNano()
		envelope.FiredAtNano = workflow.Now(workflowCtx).UTC().UnixNano()
	}
	if envelope.MessageID == "" || envelope.StreamID == "" {
		return EndpointResult{}, fmt.Errorf("invalid endpoint identity for Temporal endpoint %d", endpointID)
	}
	carrier, _ := workflowCtx.Value(temporalCarrierContextKey{}).(map[string]string)
	workflowCarrier := make(map[string]string, len(carrier))
	for _, key := range temporalCarrierKeys {
		if key == temporalHeaderDeadlineUnixNano {
			continue
		}
		if value := carrier[key]; value != "" {
			workflowCarrier[key] = value
		}
	}
	ctx := propagator.extractWorkflowContext(context.Background(), workflowCarrier)
	endpointConfigs := make(map[int]workflowEndpointConfig, len(request.Endpoints))
	for _, endpoint := range request.Endpoints {
		endpointConfigs[endpoint.ID] = endpoint
	}
	ctx = context.WithValue(ctx, workflowSubmissionContextKey{}, workflowSubmissionContext{
		workflowCtx:   workflowCtx,
		connector:     request.ConnectorName,
		endpoints:     endpointConfigs,
		runtimeConfig: request.RuntimeConfig,
	})
	durable := runtime.NewDurableWorkflowContext(
		envelope.MessageID,
		func(duration time.Duration) error { return workflow.Sleep(workflowCtx, duration) },
		func() bool { return workflow.IsReplaying(workflowCtx) },
	)
	var result EndpointResult
	err := runtime.RunDurableWorkflow(ctx, durable, func(ctx context.Context) error {
		var invokeErr error
		result, invokeErr = handler(ctx, envelope)
		return invokeErr
	})
	var continuation *runtime.TemporalContinueAsNewRequest
	if errors.As(err, &continuation) {
		payload, encodeErr := encodeInput(continuation.NextInput)
		if encodeErr != nil {
			return EndpointResult{}, encodeErr
		}
		nextEnvelope := envelope
		nextEnvelope.Scheduled = false
		nextEnvelope.ScheduleID = ""
		nextEnvelope.ScheduledAtNano = 0
		nextEnvelope.FiredAtNano = 0
		nextEnvelope.Payload = payload
		return EndpointResult{}, workflow.NewContinueAsNewError(
			workflowCtx, workflowType, DirectEndpointWorkflowRequest{
				ConnectorName: request.ConnectorName,
				Envelope:      nextEnvelope,
				Endpoints:     request.Endpoints,
				RuntimeConfig: request.RuntimeConfig,
			},
		)
	}
	return result, err
}

func (c *Connector) endpointConfig(endpointID int) (*config.TemporalEndpointConfig, error) {
	configured := c.environment.RuntimeConfig().GetEndpointConfigByID(endpointID)
	cfg, ok := configured.(*config.TemporalEndpointConfig)
	if !ok || cfg.IdDataConnector != c.id {
		return nil, fmt.Errorf("endpoint id=%d does not belong to Temporal connector %q", endpointID, c.name)
	}
	return cfg, nil
}

func (c *Connector) workflowEndpointSnapshot() ([]workflowEndpointConfig, error) {
	ids := make([]int, 0, len(c.endpointRegistrations))
	for id := range c.endpointRegistrations {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	result := make([]workflowEndpointConfig, 0, len(ids))
	for _, id := range ids {
		registration := c.endpointRegistrations[id]
		cfg, err := c.endpointConfig(id)
		if err != nil {
			return nil, err
		}
		result = append(result, workflowEndpointConfig{
			ID: id, Name: cfg.Name, TaskQueue: cfg.TaskQueue,
			ExecutionType: cfg.TemporalExecutionType,
			ActivityType:  registration.activityType, WorkflowType: registration.workflowType,
			WorkflowExecutionMillis:    cfg.WorkflowExecutionTimeout,
			ActivityStartToCloseMillis: cfg.ActivityStartToCloseTimeout,
			ActivityHeartbeatMillis:    cfg.ActivityHeartbeatTimeout,
			MaximumAttempts:            cfg.MaximumAttempts,
		})
	}
	return result, nil
}

func (c *Connector) runtimeConfigSnapshot() ([]byte, error) {
	snapshot, err := json.Marshal(c.environment.RuntimeConfig().GetConfig())
	if err != nil {
		return nil, fmt.Errorf("serialize Temporal Workflow runtime config: %w", err)
	}
	return snapshot, nil
}

// executeEndpointActivity owns the processing-side Temporal Activity scope for
// both scheduled and on-demand endpoints. Keeping this boundary independent of
// graph callers makes its cancellation, heartbeat and result semantics directly
// testable at the endpoint boundary.
func executeEndpointActivity(
	activityCtx context.Context,
	envelope EndpointEnvelope,
	registration endpointRegistration,
	heartbeat runtime.DurableCallHeartbeatRecorder,
	diagnostics runtime.DurableCallDiagnostics,
) (EndpointResult, error) {
	if envelope.Version != 1 || envelope.EndpointID != registration.id || envelope.MessageID == "" {
		return EndpointResult{}, fmt.Errorf("invalid endpoint envelope for Temporal endpoint %d", registration.id)
	}
	envelope.FiredAtNano = time.Now().UTC().UnixNano()
	durable := runtime.NewDurableCallContext(
		envelope.MessageID, heartbeat, diagnostics,
	)
	var result EndpointResult
	err := runtime.RunDurableActivity(
		activityCtx, durable, func(ctx context.Context) error {
			var invokeErr error
			result, invokeErr = registration.handler(ctx, envelope)
			return invokeErr
		},
	)
	return result, err
}

func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	cfg, err := c.temporalConfig()
	if err != nil {
		return err
	}
	clientOptions, err := c.makeClientOptions(cfg)
	if err != nil {
		return err
	}
	temporalClient, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("connect Temporal data connector %q: %w", c.name, err)
	}
	type queueWorker struct {
		worker             worker.Worker
		endpointRegistered bool
	}
	workersByQueue := make(map[string]*queueWorker)
	getWorker := func(taskQueue string) *queueWorker {
		registered := workersByQueue[taskQueue]
		if registered == nil {
			registered = &queueWorker{worker: worker.New(temporalClient, taskQueue, worker.Options{
				MaxConcurrentActivityExecutionSize:     cfg.MaxConcurrentActivities,
				MaxConcurrentWorkflowTaskExecutionSize: cfg.MaxConcurrentWorkflows,
			})}
			workersByQueue[taskQueue] = registered
		}
		return registered
	}
	for _, registration := range c.endpointRegistrations {
		cfg, err := c.endpointConfig(registration.id)
		if err != nil {
			temporalClient.Close()
			return err
		}
		if !cfg.Enabled {
			continue
		}
		registered := getWorker(cfg.TaskQueue)
		registration := registration
		switch cfg.TemporalExecutionType {
		case api.Activity:
			if !registered.endpointRegistered {
				registered.worker.RegisterWorkflowWithOptions(temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType})
				registered.endpointRegistered = true
			}
			registered.worker.RegisterActivityWithOptions(
				func(activityCtx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
					return executeEndpointActivity(
						activityCtx, envelope, registration,
						func(ctx context.Context, details any) error {
							activity.RecordHeartbeat(ctx, details)
							return nil
						},
						c.activityDiagnostics("endpoint", fmt.Sprintf("%d", registration.id)),
					)
				},
				activity.RegisterOptions{Name: registration.activityType},
			)
		case api.Workflow:
			if registration.workflow == nil {
				temporalClient.Close()
				return fmt.Errorf("Temporal Workflow endpoint %q has no generated Workflow handler", cfg.Name)
			}
			registered.worker.RegisterWorkflowWithOptions(
				registration.workflow,
				workflow.RegisterOptions{Name: registration.workflowType},
			)
		default:
			temporalClient.Close()
			return fmt.Errorf("Temporal endpoint %q has unsupported execution type %q", cfg.Name, cfg.TemporalExecutionType)
		}
	}
	startedWorkers := make([]worker.Worker, 0, len(workersByQueue))
	for _, registered := range workersByQueue {
		if err := registered.worker.Start(); err != nil {
			for _, started := range startedWorkers {
				started.Stop()
			}
			temporalClient.Close()
			return fmt.Errorf("start Temporal worker for connector %q: %w", c.name, err)
		}
		startedWorkers = append(startedWorkers, registered.worker)
	}
	for _, registration := range c.endpointRegistrations {
		cfg, err := c.endpointConfig(registration.id)
		if err != nil {
			for _, started := range startedWorkers {
				started.Stop()
			}
			temporalClient.Close()
			return err
		}
		if cfg.Enabled && cfg.Schedule != "" {
			if err := c.ensureSchedule(ctx, temporalClient, registration, cfg); err != nil {
				for _, started := range startedWorkers {
					started.Stop()
				}
				temporalClient.Close()
				return err
			}
		}
	}
	c.client = temporalClient
	c.workers = startedWorkers
	c.started = true
	return nil
}

func (c *Connector) makeClientOptions(cfg *config.TemporalDataConnectorConfig) (client.Options, error) {
	tracingInterceptor, err := sdkotel.NewTracingInterceptor(sdkotel.TracerOptions{
		TextMapPropagator: otel.GetTextMapPropagator(),
	})
	if err != nil {
		return client.Options{}, fmt.Errorf("create Temporal tracing interceptor for connector %q: %w", c.name, err)
	}
	options := client.Options{
		HostPort: cfg.Address, Namespace: cfg.Namespace, Identity: cfg.Identity,
		Interceptors: []interceptor.ClientInterceptor{tracingInterceptor},
		ContextPropagators: []workflow.ContextPropagator{
			temporalContextPropagator{tracing: c.environment.Tracing()},
		},
	}
	metricsHandler, err := sdkMetricsHandler(c.environment)
	if err != nil {
		return options, err
	}
	options.MetricsHandler = metricsHandler
	tlsConfigured := cfg.TLSEnabled || cfg.TLSServerName != "" || cfg.TLSCAFile != "" || cfg.TLSCertFile != "" || cfg.TLSKeyFile != ""
	if tlsConfigured {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}
		if cfg.TLSCAFile != "" {
			pem, err := os.ReadFile(cfg.TLSCAFile)
			if err != nil {
				return options, fmt.Errorf("read Temporal CA file for connector %q: %w", c.name, err)
			}
			roots, err := x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(pem) {
				return options, fmt.Errorf("Temporal CA file for connector %q contains no certificates", c.name)
			}
			tlsConfig.RootCAs = roots
		}
		if cfg.TLSCertFile != "" {
			certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				return options, fmt.Errorf("load Temporal mTLS key pair for connector %q: %w", c.name, err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		options.ConnectionOptions.TLS = tlsConfig
	}
	if cfg.APIKey != "" {
		// The callback reads the current immutable snapshot so a secret-only
		// configuration reload does not require caching credentials in a graph
		// object. Enabling API-key auth from an initially empty value still
		// requires reconnecting the client.
		options.Credentials = client.NewAPIKeyDynamicCredentials(func(context.Context) (string, error) {
			current, err := c.temporalConfig()
			if err != nil {
				return "", err
			}
			if current.APIKey == "" {
				return "", fmt.Errorf("Temporal API key for connector %q is empty", c.name)
			}
			return current.APIKey, nil
		})
	}
	return options, nil
}

func (c *Connector) Stop(context.Context) {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	workers, temporalClient := c.workers, c.client
	c.workers, c.client, c.started = nil, nil, false
	c.mu.Unlock()
	for _, w := range workers {
		w.Stop()
	}
	if temporalClient != nil {
		temporalClient.Close()
	}
}

// StopAdmission stops Task Queue polling but deliberately leaves the client
// open so already admitted graph work can finish outbound durable submissions.
func (c *Connector) StopAdmission(context.Context) {
	c.mu.Lock()
	workers := c.workers
	c.workers = nil
	c.mu.Unlock()
	for _, w := range workers {
		w.Stop()
	}
}

func (c *Connector) ensureSchedule(
	ctx context.Context,
	temporalClient client.Client,
	registration endpointRegistration,
	cfg *config.TemporalEndpointConfig,
) error {
	owner := temporalEndpointOwner(c.name, cfg.Name)
	envelope := EndpointEnvelope{
		Version: 1, EndpointID: cfg.ID, Scheduled: true, ScheduleID: cfg.ScheduleID,
	}
	request := endpointWorkflowRequest{
		ActivityType:               registration.activityType,
		ActivityStartToCloseMillis: cfg.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    cfg.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(cfg.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(0),
		Envelope:                   envelope,
	}
	workflowType := endpointWorkflowType
	var args []interface{} = []interface{}{request}
	if cfg.TemporalExecutionType == api.Workflow {
		endpoints, err := c.workflowEndpointSnapshot()
		if err != nil {
			return err
		}
		runtimeSnapshot, err := c.runtimeConfigSnapshot()
		if err != nil {
			return err
		}
		workflowType = registration.workflowType
		args = []interface{}{directEndpointWorkflowRequest{
			ConnectorName: c.name,
			Envelope:      envelope,
			Endpoints:     endpoints,
			RuntimeConfig: runtimeSnapshot,
		}}
	}
	overlap := enums.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	if cfg.OverlapPolicy == api.ScheduleOverlapPolicySkip {
		overlap = enums.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	catchupWindow := 10 * time.Second
	if cfg.MissedRunPolicy == api.ScheduleMissedRunPolicyFireOnce {
		// Temporal evaluates all missed instants inside the window, while SKIP
		// coalesces them against the first running execution. This retains one
		// catch-up execution for the portable Skip-overlap policy.
		catchupWindow = 365 * 24 * time.Hour
	}
	action := &client.ScheduleWorkflowAction{
		ID:        temporalScheduleWorkflowID(c.name, cfg.Name),
		Workflow:  workflowType,
		Args:      args,
		TaskQueue: cfg.TaskQueue,
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicelib",
			durableMemoOwner:     owner,
			durableMemoCallID:    cfg.ScheduleID,
		},
		Priority: sdktemporal.Priority{PriorityKey: request.Priority},
	}
	if cfg.WorkflowExecutionTimeout > 0 {
		action.WorkflowExecutionTimeout = time.Duration(cfg.WorkflowExecutionTimeout) * time.Millisecond
	}
	_, err := temporalClient.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: cfg.ScheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{temporalCronExpression(cfg.Schedule)},
			TimeZoneName:    cfg.Timezone,
		},
		Action:        action,
		Overlap:       overlap,
		CatchupWindow: catchupWindow,
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicelib",
			durableMemoOwner:     owner,
			durableMemoCallID:    cfg.ScheduleID,
		},
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, sdktemporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("create Temporal schedule %q: %w", cfg.ScheduleID, err)
	}
	description, err := temporalClient.ScheduleClient().GetHandle(ctx, cfg.ScheduleID).Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe existing Temporal schedule %q: %w", cfg.ScheduleID, err)
	}
	if err := validateMemoOwnership(description.Memo, owner, cfg.ScheduleID); err != nil {
		return fmt.Errorf("Temporal schedule %q ownership collision: %w", cfg.ScheduleID, err)
	}
	existingAction, ok := description.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || existingAction.Workflow != workflowType || existingAction.TaskQueue != cfg.TaskQueue {
		return fmt.Errorf(
			"Temporal schedule %q ownership collision: action workflow=%v taskQueue=%q",
			cfg.ScheduleID, existingActionWorkflow(existingAction), existingActionTaskQueue(existingAction),
		)
	}
	return nil
}

func temporalCronExpression(expression string) string {
	// The DSL exposes the portable five-field cron contract (minute through
	// day-of-week). Temporal accepts a leading seconds field, so make zero
	// seconds explicit instead of letting it reinterpret the minute field.
	return "0 " + strings.Join(strings.Fields(expression), " ")
}

func existingActionWorkflow(action *client.ScheduleWorkflowAction) interface{} {
	if action == nil {
		return nil
	}
	return action.Workflow
}

func existingActionTaskQueue(action *client.ScheduleWorkflowAction) string {
	if action == nil {
		return ""
	}
	return action.TaskQueue
}

func workflowEndpointConfigs(endpoints map[int]workflowEndpointConfig) []workflowEndpointConfig {
	ids := make([]int, 0, len(endpoints))
	for id := range endpoints {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	result := make([]workflowEndpointConfig, 0, len(ids))
	for _, id := range ids {
		result = append(result, endpoints[id])
	}
	return result
}

func submitEndpointFromWorkflow(
	state workflowSubmissionContext,
	endpointID int,
	envelope EndpointEnvelope,
) (EndpointResult, error) {
	cfg, ok := state.endpoints[endpointID]
	if !ok {
		return EndpointResult{}, fmt.Errorf(
			"Temporal endpoint id=%d is absent from Workflow configuration snapshot", endpointID,
		)
	}
	if state.connector == "" {
		return EndpointResult{}, fmt.Errorf("Temporal Workflow connector identity is empty")
	}
	if envelope.MessageID == "" || envelope.StreamID == "" {
		return EndpointResult{}, fmt.Errorf(
			"Temporal Workflow submission to %q requires stable message and stream identity", cfg.Name,
		)
	}
	envelope.Version = 1
	envelope.EndpointID = endpointID
	priority := sdktemporal.Priority{PriorityKey: runtime.NormalizeTemporalPriority(envelope.Priority)}
	retryPolicy := &sdktemporal.RetryPolicy{MaximumAttempts: int32(cfg.MaximumAttempts)}

	var result EndpointResult
	switch cfg.ExecutionType {
	case api.Activity:
		activityCtx := workflow.WithActivityOptions(state.workflowCtx, workflow.ActivityOptions{
			TaskQueue:           cfg.TaskQueue,
			StartToCloseTimeout: time.Duration(cfg.ActivityStartToCloseMillis) * time.Millisecond,
			HeartbeatTimeout:    time.Duration(cfg.ActivityHeartbeatMillis) * time.Millisecond,
			RetryPolicy:         retryPolicy,
			Priority:            priority,
		})
		if err := workflow.ExecuteActivity(activityCtx, cfg.ActivityType, envelope).Get(activityCtx, &result); err != nil {
			return EndpointResult{}, fmt.Errorf("execute Temporal endpoint Activity %q: %w", cfg.Name, err)
		}
	case api.Workflow:
		childCtx := workflow.WithChildOptions(state.workflowCtx, workflow.ChildWorkflowOptions{
			WorkflowID:               temporalEndpointWorkflowID(state.connector, cfg.Name, envelope.MessageID),
			TaskQueue:                cfg.TaskQueue,
			WorkflowExecutionTimeout: time.Duration(cfg.WorkflowExecutionMillis) * time.Millisecond,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			RetryPolicy:              retryPolicy,
			Priority:                 priority,
		})
		request := directEndpointWorkflowRequest{
			ConnectorName: state.connector,
			Envelope:      envelope,
			Endpoints:     workflowEndpointConfigs(state.endpoints),
			RuntimeConfig: state.runtimeConfig,
		}
		if err := workflow.ExecuteChildWorkflow(childCtx, cfg.WorkflowType, request).Get(childCtx, &result); err != nil {
			return EndpointResult{}, fmt.Errorf("execute child Temporal endpoint Workflow %q: %w", cfg.Name, err)
		}
	default:
		return EndpointResult{}, fmt.Errorf(
			"Temporal endpoint %q has unsupported execution type %q", cfg.Name, cfg.ExecutionType,
		)
	}
	return result, nil
}

// SubmitEndpoint starts one durable endpoint execution. When waitForResult is
// false it returns after Temporal accepts the Workflow. When true it waits for
// the existing input graph's result boundary and returns its serialized value.
func (c *Connector) SubmitEndpoint(
	ctx context.Context,
	endpointID int,
	envelope EndpointEnvelope,
	waitForResult bool,
) (EndpointResult, error) {
	if state, ok := ctx.Value(workflowSubmissionContextKey{}).(workflowSubmissionContext); ok {
		return submitEndpointFromWorkflow(state, endpointID, envelope)
	}
	c.mu.Lock()
	temporalClient := c.client
	started := c.started
	c.mu.Unlock()
	configured := c.environment.RuntimeConfig().GetEndpointConfigByID(endpointID)
	cfg, ok := configured.(*config.TemporalEndpointConfig)
	if !ok || cfg.IdDataConnector != c.id {
		return EndpointResult{}, fmt.Errorf("endpoint id=%d does not belong to Temporal connector %q", endpointID, c.name)
	}
	if !cfg.Enabled {
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %q is disabled", cfg.Name)
	}
	if !started || temporalClient == nil {
		return EndpointResult{}, fmt.Errorf("Temporal connector %q is not started", c.name)
	}
	if envelope.MessageID == "" {
		envelope.MessageID = runtime.NewStreamID()
	}
	if envelope.StreamID == "" {
		envelope.StreamID = envelope.MessageID
	}
	envelope.Version = 1
	envelope.EndpointID = endpointID
	request := endpointWorkflowRequest{
		ActivityType:               temporalEndpointActivityType(c.name, cfg.Name),
		ActivityStartToCloseMillis: cfg.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    cfg.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(cfg.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(envelope.Priority),
		Envelope:                   envelope,
	}
	registration, registered := c.endpointRegistrations[endpointID]
	if !registered {
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %q is not registered", cfg.Name)
	}
	workflowType := endpointWorkflowType
	var workflowInput interface{} = request
	if cfg.TemporalExecutionType == api.Workflow {
		endpoints, snapshotErr := c.workflowEndpointSnapshot()
		if snapshotErr != nil {
			return EndpointResult{}, snapshotErr
		}
		runtimeSnapshot, snapshotErr := c.runtimeConfigSnapshot()
		if snapshotErr != nil {
			return EndpointResult{}, snapshotErr
		}
		workflowType = registration.workflowType
		workflowInput = directEndpointWorkflowRequest{
			ConnectorName: c.name,
			Envelope:      envelope,
			Endpoints:     endpoints,
			RuntimeConfig: runtimeSnapshot,
		}
	}
	workflowID := temporalEndpointWorkflowID(c.name, cfg.Name, envelope.MessageID)
	owner := temporalEndpointOwner(c.name, cfg.Name)
	options := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                cfg.TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Priority:                 sdktemporal.Priority{PriorityKey: request.Priority},
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicelib",
			durableMemoOwner:     owner,
			durableMemoCallID:    envelope.MessageID,
		},
	}
	if cfg.WorkflowExecutionTimeout > 0 {
		options.WorkflowExecutionTimeout = time.Duration(cfg.WorkflowExecutionTimeout) * time.Millisecond
	}
	run, err := temporalClient.ExecuteWorkflow(ctx, options, workflowType, workflowInput)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStarted) {
			return EndpointResult{}, fmt.Errorf("submit Temporal endpoint %q: %w", cfg.Name, err)
		}
		run = temporalClient.GetWorkflow(ctx, workflowID, "")
	}
	if err := validateWorkflowOwnership(ctx, temporalClient, workflowID, run.GetRunID(), workflowType, owner, envelope.MessageID); err != nil {
		return EndpointResult{}, err
	}
	if !waitForResult {
		return EndpointResult{}, nil
	}
	var result EndpointResult
	if err := run.Get(ctx, &result); err != nil {
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %q execution failed: %w", cfg.Name, err)
	}
	return result, nil
}

func validateWorkflowOwnership(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
	expectedWorkflowType string,
	expectedOwner string,
	expectedCallID string,
) error {
	description, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("describe accepted durable workflow %q: %w", workflowID, err)
	}
	info := description.GetWorkflowExecutionInfo()
	if info == nil || info.GetType().GetName() != expectedWorkflowType {
		actual := ""
		if info != nil && info.GetType() != nil {
			actual = info.GetType().GetName()
		}
		return fmt.Errorf("durable workflow %q ownership collision: workflow type %q, expected %q", workflowID, actual, expectedWorkflowType)
	}
	if err := validateMemoOwnership(info.GetMemo(), expectedOwner, expectedCallID); err != nil {
		return fmt.Errorf("durable workflow %q ownership collision: %w", workflowID, err)
	}
	return nil
}

func validateMemoOwnership(memo *commonpb.Memo, expectedOwner string, expectedCallID string) error {
	if memo == nil {
		return fmt.Errorf("ServiceLib memo is absent")
	}
	readMemo := func(name string) (string, error) {
		payload := memo.GetFields()[name]
		if payload == nil {
			return "", fmt.Errorf("memo field %q is absent", name)
		}
		var value string
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
			return "", fmt.Errorf("decode memo field %q: %w", name, err)
		}
		return value, nil
	}
	managedBy, err := readMemo(durableMemoManagedBy)
	if err != nil {
		return err
	}
	if managedBy != "servicelib" {
		return fmt.Errorf("managedBy=%q", managedBy)
	}
	owner, err := readMemo(durableMemoOwner)
	if err != nil {
		return err
	}
	if owner != expectedOwner {
		return fmt.Errorf("owner=%q expected=%q", owner, expectedOwner)
	}
	callID, err := readMemo(durableMemoCallID)
	if err != nil {
		return err
	}
	if callID != expectedCallID {
		return fmt.Errorf("callId=%q expected=%q", callID, expectedCallID)
	}
	return nil
}

func temporalEndpointWorkflow(ctx workflow.Context, request endpointWorkflowRequest) (EndpointResult, error) {
	if request.Envelope.Scheduled {
		info := workflow.GetInfo(ctx)
		request.Envelope.MessageID = info.WorkflowExecution.ID
		request.Envelope.StreamID = info.WorkflowExecution.ID
		request.Envelope.ScheduledAtNano = scheduledTimeFromWorkflowID(
			info.WorkflowExecution.ID, info.WorkflowStartTime,
		).UnixNano()
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(request.ActivityStartToCloseMillis) * time.Millisecond,
		HeartbeatTimeout:    time.Duration(request.ActivityHeartbeatMillis) * time.Millisecond,
		RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: request.MaximumAttempts},
		Priority:            sdktemporal.Priority{PriorityKey: request.Priority},
	}
	var result EndpointResult
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), request.ActivityType, request.Envelope).Get(ctx, &result); err != nil {
		return EndpointResult{}, err
	}
	return result, nil
}

func scheduledTimeFromWorkflowID(id string, fallback time.Time) time.Time {
	match := scheduleWorkflowIDSuffix.FindStringSubmatch(id)
	if len(match) == 2 {
		if scheduledAt, err := time.Parse(time.RFC3339Nano, match[1]); err == nil {
			return scheduledAt.UTC()
		}
	}
	return fallback.UTC()
}
