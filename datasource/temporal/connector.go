/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

// Package temporal implements the official Temporal SDK boundary used by
// DurableCall and Temporal data-source endpoints. Business nodes never import
// this package.
package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

const durableWorkflowType = "servicelib.durable-link.v1"
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

func durableLinkActivityType(serviceName, sourceName, targetName string) string {
	return fmt.Sprintf("%s.durable.%s.%s.v1",
		temporalIdentityName(serviceName),
		temporalIdentityName(sourceName),
		temporalIdentityName(targetName),
	)
}

func durableContinuationActivityType(serviceName, connectorName string) string {
	return fmt.Sprintf("%s.durable_continuation.%s.v1",
		temporalIdentityName(serviceName), temporalIdentityName(connectorName),
	)
}

func durableLinkWorkflowID(serviceName, sourceName, targetName, callID string) string {
	return fmt.Sprintf("%s/durable/%s/%s/%s",
		temporalIdentityName(serviceName),
		temporalIdentityName(sourceName),
		temporalIdentityName(targetName),
		temporalOpaqueIdentityComponent(callID),
	)
}

func durableLinkOwner(serviceName, sourceName, targetName string) string {
	return fmt.Sprintf("%s/link/%s/%s/v1",
		temporalIdentityName(serviceName),
		temporalIdentityName(sourceName),
		temporalIdentityName(targetName),
	)
}

func temporalEndpointActivityType(connectorName, endpointName string) string {
	return fmt.Sprintf("%s.endpoint.%s.v1",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

func temporalEndpointWorkflowID(connectorName, endpointName, executionID string) string {
	return fmt.Sprintf("%s/endpoint/%s/%s",
		temporalIdentityName(connectorName),
		temporalIdentityName(endpointName),
		temporalOpaqueIdentityComponent(executionID),
	)
}

func temporalEndpointOwner(connectorName, endpointName string) string {
	return fmt.Sprintf("%s/endpoint/%s/v1",
		temporalIdentityName(connectorName), temporalIdentityName(endpointName),
	)
}

type durableWorkflowRequest struct {
	ActivityType               string                  `json:"activityType"`
	ContinuationActivityType   string                  `json:"continuationActivityType"`
	ActivityStartToCloseMillis int                     `json:"activityStartToCloseMillis"`
	ActivityHeartbeatMillis    int                     `json:"activityHeartbeatMillis,omitempty"`
	MaximumAttempts            int32                   `json:"maximumAttempts"`
	Priority                   int                     `json:"priority"`
	Envelope                   runtime.DurableEnvelope `json:"envelope"`
}

// EndpointEnvelope is the transport envelope used by a symmetric Temporal
// endpoint. Payload is the endpoint's declared input type. ScheduledAt and
// FiredAt are populated only for an execution started by a Temporal Schedule.
type EndpointEnvelope struct {
	Version          int    `json:"version"`
	EndpointID       int    `json:"endpointId"`
	ExecutionID      string `json:"executionId"`
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

type connectorEndpointHandler func(context.Context, EndpointEnvelope) (EndpointResult, error)

type endpointWorkflowRequest struct {
	ActivityType               string           `json:"activityType"`
	ContinuationActivityType   string           `json:"continuationActivityType"`
	ActivityStartToCloseMillis int              `json:"activityStartToCloseMillis"`
	ActivityHeartbeatMillis    int              `json:"activityHeartbeatMillis,omitempty"`
	MaximumAttempts            int32            `json:"maximumAttempts"`
	Priority                   int              `json:"priority"`
	Envelope                   EndpointEnvelope `json:"envelope"`
}

type endpointActivityResult struct {
	Durable runtime.DurableActivityResult `json:"durable"`
	Result  EndpointResult                `json:"result"`
}

type linkRegistration struct {
	id           config.LinkID
	config       config.DurableCallSemanticsConfig
	serviceName  string
	sourceName   string
	targetName   string
	activityType string
	handler      runtime.DurableLinkHandler
}

type endpointRegistration struct {
	id           int
	config       config.TemporalEndpointConfig
	activityType string
	handler      connectorEndpointHandler
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
	linkRegistrations     map[config.LinkID]linkRegistration
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
	if existing := environment.GetDurableTransport(connectorID); existing != nil {
		connector, ok := existing.(*Connector)
		if !ok {
			return nil, fmt.Errorf("durable transport id=%d is not a Go Temporal connector", connectorID)
		}
		return connector, nil
	}
	durableEvents, err := environment.Metrics().Scope(
		"durable_call",
		metrics.Labels{"connector": cfg.Name},
	).CounterVec("events_total", "Total number of DurableCall Activity lifecycle events")
	if err != nil {
		return nil, fmt.Errorf("create DurableCall metrics for Temporal connector %q: %w", cfg.Name, err)
	}
	connector := &Connector{
		id: connectorID, name: cfg.Name, environment: environment,
		linkRegistrations:     make(map[config.LinkID]linkRegistration),
		endpointRegistrations: make(map[int]endpointRegistration),
		durableEvents:         durableEvents,
	}
	environment.AddDurableTransport(connector)
	return connector, nil
}

func (c *Connector) GetID() int      { return c.id }
func (c *Connector) GetName() string { return c.name }

func (c *Connector) durableCallDiagnostics(boundary, target string) runtime.DurableCallDiagnostics {
	return func(ctx context.Context, event runtime.DurableCallEvent, err error) {
		if c.durableEvents != nil {
			c.durableEvents.With(metrics.Labels{
				"boundary": boundary,
				"target":   target,
				"event":    string(event),
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
		switch event {
		case runtime.DurableCallEventMissingOutcome,
			runtime.DurableCallEventDuplicateResult,
			runtime.DurableCallEventLateHeartbeat:
			c.environment.Log().Warn(ctx, "DurableCall Activity lifecycle misuse", fields...)
		default:
			c.environment.Log().Error(ctx, "DurableCall Activity failed", fields...)
		}
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

func (c *Connector) RegisterLink(id config.LinkID, handler runtime.DurableLinkHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return fmt.Errorf("cannot register durable link %d->%d after Temporal connector start", id.From, id.To)
	}
	if existing, present := c.linkRegistrations[id]; present {
		if existing.handler != nil {
			return fmt.Errorf("durable link %d->%d is already registered", id.From, id.To)
		}
	}
	link := c.environment.RuntimeConfig().GetLink(id.From, id.To)
	if link == nil {
		return fmt.Errorf("durable link %d->%d configuration not found", id.From, id.To)
	}
	durable, ok := link.GetCallSemantics().(*config.DurableCallSemanticsConfig)
	if !ok || durable.IdDataConnector != c.id {
		return fmt.Errorf("link %d->%d does not belong to Temporal connector %q", id.From, id.To, c.name)
	}
	source := c.environment.RuntimeConfig().GetStreamConfigByID(id.From)
	target := c.environment.RuntimeConfig().GetStreamConfigByID(id.To)
	if source == nil || target == nil {
		return fmt.Errorf("durable link %d->%d references missing stream configuration", id.From, id.To)
	}
	serviceName := c.environment.ServiceConfig().Name
	c.linkRegistrations[id] = linkRegistration{
		id: id, config: *durable, serviceName: serviceName,
		sourceName: source.GetName(), targetName: target.GetName(),
		activityType: durableLinkActivityType(serviceName, source.GetName(), target.GetName()),
		handler:      handler,
	}
	return nil
}

// RegisterEndpoint binds one configured endpoint Activity to its existing
// input graph adapter. The Activity is infrastructure; handler invokes the
// ordinary endpoint consumer and never replaces a business node.
func (c *Connector) RegisterEndpoint(endpointID int, handler connectorEndpointHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return fmt.Errorf("cannot register Temporal endpoint %d after connector start", endpointID)
	}
	if handler == nil {
		return fmt.Errorf("Temporal endpoint %d handler is nil", endpointID)
	}
	if _, exists := c.endpointRegistrations[endpointID]; exists {
		return fmt.Errorf("Temporal endpoint %d is already registered", endpointID)
	}
	configured := c.environment.RuntimeConfig().GetEndpointConfigByID(endpointID)
	cfg, ok := configured.(*config.TemporalEndpointConfig)
	if !ok || cfg.IdDataConnector != c.id {
		return fmt.Errorf("endpoint id=%d does not belong to Temporal connector %q", endpointID, c.name)
	}
	c.endpointRegistrations[endpointID] = endpointRegistration{
		id: endpointID, config: *cfg,
		activityType: temporalEndpointActivityType(c.name, cfg.Name),
		handler:      handler,
	}
	return nil
}

// executeEndpointActivity owns the processing-side Temporal Activity scope for
// both scheduled and on-demand endpoints. Keeping this boundary independent of
// graph callers makes its terminal, cancellation, heartbeat and result
// semantics directly testable without constructing a DurableCall link.
func executeEndpointActivity(
	activityCtx context.Context,
	envelope EndpointEnvelope,
	registration endpointRegistration,
	heartbeat runtime.DurableCallHeartbeatRecorder,
	diagnostics runtime.DurableCallDiagnostics,
) (endpointActivityResult, error) {
	if envelope.Version != 1 || envelope.EndpointID != registration.id || envelope.ExecutionID == "" {
		return endpointActivityResult{}, fmt.Errorf("invalid durable envelope for Temporal endpoint %d", registration.id)
	}
	envelope.FiredAtNano = time.Now().UTC().UnixNano()
	durable := runtime.NewDurableCallContext(
		envelope.ExecutionID, heartbeat, diagnostics,
	)
	var result EndpointResult
	durableResult, err := runtime.RunDurableCallActivityWithResult(
		activityCtx, durable, func(ctx context.Context) error {
			var invokeErr error
			result, invokeErr = registration.handler(ctx, envelope)
			return invokeErr
		},
	)
	return endpointActivityResult{Durable: durableResult, Result: result}, err
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
		worker                 worker.Worker
		durableRegistered      bool
		endpointRegistered     bool
		continuationRegistered bool
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
	registerContinuation := func(registered *queueWorker) {
		if registered.continuationRegistered {
			return
		}
		var continuationTracer tracing.Tracer
		if configuredTracing := c.environment.Tracing(); configuredTracing != nil {
			continuationTracer = configuredTracing.Tracer(c.environment.ServiceConfig().Name)
		}
		registered.worker.RegisterActivityWithOptions(
			func(activityCtx context.Context, continuation runtime.DurableContinuation) (runtime.DurableActivityResult, error) {
				if continuation.Version != 1 || continuation.CallID == "" || continuation.FromName == "" || continuation.ToName == "" {
					return runtime.DurableActivityResult{}, errors.New("invalid durable continuation envelope")
				}
				ctx, cancel := durableContinuationContext(activityCtx, continuation)
				defer cancel()
				if configuredTracing := c.environment.Tracing(); configuredTracing != nil && len(continuation.TraceCarrier) != 0 {
					ctx = configuredTracing.Extract(ctx, continuation.TraceCarrier)
				}
				durable := runtime.NewDurableCallContext(
					continuation.CallID,
					func(ctx context.Context, details any) error {
						activity.RecordHeartbeat(ctx, details)
						return nil
					},
					c.durableCallDiagnostics("continuation", continuation.FromName+":"+continuation.ToName),
				)
				return runtime.RunDurableCallActivityWithResult(ctx, durable, func(ctx context.Context) error {
					if continuationTracer != nil && tracing.SamplingEnabled(ctx) {
						var span tracing.Span
						ctx, span = continuationTracer.Start(ctx, "temporal.activity",
							tracing.StringAttr("boundary", "durable_delay"),
							tracing.StringAttr("from", continuation.FromName),
							tracing.StringAttr("to", continuation.ToName),
						)
						runtime.BindDurableCallSpan(ctx, span)
					}
					return c.environment.GetRuntime().ResumeDurableContinuation(ctx, continuation)
				})
			},
			activity.RegisterOptions{Name: durableContinuationActivityType(c.environment.ServiceConfig().Name, c.name)},
		)
		registered.continuationRegistered = true
	}
	for _, registration := range c.linkRegistrations {
		registered := getWorker(registration.config.TaskQueue)
		registerContinuation(registered)
		if !registered.durableRegistered {
			registered.worker.RegisterWorkflowWithOptions(durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType})
			registered.durableRegistered = true
		}
		registration := registration
		registered.worker.RegisterActivityWithOptions(
			func(activityCtx context.Context, envelope runtime.DurableEnvelope) (runtime.DurableActivityResult, error) {
				if envelope.Version != 1 || envelope.From != registration.id.From || envelope.To != registration.id.To || envelope.CallID == "" {
					return runtime.DurableActivityResult{}, fmt.Errorf("invalid durable envelope for link %d->%d", registration.id.From, registration.id.To)
				}
				durable := runtime.NewDurableCallContext(
					envelope.CallID,
					func(ctx context.Context, details any) error {
						activity.RecordHeartbeat(ctx, details)
						return nil
					},
					c.durableCallDiagnostics("link", fmt.Sprintf("%d:%d", registration.id.From, registration.id.To)),
				)
				return runtime.RunDurableCallActivityWithResult(activityCtx, durable, func(ctx context.Context) error {
					return registration.handler(ctx, envelope)
				})
			},
			activity.RegisterOptions{Name: registration.activityType},
		)
	}
	for _, registration := range c.endpointRegistrations {
		if !registration.config.Enabled {
			continue
		}
		registered := getWorker(registration.config.TaskQueue)
		registerContinuation(registered)
		if !registered.endpointRegistered {
			registered.worker.RegisterWorkflowWithOptions(temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType})
			registered.endpointRegistered = true
		}
		registration := registration
		registered.worker.RegisterActivityWithOptions(
			func(activityCtx context.Context, envelope EndpointEnvelope) (endpointActivityResult, error) {
				return executeEndpointActivity(
					activityCtx, envelope, registration,
					func(ctx context.Context, details any) error {
						activity.RecordHeartbeat(ctx, details)
						return nil
					},
					c.durableCallDiagnostics("endpoint", fmt.Sprintf("%d", registration.id)),
				)
			},
			activity.RegisterOptions{Name: registration.activityType},
		)
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
		if registration.config.Enabled && registration.config.Schedule != "" {
			if err := c.ensureSchedule(ctx, temporalClient, registration); err != nil {
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
	options := client.Options{
		HostPort: cfg.Address, Namespace: cfg.Namespace, Identity: cfg.Identity,
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

func (c *Connector) ensureSchedule(ctx context.Context, temporalClient client.Client, registration endpointRegistration) error {
	cfg := registration.config
	owner := temporalEndpointOwner(c.name, cfg.Name)
	request := endpointWorkflowRequest{
		ActivityType:               registration.activityType,
		ContinuationActivityType:   durableContinuationActivityType(c.environment.ServiceConfig().Name, c.name),
		ActivityStartToCloseMillis: cfg.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    cfg.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(cfg.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(0),
		Envelope: EndpointEnvelope{
			Version: 1, EndpointID: cfg.ID, Scheduled: true, ScheduleID: cfg.ScheduleID,
		},
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
		ID:        fmt.Sprintf("%s/schedule/%s", c.name, cfg.Name),
		Workflow:  endpointWorkflowType,
		Args:      []interface{}{request},
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
	if !ok || existingAction.Workflow != endpointWorkflowType || existingAction.TaskQueue != cfg.TaskQueue {
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

func (c *Connector) SubmitLink(ctx context.Context, id config.LinkID, envelope runtime.DurableEnvelope) error {
	c.mu.Lock()
	registration, registered := c.linkRegistrations[id]
	temporalClient := c.client
	started := c.started
	c.mu.Unlock()
	if !registered {
		return fmt.Errorf("durable link %d->%d is not registered", id.From, id.To)
	}
	if !started || temporalClient == nil {
		return fmt.Errorf("Temporal connector %q is not started", c.name)
	}
	request := durableWorkflowRequest{
		ActivityType:               registration.activityType,
		ContinuationActivityType:   durableContinuationActivityType(c.environment.ServiceConfig().Name, c.name),
		ActivityStartToCloseMillis: registration.config.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    registration.config.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(registration.config.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(envelope.Priority),
		Envelope:                   envelope,
	}
	workflowID := durableLinkWorkflowID(
		registration.serviceName, registration.sourceName, registration.targetName, envelope.CallID,
	)
	owner := durableLinkOwner(
		registration.serviceName, registration.sourceName, registration.targetName,
	)
	options := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                registration.config.TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Priority:                 sdktemporal.Priority{PriorityKey: request.Priority},
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicelib",
			durableMemoOwner:     owner,
			durableMemoCallID:    envelope.CallID,
		},
	}
	if registration.config.WorkflowExecutionTimeout > 0 {
		options.WorkflowExecutionTimeout = time.Duration(registration.config.WorkflowExecutionTimeout) * time.Millisecond
	}
	run, err := temporalClient.ExecuteWorkflow(ctx, options, durableWorkflowType, request)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStarted) {
			return fmt.Errorf("submit durable link %d->%d: %w", id.From, id.To, err)
		}
	}
	runID := ""
	if run != nil {
		runID = run.GetRunID()
	}
	return validateDurableWorkflowOwnership(ctx, temporalClient, workflowID, runID, owner, envelope.CallID)
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
	if envelope.ExecutionID == "" {
		envelope.ExecutionID = runtime.NewStreamID()
	}
	if envelope.StreamID == "" {
		envelope.StreamID = envelope.ExecutionID
	}
	envelope.Version = 1
	envelope.EndpointID = endpointID
	request := endpointWorkflowRequest{
		ActivityType:               temporalEndpointActivityType(c.name, cfg.Name),
		ContinuationActivityType:   durableContinuationActivityType(c.environment.ServiceConfig().Name, c.name),
		ActivityStartToCloseMillis: cfg.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    cfg.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(cfg.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(envelope.Priority),
		Envelope:                   envelope,
	}
	workflowID := temporalEndpointWorkflowID(c.name, cfg.Name, envelope.ExecutionID)
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
			durableMemoCallID:    envelope.ExecutionID,
		},
	}
	if cfg.WorkflowExecutionTimeout > 0 {
		options.WorkflowExecutionTimeout = time.Duration(cfg.WorkflowExecutionTimeout) * time.Millisecond
	}
	run, err := temporalClient.ExecuteWorkflow(ctx, options, endpointWorkflowType, request)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStarted) {
			return EndpointResult{}, fmt.Errorf("submit Temporal endpoint %q: %w", cfg.Name, err)
		}
		run = temporalClient.GetWorkflow(ctx, workflowID, "")
	}
	if err := validateWorkflowOwnership(ctx, temporalClient, workflowID, run.GetRunID(), endpointWorkflowType, owner, envelope.ExecutionID); err != nil {
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

func validateDurableWorkflowOwnership(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
	expectedOwner string,
	expectedCallID string,
) error {
	return validateWorkflowOwnership(ctx, temporalClient, workflowID, runID, durableWorkflowType, expectedOwner, expectedCallID)
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
		request.Envelope.ExecutionID = info.WorkflowExecution.ID
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
	var result endpointActivityResult
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), request.ActivityType, request.Envelope).Get(ctx, &result); err != nil {
		return EndpointResult{}, err
	}
	if err := runDurableContinuations(ctx, options, request.ContinuationActivityType, result.Durable); err != nil {
		return EndpointResult{}, err
	}
	return result.Result, nil
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

func durableLinkWorkflow(ctx workflow.Context, request durableWorkflowRequest) error {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(request.ActivityStartToCloseMillis) * time.Millisecond,
		HeartbeatTimeout:    time.Duration(request.ActivityHeartbeatMillis) * time.Millisecond,
		RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: request.MaximumAttempts},
		Priority:            sdktemporal.Priority{PriorityKey: request.Priority},
	}
	var result runtime.DurableActivityResult
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), request.ActivityType, request.Envelope).Get(ctx, &result); err != nil {
		return err
	}
	return runDurableContinuations(ctx, options, request.ContinuationActivityType, result)
}

func runDurableContinuations(
	ctx workflow.Context,
	options workflow.ActivityOptions,
	activityType string,
	result runtime.DurableActivityResult,
) error {
	for result.Continuation != nil {
		continuation := *result.Continuation
		wakeAt := time.Unix(0, continuation.WakeAtUnixNano).UTC()
		if delay := wakeAt.Sub(workflow.Now(ctx).UTC()); delay > 0 {
			if err := workflow.Sleep(ctx, delay); err != nil {
				return err
			}
		}
		result = runtime.DurableActivityResult{}
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, options), activityType, continuation,
		).Get(ctx, &result); err != nil {
			return err
		}
	}
	return nil
}

func durableContinuationContext(parent context.Context, continuation runtime.DurableContinuation) (context.Context, context.CancelFunc) {
	ctx := parent
	if continuation.StreamID != "" {
		ctx = runtime.WithStreamId(ctx, continuation.StreamID)
	}
	ctx = runtime.WithPriority(ctx, continuation.Priority)
	if continuation.DeadlineUnixNano > 0 {
		return context.WithDeadline(ctx, time.Unix(0, continuation.DeadlineUnixNano).UTC())
	}
	return ctx, func() {}
}
