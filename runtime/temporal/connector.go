/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

// Package temporal implements the official Temporal SDK boundary used by
// DurableCall and Temporal endpoints. Business nodes never import this package.
package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

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
)

const durableWorkflowType = "servicegen.durable-link.v1"
const endpointWorkflowType = "servicegen.temporal-endpoint.v1"

var scheduleWorkflowIDSuffix = regexp.MustCompile(`-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)$`)

const (
	durableMemoManagedBy = "servicegen.managedBy"
	durableMemoOwner     = "servicegen.owner"
	durableMemoCallID    = "servicegen.callId"
)

type durableWorkflowRequest struct {
	ActivityType               string                  `json:"activityType"`
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
	SamplingEnabled  bool   `json:"samplingEnabled,omitempty"`
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

type EndpointHandler func(context.Context, EndpointEnvelope) (EndpointResult, error)

type endpointWorkflowRequest struct {
	ActivityType               string           `json:"activityType"`
	ActivityStartToCloseMillis int              `json:"activityStartToCloseMillis"`
	ActivityHeartbeatMillis    int              `json:"activityHeartbeatMillis,omitempty"`
	MaximumAttempts            int32            `json:"maximumAttempts"`
	Priority                   int              `json:"priority"`
	Envelope                   EndpointEnvelope `json:"envelope"`
}

type linkRegistration struct {
	id           config.LinkID
	config       config.DurableCallSemanticsConfig
	activityType string
	handler      runtime.DurableLinkHandler
}

type endpointRegistration struct {
	id           int
	config       config.TemporalEndpointConfig
	activityType string
	handler      EndpointHandler
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
	connector := &Connector{
		id: connectorID, name: cfg.Name, environment: environment,
		linkRegistrations:     make(map[config.LinkID]linkRegistration),
		endpointRegistrations: make(map[int]endpointRegistration),
	}
	environment.AddDurableTransport(connector)
	return connector, nil
}

func (c *Connector) GetID() int      { return c.id }
func (c *Connector) GetName() string { return c.name }

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
	serviceID := c.environment.ServiceConfig().ID
	c.linkRegistrations[id] = linkRegistration{
		id: id, config: *durable,
		activityType: fmt.Sprintf("servicegen.durable.%d.%d.%d.v1", serviceID, id.From, id.To),
		handler:      handler,
	}
	return nil
}

// RegisterEndpoint binds one configured endpoint Activity to its existing
// input graph adapter. The Activity is infrastructure; handler invokes the
// ordinary endpoint consumer and never replaces a business node.
func (c *Connector) RegisterEndpoint(endpointID int, handler EndpointHandler) error {
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
	serviceID := c.environment.ServiceConfig().ID
	c.endpointRegistrations[endpointID] = endpointRegistration{
		id: endpointID, config: *cfg,
		activityType: fmt.Sprintf("servicegen.endpoint.%d.%d.v1", serviceID, endpointID),
		handler:      handler,
	}
	return nil
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
		durableRegistered  bool
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
	for _, registration := range c.linkRegistrations {
		registered := getWorker(registration.config.TaskQueue)
		if !registered.durableRegistered {
			registered.worker.RegisterWorkflowWithOptions(durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType})
			registered.durableRegistered = true
		}
		registration := registration
		registered.worker.RegisterActivityWithOptions(
			func(activityCtx context.Context, envelope runtime.DurableEnvelope) error {
				if envelope.Version != 1 || envelope.From != registration.id.From || envelope.To != registration.id.To || envelope.CallID == "" {
					return fmt.Errorf("invalid durable envelope for link %d->%d", registration.id.From, registration.id.To)
				}
				return registration.handler(activityCtx, envelope)
			},
			activity.RegisterOptions{Name: registration.activityType},
		)
	}
	for _, registration := range c.endpointRegistrations {
		if !registration.config.Enabled {
			continue
		}
		registered := getWorker(registration.config.TaskQueue)
		if !registered.endpointRegistered {
			registered.worker.RegisterWorkflowWithOptions(temporalEndpointWorkflow, workflow.RegisterOptions{Name: endpointWorkflowType})
			registered.endpointRegistered = true
		}
		registration := registration
		registered.worker.RegisterActivityWithOptions(
			func(activityCtx context.Context, envelope EndpointEnvelope) (EndpointResult, error) {
				if envelope.Version != 1 || envelope.EndpointID != registration.id || envelope.ExecutionID == "" {
					return EndpointResult{}, fmt.Errorf("invalid durable envelope for Temporal endpoint %d", registration.id)
				}
				envelope.FiredAtNano = time.Now().UTC().UnixNano()
				return registration.handler(activityCtx, envelope)
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
	}
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
	owner := fmt.Sprintf("servicegen/%d/endpoint/%d/v1", c.environment.ServiceConfig().ID, cfg.ID)
	request := endpointWorkflowRequest{
		ActivityType:               registration.activityType,
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
		ID:        fmt.Sprintf("servicegen/schedule/%d/%d", c.environment.ServiceConfig().ID, cfg.ID),
		Workflow:  endpointWorkflowType,
		Args:      []interface{}{request},
		TaskQueue: cfg.TaskQueue,
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicegen",
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
			CronExpressions: []string{cfg.Schedule},
			TimeZoneName:    cfg.Timezone,
		},
		Action:        action,
		Overlap:       overlap,
		CatchupWindow: catchupWindow,
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicegen",
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
		ActivityStartToCloseMillis: registration.config.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    registration.config.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(registration.config.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(envelope.Priority),
		Envelope:                   envelope,
	}
	workflowID := fmt.Sprintf("servicegen/durable/%d/%d/%d/%s", c.environment.ServiceConfig().ID, id.From, id.To, envelope.CallID)
	owner := fmt.Sprintf("servicegen/%d/link/%d/%d/v1", c.environment.ServiceConfig().ID, id.From, id.To)
	options := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                registration.config.TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Priority:                 sdktemporal.Priority{PriorityKey: request.Priority},
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicegen",
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
	registration, registered := c.endpointRegistrations[endpointID]
	temporalClient := c.client
	started := c.started
	c.mu.Unlock()
	if !registered {
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %d is not registered", endpointID)
	}
	if !registration.config.Enabled {
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %q is disabled", registration.config.Name)
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
		ActivityType:               registration.activityType,
		ActivityStartToCloseMillis: registration.config.ActivityStartToCloseTimeout,
		ActivityHeartbeatMillis:    registration.config.ActivityHeartbeatTimeout,
		MaximumAttempts:            int32(registration.config.MaximumAttempts),
		Priority:                   runtime.NormalizeTemporalPriority(envelope.Priority),
		Envelope:                   envelope,
	}
	workflowID := fmt.Sprintf("servicegen/endpoint/%d/%d/%s", c.environment.ServiceConfig().ID, endpointID, envelope.ExecutionID)
	owner := fmt.Sprintf("servicegen/%d/endpoint/%d/v1", c.environment.ServiceConfig().ID, endpointID)
	options := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                registration.config.TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Priority:                 sdktemporal.Priority{PriorityKey: request.Priority},
		Memo: map[string]interface{}{
			durableMemoManagedBy: "servicegen",
			durableMemoOwner:     owner,
			durableMemoCallID:    envelope.ExecutionID,
		},
	}
	if registration.config.WorkflowExecutionTimeout > 0 {
		options.WorkflowExecutionTimeout = time.Duration(registration.config.WorkflowExecutionTimeout) * time.Millisecond
	}
	run, err := temporalClient.ExecuteWorkflow(ctx, options, endpointWorkflowType, request)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStarted) {
			return EndpointResult{}, fmt.Errorf("submit Temporal endpoint %q: %w", registration.config.Name, err)
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
		return EndpointResult{}, fmt.Errorf("Temporal endpoint %q execution failed: %w", registration.config.Name, err)
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
		return fmt.Errorf("ServiceGen memo is absent")
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
	if managedBy != "servicegen" {
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
	var result EndpointResult
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), request.ActivityType, request.Envelope).Get(ctx, &result)
	return result, err
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
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), request.ActivityType, request.Envelope).Get(ctx, nil)
}
