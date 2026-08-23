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
	"errors"
	"fmt"
	"sync"
	"time"

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

type linkRegistration struct {
	id           config.LinkID
	config       config.DurableCallSemanticsConfig
	activityType string
	handler      runtime.DurableLinkHandler
}

// Connector owns exactly one Temporal client and the Workers registered for
// one configured Temporal DataConnector.
type Connector struct {
	id          int
	name        string
	environment runtime.RuntimeEnvironment

	mu            sync.Mutex
	client        client.Client
	workers       []worker.Worker
	registrations map[config.LinkID]linkRegistration
	started       bool
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
		registrations: make(map[config.LinkID]linkRegistration),
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
	if existing, present := c.registrations[id]; present {
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
	c.registrations[id] = linkRegistration{
		id: id, config: *durable,
		activityType: fmt.Sprintf("servicegen.durable.%d.%d.%d.v1", serviceID, id.From, id.To),
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
	temporalClient, err := client.Dial(client.Options{
		HostPort: cfg.Address, Namespace: cfg.Namespace, Identity: cfg.Identity,
	})
	if err != nil {
		return fmt.Errorf("connect Temporal data connector %q: %w", c.name, err)
	}
	workersByQueue := make(map[string]worker.Worker)
	for _, registration := range c.registrations {
		w := workersByQueue[registration.config.TaskQueue]
		if w == nil {
			w = worker.New(temporalClient, registration.config.TaskQueue, worker.Options{
				MaxConcurrentActivityExecutionSize:     cfg.MaxConcurrentActivities,
				MaxConcurrentWorkflowTaskExecutionSize: cfg.MaxConcurrentWorkflows,
			})
			w.RegisterWorkflowWithOptions(durableLinkWorkflow, workflow.RegisterOptions{Name: durableWorkflowType})
			workersByQueue[registration.config.TaskQueue] = w
		}
		registration := registration
		w.RegisterActivityWithOptions(
			func(activityCtx context.Context, envelope runtime.DurableEnvelope) error {
				if envelope.Version != 1 || envelope.From != registration.id.From || envelope.To != registration.id.To || envelope.CallID == "" {
					return fmt.Errorf("invalid durable envelope for link %d->%d", registration.id.From, registration.id.To)
				}
				return registration.handler(activityCtx, envelope)
			},
			activity.RegisterOptions{Name: registration.activityType},
		)
	}
	startedWorkers := make([]worker.Worker, 0, len(workersByQueue))
	for _, w := range workersByQueue {
		if err := w.Start(); err != nil {
			for _, started := range startedWorkers {
				started.Stop()
			}
			temporalClient.Close()
			return fmt.Errorf("start Temporal worker for connector %q: %w", c.name, err)
		}
		startedWorkers = append(startedWorkers, w)
	}
	c.client = temporalClient
	c.workers = startedWorkers
	c.started = true
	_ = ctx
	return nil
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

func (c *Connector) SubmitLink(ctx context.Context, id config.LinkID, envelope runtime.DurableEnvelope) error {
	c.mu.Lock()
	registration, registered := c.registrations[id]
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

func validateDurableWorkflowOwnership(
	ctx context.Context,
	temporalClient client.Client,
	workflowID string,
	runID string,
	expectedOwner string,
	expectedCallID string,
) error {
	description, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("describe accepted durable workflow %q: %w", workflowID, err)
	}
	info := description.GetWorkflowExecutionInfo()
	if info == nil || info.GetType().GetName() != durableWorkflowType {
		actual := ""
		if info != nil && info.GetType() != nil {
			actual = info.GetType().GetName()
		}
		return fmt.Errorf("durable workflow %q ownership collision: workflow type %q, expected %q", workflowID, actual, durableWorkflowType)
	}
	memo := info.GetMemo()
	if memo == nil {
		return fmt.Errorf("durable workflow %q ownership collision: ServiceGen memo is absent", workflowID)
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
		return fmt.Errorf("durable workflow %q ownership collision: %w", workflowID, err)
	}
	if managedBy != "servicegen" {
		return fmt.Errorf("durable workflow %q ownership collision: managedBy=%q", workflowID, managedBy)
	}
	owner, err := readMemo(durableMemoOwner)
	if err != nil {
		return fmt.Errorf("durable workflow %q ownership collision: %w", workflowID, err)
	}
	if owner != expectedOwner {
		return fmt.Errorf("durable workflow %q ownership collision: owner=%q expected=%q", workflowID, owner, expectedOwner)
	}
	callID, err := readMemo(durableMemoCallID)
	if err != nil {
		return fmt.Errorf("durable workflow %q ownership collision: %w", workflowID, err)
	}
	if callID != expectedCallID {
		return fmt.Errorf("durable workflow %q ownership collision: callId=%q expected=%q", workflowID, callID, expectedCallID)
	}
	return nil
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
