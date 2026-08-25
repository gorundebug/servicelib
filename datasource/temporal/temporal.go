/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

// Package temporal adapts official Temporal Activity tasks to ordinary input
// streams. It contains transport lifecycle only; business nodes are unchanged.
package temporal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type StreamContext[T, R, E any] = runtime.StreamContext[T, R, E]

// EndpointHandler is the user-visible lifecycle at a Temporal input boundary.
// ConsumeMessage emits the already typed job into the existing graph. The
// framework, not the handler, correlates the graph result by StreamID.
type EndpointHandler[HandlerState, Input, T, R, E any] interface {
	BeginRequest(context.Context, StreamContext[T, R, E]) (context.Context, HandlerState, error)
	ConsumeMessage(context.Context, StreamContext[T, R, E], HandlerState, Input) error
	EndRequest(context.Context, StreamContext[T, R, E], error, HandlerState)
}

// directEndpointHandler is the transport-only adapter used by generated
// Temporal inputs. It performs no business work: the acquired Activity simply
// activates the existing input stream and the normal graph owns everything
// downstream of that boundary.
type directEndpointHandler[T, R, E any] struct{}

func (directEndpointHandler[T, R, E]) BeginRequest(
	ctx context.Context,
	_ StreamContext[T, R, E],
) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (directEndpointHandler[T, R, E]) ConsumeMessage(
	ctx context.Context,
	sc StreamContext[T, R, E],
	_ struct{},
	value T,
) error {
	sc.Collect(ctx, value)
	return nil
}

func (directEndpointHandler[T, R, E]) EndRequest(
	context.Context,
	StreamContext[T, R, E],
	error,
	struct{},
) {
}

type inputDataSource struct{ *runtime.InputDataSource }

func (*inputDataSource) Start(context.Context) error { return nil }
func (*inputDataSource) Stop(context.Context)        {}

type inputEndpoint struct{ *runtime.DataSourceEndpoint }

type resultConsumer[R any] struct{ consume func(context.Context, R) }

func (c *resultConsumer[R]) Consume(ctx context.Context, value R) { c.consume(ctx, value) }

type endpointConsumer[HandlerState, Input, T, R, E any] struct {
	*runtime.DataSourceEndpointConsumer[T, R, E]
	handler         EndpointHandler[HandlerState, Input, T, R, E]
	sc              StreamContext[T, R, E]
	decode          func(EndpointEnvelope) (Input, error)
	resultSer       runtime.TypedSerializedStream[R]
	mu              sync.Mutex
	pending         map[string]chan R
	workflowPending map[string]workflow.Channel
	tracer          tracing.Tracer
	tracing         tracing.Tracing
}

func (ec *endpointConsumer[HandlerState, Input, T, R, E]) GetID() int { return ec.Endpoint().GetID() }

func (ec *endpointConsumer[HandlerState, Input, T, R, E]) FunctionImplementation() interface{} {
	return ec.handler
}

func (ec *endpointConsumer[HandlerState, Input, T, R, E]) consumeResult(ctx context.Context, value R) {
	sid, ok := runtime.StreamIdFromContext(ctx)
	if !ok {
		ec.Endpoint().OnMissingStreamID(ctx)
		return
	}
	if state, workflowMode := ctx.Value(workflowSubmissionContextKey{}).(workflowSubmissionContext); workflowMode {
		result := ec.workflowPending[sid.GetID()]
		if result == nil {
			ec.Endpoint().OnLateResult(ctx, sid.GetID())
			return
		}
		result.Send(state.workflowCtx, value)
		return
	}
	ec.mu.Lock()
	result := ec.pending[sid.GetID()]
	ec.mu.Unlock()
	if result == nil {
		ec.Endpoint().OnLateResult(ctx, sid.GetID())
		return
	}
	select {
	case result <- value:
	default:
		ec.Endpoint().OnDuplicateMessageID(ctx, sid.GetID(), sid.GetID())
	}
}

func (ec *endpointConsumer[HandlerState, Input, T, R, E]) handle(
	activityCtx context.Context,
	envelope EndpointEnvelope,
) (result EndpointResult, err error) {
	value, err := ec.decode(envelope)
	if err != nil {
		return result, err
	}
	ctx, cancel := endpointContext(activityCtx, envelope)
	defer cancel()
	ctx = runtime.ApplyDataSourceEndpointTracing(
		ctx, ec.Stream().GetRuntimeEnvironment(), ec.Endpoint().GetID(),
	)
	var span tracing.Span
	if ec.tracer != nil && tracing.SamplingEnabled(ctx) {
		ctx, span = ec.tracer.Start(ctx, "temporal.input",
			tracing.StringAttr("stream", ec.Stream().GetName()),
			tracing.StringAttr("endpoint", ec.Endpoint().GetName()),
		)
		if !runtime.BindDurableCallSpan(ctx, span) {
			defer span.End()
		}
	}
	handlerCtx, state, err := ec.handler.BeginRequest(ctx, ec.sc)
	if err != nil {
		ec.Endpoint().OnBeginRequestFailed(ctx, err)
		tracing.SpanError(span, err)
		return result, err
	}
	start := ec.Endpoint().OnRequestStart(handlerCtx)
	defer func() {
		ec.handler.EndRequest(handlerCtx, ec.sc, err, state)
		ec.Endpoint().OnRequestEnd(handlerCtx, start, err)
	}()

	hasResult := ec.resultSer != nil
	var resultCh chan R
	var workflowResult workflow.Channel
	workflowState, workflowMode := handlerCtx.Value(workflowSubmissionContextKey{}).(workflowSubmissionContext)
	if hasResult {
		if workflowMode {
			if _, exists := ec.workflowPending[envelope.StreamID]; exists {
				return result, fmt.Errorf("Temporal endpoint %q already has an active execution %q", ec.Endpoint().GetName(), envelope.StreamID)
			}
			workflowResult = workflow.NewBufferedChannel(workflowState.workflowCtx, 1)
			ec.workflowPending[envelope.StreamID] = workflowResult
		} else {
			resultCh = make(chan R, 1)
			ec.mu.Lock()
			if _, exists := ec.pending[envelope.StreamID]; exists {
				ec.mu.Unlock()
				return result, fmt.Errorf("Temporal endpoint %q already has an active execution %q", ec.Endpoint().GetName(), envelope.StreamID)
			}
			ec.pending[envelope.StreamID] = resultCh
			ec.mu.Unlock()
		}
		ec.Endpoint().OnPendingAdd(handlerCtx, envelope.StreamID)
		defer func() {
			if workflowMode {
				delete(ec.workflowPending, envelope.StreamID)
			} else {
				ec.mu.Lock()
				delete(ec.pending, envelope.StreamID)
				ec.mu.Unlock()
			}
			ec.Endpoint().OnPendingRemove(handlerCtx, envelope.StreamID)
		}()
	}

	if err = ec.handler.ConsumeMessage(handlerCtx, ec.sc, state, value); err != nil {
		tracing.SpanError(span, err)
		return result, err
	}
	if !hasResult {
		return result, nil
	}
	if workflowMode {
		var value R
		cancelled := false
		selector := workflow.NewSelector(workflowState.workflowCtx)
		selector.AddReceive(workflowResult, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(workflowState.workflowCtx, &value)
		})
		selector.AddReceive(workflowState.workflowCtx.Done(), func(workflow.ReceiveChannel, bool) {
			cancelled = true
		})
		selector.Select(workflowState.workflowCtx)
		if cancelled {
			return result, workflowState.workflowCtx.Err()
		}
		result.Payload, err = ec.resultSer.GetSerde().Serialize(value)
		return result, err
	}
	select {
	case value := <-resultCh:
		result.Payload, err = ec.resultSer.GetSerde().Serialize(value)
		return result, err
	case <-handlerCtx.Done():
		return result, handlerCtx.Err()
	}
}

func endpointContext(parent context.Context, envelope EndpointEnvelope) (context.Context, context.CancelFunc) {
	ctx := parent
	ctx = runtime.WithStreamId(ctx, envelope.StreamID)
	ctx = runtime.WithPriority(ctx, envelope.Priority)
	if envelope.DeadlineUnixNano > 0 {
		deadline := time.Unix(0, envelope.DeadlineUnixNano)
		if current, ok := ctx.Deadline(); !ok || deadline.Before(current) {
			if runtime.IsDurableWorkflowContext(ctx) {
				return workflowDeadlineContext{Context: ctx, deadline: deadline}, func() {}
			}
			return context.WithDeadline(ctx, deadline)
		}
	}
	return ctx, func() {}
}

type workflowDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx workflowDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func getOrCreateDataSource(id int, env runtime.RuntimeEnvironment) (runtime.DataSource, *Connector, error) {
	connector, err := MakeConnector(id, env)
	if err != nil {
		return nil, nil, err
	}
	if existing := env.GetDataSource(id); existing != nil {
		return existing, connector, nil
	}
	cfg := env.RuntimeConfig().GetDataConnectorByID(id)
	base, err := runtime.MakeInputDataSource(cfg, env)
	if err != nil {
		return nil, nil, err
	}
	ds := &inputDataSource{InputDataSource: base}
	env.AddDataSource(ds)
	return ds, connector, nil
}

func makeEndpointConsumer[HandlerState, Input, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, Input, T, R, E],
	decode func(EndpointEnvelope) (Input, error),
	workflowHandler WorkflowEndpointHandler,
) (runtime.Consumer[T], error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil for Temporal endpoint stream %q", stream.GetName())
	}
	env := stream.GetRuntimeEnvironment()
	configured := env.RuntimeConfig().GetEndpointConfigByID(stream.GetEndpointId())
	cfg, ok := configured.(*config.TemporalEndpointConfig)
	if !ok {
		return nil, fmt.Errorf("endpoint id=%d is not Temporal", stream.GetEndpointId())
	}
	ds, connector, err := getOrCreateDataSource(cfg.IdDataConnector, env)
	if err != nil {
		return nil, err
	}
	if ds.GetEndpoint(cfg.ID) != nil {
		return nil, fmt.Errorf("Temporal source endpoint %q already exists", cfg.Name)
	}
	base, err := runtime.MakeDataSourceEndpoint(ds, cfg.ID, env)
	if err != nil {
		return nil, err
	}
	ep := &inputEndpoint{DataSourceEndpoint: base}
	ds.AddEndpoint(ep)
	consumer := &endpointConsumer[HandlerState, Input, T, R, E]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[T, R, E](ep, stream),
		handler:                    handler, decode: decode, pending: make(map[string]chan R),
		workflowPending: make(map[string]workflow.Channel),
		resultSer:       stream.GetResultStream(),
	}
	if tracer := env.Tracing(); tracer != nil {
		consumer.tracing = tracer
		consumer.tracer = tracer.Tracer(env.ServiceConfig().Name)
	}
	consumer.sc = runtime.MakeStreamContext[T, R, E](
		stream, stream.GetResultStream(), runtime.CollectFunc[T](consumer.Consume),
		runtime.CollectFunc[E](stream.GetErrorStream().Consume),
	)
	if consumer.resultSer != nil {
		stream.SetResultConsumer(&resultConsumer[R]{consume: consumer.consumeResult})
	}
	if err := connector.RegisterEndpoint(cfg.ID, consumer.handle, func(value any) ([]byte, error) {
		typed, ok := value.(T)
		if !ok {
			return nil, fmt.Errorf("Temporal endpoint %q Continue-As-New input has type %T", cfg.Name, value)
		}
		return stream.GetSerde().Serialize(typed)
	}); err != nil {
		return nil, err
	}
	if cfg.TemporalExecutionType == api.Workflow {
		if workflowHandler == nil {
			return nil, fmt.Errorf("Temporal Workflow endpoint %q requires a generated Workflow handler", cfg.Name)
		}
		if err := connector.RegisterWorkflowEndpoint(cfg.ID, workflowHandler); err != nil {
			return nil, err
		}
	}
	env.RegisterEndpointConsumer(consumer)
	return consumer, nil
}

// MakeEndpointConsumer registers one on-demand typed Temporal endpoint.
func MakeEndpointConsumer[HandlerState, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, T, T, R, E],
) (runtime.Consumer[T], error) {
	serde := stream.GetSerde()
	return makeEndpointConsumer(stream, handler, func(envelope EndpointEnvelope) (T, error) {
		return serde.Deserialize(envelope.Payload)
	}, nil)
}

// MakeDirectEndpointConsumer registers a generated on-demand Temporal input
// without inventing a transport-specific business function. The Activity calls
// the ordinary input consumer directly.
func MakeDirectEndpointConsumer[T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
) (runtime.Consumer[T], error) {
	return MakeEndpointConsumer[struct{}](stream, directEndpointHandler[T, R, E]{})
}

// MakeDirectWorkflowEndpointConsumer registers a generated static Workflow
// function while retaining the same ordinary input-stream business contract.
func MakeDirectWorkflowEndpointConsumer[T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	workflowHandler WorkflowEndpointHandler,
) (runtime.Consumer[T], error) {
	serde := stream.GetSerde()
	return makeEndpointConsumer(
		stream,
		directEndpointHandler[T, R, E]{},
		func(envelope EndpointEnvelope) (T, error) {
			return serde.Deserialize(envelope.Payload)
		},
		workflowHandler,
	)
}

type scheduleEndpointInput[T any] struct {
	scheduled bool
	trigger   runtime.ScheduleTrigger
	value     T
}

type scheduleOrValueEndpointHandler[HandlerState, T, R, E any] struct {
	scheduled EndpointHandler[HandlerState, runtime.ScheduleTrigger, T, R, E]
}

func (handler scheduleOrValueEndpointHandler[HandlerState, T, R, E]) BeginRequest(
	ctx context.Context,
	sc StreamContext[T, R, E],
) (context.Context, HandlerState, error) {
	return handler.scheduled.BeginRequest(ctx, sc)
}

func (handler scheduleOrValueEndpointHandler[HandlerState, T, R, E]) ConsumeMessage(
	ctx context.Context,
	sc StreamContext[T, R, E],
	state HandlerState,
	input scheduleEndpointInput[T],
) error {
	if input.scheduled {
		return handler.scheduled.ConsumeMessage(ctx, sc, state, input.trigger)
	}
	sc.Collect(ctx, input.value)
	return nil
}

func (handler scheduleOrValueEndpointHandler[HandlerState, T, R, E]) EndRequest(
	ctx context.Context,
	sc StreamContext[T, R, E],
	err error,
	state HandlerState,
) {
	handler.scheduled.EndRequest(ctx, sc, err, state)
}

// MakeScheduleEndpointConsumer registers a Temporal endpoint that supports
// both entry paths of the same durable job contract. Schedule-created
// executions invoke the endpoint function with ScheduleTrigger. On-demand
// submissions deserialize T and enter the existing input stream directly.
func MakeScheduleEndpointConsumer[HandlerState, T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	handler EndpointHandler[HandlerState, runtime.ScheduleTrigger, T, R, E],
) (runtime.Consumer[T], error) {
	serde := stream.GetSerde()
	return makeEndpointConsumer(
		stream,
		scheduleOrValueEndpointHandler[HandlerState, T, R, E]{scheduled: handler},
		func(envelope EndpointEnvelope) (scheduleEndpointInput[T], error) {
			if !envelope.Scheduled {
				value, err := serde.Deserialize(envelope.Payload)
				return scheduleEndpointInput[T]{value: value}, err
			}
			if envelope.ScheduleID == "" || envelope.ScheduledAtNano == 0 || envelope.FiredAtNano == 0 {
				return scheduleEndpointInput[T]{}, fmt.Errorf("invalid Temporal schedule envelope for endpoint %d", envelope.EndpointID)
			}
			return scheduleEndpointInput[T]{
				scheduled: true,
				trigger: runtime.NewScheduleTrigger(
					envelope.EndpointID, envelope.ScheduleID,
					time.Unix(0, envelope.ScheduledAtNano), time.Unix(0, envelope.FiredAtNano),
					runtime.ScheduleBackendTemporal,
				),
			}, nil
		}, nil,
	)
}

type scheduleEndpointHandler[T, R, E any] struct {
	function runtime.ScheduleEndpointFunction[T]
}

func (handler scheduleEndpointHandler[T, R, E]) BeginRequest(
	ctx context.Context,
	_ StreamContext[T, R, E],
) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (handler scheduleEndpointHandler[T, R, E]) ConsumeMessage(
	ctx context.Context,
	sc StreamContext[T, R, E],
	_ struct{},
	trigger runtime.ScheduleTrigger,
) error {
	handler.function.OnTrigger(ctx, trigger, runtime.CollectFunc[T](sc.Collect))
	return nil
}

func (scheduleEndpointHandler[T, R, E]) EndRequest(
	context.Context,
	StreamContext[T, R, E],
	error,
	struct{},
) {
}

// MakeScheduleFunctionEndpointConsumer binds a Temporal Schedule trigger to a
// normal typed input through the same user function contract as local Cron.
func MakeScheduleFunctionEndpointConsumer[T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	function runtime.ScheduleEndpointFunction[T],
) (runtime.Consumer[T], error) {
	if function == nil {
		return nil, fmt.Errorf("Temporal schedule endpoint function is nil for stream %q", stream.GetName())
	}
	return MakeScheduleEndpointConsumer[struct{}](
		stream,
		scheduleEndpointHandler[T, R, E]{function: function},
	)
}

// MakeScheduleFunctionWorkflowEndpointConsumer is the Workflow equivalent of
// MakeScheduleFunctionEndpointConsumer with a statically generated Workflow.
func MakeScheduleFunctionWorkflowEndpointConsumer[T, R, E any](
	stream runtime.TypedInputStream[T, R, E],
	function runtime.ScheduleEndpointFunction[T],
	workflowHandler WorkflowEndpointHandler,
) (runtime.Consumer[T], error) {
	if function == nil {
		return nil, fmt.Errorf("Temporal schedule endpoint function is nil for stream %q", stream.GetName())
	}
	serde := stream.GetSerde()
	return makeEndpointConsumer(
		stream,
		scheduleOrValueEndpointHandler[struct{}, T, R, E]{
			scheduled: scheduleEndpointHandler[T, R, E]{function: function},
		},
		func(envelope EndpointEnvelope) (scheduleEndpointInput[T], error) {
			if !envelope.Scheduled {
				value, err := serde.Deserialize(envelope.Payload)
				return scheduleEndpointInput[T]{value: value}, err
			}
			if envelope.ScheduleID == "" || envelope.ScheduledAtNano == 0 || envelope.FiredAtNano == 0 {
				return scheduleEndpointInput[T]{}, fmt.Errorf("invalid Temporal schedule envelope for endpoint %d", envelope.EndpointID)
			}
			return scheduleEndpointInput[T]{
				scheduled: true,
				trigger: runtime.NewScheduleTrigger(
					envelope.EndpointID, envelope.ScheduleID,
					time.Unix(0, envelope.ScheduledAtNano), time.Unix(0, envelope.FiredAtNano),
					runtime.ScheduleBackendTemporal,
				),
			}, nil
		},
		workflowHandler,
	)
}

var _ runtime.DataSource = (*inputDataSource)(nil)
var _ runtime.InputEndpoint = (*inputEndpoint)(nil)
