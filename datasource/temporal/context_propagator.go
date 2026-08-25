/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

const (
	temporalHeaderStreamID         = "x-stream-id"
	temporalHeaderPriority         = "servicelib-priority"
	temporalHeaderDeadlineUnixNano = "servicelib-deadline-unix-nano"
)

var temporalCarrierKeys = [...]string{
	"traceparent",
	"tracestate",
	"baggage",
	"x-trace",
	temporalHeaderStreamID,
	temporalHeaderPriority,
	temporalHeaderDeadlineUnixNano,
}

type temporalCarrierContextKey struct{}

// temporalContextPropagator carries the ordinary ServiceLib MessageContext
// through Temporal's native Header field. The Workflow only stores and forwards
// serializable strings; it never inspects tracing state or performs runtime IO.
type temporalContextPropagator struct {
	tracing tracing.Tracing
}

func (p temporalContextPropagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	carrier := make(map[string]string)
	if p.tracing != nil {
		p.tracing.Inject(ctx, carrier)
	}
	if tracing.SamplingEnabled(ctx) {
		carrier["x-trace"] = "1"
	}
	if streamID, ok := runtime.StreamIdFromContext(ctx); ok {
		carrier[temporalHeaderStreamID] = streamID.GetID()
	}
	if priority, ok := runtime.PriorityFromContext(ctx); ok {
		carrier[temporalHeaderPriority] = strconv.Itoa(priority)
	}
	if deadline, ok := ctx.Deadline(); ok {
		carrier[temporalHeaderDeadlineUnixNano] = strconv.FormatInt(deadline.UTC().UnixNano(), 10)
	}
	return writeTemporalCarrier(writer, carrier)
}

func (p temporalContextPropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	carrier, err := readTemporalCarrier(reader)
	if err != nil {
		return ctx, err
	}
	return p.extractContext(ctx, carrier), nil
}

func (p temporalContextPropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	carrier, _ := ctx.Value(temporalCarrierContextKey{}).(map[string]string)
	return writeTemporalCarrier(writer, carrier)
}

func (p temporalContextPropagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	carrier, err := readTemporalCarrier(reader)
	if err != nil {
		return ctx, err
	}
	if len(carrier) == 0 {
		return ctx, nil
	}
	return workflow.WithValue(ctx, temporalCarrierContextKey{}, carrier), nil
}

func (p temporalContextPropagator) extractContext(ctx context.Context, carrier map[string]string) context.Context {
	ctx = p.extractWorkflowContext(ctx, carrier)
	if rawDeadline := carrier[temporalHeaderDeadlineUnixNano]; rawDeadline != "" {
		if nanos, err := strconv.ParseInt(rawDeadline, 10, 64); err == nil {
			deadline := time.Unix(0, nanos)
			if current, present := ctx.Deadline(); !present || deadline.Before(current) {
				withDeadline, _ := context.WithDeadline(ctx, deadline)
				ctx = withDeadline
			}
		}
	}
	return ctx
}

// extractWorkflowContext deliberately excludes process-local deadline timers.
// Workflow code receives its time semantics from Temporal; the absolute
// deadline remains in the serializable envelope and is applied by the Activity
// adapter after the durable boundary.
func (p temporalContextPropagator) extractWorkflowContext(ctx context.Context, carrier map[string]string) context.Context {
	if p.tracing != nil && len(carrier) > 0 {
		ctx = p.tracing.Extract(ctx, carrier)
	}
	if tracing.SamplingRequestedByCarrier(carrier) {
		ctx = tracing.EnableSampling(ctx)
	}
	if streamID := carrier[temporalHeaderStreamID]; streamID != "" {
		ctx = runtime.WithStreamId(ctx, streamID)
	}
	if rawPriority := carrier[temporalHeaderPriority]; rawPriority != "" {
		if priority, err := strconv.Atoi(rawPriority); err == nil {
			ctx = runtime.WithPriority(ctx, priority)
		}
	}
	return ctx
}

func writeTemporalCarrier(writer workflow.HeaderWriter, carrier map[string]string) error {
	dataConverter := converter.GetDefaultDataConverter()
	for _, key := range temporalCarrierKeys {
		value := carrier[key]
		if value == "" {
			continue
		}
		payload, err := dataConverter.ToPayload(value)
		if err != nil {
			return fmt.Errorf("encode Temporal header %q: %w", key, err)
		}
		writer.Set(key, payload)
	}
	return nil
}

func readTemporalCarrier(reader workflow.HeaderReader) (map[string]string, error) {
	dataConverter := converter.GetDefaultDataConverter()
	carrier := make(map[string]string)
	for _, key := range temporalCarrierKeys {
		payload, present := reader.Get(key)
		if !present {
			continue
		}
		var value string
		if err := dataConverter.FromPayload(payload, &value); err != nil {
			return nil, fmt.Errorf("decode Temporal header %q: %w", key, err)
		}
		if value != "" {
			carrier[key] = value
		}
	}
	return carrier, nil
}

var _ workflow.ContextPropagator = temporalContextPropagator{}
