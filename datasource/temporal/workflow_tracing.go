/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/workflow"

	servicetracing "github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// workflowTracing adapts ServiceLib graph spans to the W3C parent carried in
// Temporal Header. Calls are suppressed by the durable context recording
// policy during replay; exporters therefore observe each graph span once.
type workflowTracing struct {
	root context.Context
}

func newWorkflowTracing(ctx workflow.Context) servicetracing.Tracing {
	carrier, _ := ctx.Value(temporalCarrierContextKey{}).(map[string]string)
	root := otel.GetTextMapPropagator().Extract(
		context.Background(), propagation.MapCarrier(carrier),
	)
	return &workflowTracing{root: root}
}

func (t *workflowTracing) Tracer(name string) servicetracing.Tracer {
	return &workflowTracer{tracer: otel.GetTracerProvider().Tracer(name), root: t.root}
}

func (*workflowTracing) Inject(ctx context.Context, carrier map[string]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))
}

func (*workflowTracing) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

type workflowTracer struct {
	tracer oteltrace.Tracer
	root   context.Context
}

func (t *workflowTracer) Start(
	ctx context.Context,
	name string,
	attrs ...servicetracing.Attribute,
) (context.Context, servicetracing.Span) {
	if !oteltrace.SpanContextFromContext(ctx).IsValid() && t.root != nil {
		if root := oteltrace.SpanContextFromContext(t.root); root.IsValid() {
			ctx = oteltrace.ContextWithRemoteSpanContext(ctx, root)
		}
	}
	ctx, span := t.tracer.Start(ctx, name, oteltrace.WithAttributes(workflowAttributes(attrs)...))
	return ctx, workflowSpan{span: span}
}

type workflowSpan struct {
	span oteltrace.Span
}

func (s workflowSpan) End() { s.span.End() }

func (s workflowSpan) SetAttributes(attrs ...servicetracing.Attribute) {
	s.span.SetAttributes(workflowAttributes(attrs)...)
}

func (s workflowSpan) RecordError(err error) { s.span.RecordError(err) }

func (s workflowSpan) SetStatus(code servicetracing.StatusCode, description string) {
	switch code {
	case servicetracing.StatusOK:
		s.span.SetStatus(codes.Ok, description)
	case servicetracing.StatusError:
		s.span.SetStatus(codes.Error, description)
	default:
		s.span.SetStatus(codes.Unset, description)
	}
}

func (s workflowSpan) AddEvent(name string, attrs ...servicetracing.Attribute) {
	s.span.AddEvent(name, oteltrace.WithAttributes(workflowAttributes(attrs)...))
}

func (s workflowSpan) SpanContext() servicetracing.SpanContext {
	ctx := s.span.SpanContext()
	return servicetracing.SpanContext{
		TraceID: ctx.TraceID().String(), SpanID: ctx.SpanID().String(), IsValid: ctx.IsValid(),
	}
}

func workflowAttributes(values []servicetracing.Attribute) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, len(values))
	for _, value := range values {
		switch value.Value.Type() {
		case servicetracing.StringAttribute:
			result = append(result, attribute.String(value.Key, value.Value.AsString()))
		case servicetracing.Int64Attribute:
			result = append(result, attribute.Int64(value.Key, value.Value.AsInt64()))
		case servicetracing.Float64Attribute:
			result = append(result, attribute.Float64(value.Key, value.Value.AsFloat64()))
		case servicetracing.BoolAttribute:
			result = append(result, attribute.Bool(value.Key, value.Value.AsBool()))
		default:
			result = append(result, attribute.KeyValue{Key: attribute.Key(value.Key)})
		}
	}
	return result
}

var _ servicetracing.Tracing = (*workflowTracing)(nil)
var _ servicetracing.Tracer = (*workflowTracer)(nil)
var _ servicetracing.Span = workflowSpan{}
