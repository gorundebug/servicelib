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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	sdkotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/workflow"

	servicetracing "github.com/gorundebug/servicelib/runtime/environment/tracing"
)

// workflowTracing adapts ServiceLib graph spans to the replay-safe Temporal
// interceptor span. Calls are suppressed by the durable context recording
// policy during replay; exporters therefore observe each graph span once.
type workflowTracing struct {
	root oteltrace.Span
}

func newWorkflowTracing(ctx workflow.Context) servicetracing.Tracing {
	root, _ := sdkotel.SpanFromWorkflowContext(ctx)
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
	root   oteltrace.Span
}

func (t *workflowTracer) Start(
	ctx context.Context,
	name string,
	attrs ...servicetracing.Attribute,
) (context.Context, servicetracing.Span) {
	if !oteltrace.SpanContextFromContext(ctx).IsValid() && t.root != nil {
		ctx = oteltrace.ContextWithSpan(ctx, t.root)
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
		switch typed := value.Value.(type) {
		case string:
			result = append(result, attribute.String(value.Key, typed))
		case int:
			result = append(result, attribute.Int(value.Key, typed))
		case int64:
			result = append(result, attribute.Int64(value.Key, typed))
		case float64:
			result = append(result, attribute.Float64(value.Key, typed))
		case bool:
			result = append(result, attribute.Bool(value.Key, typed))
		default:
			result = append(result, attribute.String(value.Key, fmt.Sprint(typed)))
		}
	}
	return result
}

var _ servicetracing.Tracing = (*workflowTracing)(nil)
var _ servicetracing.Tracer = (*workflowTracer)(nil)
var _ servicetracing.Span = workflowSpan{}
