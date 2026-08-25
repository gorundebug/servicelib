/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type workflowLogger struct {
	debug func(string, ...interface{})
	info  func(string, ...interface{})
	warn  func(string, ...interface{})
	error func(string, ...interface{})
}

func newWorkflowLogger(ctx workflow.Context) log.Logger {
	logger := workflow.GetLogger(ctx)
	return workflowLogger{
		debug: logger.Debug, info: logger.Info,
		warn: logger.Warn, error: logger.Error,
	}
}

func workflowLogFields(fields []log.Field) []interface{} {
	values := make([]interface{}, 0, len(fields)*2)
	for _, field := range fields {
		values = append(values, field.Key, field.Value())
	}
	return values
}

func (l workflowLogger) Debug(_ context.Context, msg string, fields ...log.Field) {
	l.debug(msg, workflowLogFields(fields)...)
}
func (l workflowLogger) Info(_ context.Context, msg string, fields ...log.Field) {
	l.info(msg, workflowLogFields(fields)...)
}
func (l workflowLogger) Warn(_ context.Context, msg string, fields ...log.Field) {
	l.warn(msg, workflowLogFields(fields)...)
}
func (l workflowLogger) Error(_ context.Context, msg string, fields ...log.Field) {
	l.error(msg, workflowLogFields(fields)...)
}

type workflowMetricFactories struct {
	counter func(string, metrics.Labels) func(int64)
	gauge   func(string, metrics.Labels) func(float64)
	timer   func(string, metrics.Labels) func(time.Duration)
}

type workflowMetrics struct{ factories workflowMetricFactories }

func newWorkflowMetrics(ctx workflow.Context) metrics.Metrics {
	handler := workflow.GetMetricsHandler(ctx)
	return workflowMetrics{factories: workflowMetricFactories{
		counter: func(name string, labels metrics.Labels) func(int64) {
			counter := handler.WithTags(labels).Counter(name)
			return counter.Inc
		},
		gauge: func(name string, labels metrics.Labels) func(float64) {
			gauge := handler.WithTags(labels).Gauge(name)
			return gauge.Update
		},
		timer: func(name string, labels metrics.Labels) func(time.Duration) {
			timer := handler.WithTags(labels).Timer(name)
			return timer.Record
		},
	}}
}

func (m workflowMetrics) Scope(prefix string, labels metrics.Labels) metrics.MetricsScope {
	return workflowMetricsScope{prefix: prefix, labels: labels, factories: m.factories}
}

type workflowMetricsScope struct {
	prefix    string
	labels    metrics.Labels
	factories workflowMetricFactories
}

func (s workflowMetricsScope) name(name string) string {
	if name == "" {
		return s.prefix
	}
	return s.prefix + "_" + name
}

func mergeWorkflowLabels(base, extra metrics.Labels) metrics.Labels {
	result := make(metrics.Labels, len(base)+len(extra))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func (s workflowMetricsScope) Counter(name, _ string, labels metrics.Labels) (metrics.Int64Counter, error) {
	return workflowCounter{s.factories.counter(s.name(name), mergeWorkflowLabels(s.labels, labels))}, nil
}
func (s workflowMetricsScope) CounterVec(name, _ string) (metrics.Int64CounterVec, error) {
	return workflowCounterVec{name: s.name(name), labels: s.labels, factory: s.factories.counter}, nil
}
func (s workflowMetricsScope) Gauge(name, _ string, labels metrics.Labels) (metrics.Int64Gauge, error) {
	return &workflowGauge{set: s.factories.gauge(s.name(name), mergeWorkflowLabels(s.labels, labels))}, nil
}
func (s workflowMetricsScope) GaugeVec(name, _ string) (metrics.Int64GaugeVec, error) {
	return workflowGaugeVec{name: s.name(name), labels: s.labels, factory: s.factories.gauge}, nil
}
func (s workflowMetricsScope) Histogram(name, _ string, labels metrics.Labels, _ ...float64) (metrics.Float64Histogram, error) {
	return workflowHistogram{s.factories.timer(s.name(name), mergeWorkflowLabels(s.labels, labels))}, nil
}
func (s workflowMetricsScope) HistogramVec(name, _ string, _ ...float64) (metrics.Float64HistogramVec, error) {
	return workflowHistogramVec{name: s.name(name), labels: s.labels, factory: s.factories.timer}, nil
}
func (workflowMetricsScope) ObservableFloat64Gauge(string, string, func() float64) error {
	// Temporal Workflows have no scrape cycle. Runtime gauges are recorded at
	// the mutation points; process-owned observable callbacks do not belong in
	// replayable code.
	return nil
}

type workflowCounter struct{ add func(int64) }

func (c workflowCounter) Inc(ctx context.Context) { c.Add(ctx, 1) }
func (c workflowCounter) Add(ctx context.Context, value int64) {
	if metrics.RecordingEnabled(ctx) {
		c.add(value)
	}
}

type workflowCounterVec struct {
	name    string
	labels  metrics.Labels
	factory func(string, metrics.Labels) func(int64)
}

func (v workflowCounterVec) With(labels metrics.Labels) metrics.Int64Counter {
	return workflowCounter{v.factory(v.name, mergeWorkflowLabels(v.labels, labels))}
}

type workflowGauge struct {
	value int64
	set   func(float64)
}

func (g *workflowGauge) Set(value int64) { g.value = value; g.set(float64(value)) }
func (g *workflowGauge) Inc()            { g.Add(1) }
func (g *workflowGauge) Dec()            { g.Sub(1) }
func (g *workflowGauge) Add(value int64) { g.Set(g.value + value) }
func (g *workflowGauge) Sub(value int64) { g.Set(g.value - value) }

type workflowGaugeVec struct {
	name    string
	labels  metrics.Labels
	factory func(string, metrics.Labels) func(float64)
}

func (v workflowGaugeVec) With(labels metrics.Labels) metrics.Int64Gauge {
	return &workflowGauge{set: v.factory(v.name, mergeWorkflowLabels(v.labels, labels))}
}
func (v workflowGaugeVec) Delete(labels metrics.Labels) {
	v.factory(v.name, mergeWorkflowLabels(v.labels, labels))(0)
}

type workflowHistogram struct{ record func(time.Duration) }

func (h workflowHistogram) Observe(ctx context.Context, value float64) {
	if metrics.RecordingEnabled(ctx) {
		h.record(time.Duration(value * float64(time.Second)))
	}
}

type workflowHistogramVec struct {
	name    string
	labels  metrics.Labels
	factory func(string, metrics.Labels) func(time.Duration)
}

func (v workflowHistogramVec) With(labels metrics.Labels) metrics.Float64Histogram {
	return workflowHistogram{v.factory(v.name, mergeWorkflowLabels(v.labels, labels))}
}

var _ log.Logger = workflowLogger{}
var _ metrics.Metrics = workflowMetrics{}
