/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package prometheus

import (
	"fmt"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"sync"
)

type Metrics struct {
	Namespace string
}

func labelsToPrometheusLabels(labels metrics.Labels) prometheus.Labels {
	prometheusLabels := prometheus.Labels{}
	for k, v := range labels {
		prometheusLabels[k] = v
	}
	return prometheusLabels
}

func counterOptsToPrometheusCounterOtps(namespace string, opts metrics.CounterOpts) prometheus.CounterOpts {
	if len(namespace) > 0 && len(opts.Namespace) > 0 {
		namespace += "_"
	}
	return prometheus.CounterOpts{
		Namespace:   fmt.Sprintf("%s%s", namespace, opts.Namespace),
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometheusLabels(opts.ConstLabels),
	}
}

func summaryOptsToPrometheusSummaryOpts(namespace string, opts metrics.SummaryOpts) prometheus.SummaryOpts {
	if len(namespace) > 0 && len(opts.Namespace) > 0 {
		namespace += "_"
	}
	return prometheus.SummaryOpts{
		Namespace:   fmt.Sprintf("%s%s", namespace, opts.Namespace),
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometheusLabels(opts.ConstLabels),
		Objectives:  opts.Objectives,
		MaxAge:      opts.MaxAge,
		AgeBuckets:  opts.AgeBuckets,
		BufCap:      opts.BufCap,
	}
}

func gaugeOptsToPrometheusGaugeOpts(namespace string, opts metrics.GaugeOpts) prometheus.GaugeOpts {
	if len(namespace) > 0 && len(opts.Namespace) > 0 {
		namespace += "_"
	}
	return prometheus.GaugeOpts{
		Namespace:   fmt.Sprintf("%s%s", namespace, opts.Namespace),
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometheusLabels(opts.ConstLabels),
	}
}

func histogramOptsToPrometheusHistogramOpts(namespace string, opts metrics.HistogramOpts) prometheus.HistogramOpts {
	if len(namespace) > 0 && len(opts.Namespace) > 0 {
		namespace += "_"
	}
	return prometheus.HistogramOpts{
		Namespace:                       fmt.Sprintf("%s%s", namespace, opts.Namespace),
		Name:                            opts.Name,
		Help:                            opts.Help,
		Subsystem:                       opts.Subsystem,
		ConstLabels:                     labelsToPrometheusLabels(opts.ConstLabels),
		Buckets:                         opts.Buckets,
		NativeHistogramBucketFactor:     opts.NativeHistogramBucketFactor,
		NativeHistogramZeroThreshold:    opts.NativeHistogramZeroThreshold,
		NativeHistogramMaxBucketNumber:  opts.NativeHistogramMaxBucketNumber,
		NativeHistogramMinResetDuration: opts.NativeHistogramMinResetDuration,
		NativeHistogramMaxZeroThreshold: opts.NativeHistogramMaxZeroThreshold,
		NativeHistogramMaxExemplars:     opts.NativeHistogramMaxExemplars,
		NativeHistogramExemplarTTL:      opts.NativeHistogramExemplarTTL,
	}
}

func (mf Metrics) CounterVec(opts metrics.CounterOpts, labelNames []string) metrics.CounterVec {
	return &CounterVec{counterVec: promauto.NewCounterVec(counterOptsToPrometheusCounterOtps(mf.Namespace, opts), labelNames)}
}

type CounterVec struct {
	counterVec *prometheus.CounterVec
}

func (p *CounterVec) WithLabelValues(lvs ...string) metrics.Counter {
	return p.counterVec.WithLabelValues(lvs...)
}

func (mf Metrics) Counter(opts metrics.CounterOpts) metrics.Counter {
	return promauto.NewCounter(counterOptsToPrometheusCounterOtps(mf.Namespace, opts))
}

type SummaryVec struct {
	summaryVec *prometheus.SummaryVec
}

func (p *SummaryVec) WithLabelValues(lvs ...string) metrics.Summary {
	return p.summaryVec.WithLabelValues(lvs...)
}

func (mf Metrics) SummaryVec(opts metrics.SummaryOpts, labelNames []string) metrics.SummaryVec {
	return &SummaryVec{summaryVec: promauto.NewSummaryVec(summaryOptsToPrometheusSummaryOpts(mf.Namespace, opts), labelNames)}
}

func (mf Metrics) Summary(opts metrics.SummaryOpts) metrics.Summary {
	return promauto.NewSummary(summaryOptsToPrometheusSummaryOpts(mf.Namespace, opts))
}

func (mf Metrics) GaugeVec(opts metrics.GaugeOpts, labelNames []string) metrics.GaugeVec {
	return &GaugeVec{gaugeVec: promauto.NewGaugeVec(gaugeOptsToPrometheusGaugeOpts(mf.Namespace, opts), labelNames)}
}

func (mf Metrics) Gauge(opts metrics.GaugeOpts) metrics.Gauge {
	return promauto.NewGauge(gaugeOptsToPrometheusGaugeOpts(mf.Namespace, opts))
}

type GaugeVec struct {
	gaugeVec *prometheus.GaugeVec
}

func (p *GaugeVec) WithLabelValues(lvs ...string) metrics.Gauge {
	return p.gaugeVec.WithLabelValues(lvs...)
}

func (mf Metrics) HistogramVec(opts metrics.HistogramOpts, labelNames []string) metrics.HistogramVec {
	return &HistogramVec{histogramVec: promauto.NewHistogramVec(histogramOptsToPrometheusHistogramOpts(mf.Namespace, opts), labelNames)}
}

func (mf Metrics) Histogram(opts metrics.HistogramOpts) metrics.Histogram {
	return promauto.NewHistogram(histogramOptsToPrometheusHistogramOpts(mf.Namespace, opts))
}

type HistogramVec struct {
	histogramVec *prometheus.HistogramVec
}

func (p *HistogramVec) WithLabelValues(lvs ...string) metrics.Histogram {
	return p.histogramVec.WithLabelValues(lvs...)
}

var metricsEngine *MetricsEngine
var once sync.Once

type MetricsEngine struct {
	environment environment.ServiceEnvironment
	metrics     Metrics
}

func (m *MetricsEngine) Metrics() metrics.Metrics {
	return m.metrics
}

func (m *MetricsEngine) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func CreateMetricsEngine(env environment.ServiceEnvironment) metrics.MetricsEngine {
	once.Do(func() {
		metricsEngine = &MetricsEngine{
			environment: env,
			metrics:     Metrics{Namespace: env.ServiceConfig().Name},
		}
	})
	return metricsEngine
}
