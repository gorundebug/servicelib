/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package prometeus

import (
	"github.com/gorundebug/servicelib/telemetry/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
}

func labelsToPrometeusLabels(labels metrics.Labels) prometheus.Labels {
	prometheusLabels := prometheus.Labels{}
	for k, v := range labels {
		prometheusLabels[k] = v
	}
	return prometheusLabels
}

func counterOptsToPrometeusCounterOtps(opts metrics.CounterOpts) prometheus.CounterOpts {
	return prometheus.CounterOpts{
		Namespace:   opts.Namespace,
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometeusLabels(opts.ConstLabels),
	}
}

func summaryOptsToPrometeusSummaryOpts(opts metrics.SummaryOpts) prometheus.SummaryOpts {
	return prometheus.SummaryOpts{
		Namespace:   opts.Namespace,
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometeusLabels(opts.ConstLabels),
		Objectives:  opts.Objectives,
		MaxAge:      opts.MaxAge,
		AgeBuckets:  opts.AgeBuckets,
		BufCap:      opts.BufCap,
	}
}

func gaugeOptsToPrometeusGaugeOpts(opts metrics.GaugeOpts) prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace:   opts.Namespace,
		Name:        opts.Name,
		Help:        opts.Help,
		Subsystem:   opts.Subsystem,
		ConstLabels: labelsToPrometeusLabels(opts.ConstLabels),
	}
}

func histogramOptsToPrometeusHistogramOpts(opts metrics.HistogramOpts) prometheus.HistogramOpts {
	return prometheus.HistogramOpts{
		Namespace:                       opts.Namespace,
		Name:                            opts.Name,
		Help:                            opts.Help,
		Subsystem:                       opts.Subsystem,
		ConstLabels:                     labelsToPrometeusLabels(opts.ConstLabels),
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
	return &CounterVec{counterVec: promauto.NewCounterVec(counterOptsToPrometeusCounterOtps(opts), labelNames)}
}

type CounterVec struct {
	counterVec *prometheus.CounterVec
}

func (p *CounterVec) WithLabelValues(lvs ...string) metrics.Counter {
	return p.counterVec.WithLabelValues(lvs...)
}

func (mf Metrics) Counter(opts metrics.CounterOpts) metrics.Counter {
	return promauto.NewCounter(counterOptsToPrometeusCounterOtps(opts))
}

type SummaryVec struct {
	summaryVec *prometheus.SummaryVec
}

func (p *SummaryVec) WithLabelValues(lvs ...string) metrics.Summary {
	return p.summaryVec.WithLabelValues(lvs...)
}

func (mf Metrics) SummaryVec(opts metrics.SummaryOpts, labelNames []string) metrics.SummaryVec {
	return &SummaryVec{summaryVec: promauto.NewSummaryVec(summaryOptsToPrometeusSummaryOpts(opts), labelNames)}
}

func (mf Metrics) Summary(opts metrics.SummaryOpts) metrics.Summary {
	return promauto.NewSummary(summaryOptsToPrometeusSummaryOpts(opts))
}

func (mf Metrics) GaugeVec(opts metrics.GaugeOpts, labelNames []string) metrics.GaugeVec {
	return &GaugeVec{gaugeVec: promauto.NewGaugeVec(gaugeOptsToPrometeusGaugeOpts(opts), labelNames)}
}

func (mf Metrics) Gauge(opts metrics.GaugeOpts) metrics.Gauge {
	return promauto.NewGauge(gaugeOptsToPrometeusGaugeOpts(opts))
}

type GaugeVec struct {
	gaugeVec *prometheus.GaugeVec
}

func (p *GaugeVec) WithLabelValues(lvs ...string) metrics.Gauge {
	return p.gaugeVec.WithLabelValues(lvs...)
}

func (mf Metrics) HistogramVec(opts metrics.HistogramOpts, labelNames []string) metrics.HistogramVec {
	return &HistogramVec{histogramVec: promauto.NewHistogramVec(histogramOptsToPrometeusHistogramOpts(opts), labelNames)}
}

func (mf Metrics) Histogram(opts metrics.HistogramOpts) metrics.Histogram {
	return promauto.NewHistogram(histogramOptsToPrometeusHistogramOpts(opts))
}

type HistogramVec struct {
	histogramVec *prometheus.HistogramVec
}

func (p *HistogramVec) WithLabelValues(lvs ...string) metrics.Histogram {
	return p.histogramVec.WithLabelValues(lvs...)
}
