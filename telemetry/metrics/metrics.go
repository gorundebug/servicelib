/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package metrics

import "time"

type Counter interface {
	Inc()
}

type CounterVec interface {
	WithLabelValues(lvs ...string) Counter
}

type Gauge interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	Sub(float64)
	SetToCurrentTime()
}

type GaugeVec interface {
	WithLabelValues(lvs ...string) Gauge
}

type Histogram interface {
	Observe(float64)
}

type HistogramVec interface {
	WithLabelValues(lvs ...string) Histogram
}

type Summary interface {
	Observe(float64)
}

type SummaryVec interface {
	WithLabelValues(lvs ...string) Summary
}

type Labels map[string]string

type Opts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Help        string
	ConstLabels Labels
}

type CounterOpts struct {
	Opts
}

type SummaryOpts struct {
	Opts
	Objectives map[float64]float64
	MaxAge     time.Duration
	AgeBuckets uint32
	BufCap     uint32
}

type GaugeOpts struct {
	Opts
}

type HistogramOpts struct {
	Opts
	Buckets                         []float64
	NativeHistogramBucketFactor     float64
	NativeHistogramZeroThreshold    float64
	NativeHistogramMaxBucketNumber  uint32
	NativeHistogramMinResetDuration time.Duration
	NativeHistogramMaxZeroThreshold float64
	NativeHistogramMaxExemplars     int
	NativeHistogramExemplarTTL      time.Duration
}

type Metrics interface {
	CounterVec(opts CounterOpts, labelNames []string) CounterVec
	Counter(opts CounterOpts) Counter
	SummaryVec(opts SummaryOpts, labelNames []string) SummaryVec
	Summary(opts SummaryOpts) Summary
	GaugeVec(opts GaugeOpts, labelNames []string) GaugeVec
	Gauge(opts GaugeOpts) Gauge
	HistogramVec(opts HistogramOpts, labelNames []string) HistogramVec
	Histogram(opts HistogramOpts) Histogram
}
