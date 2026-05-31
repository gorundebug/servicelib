/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package testmetrics

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

// Expect starts a fluent assertion chain.
//
//	Expect(t, engine).Counter("events_total").With("event", "error").Eq(1)
//	Expect(t, engine).Histogram("request_duration_seconds").With("connector", "http").HasObservations(3)
//	Expect(t, engine).Counter("events_total").With("event", "error").Sum().Gt(0)
func Expect(t *testing.T, engine *TestMetrics) *BaseExpectation {
	t.Helper()
	return &BaseExpectation{t: t, engine: engine}
}

// BaseExpectation is the entry point for metric-type selection.
type BaseExpectation struct {
	t      *testing.T
	engine *TestMetrics
}

func (b *BaseExpectation) Counter(name string) *CounterAssertion {
	return &CounterAssertion{t: b.t, engine: b.engine, name: name, labels: make(metrics.Labels)}
}

func (b *BaseExpectation) Gauge(name string) *GaugeAssertion {
	return &GaugeAssertion{t: b.t, engine: b.engine, name: name, labels: make(metrics.Labels)}
}

func (b *BaseExpectation) Histogram(name string) *HistogramAssertion {
	return &HistogramAssertion{t: b.t, engine: b.engine, name: name, labels: make(metrics.Labels)}
}

// ── CounterAssertion ──────────────────────────────────────────────────────────

type CounterAssertion struct {
	t      *testing.T
	engine *TestMetrics
	name   string
	labels metrics.Labels
}

// With adds a label filter to the assertion.
func (c *CounterAssertion) With(key, value string) *CounterAssertion {
	next := copyLabels(c.labels)
	next[key] = value
	return &CounterAssertion{t: c.t, engine: c.engine, name: c.name, labels: next}
}

// Eq asserts the counter value equals expected.
func (c *CounterAssertion) Eq(expected int64) {
	c.t.Helper()
	got := c.engine.Counter(c.name, c.labels).Count()
	if got != expected {
		c.t.Errorf("Counter(%q%s): expected %d, got %d",
			c.name, formatLabels(c.labels), expected, got)
	}
}

// Gt asserts the counter value is greater than min.
func (c *CounterAssertion) Gt(min int64) {
	c.t.Helper()
	got := c.engine.Counter(c.name, c.labels).Count()
	if got <= min {
		c.t.Errorf("Counter(%q%s): expected > %d, got %d",
			c.name, formatLabels(c.labels), min, got)
	}
}

// Sum aggregates all counter series that match the name and the (possibly
// partial) label set, then returns an Int64Aggregation for further assertions.
func (c *CounterAssertion) Sum() *Int64Aggregation {
	c.t.Helper()
	var total int64
	c.engine.mu.Lock()
	for key, counter := range c.engine.counters {
		if keyMatchesFilter(key, c.name, c.labels) {
			total += counter.Count()
		}
	}
	c.engine.mu.Unlock()
	return &Int64Aggregation{
		t:     c.t,
		value: total,
		desc:  fmt.Sprintf("Counter(%q%s).Sum()", c.name, formatLabels(c.labels)),
	}
}

// ── GaugeAssertion ────────────────────────────────────────────────────────────

type GaugeAssertion struct {
	t      *testing.T
	engine *TestMetrics
	name   string
	labels metrics.Labels
}

// With adds a label filter.
func (g *GaugeAssertion) With(key, value string) *GaugeAssertion {
	next := copyLabels(g.labels)
	next[key] = value
	return &GaugeAssertion{t: g.t, engine: g.engine, name: g.name, labels: next}
}

// Eq asserts the gauge value equals expected.
func (g *GaugeAssertion) Eq(expected int64) {
	g.t.Helper()
	got := g.engine.Gauge(g.name, g.labels).Value()
	if got != expected {
		g.t.Errorf("Gauge(%q%s): expected %d, got %d",
			g.name, formatLabels(g.labels), expected, got)
	}
}

// Gt asserts the gauge value is greater than min.
func (g *GaugeAssertion) Gt(min int64) {
	g.t.Helper()
	got := g.engine.Gauge(g.name, g.labels).Value()
	if got <= min {
		g.t.Errorf("Gauge(%q%s): expected > %d, got %d",
			g.name, formatLabels(g.labels), min, got)
	}
}

// ── HistogramAssertion ────────────────────────────────────────────────────────

type HistogramAssertion struct {
	t      *testing.T
	engine *TestMetrics
	name   string
	labels metrics.Labels
}

// With adds a label filter.
func (h *HistogramAssertion) With(key, value string) *HistogramAssertion {
	next := copyLabels(h.labels)
	next[key] = value
	return &HistogramAssertion{t: h.t, engine: h.engine, name: h.name, labels: next}
}

// HasObservations asserts the histogram has exactly n observations.
func (h *HistogramAssertion) HasObservations(expected int64) {
	h.t.Helper()
	got := h.engine.Histogram(h.name, h.labels).Count()
	if got != expected {
		h.t.Errorf("Histogram(%q%s): expected %d observations, got %d",
			h.name, formatLabels(h.labels), expected, got)
	}
}

// SumEq asserts the sum of histogram observations equals expected (within delta).
func (h *HistogramAssertion) SumEq(expected, delta float64) {
	h.t.Helper()
	got := h.engine.Histogram(h.name, h.labels).Sum()
	if got < expected-delta || got > expected+delta {
		h.t.Errorf("Histogram(%q%s).Sum(): expected %g ±%g, got %g",
			h.name, formatLabels(h.labels), expected, delta, got)
	}
}

// SumGt asserts the sum of histogram observations is greater than min.
func (h *HistogramAssertion) SumGt(min float64) {
	h.t.Helper()
	got := h.engine.Histogram(h.name, h.labels).Sum()
	if got <= min {
		h.t.Errorf("Histogram(%q%s).Sum(): expected > %g, got %g",
			h.name, formatLabels(h.labels), min, got)
	}
}

// ── Int64Aggregation ──────────────────────────────────────────────────────────

// Int64Aggregation holds a pre-computed int64 value and supports terminal assertions.
type Int64Aggregation struct {
	t     *testing.T
	value int64
	desc  string
}

// Eq asserts the aggregated value equals expected.
func (a *Int64Aggregation) Eq(expected int64) {
	a.t.Helper()
	if a.value != expected {
		a.t.Errorf("%s: expected %d, got %d", a.desc, expected, a.value)
	}
}

// Gt asserts the aggregated value is greater than min.
func (a *Int64Aggregation) Gt(min int64) {
	a.t.Helper()
	if a.value <= min {
		a.t.Errorf("%s: expected > %d, got %d", a.desc, min, a.value)
	}
}

// ── internal helpers ──────────────────────────────────────────────────────────

func copyLabels(src metrics.Labels) metrics.Labels {
	dst := make(metrics.Labels, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func formatLabels(labels metrics.Labels) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// keyMatchesFilter returns true when the storage key (produced by metricKey)
// belongs to the given metric name and contains every label in filter as a
// subset of its label set.
func keyMatchesFilter(storageKey, name string, filter metrics.Labels) bool {
	prefix := name + "\x00"
	if !strings.HasPrefix(storageKey, prefix) {
		return false
	}
	labelPart := storageKey[len(prefix):]
	for k, v := range filter {
		if !strings.Contains(labelPart, k+"="+v+"\x00") {
			return false
		}
	}
	return true
}
