/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package testmetrics

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

func TestCounter_ScalarViaScope(t *testing.T) {
	m := New()
	scope := m.Scope("datasource_endpoint", metrics.Labels{
		"connector": "http",
		"endpoint":  "data",
	})

	c, err := scope.Counter("messages_total", "help", nil)
	if err != nil {
		t.Fatal(err)
	}

	c.Inc(context.Background())
	c.Inc(context.Background())
	c.Add(context.Background(), 3)

	got := m.Counter("datasource_endpoint_messages_total", metrics.Labels{
		"connector": "http",
		"endpoint":  "data",
	}).Count()
	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestCounter_WithExtraLabels(t *testing.T) {
	m := New()
	scope := m.Scope("datasource_endpoint", metrics.Labels{
		"connector": "http",
		"endpoint":  "data",
	})

	c, err := scope.Counter("events_total", "help", metrics.Labels{"event": "request_error"})
	if err != nil {
		t.Fatal(err)
	}
	c.Inc(context.Background())

	got := m.Counter("datasource_endpoint_events_total", metrics.Labels{
		"connector": "http",
		"endpoint":  "data",
		"event":     "request_error",
	}).Count()
	if got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
}

func TestGauge_ViaScope(t *testing.T) {
	m := New()
	scope := m.Scope("task_pool", metrics.Labels{"service": "svc", "name": "pool1"})

	g, err := scope.Gauge("queue_length", "help", nil)
	if err != nil {
		t.Fatal(err)
	}
	g.Inc()
	g.Inc()
	g.Dec()

	got := m.Gauge("task_pool_queue_length", metrics.Labels{"service": "svc", "name": "pool1"}).Value()
	if got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
}

func TestHistogram_ViaScope(t *testing.T) {
	m := New()
	scope := m.Scope("datasource_endpoint", metrics.Labels{"connector": "http", "endpoint": "data"})

	h, err := scope.Histogram("request_duration_seconds", "help", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.Observe(context.Background(), 0.1)
	h.Observe(context.Background(), 0.2)

	hist := m.Histogram("datasource_endpoint_request_duration_seconds", metrics.Labels{
		"connector": "http",
		"endpoint":  "data",
	})
	if hist.Count() != 2 {
		t.Fatalf("expected count=2, got %d", hist.Count())
	}
	if hist.Sum() < 0.29 || hist.Sum() > 0.31 {
		t.Fatalf("expected sum≈0.3, got %f", hist.Sum())
	}
}

func TestCounterVec_ViaScope(t *testing.T) {
	m := New()
	scope := m.Scope("stream", metrics.Labels{"service": "svc"})

	cv, err := scope.CounterVec("messages_total", "help")
	if err != nil {
		t.Fatal(err)
	}
	cv.With(metrics.Labels{"from": "a", "to": "b"}).Inc(context.Background())
	cv.With(metrics.Labels{"from": "a", "to": "b"}).Inc(context.Background())
	cv.With(metrics.Labels{"from": "x", "to": "y"}).Inc(context.Background())

	got := m.Counter("stream_messages_total", metrics.Labels{
		"service": "svc",
		"from":    "a",
		"to":      "b",
	}).Count()
	if got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
}

func TestScopeCaching(t *testing.T) {
	m := New()
	s1 := m.Scope("prefix", metrics.Labels{"k": "v"})
	s2 := m.Scope("prefix", metrics.Labels{"k": "v"})
	if s1 != s2 {
		t.Fatal("expected same scope instance to be returned")
	}
}

func TestZeroValueBeforeRecord(t *testing.T) {
	m := New()
	// Counter not yet created by framework — should return 0, not panic.
	if m.Counter("nonexistent", nil).Count() != 0 {
		t.Fatal("expected 0 for unrecorded counter")
	}
}

func TestReset(t *testing.T) {
	m := New()
	scope := m.Scope("svc", nil)
	c, _ := scope.Counter("ops_total", "help", nil)
	c.Inc(context.Background())

	m.Reset()

	if m.Counter("svc_ops_total", nil).Count() != 0 {
		t.Fatal("expected 0 after Reset")
	}
}

// ── Expect DSL tests ──────────────────────────────────────────────────────────

func TestExpect_Counter_Eq(t *testing.T) {
	m := New()
	scope := m.Scope("datasource_endpoint", metrics.Labels{"connector": "http", "endpoint": "data"})
	c, _ := scope.Counter("events_total", "help", metrics.Labels{"event": "request_error"})
	c.Inc(context.Background())
	c.Inc(context.Background())

	Expect(t, m).
		Counter("datasource_endpoint_events_total").
		With("connector", "http").
		With("endpoint", "data").
		With("event", "request_error").
		Eq(2)
}

func TestExpect_Counter_Gt(t *testing.T) {
	m := New()
	scope := m.Scope("svc", nil)
	c, _ := scope.Counter("ops_total", "help", nil)
	c.Inc(context.Background())

	Expect(t, m).Counter("svc_ops_total").Gt(0)
}

func TestExpect_Counter_Sum(t *testing.T) {
	m := New()
	s1 := m.Scope("stream", metrics.Labels{"service": "svc", "stream": "a"})
	s2 := m.Scope("stream", metrics.Labels{"service": "svc", "stream": "b"})
	c1, _ := s1.Counter("messages_total", "help", nil)
	c2, _ := s2.Counter("messages_total", "help", nil)
	c1.Add(context.Background(), 3)
	c2.Add(context.Background(), 5)

	// Sum across both streams
	Expect(t, m).
		Counter("stream_messages_total").
		With("service", "svc").
		Sum().Eq(8)
}

func TestExpect_Gauge_Eq(t *testing.T) {
	m := New()
	scope := m.Scope("task_pool", metrics.Labels{"service": "svc", "name": "pool1"})
	g, _ := scope.Gauge("queue_length", "help", nil)
	g.Inc()
	g.Inc()
	g.Dec()

	Expect(t, m).
		Gauge("task_pool_queue_length").
		With("service", "svc").
		With("name", "pool1").
		Eq(1)
}

func TestExpect_Histogram_HasObservations(t *testing.T) {
	m := New()
	scope := m.Scope("datasource_endpoint", metrics.Labels{"connector": "http", "endpoint": "data"})
	h, _ := scope.Histogram("request_duration_seconds", "help", nil)
	h.Observe(context.Background(), 0.1)
	h.Observe(context.Background(), 0.2)
	h.Observe(context.Background(), 0.05)

	Expect(t, m).
		Histogram("datasource_endpoint_request_duration_seconds").
		With("connector", "http").
		With("endpoint", "data").
		HasObservations(3)
}

func TestExpect_Histogram_SumEq(t *testing.T) {
	m := New()
	scope := m.Scope("svc", nil)
	h, _ := scope.Histogram("latency_seconds", "help", nil)
	h.Observe(context.Background(), 0.1)
	h.Observe(context.Background(), 0.2)

	Expect(t, m).
		Histogram("svc_latency_seconds").
		SumEq(0.3, 0.01)
}

func TestExpect_Histogram_SumGt(t *testing.T) {
	m := New()
	scope := m.Scope("svc", nil)
	h, _ := scope.Histogram("latency_seconds", "help", nil)
	h.Observe(context.Background(), 1.5)

	Expect(t, m).Histogram("svc_latency_seconds").SumGt(0)
}
