package metrics

import (
	"context"
	"testing"
)

func TestNoopMetricsImplementsFullSurface(t *testing.T) {
	engine := NewNoopMetricsEngine()
	scope := engine.Metrics().Scope("test", Labels{"label": "value"})
	counter, err := scope.Counter("counter", "help", nil)
	if err != nil {
		t.Fatal(err)
	}
	gauge, err := scope.Gauge("gauge", "help", nil)
	if err != nil {
		t.Fatal(err)
	}
	histogram, err := scope.Histogram("histogram", "help", nil)
	if err != nil {
		t.Fatal(err)
	}
	counter.Inc(context.Background())
	gauge.Set(42)
	histogram.Observe(context.Background(), 1.5)
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
