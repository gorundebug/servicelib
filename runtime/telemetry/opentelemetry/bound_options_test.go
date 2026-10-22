// Regression coverage for immutable options on bound metric instruments.

package opentelemetry

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestBoundOptionsPreserveMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	m := &Metrics{meter: provider.Meter("bound-options")}
	labels := metrics.Labels{"stage": "auction", "service": "adserver", "role": "internal"}
	counterOpts := func(name string) metrics.CounterOpts {
		return metrics.CounterOpts{Opts: metrics.Opts{Name: name, ConstLabels: labels}}
	}
	histogramOpts := func(name string) metrics.HistogramOpts {
		return metrics.HistogramOpts{Opts: metrics.Opts{Name: name, ConstLabels: labels}, Buckets: []float64{1, 2, 4}}
	}
	i, err := m.Int64Counter(counterOpts("integer"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := m.Float64Counter(counterOpts("float"))
	if err != nil {
		t.Fatal(err)
	}
	ih, err := m.Int64Histogram(histogramOpts("integer_hist"))
	if err != nil {
		t.Fatal(err)
	}
	fh, err := m.Float64Histogram(histogramOpts("float_hist"))
	if err != nil {
		t.Fatal(err)
	}
	iv, err := m.Int64CounterVec(counterOpts("integer_vec"))
	if err != nil {
		t.Fatal(err)
	}
	fv, err := m.Float64CounterVec(counterOpts("float_vec"))
	if err != nil {
		t.Fatal(err)
	}
	ihv, err := m.Int64HistogramVec(histogramOpts("integer_hist_vec"))
	if err != nil {
		t.Fatal(err)
	}
	fhv, err := m.Float64HistogramVec(histogramOpts("float_hist_vec"))
	if err != nil {
		t.Fatal(err)
	}
	ib, fb, ihb, fhb := iv.With(labels), fv.With(labels), ihv.With(labels), fhv.With(labels)
	// Binding snapshots the labels. Later caller mutation must not alter a series.
	labels["stage"] = "mutated"
	delete(labels, "role")
	observe := func(ctx context.Context) {
		i.Inc(ctx)
		i.Add(ctx, 3)
		f.Add(ctx, .25)
		ih.Observe(ctx, 2)
		fh.Observe(ctx, 1.5)
		ib.Inc(ctx)
		ib.Add(ctx, 3)
		fb.Add(ctx, .25)
		ihb.Observe(ctx, 2)
		fhb.Observe(ctx, 1.5)
	}
	observe(metrics.WithoutRecording(context.Background()))
	var enabled atomic.Bool
	policy := metrics.WithRecordingPolicy(context.Background(), enabled.Load)
	observe(policy)
	enabled.Store(true)
	observe(policy)
	const workers, iterations = 16, 200
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				observe(context.Background())
			}
		}()
	}
	wg.Wait()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	expected := uint64(workers*iterations + 1)
	seen := map[string]bool{}
	checkLabels := func(attrs attribute.Set) {
		t.Helper()
		for key, want := range map[string]string{"stage": "auction", "service": "adserver", "role": "internal"} {
			value, ok := attrs.Value(attribute.Key(key))
			if !ok || value.AsString() != want {
				t.Fatalf("label %s = %v, expected %s", key, value, want)
			}
		}
		if attrs.Len() != 3 {
			t.Fatalf("extra labels: %v", attrs)
		}
	}
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			seen[item.Name] = true
			switch value := item.Data.(type) {
			case metricdata.Sum[int64]:
				if len(value.DataPoints) != 1 || value.DataPoints[0].Value != int64(expected)*4 {
					t.Fatalf("%s: %+v", item.Name, value)
				}
				checkLabels(value.DataPoints[0].Attributes)
			case metricdata.Sum[float64]:
				if len(value.DataPoints) != 1 || value.DataPoints[0].Value != float64(expected)*.25 {
					t.Fatalf("%s: %+v", item.Name, value)
				}
				checkLabels(value.DataPoints[0].Attributes)
			case metricdata.Histogram[int64]:
				if len(value.DataPoints) != 1 || value.DataPoints[0].Count != expected || value.DataPoints[0].Sum != int64(expected)*2 {
					t.Fatalf("%s: %+v", item.Name, value)
				}
				checkLabels(value.DataPoints[0].Attributes)
			case metricdata.Histogram[float64]:
				if len(value.DataPoints) != 1 || value.DataPoints[0].Count != expected || value.DataPoints[0].Sum != float64(expected)*1.5 {
					t.Fatalf("%s: %+v", item.Name, value)
				}
				checkLabels(value.DataPoints[0].Attributes)
			default:
				t.Fatalf("unexpected aggregation %T", item.Data)
			}
		}
	}
	for _, name := range []string{"integer", "float", "integer_hist", "float_hist", "integer_vec", "float_vec", "integer_hist_vec", "float_hist_vec"} {
		if !seen[name] {
			t.Fatalf("missing %s", name)
		}
	}
	if len(seen) != 8 {
		t.Fatalf("unexpected instruments: %v", seen)
	}
}
func BenchmarkBoundCounter(b *testing.B) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	m := &Metrics{meter: provider.Meter("bound-options-benchmark")}
	counter, err := m.Int64Counter(metrics.CounterOpts{Opts: metrics.Opts{Name: "events", ConstLabels: metrics.Labels{"service": "adserver", "stream": "choose-winners", "link": "output"}}})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	counter.Inc(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		counter.Inc(ctx)
	}
}
