package sarama

import (
	"testing"

	envmetrics "github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/stretchr/testify/require"
)

type fakeRegistry map[string]interface{}

func (r fakeRegistry) Each(fn func(string, interface{})) {
	for name, metric := range r {
		fn(name, metric)
	}
}

func (r fakeRegistry) Get(name string) interface{} { return r[name] }

type fakeMeter struct {
	count int64
	rate  float64
}

func (m fakeMeter) Count() int64   { return m.count }
func (m fakeMeter) Rate1() float64 { return m.rate }

type fakeCounter struct{ count int64 }

func (m fakeCounter) Count() int64 { return m.count }

type fakeMetrics struct{ scope *fakeScope }

func (m *fakeMetrics) Scope(prefix string, labels envmetrics.Labels) envmetrics.MetricsScope {
	m.scope.prefix = prefix
	m.scope.labels = labels
	return m.scope
}

type fakeScope struct {
	prefix    string
	labels    envmetrics.Labels
	callbacks map[string]func() float64
}

func (s *fakeScope) Counter(string, string, envmetrics.Labels) (envmetrics.Int64Counter, error) {
	return nil, nil
}
func (s *fakeScope) CounterVec(string, string) (envmetrics.Int64CounterVec, error) {
	return nil, nil
}
func (s *fakeScope) Gauge(string, string, envmetrics.Labels) (envmetrics.Int64Gauge, error) {
	return nil, nil
}
func (s *fakeScope) GaugeVec(string, string) (envmetrics.Int64GaugeVec, error) {
	return nil, nil
}
func (s *fakeScope) Histogram(string, string, envmetrics.Labels, ...float64) (envmetrics.Float64Histogram, error) {
	return nil, nil
}
func (s *fakeScope) HistogramVec(string, string, ...float64) (envmetrics.Float64HistogramVec, error) {
	return nil, nil
}
func (s *fakeScope) ObservableFloat64Gauge(name, _ string, fn func() float64) error {
	s.callbacks[name] = fn
	return nil
}

func TestRegisterProducerUsesDocumentedRegistryMetrics(t *testing.T) {
	scope := &fakeScope{callbacks: map[string]func() float64{}}
	target := &fakeMetrics{scope: scope}
	registry := fakeRegistry{
		"request-rate":              fakeMeter{count: 12, rate: 3.5},
		"record-send-rate":          fakeMeter{count: 9, rate: 2.25},
		"requests-in-flight":        fakeCounter{count: 2},
		"request-rate-for-broker-1": fakeMeter{},
		"request-rate-for-broker-2": fakeMeter{},
	}

	require.NoError(t, Register(target, registry, RoleProducer, ""))
	require.Equal(t, "sarama_kafka_client", scope.prefix)
	require.Equal(t, envmetrics.Labels{"role": "producer"}, scope.labels)
	require.Equal(t, 12.0, scope.callbacks["requests_count"]())
	require.Equal(t, 3.5, scope.callbacks["request_rate"]())
	require.Equal(t, 9.0, scope.callbacks["records_sent_count"]())
	require.Equal(t, 2.25, scope.callbacks["record_send_rate"]())
	require.Equal(t, 2.0, scope.callbacks["requests_in_flight"]())
	require.Equal(t, 2.0, scope.callbacks["broker_metric_sets"]())
	_, hasFetchRate := scope.callbacks["fetch_rate"]
	require.False(t, hasFetchRate)
}

func TestRegisterConsumerIncludesGroupLifecycle(t *testing.T) {
	scope := &fakeScope{callbacks: map[string]func() float64{}}
	target := &fakeMetrics{scope: scope}
	registry := fakeRegistry{
		"consumer-fetch-rate":                  fakeMeter{count: 7, rate: 1.5},
		"consumer-group-join-total-analytics":  fakeCounter{count: 3},
		"consumer-group-join-failed-analytics": fakeCounter{count: 1},
		"consumer-group-sync-total-analytics":  fakeCounter{count: 2},
		"consumer-group-sync-failed-analytics": fakeCounter{},
	}

	require.NoError(t, Register(target, registry, RoleConsumer, "analytics"))
	require.Equal(t, 7.0, scope.callbacks["fetches_count"]())
	require.Equal(t, 1.5, scope.callbacks["fetch_rate"]())
	require.Equal(t, 3.0, scope.callbacks["consumer_group_joins_count"]())
	require.Equal(t, 1.0, scope.callbacks["consumer_group_join_failures_count"]())
	require.Equal(t, 2.0, scope.callbacks["consumer_group_syncs_count"]())
	require.Equal(t, 0.0, scope.callbacks["consumer_group_sync_failures_count"]())
	_, hasRecordRate := scope.callbacks["record_send_rate"]
	require.False(t, hasRecordRate)
}
