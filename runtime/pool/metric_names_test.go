/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// TestPoolMetricNames verifies that task pool constructors register exactly the
// expected metric names. If this test fails after a metric rename, update both
// the expected slices below AND the corresponding grafana/dashboards/05_pools.jsonnet.
package pool

import (
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
)

// mockServiceEnv is a minimal ServiceEnvironment that only supplies Metrics and
// ServiceConfig. All other methods panic — they should not be called during
// metric registration.
type mockServiceEnv struct {
	environment.ServiceEnvironment // nil embedding: panics if unimplemented method called
	m metrics.Metrics
}

func (e *mockServiceEnv) Metrics() metrics.Metrics             { return e.m }
func (e *mockServiceEnv) ServiceConfig() *config.ServiceConfig { return &config.ServiceConfig{Name: "test-svc"} }
func (e *mockServiceEnv) Log() envlog.Logger                   { return nil }

var expectedTaskPoolMetrics = []string{
	"task_pool_events_total",
	"task_pool_queue_length",
	"task_pool_task_execution_duration_seconds",
	"task_pool_tasks_total",
}

var expectedPriorityTaskPoolMetrics = []string{
	"priority_task_pool_events_total",
	"priority_task_pool_queue_length",
	"priority_task_pool_task_execution_duration_seconds",
	"priority_task_pool_tasks_total",
}

var expectedDelayPoolMetrics = []string{
	"delay_pool_events_total",
	"delay_pool_task_execution_duration_seconds",
	"delay_pool_tasks_total",
	"delay_pool_wait_queue_length",
}
// delay_pool_events_total labels: event=stop_timeout | task_cancelled

func TestTaskPoolRegistersMetrics(t *testing.T) {
	m := testmetrics.New()
	env := &mockServiceEnv{m: m}
	_, err := makeTaskPool(env, &config.PoolConfig{Name: "worker"})
	require.NoError(t, err)

	require.ElementsMatch(t, expectedTaskPoolMetrics, m.RegisteredNames(),
		"task pool metrics changed — update expectedTaskPoolMetrics AND grafana/dashboards/05_pools.jsonnet")
}

func TestPriorityTaskPoolRegistersMetrics(t *testing.T) {
	m := testmetrics.New()
	env := &mockServiceEnv{m: m}
	_, err := makePriorityTaskPool(env, &config.PoolConfig{Name: "priority-worker"})
	require.NoError(t, err)

	require.ElementsMatch(t, expectedPriorityTaskPoolMetrics, m.RegisteredNames(),
		"priority task pool metrics changed — update expectedPriorityTaskPoolMetrics AND grafana/dashboards/05_pools.jsonnet")
}

func TestDelayPoolRegistersMetrics(t *testing.T) {
	m := testmetrics.New()
	env := &mockServiceEnv{m: m}
	_, err := makeDelayPool(env)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedDelayPoolMetrics, m.RegisteredNames(),
		"delay pool metrics changed — update expectedDelayPoolMetrics AND grafana/dashboards/05_pools.jsonnet")
}
