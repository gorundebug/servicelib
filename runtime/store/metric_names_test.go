/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// TestStorageMetricNames verifies that storage constructors register exactly the
// expected metric names. If this test fails after a metric rename, update both
// the expected slices below AND grafana/dashboards/06_storage.jsonnet.
package store

import (
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	envlog "github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/stretchr/testify/require"
)

type mockServiceEnv struct {
	environment.ServiceEnvironment
	m metrics.Metrics
}

func (e *mockServiceEnv) Metrics() metrics.Metrics             { return e.m }
func (e *mockServiceEnv) ServiceConfig() *config.ServiceConfig { return &config.ServiceConfig{Name: "test-svc"} }
func (e *mockServiceEnv) Log() envlog.Logger                   { return nil }

type testJoinStorageConfig struct {
	name string
}

func (c *testJoinStorageConfig) GetTTL() time.Duration { return 0 }
func (c *testJoinStorageConfig) GetRenewTTL() bool     { return false }
func (c *testJoinStorageConfig) GetName() string       { return c.name }

var expectedHashMapJoinStorageMetrics = []string{
	"hashmap_join_storage_count",
	"hashmap_join_storage_evictions_total",
}

func TestHashMapJoinStorageRegistersMetrics(t *testing.T) {
	m := testmetrics.New()
	env := &mockServiceEnv{m: m}
	_, err := MakeHashMapJoinStorage[string](env, &testJoinStorageConfig{name: "join1"})
	require.NoError(t, err)

	require.ElementsMatch(t, expectedHashMapJoinStorageMetrics, m.RegisteredNames(),
		"hashmap join storage metrics changed — update expectedHashMapJoinStorageMetrics AND grafana/dashboards/06_storage.jsonnet")
}
