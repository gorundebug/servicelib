package temporal

import (
	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

type externalSnapshotConfig struct{ endpointMetricConfig }

func (externalSnapshotConfig) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{
		&config.TemporalDataConnectorConfig{ID: 1, Name: "Temporal", Address: "unused:7233", Namespace: "default", Implementation: api.DataConnectorImplementationTemporalGo},
		&config.TemporalDataConnectorConfig{ID: 2, Name: "Other", Address: "unused:7233", Namespace: "default", Implementation: api.DataConnectorImplementationTemporalGo},
	}
}

func (externalSnapshotConfig) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{
		&config.TemporalEndpointConfig{ID: 9, Name: "Local", IdDataConnector: 1, TemporalExecutionType: api.Activity, TaskQueue: "local", ActivityStartToCloseTimeout: 1000, MaxConcurrentActivities: 1, MaximumAttempts: 1},
		&config.TemporalEndpointConfig{ID: 3, Name: "External", IdDataConnector: 1, TemporalExecutionType: api.Activity, TaskQueue: "remote", ActivityStartToCloseTimeout: 2000, MaxConcurrentActivities: 1, MaximumAttempts: 2},
		&config.TemporalEndpointConfig{ID: 5, Name: "Unrelated", IdDataConnector: 2, TemporalExecutionType: api.Activity, TaskQueue: "other", ActivityStartToCloseTimeout: 1000, MaxConcurrentActivities: 1, MaximumAttempts: 1},
	}
}

func TestTemporalWorkflowSnapshotIncludesRemoteContractsWithoutHostingThem(t *testing.T) {
	rc, err := config.NewRuntimeConfig(externalSnapshotConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c := &Connector{id: 1, name: "Temporal", environment: endpointMetricEnvironment{configuration: rc}, endpointRegistrations: map[int]endpointRegistration{9: {id: 9}}}
	for attempt := 0; attempt < 2; attempt++ {
		snapshot, err := c.workflowEndpointSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot) != 2 || snapshot[0].ID != 3 || snapshot[1].ID != 9 {
			t.Fatal("snapshot omitted outgoing contract, mixed connectors or was not sorted", snapshot)
		}
		remote := snapshot[0]
		if remote.Name != "External" || remote.TaskQueue != "remote" || remote.ActivityType != "temporal.endpoint.external.v1" || remote.ActivityStartToCloseMillis != 2000 || remote.MaximumAttempts != 2 {
			t.Fatal("remote contract was not derived from immutable endpoint configuration", remote)
		}
		if len(c.endpointRegistrations) != 1 {
			t.Fatal("snapshot registered a local worker for remote work")
		}
	}
}
