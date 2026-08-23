package config

import (
	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
)

type temporalTestConfig struct {
	connectors []DataConnectorConfig
	endpoints  []EndpointConfig
	links      []*LinkConfig
}

func (*temporalTestConfig) GetServices() []*ServiceConfig              { return nil }
func (*temporalTestConfig) GetStreams() []StreamConfig                 { return nil }
func (c *temporalTestConfig) GetDataConnectors() []DataConnectorConfig { return c.connectors }
func (c *temporalTestConfig) GetEndpoints() []EndpointConfig           { return c.endpoints }
func (*temporalTestConfig) GetPools() []*PoolConfig                    { return nil }
func (c *temporalTestConfig) GetLinks() []*LinkConfig                  { return c.links }
func (*temporalTestConfig) GetModules() []*ModuleConfig                { return nil }
func (*temporalTestConfig) GetTypes() []*TypeConfig                    { return nil }
func (*temporalTestConfig) GetProperty(string) interface{}             { return nil }
func (*temporalTestConfig) ApplyEnvironment() error                    { return nil }

func TestTemporalConfigRoundTrip(t *testing.T) {
	connector := &TemporalDataConnectorConfig{
		ID: 7, Name: "temporal", Implementation: api.DataConnectorImplementationTemporalGo,
		Address: "temporal:7233", Namespace: "default", Identity: "automation",
		MaxConcurrentActivities: 2, MaxConcurrentWorkflows: 3,
		APIKey: "secret", TLSEnabled: true, TLSServerName: "temporal.example.com",
	}
	endpoint := &TemporalEndpointConfig{
		ID: 11, Name: "scheduledJob", IdDataConnector: 7, Enabled: true,
		TaskQueue: "automation", Schedule: "*/5 * * * *", ScheduleID: "scheduled-job",
		Timezone: "UTC", OverlapPolicy: api.ScheduleOverlapPolicySkip,
		MissedRunPolicy:             api.ScheduleMissedRunPolicyFireOnce,
		ActivityStartToCloseTimeout: 30_000, MaximumAttempts: 3,
	}
	link := &LinkConfig{From: 1, To: 2, CallSemantics: &CallSemanticsGroup{
		DurableCall: &DurableCallSemanticsConfig{
			IdDataConnector: 7, TaskQueue: "automation",
			ActivityStartToCloseTimeout: 30_000, MaximumAttempts: 3,
		},
	}}
	cfg := &temporalTestConfig{
		connectors: []DataConnectorConfig{connector},
		endpoints:  []EndpointConfig{endpoint},
		links:      []*LinkConfig{link},
	}

	_, err := NewRuntimeConfig(cfg)
	require.NoError(t, err)

	app := ConfigToStreamApp(cfg)
	require.Equal(t, api.DataConnectorTypeTemporal, app.DataConnectors[0].Type)
	require.Equal(t, "temporal:7233", *app.DataConnectors[0].Address)
	require.Equal(t, "automation", *app.Endpoints[0].TaskQueue)
	require.Equal(t, "secret", *app.DataConnectors[0].ApiKey)
	require.True(t, *app.DataConnectors[0].TlsEnabled)
	require.Equal(t, "temporal.example.com", *app.DataConnectors[0].TlsServerName)
	require.Equal(t, 7, *app.Links[0].IdDataConnector)
	require.Equal(t, "automation", *app.Links[0].TaskQueue)
	require.Equal(t, api.CallSemanticsDurableCall, app.Links[0].CallSemantics)
}

func TestDurableCallRejectsNonTemporalConnector(t *testing.T) {
	cfg := &temporalTestConfig{
		connectors: []DataConnectorConfig{&CustomDataConnectorConfig{ID: 7, Name: "local"}},
		links: []*LinkConfig{{From: 1, To: 2, CallSemantics: &CallSemanticsGroup{
			DurableCall: &DurableCallSemanticsConfig{
				IdDataConnector: 7, TaskQueue: "automation",
				ActivityStartToCloseTimeout: 30_000, MaximumAttempts: 3,
			},
		}}},
	}

	_, err := NewRuntimeConfig(cfg)
	require.ErrorContains(t, err, "requires a Temporal data connector")
}

func TestDurableCallRequiresConnector(t *testing.T) {
	link := &LinkConfig{From: 1, To: 2, CallSemantics: &CallSemanticsGroup{
		DurableCall: &DurableCallSemanticsConfig{},
	}}
	require.ErrorContains(t, link.Validate(), "positive idDataConnector")
}

func TestTemporalEndpointRequiresCompleteScheduleIdentity(t *testing.T) {
	cfg := &temporalTestConfig{
		connectors: []DataConnectorConfig{&TemporalDataConnectorConfig{
			ID: 7, Name: "temporal", Implementation: api.DataConnectorImplementationTemporalGo,
			Address: "temporal:7233", Namespace: "default", MaxConcurrentActivities: 1, MaxConcurrentWorkflows: 1,
		}},
		endpoints: []EndpointConfig{&TemporalEndpointConfig{
			ID: 11, Name: "scheduledJob", IdDataConnector: 7, Enabled: true,
			TaskQueue: "automation", Schedule: "*/5 * * * *",
			ActivityStartToCloseTimeout: 1_000, MaximumAttempts: 1,
		}},
	}
	_, err := NewRuntimeConfig(cfg)
	require.ErrorContains(t, err, "requires scheduleId and timezone")
}

func TestTemporalEndpointRejectsNonTemporalConnector(t *testing.T) {
	cfg := &temporalTestConfig{
		connectors: []DataConnectorConfig{&CustomDataConnectorConfig{ID: 7, Name: "custom"}},
		endpoints: []EndpointConfig{&TemporalEndpointConfig{
			ID: 11, Name: "job", IdDataConnector: 7, TaskQueue: "automation",
			ActivityStartToCloseTimeout: 1_000, MaximumAttempts: 1,
		}},
	}
	_, err := NewRuntimeConfig(cfg)
	require.ErrorContains(t, err, "requires a Temporal data connector")
}

func TestTemporalConnectorRejectsIncompleteMTLSKeyPair(t *testing.T) {
	cfg := &temporalTestConfig{connectors: []DataConnectorConfig{&TemporalDataConnectorConfig{
		ID: 7, Name: "temporal", Implementation: api.DataConnectorImplementationTemporalGo,
		Address: "temporal:7233", Namespace: "default", MaxConcurrentActivities: 1, MaxConcurrentWorkflows: 1,
		TLSEnabled: true, TLSCertFile: "/secrets/client.crt",
	}}}
	_, err := NewRuntimeConfig(cfg)
	require.ErrorContains(t, err, "both tlsCertFile and tlsKeyFile")
}
