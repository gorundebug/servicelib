package operators

import (
	"context"
	"encoding/json"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
	"testing"
)

type queryConfigFixture struct {
	transformSerdeConfig
	endpoint  *config.QueryEndpointConfig
	connector config.DataConnectorConfig
}

func (c *queryConfigFixture) GetEndpoints() []config.EndpointConfig {
	return []config.EndpointConfig{c.endpoint}
}
func (c *queryConfigFixture) GetDataConnectors() []config.DataConnectorConfig {
	return []config.DataConnectorConfig{c.connector}
}

type queryCounter struct{ calls int }

func (c *queryCounter) Consume(context.Context, int) { c.calls++ }

func TestQueryEnabledAndRuntimeGraph(t *testing.T) {
	for _, connector := range []config.DataConnectorConfig{
		&config.S3AWSDataConnectorConfig{ID: 10, Name: "objects", Region: "us-east-1", Address: "http://private-store:9000"},
		&config.PostgreSQLBunDataConnectorConfig{ID: 10, Name: "database", ConnectionString: "postgres://user:secret@db/app"},
	} {
		t.Run(connector.GetName(), func(t *testing.T) {
			inputCfg := &config.InputStreamConfig{ID: 1, Name: "input", IdService: 1}
			queryCfg := &config.QueryStreamConfig{ID: 2, Name: "query", IdService: 1, IdSource: 1, IdEndpoint: 11}
			operation := api.EndpointQueryOperationGetObject
			if connector.GetType() == api.DataConnectorTypePostgreSQLBun {
				operation = api.EndpointQueryOperationRows
			}
			endpoint := &config.QueryEndpointConfig{ID: 11, Name: "request", IdDataConnector: 10, QueryOperation: operation, Enabled: false}
			cfg := &queryConfigFixture{transformSerdeConfig: transformSerdeConfig{streams: []config.StreamConfig{inputCfg, queryCfg}}, endpoint: endpoint, connector: connector}
			rc, err := config.NewRuntimeConfig(cfg)
			require.NoError(t, err)
			env := &subStreamTestEnv{}
			require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
			input, err := MakeInputStream[int, any, any](inputCfg, env)
			require.NoError(t, err)
			query, err := MakeQueryStream[int, int, string](queryCfg, input)
			require.NoError(t, err)
			consumer := &queryCounter{}
			query.SetQueryConsumer(consumer)
			query.Consume(t.Context(), 1)
			require.Zero(t, consumer.calls)
			endpoint.Enabled = true
			query.Consume(t.Context(), 2)
			require.Equal(t, 1, consumer.calls)
			graph := env.RuntimeToStreamApp()
			require.Len(t, graph.DataConnectors, 1)
			require.Len(t, graph.Endpoints, 1)
			require.Equal(t, 11, graph.Endpoints[0].Id)
			require.Equal(t, operation, *graph.Endpoints[0].QueryOperation)
			encoded, err := json.Marshal(graph)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "secret")
			require.NotContains(t, string(encoded), "private-store")
			endpoint.QueryOperation = "invalid"
			_, err = config.NewRuntimeConfig(cfg)
			require.ErrorContains(t, err, "not supported")
		})
	}
}
