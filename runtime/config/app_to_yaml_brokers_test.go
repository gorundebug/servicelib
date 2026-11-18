package config

import (
	"encoding/json"
	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestAppToYamlNativeBrokerPresence(t *testing.T) {
	for _, mode := range []string{"absent", "false", "true"} {
		t.Run(mode, func(t *testing.T) {
			connector := map[string]any{}
			endpoint := map[string]any{}
			if mode != "absent" {
				connector = map[string]any{"servers": []string{"amqp://first:5672", "amqp://second:5672"}, "virtualHost": "", "connectTimeoutMs": 0, "operationTimeoutMs": 5000, "token": "", "credentialsFile": "", "tlsEnabled": mode == "true"}
				endpoint = map[string]any{"subject": "", "queueGroup": "workers", "streamName": "events", "durableName": "consumer", "queueName": "queue", "exchangeName": "exchange", "routingKey": "", "queueType": "quorum", "exchangeType": "topic", "consumerCredits": 0, "publisherMaxInFlight": 256}
				for _, key := range []string{"createStream", "createConsumer", "createQueue", "createExchange", "createBinding", "enabled", "tracingEnabled"} {
					endpoint[key] = mode == "true"
				}
			}
			var dc api.DataConnector
			var ep api.Endpoint
			raw, err := json.Marshal(connector)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &dc))
			raw, err = json.Marshal(endpoint)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &ep))
			dc.Id = 1
			dc.Name = "Broker"
			dc.Type = api.DataConnectorTypeRabbitMQ
			ep.Id = 1
			ep.IdDataConnector = 1
			ep.Name = "Receive"
			ep.FunctionName = "Receive"
			data, err := AppToYaml(&api.StreamApp{DataConnectors: []api.DataConnector{dc}, Endpoints: []api.Endpoint{ep}})
			require.NoError(t, err)
			var doc map[string]any
			require.NoError(t, yaml.Unmarshal(data, &doc))
			actualDC := doc["dataConnectors"].(map[string]any)["broker"].(map[string]any)
			actualEP := actualDC["endpoints"].(map[string]any)["receive"].(map[string]any)
			delete(actualDC, "endpoints")
			delete(actualDC, "name")
			delete(actualDC, "type")
			delete(actualEP, "name")
			delete(actualEP, "functionName")
			wantDC, _ := json.Marshal(connector)
			gotDC, _ := json.Marshal(actualDC)
			require.JSONEq(t, string(wantDC), string(gotDC))
			wantEP, _ := json.Marshal(endpoint)
			gotEP, _ := json.Marshal(actualEP)
			require.JSONEq(t, string(wantEP), string(gotEP))
		})
	}
}
