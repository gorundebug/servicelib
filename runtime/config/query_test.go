package config

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQueryOperationConnectorValidation(t *testing.T) {
	postgres := &PostgreSQLBunDataConnectorConfig{ID: 1, Name: "database"}
	objects := &S3AWSDataConnectorConfig{ID: 2, Name: "objects"}
	for _, operation := range []api.EndpointQueryOperation{"Rows", "Execute", "GetObject", "HeadObject", "ListObjects", "PutObject", "DeleteObject"} {
		t.Run(string(operation), func(t *testing.T) {
			endpoint := &QueryEndpointConfig{Name: "query", QueryOperation: operation}
			pg := operation == "Rows" || operation == "Execute"
			for _, connector := range []DataConnectorConfig{postgres, objects} {
				err := ValidateQueryEndpoint(endpoint, connector)
				if (connector == postgres) == pg {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
			require.Error(t, ValidateQueryEndpoint(endpoint, nil))
		})
	}
}
