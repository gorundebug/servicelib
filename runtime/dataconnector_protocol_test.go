/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gorundebug/servicelib/api"
)

func TestDataConnectorProtocol(t *testing.T) {
	tests := map[api.DataConnectorType]string{
		api.DataConnectorTypeUndefined: "",
		api.DataConnectorTypeHTTP:      "",
		api.DataConnectorTypeGRPC:      "grpc",
		api.DataConnectorTypeKafka:     "",
		api.DataConnectorTypeCustom:    "",
	}

	for connectorType, expected := range tests {
		require.Equal(t, expected, dataConnectorProtocol(connectorType))
	}
}
