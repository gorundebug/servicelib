/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import "github.com/gorundebug/servicelib/api"

func dataConnectorProtocol(connectorType api.DataConnectorType) string {
	if connectorType == api.DataConnectorTypeGRPC {
		return "grpc"
	}
	return ""
}
