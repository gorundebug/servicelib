/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package httproute

import "net/http"

type HttpRoute interface {
	Handle(pattern string, handler http.Handler)
}
