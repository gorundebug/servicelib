/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grafana

import "embed"

// DashboardFiles contains the Grafana dashboard sources (Grafonnet/Jsonnet)
// and the Docker-based generator (Dockerfile + generate.sh).
// Embedded at compile time so servicegen can copy them into generated projects.

//go:embed Dockerfile generate.sh
//go:embed all:dashboards
var DashboardFiles embed.FS
