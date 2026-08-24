/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See LICENSE for details.
 */

package temporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
)

func TestTemporalMetricsHandlerUsesPrometheusNamesAndTags(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	handler := temporalMetricsHandler{scope: scope}.WithTags(map[string]string{
		"task_queue": "orders",
	})

	handler.Counter("workflow_completed").Inc(2)
	handler.Counter("already_total").Inc(1)
	handler.Gauge("worker_slots").Update(3)
	handler.Timer("workflow_latency").Record(250 * time.Millisecond)
	handler.Timer("already_seconds").Record(time.Second)

	snapshot := scope.Snapshot()
	require.EqualValues(t, 2, snapshot.Counters()["workflow_completed_total+task_queue=orders"].Value())
	require.EqualValues(t, 1, snapshot.Counters()["already_total+task_queue=orders"].Value())
	require.EqualValues(t, 3, snapshot.Gauges()["worker_slots+task_queue=orders"].Value())
	require.Equal(t, []time.Duration{250 * time.Millisecond}, snapshot.Timers()["workflow_latency_seconds+task_queue=orders"].Values())
	require.Equal(t, []time.Duration{time.Second}, snapshot.Timers()["already_seconds+task_queue=orders"].Values())
}
