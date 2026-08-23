package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScheduleTriggerIdentityIsStableAcrossRetryAndTimezone(t *testing.T) {
	scheduledUTC := time.Date(2026, 8, 24, 12, 30, 0, 123456000, time.UTC)
	firedAt := scheduledUTC.Add(2 * time.Second)
	first := NewScheduleTrigger(17, "hourly", scheduledUTC, firedAt, ScheduleBackendTemporal)
	retry := NewScheduleTrigger(17, "hourly", scheduledUTC.In(time.FixedZone("offset", 3*60*60)), firedAt.Add(time.Second), ScheduleBackendTemporal)

	require.Equal(t, first.TriggerID, retry.TriggerID)
	require.Equal(t, scheduledUTC, first.ScheduledAt)
	require.Equal(t, ScheduleBackendTemporal, first.Backend)
	require.Len(t, first.TriggerID, 64)
	require.Equal(t, "29b272e3eeee0c67fe5b5a121f8f39d4b5d9625d656e8a0ec7f2b0f1615e2914", first.TriggerID)
}

func TestNormalizeTemporalPriority(t *testing.T) {
	values := map[int]int{-100: 1, -2: 1, -1: 2, 0: 3, 1: 4, 2: 5, 100: 5}
	for input, expected := range values {
		require.Equal(t, expected, NormalizeTemporalPriority(input), "priority %d", input)
	}
}
