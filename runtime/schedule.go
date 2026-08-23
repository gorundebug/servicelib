/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type ScheduleBackend string

const (
	ScheduleBackendLocal    ScheduleBackend = "local"
	ScheduleBackendTemporal ScheduleBackend = "temporal"
)

// ScheduleTrigger is the common input payload emitted by local Cron and
// Temporal Schedule endpoints. A delivery retry retains the same TriggerID.
type ScheduleTrigger struct {
	TriggerID   string          `json:"triggerId" yaml:"triggerId"`
	ScheduleID  string          `json:"scheduleId" yaml:"scheduleId"`
	ScheduledAt time.Time       `json:"scheduledAt" yaml:"scheduledAt"`
	FiredAt     time.Time       `json:"firedAt" yaml:"firedAt"`
	Backend     ScheduleBackend `json:"backend" yaml:"backend"`
}

// NewScheduleTrigger constructs the byte-identical logical trigger used by all
// runtimes. endpointID is immutable topology identity; scheduleID is the
// operator-visible item identity.
func NewScheduleTrigger(endpointID int, scheduleID string, scheduledAt, firedAt time.Time, backend ScheduleBackend) ScheduleTrigger {
	scheduledAt = scheduledAt.UTC()
	firedAt = firedAt.UTC()
	identity := fmt.Sprintf("servicegen:schedule-trigger:v1\n%d\n%s\n%s", endpointID, scheduleID, scheduledAt.Format(time.RFC3339Nano))
	digest := sha256.Sum256([]byte(identity))
	return ScheduleTrigger{
		TriggerID:   hex.EncodeToString(digest[:]),
		ScheduleID:  scheduleID,
		ScheduledAt: scheduledAt,
		FiredAt:     firedAt,
		Backend:     backend,
	}
}

// NormalizeTemporalPriority maps the existing unbounded MessageContext
// priority monotonically to Temporal's portable five priority levels.
func NormalizeTemporalPriority(priority int) int {
	switch {
	case priority <= -2:
		return 1
	case priority == -1:
		return 2
	case priority == 0:
		return 3
	case priority == 1:
		return 4
	default:
		return 5
	}
}
