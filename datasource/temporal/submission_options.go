package temporal

import (
	"fmt"
	"strings"
	"time"

	"github.com/gorundebug/servicelib/api"
)

// SubmissionOptions explicitly overrides transport scheduling for one call.
// The zero value preserves the endpoint configuration. Queue selection must be
// deterministic inside a Workflow. These options do not register workers or
// implement placement, failover, application affinity, or cache ownership.
type SubmissionOptions struct {
	TaskQueue string
	// ScheduleToStartTimeout bounds an Activity's wait for a worker. It does
	// not limit execution time and is not supported for Workflow endpoints.
	ScheduleToStartTimeout time.Duration
}

func (o SubmissionOptions) resolve(queue string, execution api.TemporalExecutionType) (string, error) {
	if o.TaskQueue != "" {
		if strings.TrimSpace(o.TaskQueue) == "" || strings.ContainsRune(o.TaskQueue, 0) {
			return "", fmt.Errorf("Temporal submission task queue is invalid")
		}
		queue = o.TaskQueue
	}
	if o.ScheduleToStartTimeout < 0 {
		return "", fmt.Errorf("Temporal submission schedule-to-start timeout must not be negative")
	}
	if o.ScheduleToStartTimeout != 0 && execution != api.Activity {
		return "", fmt.Errorf("Temporal submission schedule-to-start timeout requires an Activity endpoint")
	}
	return queue, nil
}
