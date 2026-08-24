package cron

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
)

type scheduleFunctionProbe struct {
	called  bool
	trigger runtime.ScheduleTrigger
}

func (probe *scheduleFunctionProbe) OnTrigger(
	ctx context.Context,
	trigger runtime.ScheduleTrigger,
	out runtime.Collect[string],
) {
	probe.called = true
	probe.trigger = trigger
	out.Out(ctx, "job:"+trigger.ScheduleID)
}

func TestCronExpressionUsesUTC(t *testing.T) {
	if actual := cronExpression(" 0 * * * * ", " UTC "); actual != "CRON_TZ=UTC 0 * * * *" {
		t.Fatalf("unexpected expression %q", actual)
	}
}

func TestPortableCronRetainsExactOccurrenceAndMissedCount(t *testing.T) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}
	tracker := &portableCron{location: location}
	start := time.Date(2026, 8, 24, 12, 0, 1, 0, time.UTC)
	if err := tracker.IsValid("* * * * *", location, start); err != nil {
		t.Fatal(err)
	}
	first := tracker.Next(start)
	second := tracker.Next(first)
	third := tracker.Next(second)
	if actual, count := tracker.consumeDue(); count != 2 || !actual.Equal(second) {
		t.Fatalf("unexpected due occurrence=%s count=%d", actual, count)
	}
	if !first.Equal(time.Date(2026, 8, 24, 12, 1, 0, 0, time.UTC)) ||
		!second.Equal(time.Date(2026, 8, 24, 12, 2, 0, 0, time.UTC)) ||
		!third.Equal(time.Date(2026, 8, 24, 12, 3, 0, 0, time.UTC)) {
		t.Fatalf("unexpected exact occurrences: %s %s %s", first, second, third)
	}
}

func TestEndpointConsumerInvokesUserFunctionAndCollectsItsOutput(t *testing.T) {
	trigger := runtime.NewScheduleTrigger(
		17,
		"hourly",
		time.Date(2026, 8, 24, 12, 30, 0, 0, time.UTC),
		time.Date(2026, 8, 24, 12, 30, 1, 0, time.UTC),
		runtime.ScheduleBackendLocal,
	)
	function := &scheduleFunctionProbe{}
	var collected string
	consumer := endpointConsumer[string, struct{}, error]{
		function: function,
		out: runtime.CollectFunc[string](func(_ context.Context, value string) {
			collected = value
		}),
	}

	consumer.onTrigger(context.Background(), trigger)

	if !function.called {
		t.Fatal("scheduled endpoint did not invoke its user function")
	}
	if function.trigger != trigger {
		t.Fatalf("user function received trigger %#v, want %#v", function.trigger, trigger)
	}
	if collected != "job:hourly" {
		t.Fatalf("collector received %q, want user function output", collected)
	}
}
