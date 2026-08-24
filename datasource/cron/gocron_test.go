package cron

import (
	"testing"
	"time"
)

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
