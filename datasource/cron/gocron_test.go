package cron

import (
	"testing"
	"time"
)

func TestCronExpressionUsesEndpointTimezone(t *testing.T) {
	if actual := cronExpression(" 0 * * * * ", " Europe/Moscow "); actual != "CRON_TZ=Europe/Moscow 0 * * * *" {
		t.Fatalf("unexpected expression %q", actual)
	}
}

func TestPortableScheduledTimeUsesFirstDSTFoldOnce(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	second := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)
	if !portableScheduledTime(first, location) {
		t.Fatal("first ambiguous occurrence must be accepted")
	}
	if portableScheduledTime(second, location) {
		t.Fatal("second ambiguous occurrence must be skipped")
	}
	if !portableScheduledTime(
		time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC), location,
	) {
		t.Fatal("ordinary occurrence must be accepted")
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

func TestPortableCronFiltersSecondDSTFold(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	tracker := &portableCron{location: location}
	start := time.Date(2026, 10, 31, 6, 0, 0, 0, time.UTC)
	if err := tracker.IsValid("30 1 * * *", location, start); err != nil {
		t.Fatal(err)
	}
	first := tracker.Next(start)
	second := tracker.Next(first)
	if !first.Equal(time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)) {
		t.Fatalf("unexpected first fold %s", first)
	}
	if !second.Equal(time.Date(2026, 11, 2, 6, 30, 0, 0, time.UTC)) {
		t.Fatalf("second fold was not filtered: %s", second)
	}
}
