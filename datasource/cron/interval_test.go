package cron

import (
	"context"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
)

func TestLocalIntervalExactSlotsAndMissedCount(t *testing.T) {
	start := time.Date(2026, 10, 27, 12, 0, 0, 123456789, time.UTC)
	tracker := &portableCron{location: time.UTC}
	if err := tracker.IsValid("@every 100ms", time.UTC, start); err != nil {
		t.Fatal(err)
	}
	first := tracker.Next(start)
	if want := start.Truncate(time.Millisecond).Add(100 * time.Millisecond); !first.Equal(want) {
		t.Fatal("interval was rounded or fired immediately", first, want)
	}
	next := first
	for i := 0; i < 10000; i++ {
		next = tracker.Next(next)
	}
	latest, count := tracker.consumeDue()
	if count != 10000 || !latest.Equal(first.Add(9999*100*time.Millisecond)) {
		t.Fatal("missed occurrences lost", latest, count)
	}
	if _, count := tracker.consumeDue(); count != 0 {
		t.Fatal("occurrences replayed")
	}
	if err := tracker.IsValid("* * * * *", time.UTC, start); err != nil {
		t.Fatal(err)
	}
	if next := tracker.Next(start); !next.Equal(time.Date(2026, 10, 27, 12, 1, 0, 0, time.UTC)) {
		t.Fatal("calendar scheduling changed", next)
	}
}

func TestLocalIntervalUsesNativeScheduler(t *testing.T) {
	owner, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()
	tracker := &portableCron{location: time.UTC}
	ticks := make(chan time.Time, 8)
	_, err = scheduler.NewJob(gocron.CronJob("@every 100ms", false),
		cronOccurrenceTask(owner, func(ctx context.Context) {
			if ctx != owner {
				t.Error("scheduler replaced the owner context")
			}
			when, count := tracker.consumeDue()
			if count > 0 {
				select {
				case ticks <- when:
				case <-ctx.Done():
				}
			}
		}), gocron.WithCronImplementation(tracker),
		gocron.WithSingletonMode(gocron.LimitModeReschedule))
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Start()
	var previous time.Time
	for i := 0; i < 3; i++ {
		select {
		case current := <-ticks:
			if !previous.IsZero() {
				delta := current.Sub(previous)
				if delta < 100*time.Millisecond || delta%(100*time.Millisecond) != 0 {
					t.Fatal("logical cadence drifted", delta)
				}
			}
			previous = current
		case <-owner.Done():
			t.Fatal("native subsecond scheduling did not fire", owner.Err())
		}
	}
	cancel()
}
