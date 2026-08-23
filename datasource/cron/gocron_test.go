package cron

import "testing"

func TestCronExpressionUsesEndpointTimezone(t *testing.T) {
	if actual := cronExpression(" 0 * * * * ", " Europe/Moscow "); actual != "CRON_TZ=Europe/Moscow 0 * * * *" {
		t.Fatalf("unexpected expression %q", actual)
	}
}
