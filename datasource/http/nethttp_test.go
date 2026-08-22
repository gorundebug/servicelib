package http

import "testing"

func TestCompletedResultWinsWhenResultAndDeadlineRace(t *testing.T) {
	done := make(chan struct{})
	close(done)

	if !completedResultWins(done) {
		t.Fatal("a completed HTTP result must win over simultaneous cancellation")
	}
}

func TestCompletedResultDoesNotWinBeforeDone(t *testing.T) {
	done := make(chan struct{})

	if completedResultWins(done) {
		t.Fatal("an incomplete HTTP result must preserve cancellation handling")
	}
}
