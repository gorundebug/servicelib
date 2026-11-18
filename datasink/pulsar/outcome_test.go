package pulsar

import (
	"context"
	"errors"
	"github.com/gorundebug/servicelib/internal/pulsarclient"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdmissionAndAmbiguousOutcomeRemainDistinct(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("invalid local message")} {
		require.Equal(t, NotSent, nativeOutcome(nil, errors.Join(pulsarclient.ErrNotSent, cause)).Outcome)
		require.Equal(t, Unknown, nativeOutcome(nil, cause).Outcome)
	}
}
