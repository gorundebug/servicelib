package runtime

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

func TestDurableChildCallIDsAreStableAcrossActivityRetry(t *testing.T) {
	link := config.LinkID{From: 11, To: 12}
	envelope := DurableEnvelope{CallID: "parent-call"}

	firstAttempt, cancelFirst := durableEnvelopeContext(context.Background(), envelope)
	defer cancelFirst()
	first := nextDurableCallID(firstAttempt, link, []byte("same-value"))
	second := nextDurableCallID(firstAttempt, link, []byte("same-value"))
	require.NotEqual(t, first, second, "equal emissions are separate logical calls")

	retryAttempt, cancelRetry := durableEnvelopeContext(context.Background(), envelope)
	defer cancelRetry()
	require.Equal(t, first, nextDurableCallID(retryAttempt, link, []byte("same-value")))
	require.Equal(t, second, nextDurableCallID(retryAttempt, link, []byte("same-value")))
}

func TestDurableActivityCancellationReachesTargetContext(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancelEnvelope := durableEnvelopeContext(parent, DurableEnvelope{CallID: "call"})
	cancelParent()
	defer cancelEnvelope()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestRootDurableCallsNeverCollapseEqualMessages(t *testing.T) {
	link := config.LinkID{From: 11, To: 12}
	first := nextDurableCallID(context.Background(), link, []byte("same-value"))
	second := nextDurableCallID(context.Background(), link, []byte("same-value"))
	require.NotEqual(t, first, second)
}
