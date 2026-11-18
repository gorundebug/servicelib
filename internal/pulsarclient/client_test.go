package pulsarclient

import (
	"context"
	"errors"
	native "github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type recordingProducer struct {
	native.Producer
	calls int
	err   error
}

func (p *recordingProducer) Send(context.Context, *native.ProducerMessage) (native.MessageID, error) {
	p.calls++
	return nil, p.err
}
func (p *recordingProducer) SendAsync(context.Context, *native.ProducerMessage, func(native.MessageID, *native.ProducerMessage, error)) {
	p.calls++
}

func TestPublicationAdmissionBoundary(t *testing.T) {
	for _, name := range []string{"missing", "unsupported", "cancelled", "full", "closed", "source"} {
		t.Run(name, func(t *testing.T) {
			producer := &recordingProducer{}
			session := &Session{producer: producer, capacity: 1}
			ctx := t.Context()
			message := &native.ProducerMessage{Payload: []byte("event")}
			var cause error
			switch name {
			case "missing":
				message = nil
			case "unsupported":
				message.DeliverAfter = time.Second
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				cause = context.Canceled
			case "full":
				session.active = 1
				cause = ErrCapacity
			case "closed":
				session.closed = true
				cause = ErrClosed
			case "source":
				session.producer = nil
			}
			id, err := session.Send(ctx, message)
			require.Nil(t, id)
			require.ErrorIs(t, err, ErrNotSent)
			if cause != nil {
				require.ErrorIs(t, err, cause)
			}
			callbackCalled := false
			err = session.SendAsync(ctx, message, func(native.MessageID, error) { callbackCalled = true })
			require.Error(t, err)
			require.False(t, callbackCalled)
			require.Zero(t, producer.calls, "admission failure must never reach native producer")
		})
	}
}

func TestNativeFailureIsNotLocalRejection(t *testing.T) {
	// Once handed to the native producer, even cancellation can have an ambiguous
	// outcome. Do not label it NotSent merely because errors.Is matches context.
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("connection lost")} {
		producer := &recordingProducer{err: cause}
		session := &Session{producer: producer, capacity: 1}
		_, err := session.Send(t.Context(), &native.ProducerMessage{Payload: []byte("event")})
		require.ErrorIs(t, err, cause)
		require.NotErrorIs(t, err, ErrNotSent)
		require.Equal(t, 1, producer.calls)
		require.Zero(t, session.active)
	}
}
