package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	native "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSendDelegatesNativeAsyncCompletion(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(map[bool]string{false: "core", true: "jetstream"}[durable], func(t *testing.T) {
			sentinel := errors.New("delivery failed")
			var got error
			var emitted bool
			key := struct{}{}
			ctx := context.WithValue(t.Context(), key, "trace")
			m := &SinkMessage[error]{subject: "events", Data: []byte("payload"), Header: native.Header{"X-Test": []string{"yes"}}, result: runtime.ConsumerFunc[error](func(out context.Context, e error) {
				if out.Value(key) != "trace" {
					t.Error("context lost")
				}
				emitted = true
				got = e
			})}
			m.publish = func(_ context.Context, msg *native.Msg) (Delivery, error) {
				if msg.Subject != "events" || string(msg.Data) != "payload" || msg.Header.Get("X-Test") != "yes" {
					t.Fatal("message changed")
				}
				if durable {
					return Delivery{Ack: &jetstream.PubAck{Stream: "EVENTS", Sequence: 3}}, sentinel
				}
				return Delivery{}, sentinel
			}
			var complete func()
			m.sendAsync = func(sendCtx context.Context, msg *native.Msg, cb func(Delivery, error)) {
				complete = func() { d, e := m.publish(sendCtx, msg); cb(d, e) }
			}
			m.Send(ctx, func(d Delivery, e error) error {
				if (d.Ack != nil) != durable {
					t.Fatal("invented/missing ACK")
				}
				return e
			})
			if emitted {
				t.Fatal("Send waited for simulated native completion")
			}
			complete()
			if !emitted || !errors.Is(got, sentinel) {
				t.Fatal("completion result lost")
			}
		})
	}
}

type intStream struct {
	runtime.TypedConsumedStream[int]
	values []int
}

func (s *intStream) Consume(_ context.Context, v int) { s.values = append(s.values, v) }

func TestSinkMessageSkipAndOutDoNotPublish(t *testing.T) {
	s := &intStream{}
	var publishes int
	m := &SinkMessage[int]{result: s, publish: func(context.Context, *native.Msg) (Delivery, error) { publishes++; return Delivery{}, nil }}
	m.Skip(t.Context(), 1)
	m.Out(t.Context(), 2)
	if publishes != 0 || len(s.values) != 2 || s.values[0] != 1 || s.values[1] != 2 {
		t.Fatal("Skip/Out changed publication semantics")
	}
}

func TestShutdownGateIncludesDeliveryCallback(t *testing.T) {
	ep := &endpoint{}
	ep.gate.Open()
	ctx, cancel := context.WithCancel(t.Context())
	ep.ctx = ctx
	ep.cancel = cancel
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var callbacks atomic.Int32
	go func() {
		defer close(finished)
		if !ep.gate.Enter() {
			return
		}
		defer ep.gate.Leave()
		m := &SinkMessage[int]{result: runtime.ConsumerFunc[int](func(context.Context, int) { callbacks.Add(1); close(entered); <-release }), publish: func(context.Context, *native.Msg) (Delivery, error) { return Delivery{}, nil }}
		m.sendAsync = func(ctx context.Context, msg *native.Msg, cb func(Delivery, error)) {
			d, e := m.publish(ctx, msg)
			cb(d, e)
		}
		m.Send(ctx, func(Delivery, error) int { return 1 })
	}()
	<-entered
	done := ep.gate.Close()
	select {
	case <-done:
		t.Fatal("callback not tracked")
	default:
	}
	if ep.gate.Enter() {
		t.Fatal("admission after stop")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delivery callback failed to drain")
	}
	<-finished
	if callbacks.Load() != 1 {
		t.Fatal("callback repeated")
	}
}
