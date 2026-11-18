package redisstreams

import (
	"context"
	sinkredis "github.com/gorundebug/servicelib/datasink/redisstreams"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type drainingHandler struct {
	outputHandler
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	calls    atomic.Int32
	retained *sinkredis.SinkMessage[mapped]
}

func (h *drainingHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, m *sinkredis.SinkMessage[mapped]) error {
	h.calls.Add(1)
	h.retained = m
	m.Fields = []sinkredis.Field{{Name: "value", Value: strconv.Itoa(v)}, {Name: "value", Value: "\x00\xff"}}
	m.Send(ctx, func(d sinkredis.Delivery, e error) mapped {
		if d.Outcome == sinkredis.Accepted {
			h.once.Do(func() { close(h.entered) })
			<-h.release
		}
		return mapped{d, e}
	})
	return nil
}
func TestNativeSinkAdmissionAndDrain(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, _ := fixture(t, address, false)
	cfg := s.env.cfg.GetEndpointConfigByID(3).(*config.RedisStreamsEndpointConfig)
	cfg.PublisherMaxInFlight = 1
	options, err := redis.ParseURL(address)
	require.NoError(t, err)
	options.Password = "adapter-test"
	admin := redis.NewClient(options)
	defer admin.Close()
	defer admin.Del(ctx, cfg.StreamKey)
	h := &drainingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(h.release) }) }
	defer release()
	out := &outputStream{env: s.env, results: make(chan mapped, 4)}
	p, err := sinkredis.MakeRedisStreamsEndpointConsumerWithResult[struct{}, int, mapped, error](out, h)
	require.NoError(t, err)
	require.NoError(t, s.env.sink.Start(ctx))
	defer s.env.sink.Stop(ctx)
	p.Consume(ctx, 1)
	receive(t, h.entered)
	// The first accepted callback still owns the request context. A retained
	// message must nevertheless reject a new send after ConsumeMessage returned.
	d, retiredErr := h.retained.SendSync(ctx)
	require.ErrorIs(t, retiredErr, sinkredis.ErrRetired)
	require.Equal(t, sinkredis.NotSent, d.Outcome)
	h.retained.Send(ctx, func(d sinkredis.Delivery, err error) mapped { return mapped{d, err} })
	late := receive(t, out.results)
	require.ErrorIs(t, late.err, sinkredis.ErrRetired)
	require.Equal(t, sinkredis.NotSent, late.delivery.Outcome)
	p.Consume(ctx, 2)
	rejected := receive(t, out.results)
	require.ErrorIs(t, rejected.err, sinkredis.ErrBackpressure)
	require.Equal(t, sinkredis.NotSent, rejected.delivery.Outcome)
	require.EqualValues(t, 1, admin.XLen(ctx, cfg.StreamKey).Val())
	raw, readErr := admin.Do(ctx, "XRANGE", cfg.StreamKey, "-", "+").Slice()
	require.NoError(t, readErr)
	require.Len(t, raw, 1)
	require.Equal(t, []interface{}{"value", "1", "value", "\x00\xff"}, raw[0].([]interface{})[1])
	stopped := make(chan struct{})
	go func() { s.env.sink.Stop(ctx); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before admitted callback completed")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	accepted := receive(t, out.results)
	require.NoError(t, accepted.err)
	require.Equal(t, sinkredis.Accepted, accepted.delivery.Outcome)
	receive(t, stopped)
	calls := h.calls.Load()
	p.Consume(ctx, 3)
	require.Equal(t, calls, h.calls.Load())
	require.EqualValues(t, 1, admin.XLen(ctx, cfg.StreamKey).Val())
}

func TestNativeSinkStopDeadline(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, _ := fixture(t, address, false)
	cfg := s.env.cfg.GetEndpointConfigByID(3).(*config.RedisStreamsEndpointConfig)
	options, err := redis.ParseURL(address)
	require.NoError(t, err)
	options.Password = "adapter-test"
	admin := redis.NewClient(options)
	defer admin.Close()
	defer admin.Del(ctx, cfg.StreamKey)
	h := &drainingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(h.release) }) }
	defer release()
	out := &outputStream{env: s.env, results: make(chan mapped, 2)}
	p, err := sinkredis.MakeRedisStreamsEndpointConsumerWithResult[struct{}, int, mapped, error](out, h)
	require.NoError(t, err)
	require.NoError(t, s.env.sink.Start(ctx))
	p.Consume(ctx, 1)
	receive(t, h.entered)
	stop, cancelStop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancelStop()
	start := time.Now()
	s.env.sink.Stop(stop)
	require.Less(t, time.Since(start), time.Second)
	require.Error(t, stop.Err())
	release()
	receive(t, out.results)
	s.env.sink.Stop(ctx)
}
