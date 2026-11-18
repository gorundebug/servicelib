package redisstreams

import (
	"context"
	"fmt"
	sinkredis "github.com/gorundebug/servicelib/datasink/redisstreams"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"
)

type mapped struct {
	delivery sinkredis.Delivery
	err      error
}
type outputStream struct {
	runtime.TypedSinkStreamWithResult[int, mapped, error]
	env      *fixtureEnv
	results  chan mapped
	consumer runtime.Consumer[int]
}

func (s *outputStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return s.env }
func (*outputStream) GetEndpointId() int                                  { return 3 }
func (*outputStream) GetName() string                                     { return "output" }
func (s *outputStream) SetSinkConsumer(c runtime.Consumer[int])           { s.consumer = c }
func (s *outputStream) ConsumeResult(_ context.Context, m mapped)         { s.results <- m }

type outputHandler struct {
	sync       bool
	onDelivery func()
	ended      chan error
	errors     chan error
}

func (h *outputHandler) OnError(_ context.Context, _ runtime.Stream, e error) {
	if h.errors != nil {
		h.errors <- e
	}
}
func (*outputHandler) GetStreamID(context.Context, int) string { return "publication" }
func (*outputHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}
func (h *outputHandler) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, v int, m *sinkredis.SinkMessage[mapped]) error {
	m.Message = map[string]interface{}{"value": strconv.Itoa(v)}
	if h.sync {
		d, e := m.SendSync(ctx)
		m.Out(ctx, mapped{d, e})
		return e
	}
	m.Send(ctx, func(d sinkredis.Delivery, e error) mapped {
		if h.onDelivery != nil {
			h.onDelivery()
		}
		return mapped{d, e}
	})
	return nil
}
func (h *outputHandler) EndRequest(_ context.Context, _ runtime.Stream, e error, _ struct{}) {
	if h.ended != nil {
		h.ended <- e
	}
}

func TestNativeGraphCallbacksAndSettlement(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	for _, ack := range []bool{false, true} {
		t.Run(strconv.FormatBool(ack), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			s, ep := fixture(t, address, true)
			observed := testmetrics.New()
			s.env.observedMetrics = observed.Metrics()
			options, err := redis.ParseURL(address)
			require.NoError(t, err)
			options.Password = "adapter-test"
			admin := redis.NewClient(options)
			defer admin.Close()
			defer admin.Del(ctx, ep.StreamKey)
			received := make(chan ConsumerMessage, 1)
			h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, sc StreamContext[int, int, error], m ConsumerMessage, r ResultContext[struct{}, int, int, error]) error {
				received <- m
				r.SetResultCallback("id", func(ctx context.Context, _ StreamContext[int, int, error], _ struct{}, _ int) bool {
					if ack {
						require.NoError(t, m.Ack(ctx))
					}
					r.Done()
					return true
				})
				sc.Collect(ctx, 1)
				return nil
			}}
			_, err = MakeRedisStreamsEndpointConsumer[struct{}, int, int, error](s, h)
			require.NoError(t, err)
			s.emit = func(ctx context.Context, v int) { s.result.Consume(ctx, v) }
			out := &outputStream{env: s.env, results: make(chan mapped, 1)}
			publisher, err := sinkredis.MakeRedisStreamsEndpointConsumerWithResult[struct{}, int, mapped, error](out, &outputHandler{})
			require.NoError(t, err)
			require.NoError(t, s.env.source.Start(ctx))
			defer s.env.source.Stop(ctx)
			require.NoError(t, s.env.sink.Start(ctx))
			defer s.env.sink.Stop(ctx)
			publisher.Consume(ctx, 42)
			result := receive(t, out.results)
			require.NoError(t, result.err)
			require.Equal(t, sinkredis.Accepted, result.delivery.Outcome)
			m := receive(t, received)
			require.Equal(t, result.delivery.EntryID, m.Message.ID)
			require.Equal(t, "42", m.Message.Values["value"])
			require.NoError(t, receive(t, h.ended))
			require.EqualValues(t, 1, observed.Counter("broker_endpoint_source_observations_total", metrics.Labels{"protocol": "redisstreams", "connector": "redis", "endpoint": ep.Name, "observation": "received"}).Count())
			pending, err := admin.XPending(ctx, ep.StreamKey, "workers").Result()
			require.NoError(t, err)
			if ack {
				require.Zero(t, pending.Count)
			} else {
				require.EqualValues(t, 1, pending.Count)
			}
			require.Error(t, m.Ack(ctx)) // completed request cannot settle a retained handle
			s.env.source.Stop(ctx)
			s.env.sink.Stop(ctx)
		})
	}
}

// The metric counts records returned by XAUTOCLAIM, not attempts or ACKs.
func TestNativeReclaimSourceObservations(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, ep := fixture(t, address, false)
	ep.ClaimIdleMs = 1000
	ep.ClaimIntervalMs = 10
	options, err := redis.ParseURL(address)
	require.NoError(t, err)
	options.Password = "adapter-test"
	admin := redis.NewClient(options)
	defer admin.Close()
	defer admin.Del(t.Context(), ep.StreamKey)
	require.NoError(t, admin.XGroupCreateMkStream(ctx, ep.StreamKey, ep.ConsumerGroup, "0-0").Err())
	id, err := admin.XAdd(ctx, &redis.XAddArgs{Stream: ep.StreamKey, Values: map[string]interface{}{"value": "42"}}).Result()
	require.NoError(t, err)
	_, err = admin.XReadGroup(ctx, &redis.XReadGroupArgs{Group: ep.ConsumerGroup, Consumer: "old", Streams: []string{ep.StreamKey, ">"}, Count: 1}).Result()
	require.NoError(t, err)
	require.NoError(t, admin.Do(ctx, "XCLAIM", ep.StreamKey, ep.ConsumerGroup, "old", 0, id, "IDLE", 2000).Err())
	observed := testmetrics.New()
	s.env.observedMetrics = observed.Metrics()
	h := &handler{ended: make(chan error, 1), consume: func(ctx context.Context, _ StreamContext[int, int, error], m ConsumerMessage, _ ResultContext[struct{}, int, int, error]) error {
		if m.Message.ID != id {
			return fmt.Errorf("unexpected reclaimed ID %s", m.Message.ID)
		}
		return m.Ack(ctx)
	}}
	_, err = MakeRedisStreamsEndpointConsumer[struct{}, int, int, error](s, h)
	require.NoError(t, err)
	require.NoError(t, s.env.source.Start(ctx))
	defer s.env.source.Stop(t.Context())
	require.NoError(t, receive(t, h.ended))
	for _, observation := range []string{"received", "reclaimed"} {
		require.EqualValues(t, 1, observed.Counter("broker_endpoint_source_observations_total", metrics.Labels{"protocol": "redisstreams", "connector": "redis", "endpoint": ep.Name, "observation": observation}).Count())
	}
	pending, err := admin.XPending(ctx, ep.StreamKey, ep.ConsumerGroup).Result()
	require.NoError(t, err)
	require.Zero(t, pending.Count)
}
