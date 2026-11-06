package kafka

import (
	"context"
	"errors"
	"github.com/gorundebug/servicelib/api"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

type checkpointHandler struct {
	*requestLifecycleHandler
	offsets map[int32]int64
	err     error
	entered chan struct{}
	ready   <-chan struct{}
}

func (h *checkpointHandler) RestoreCheckpoint(ctx context.Context, _ StreamContext[int, int, error]) (map[int32]int64, error) {
	if h.entered != nil {
		close(h.entered)
	}
	if h.ready != nil {
		select {
		case <-h.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return h.offsets, h.err
}

type checkpointClient struct {
	sarama.Client
	low, high map[int32]int64
	calls     int
}

func (c *checkpointClient) GetOffset(_ string, p int32, at int64) (int64, error) {
	c.calls++
	if at == sarama.OffsetOldest {
		return c.low[p], nil
	}
	return c.high[p], nil
}

type checkpointSession struct {
	lifecycleSession
	claims  map[string][]int32
	offsets map[int32]int64
	updates int
}

func (s *checkpointSession) Claims() map[string][]int32 { return s.claims }
func (s *checkpointSession) ResetOffset(_ string, p int32, n int64, _ string) {
	if n <= s.offsets[p] {
		s.offsets[p] = n
	}
	s.updates++
}
func (s *checkpointSession) MarkOffset(_ string, p int32, n int64, _ string) {
	if n > s.offsets[p] {
		s.offsets[p] = n
	}
	s.updates++
}
func checkpointFixture(t *testing.T) (*saramaKafkaEndpoint, *checkpointHandler, *checkpointClient, *checkpointSession) {
	t.Helper()
	cfg, err := config.NewRuntimeConfig(constructorConfig{&config.KafkaEndpointConfig{ID: 2, Name: "events", IdDataConnector: 1, Topic: "events"}})
	if err != nil {
		t.Fatal(err)
	}
	handler := &checkpointHandler{requestLifecycleHandler: &requestLifecycleHandler{}, offsets: map[int32]int64{0: 12, 1: 18}}
	c, err := MakeSaramaKafkaEndpointConsumer[int, int, int, error](constructorStream{env: &constructorEnvironment{cfg: cfg}}, handler)
	if err != nil {
		t.Fatal(err)
	}
	ep := c.(runtime.InputEndpointConsumer).Endpoint().(*saramaKafkaEndpoint)
	client := &checkpointClient{low: map[int32]int64{0: 10, 1: 15}, high: map[int32]int64{0: 40, 1: 50}}
	ep.client, ep.restoring = client, true
	session := &checkpointSession{lifecycleSession: lifecycleSession{ctx: t.Context()}, claims: map[string][]int32{"events": {0, 1}}, offsets: map[int32]int64{0: 30, 1: -1}}
	return ep, handler, client, session
}
func TestCheckpointRestoresDurablePrefixInEitherDirection(t *testing.T) {
	ep, h, _, s := checkpointFixture(t)
	if err := ep.Setup(s); err != nil {
		t.Fatal(err)
	}
	if s.offsets[0] != 12 || s.offsets[1] != 18 {
		t.Fatal(s.offsets)
	}
	// A new group assignment obtains fresh durable state. No permanently cached
	// initial snapshot or broker offset can override the returned checkpoint.
	h.offsets = map[int32]int64{0: 20, 1: 22}
	if err := ep.Setup(s); err != nil {
		t.Fatal(err)
	}
	if s.offsets[0] != 20 || s.offsets[1] != 22 {
		t.Fatal(s.offsets)
	}
	h.offsets[0] = 99
	if ep.restored[0] != 20 {
		t.Fatal("retained mutable handler map")
	}
}
func TestCheckpointRejectsEntireInvalidAssignmentBeforeOffsetMutation(t *testing.T) {
	for _, failure := range []string{"retained-away", "beyond-end", "missing-partition", "negative", "mode-changed", "provider-error", "unexpected-topic", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			ep, h, _, s := checkpointFixture(t)
			switch failure {
			case "retained-away":
				h.offsets[1] = 14
			case "beyond-end":
				h.offsets[1] = 51
			case "missing-partition":
				delete(h.offsets, 1)
			case "negative":
				h.offsets[1] = -1
			case "mode-changed":
				h.offsets = nil
			case "provider-error":
				h.err = errors.New("snapshot unavailable")
			case "unexpected-topic":
				s.claims = map[string][]int32{"other": {0}}
			case "cancelled":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				s.ctx = ctx
			}
			if err := ep.Setup(s); err == nil {
				t.Fatal("invalid restoration accepted")
			}
			if s.updates != 0 || s.offsets[0] != 30 || s.offsets[1] != -1 {
				t.Fatal("partial assignment acknowledged", s.offsets)
			}
		})
	}
}

type restoredClaim struct {
	lifecycleClaim
	initial int64
}

func (c restoredClaim) Partition() int32     { return 0 }
func (c restoredClaim) InitialOffset() int64 { return c.initial }
func TestCheckpointRejectsImplicitBrokerOffsetReset(t *testing.T) {
	ep, _, _, s := checkpointFixture(t)
	if err := ep.Setup(s); err != nil {
		t.Fatal(err)
	}
	// No message callback is allowed after a client has reset the requested start.
	if err := ep.ConsumeClaim(s, restoredClaim{initial: sarama.OffsetOldest}); !errors.Is(err, ErrCheckpointRestore) {
		t.Fatal(err)
	}
}
func TestCheckpointDefaultLeavesBrokerPositionsUntouched(t *testing.T) {
	ep, h, client, s := checkpointFixture(t)
	ep.restoring = false
	h.offsets = nil
	if err := ep.Setup(s); err != nil {
		t.Fatal(err)
	}
	if client.calls != 0 || s.updates != 0 {
		t.Fatal("ordinary input performed restoration")
	}
}

func TestCheckpointRevokedMessageCannotAcknowledge(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: ctx}}
	message := ConsumerMessage{handlerData: &handlerData{session: session, message: &sarama.ConsumerMessage{Topic: "events", Offset: 19}}}
	cancel()
	message.MarkMessage("")
	message.Commit()
	if session.marks.Load() != 0 || session.commits.Load() != 0 {
		t.Fatal("revoked message acknowledged")
	}
}

func TestCheckpointWaitDoesNotBlockSourceStartupAndStopCancelsIt(t *testing.T) {
	ep, handler, _, _ := checkpointFixture(t)
	ep.client = nil
	cfg := ep.GetConfig().(*config.KafkaEndpointConfig)
	cfg.Enabled, cfg.ConsumerGroup = true, "checkpoint-startup"
	connector := ep.GetDataSource().GetConfig().(*config.KafkaDataConnectorConfig)
	connector.Implementation = api.DataConnectorImplementationIBMsarama
	connector.Brokers = "127.0.0.1:1"
	connector.SecurityProtocol = api.PLAINTEXT
	handler.entered = make(chan struct{})
	handler.ready = make(chan struct{}) // S3 has not installed state yet.
	owner, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := ep.Start(owner); err != nil {
		t.Fatal(err)
	}
	// Reaching this point allows the ordinary service loop to start S3 next.
	select {
	case <-handler.entered:
	case <-owner.Done():
		t.Fatal("checkpoint callback never entered")
	}
	stop, cancelStop := context.WithTimeout(t.Context(), time.Second)
	defer cancelStop()
	ep.Stop(stop)
	if stop.Err() != nil {
		t.Fatal("stop did not cancel checkpoint wait")
	}
	if ep.client != nil || ep.consumerGroup != nil {
		t.Fatal("Kafka transport opened before state installation")
	}
}
