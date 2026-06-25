/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/tests/operatorservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	baseURL = "http://127.0.0.1:9092"
	timeout = 5 * time.Second
)

var testEnv *operatorservice.TestEnv

func TestMain(m *testing.M) {
	operatorservice.Main(func(env *operatorservice.TestEnv) int {
		testEnv = env
		return m.Run()
	})
}

// post sends a JSON-encoded value to the given path.
func post(path string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func msg(key string, value int, text string) *operatorservice.Message {
	return &operatorservice.Message{Key: key, Value: value, Text: text}
}

// ── Map ──────────────────────────────────────────────────────────────────────

func TestMap(t *testing.T) {
	svc := testEnv.Service
	svc.Map.Reset()

	require.NoError(t, post("/map", msg("k1", 1, "hello")))

	got, ok := svc.Map.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Map result")
	assert.Equal(t, "k1", got.Key)
	assert.Equal(t, 1, got.Value)
	assert.Equal(t, "HELLO", got.Text)
}

// ── Filter ────────────────────────────────────────────────────────────────────

func TestFilter_Pass(t *testing.T) {
	svc := testEnv.Service
	svc.Filter.Reset()

	require.NoError(t, post("/filter", msg("k2", 5, "pass")))

	got, ok := svc.Filter.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Filter result")
	assert.Equal(t, "k2", got.Key)
	assert.Equal(t, 5, got.Value)
}

func TestFilter_Block(t *testing.T) {
	svc := testEnv.Service
	svc.Filter.Reset()

	require.NoError(t, post("/filter", msg("k2b", -1, "blocked")))

	_, ok := svc.Filter.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "message with negative value should be filtered out")
}

// ── FlatMap ───────────────────────────────────────────────────────────────────

func TestFlatMap(t *testing.T) {
	svc := testEnv.Service
	svc.FlatMap.Reset()

	require.NoError(t, post("/flatmap", msg("k3", 0, "one two three")))

	got, ok := svc.FlatMap.WaitN(3, timeout)
	require.True(t, ok, "timeout waiting for FlatMap results")
	require.Len(t, got, 3)

	texts := []string{got[0].Text, got[1].Text, got[2].Text}
	assert.ElementsMatch(t, []string{"one", "two", "three"}, texts)
}

// ── FlatMapIterable ───────────────────────────────────────────────────────────

func TestFlatMapIterable(t *testing.T) {
	svc := testEnv.Service
	svc.FlatMapIterable.Reset()

	items := []*operatorservice.Message{
		msg("a", 1, "first"),
		msg("b", 2, "second"),
		msg("c", 3, "third"),
	}
	require.NoError(t, post("/flatmap-iterable", items))

	got, ok := svc.FlatMapIterable.WaitN(3, timeout)
	require.True(t, ok, "timeout waiting for FlatMapIterable results")
	require.Len(t, got, 3)

	texts := []string{got[0].Text, got[1].Text, got[2].Text}
	assert.ElementsMatch(t, []string{"first", "second", "third"}, texts)
}

// ── Process ───────────────────────────────────────────────────────────────────

func TestProcess_Success(t *testing.T) {
	svc := testEnv.Service
	svc.Process.Reset()
	svc.ProcessError.Reset()

	require.NoError(t, post("/process", msg("k4", 1, "ok")))

	got, ok := svc.Process.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Process success result")
	assert.Equal(t, "ok:ok", got.Text)

	assert.Equal(t, 0, svc.ProcessError.Len(), "error collector should be empty")
}

func TestProcess_Error(t *testing.T) {
	svc := testEnv.Service
	svc.Process.Reset()
	svc.ProcessError.Reset()

	require.NoError(t, post("/process", msg("k4e", -1, "bad")))

	got, ok := svc.ProcessError.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Process error result")
	assert.Equal(t, "err:bad", got.Text)

	assert.Equal(t, 0, svc.Process.Len(), "success collector should be empty")
}

// ── Split ─────────────────────────────────────────────────────────────────────

func TestSplit(t *testing.T) {
	svc := testEnv.Service
	svc.Split1.Reset()
	svc.Split2.Reset()

	require.NoError(t, post("/split", msg("k5", 0, "fan")))

	got1, ok1 := svc.Split1.Wait1(timeout)
	got2, ok2 := svc.Split2.Wait1(timeout)
	require.True(t, ok1, "timeout waiting for Split1")
	require.True(t, ok2, "timeout waiting for Split2")
	assert.Equal(t, "fan", got1.Text)
	assert.Equal(t, "fan", got2.Text)
}

// ── Merge ─────────────────────────────────────────────────────────────────────

func TestMerge(t *testing.T) {
	svc := testEnv.Service
	svc.Merge.Reset()

	require.NoError(t, post("/merge1", msg("m1", 1, "from-merge1")))
	require.NoError(t, post("/merge2", msg("m2", 2, "from-merge2")))

	got, ok := svc.Merge.WaitN(2, timeout)
	require.True(t, ok, "timeout waiting for Merge results")
	require.Len(t, got, 2)

	texts := []string{got[0].Text, got[1].Text}
	assert.ElementsMatch(t, []string{"from-merge1", "from-merge2"}, texts)
}

// ── Case/When ─────────────────────────────────────────────────────────────────

func TestCase_A(t *testing.T) {
	svc := testEnv.Service
	svc.CaseA.Reset()
	svc.CaseB.Reset()

	require.NoError(t, post("/case", msg("c1", 1, "positive")))

	got, ok := svc.CaseA.Wait1(timeout)
	require.True(t, ok, "timeout waiting for CaseA result")
	assert.Equal(t, "positive", got.Text)
	assert.Equal(t, 0, svc.CaseB.Len(), "CaseB should be empty")
}

func TestCase_B(t *testing.T) {
	svc := testEnv.Service
	svc.CaseA.Reset()
	svc.CaseB.Reset()

	require.NoError(t, post("/case", msg("c2", -5, "negative")))

	got, ok := svc.CaseB.Wait1(timeout)
	require.True(t, ok, "timeout waiting for CaseB result")
	assert.Equal(t, "negative", got.Text)
	assert.Equal(t, 0, svc.CaseA.Len(), "CaseA should be empty")
}

// ── Delay ─────────────────────────────────────────────────────────────────────

func TestDelay(t *testing.T) {
	svc := testEnv.Service
	svc.DelayResult.Reset()

	before := time.Now()
	require.NoError(t, post("/delay", msg("d1", 0, "delayed")))

	got, ok := svc.DelayResult.Wait1(timeout)
	elapsed := time.Since(before)
	require.True(t, ok, "timeout waiting for Delay result")
	assert.Equal(t, "delayed", got.Text)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(40), "delay should be at least 40ms")
}

// TestDelay_ZeroDuration verifies that a Delay stream with duration ≤ 0 passes
// values through immediately (the non-timer path in Consume).
func TestDelay_ZeroDuration(t *testing.T) {
	svc := testEnv.Service
	svc.DelayResult.Reset()

	svc.DelayFunc.SetDuration(0)
	defer svc.DelayFunc.SetDuration(50 * time.Millisecond)

	require.NoError(t, post("/delay", msg("d-zero", 0, "immediate")))

	got, ok := svc.DelayResult.Wait1(timeout)
	require.True(t, ok, "timeout waiting for immediate delay result")
	assert.Equal(t, "immediate", got.Text)
}

// ── KeyBy ─────────────────────────────────────────────────────────────────────

func TestKeyBy(t *testing.T) {
	svc := testEnv.Service
	svc.KeyBy.Reset()

	require.NoError(t, post("/keyby", msg("mykey", 7, "kv")))

	got, ok := svc.KeyBy.Wait1(timeout)
	require.True(t, ok, "timeout waiting for KeyBy result")
	assert.Equal(t, "mykey", got.Key)
	require.NotNil(t, got.Value)
	assert.Equal(t, "mykey", got.Value.Key)
	assert.Equal(t, 7, got.Value.Value)
}

// ── Join ──────────────────────────────────────────────────────────────────────

func TestJoin(t *testing.T) {
	svc := testEnv.Service
	svc.Join.Reset()

	require.NoError(t, post("/join-left", msg("jkey", 10, "left")))
	require.NoError(t, post("/join-right", msg("jkey", 20, "right")))

	got, ok := svc.Join.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Join result")
	assert.Equal(t, "jkey", got.Key)
	assert.Equal(t, 30, got.Value)
	assert.Equal(t, "left+right", got.Text)
}

// ── Left Join ─────────────────────────────────────────────────────────────────

// TestJoinLeft_LeftOnly: left arrives alone → result emitted immediately (right slot empty).
func TestJoinLeft_LeftOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinLeft.Reset()

	require.NoError(t, post("/join-left-l", msg("jl1", 10, "left")))

	got, ok := svc.JoinLeft.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Left join result")
	assert.Equal(t, "jl1", got.Key)
	assert.Equal(t, 10, got.Value)
	assert.Equal(t, "left", got.Text)
}

// TestJoinLeft_RightOnly: right arrives alone → no result (handler not called when left is empty).
func TestJoinLeft_RightOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinLeft.Reset()

	require.NoError(t, post("/join-right-l", msg("jl2", 20, "right")))

	_, ok := svc.JoinLeft.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "right-only should produce no result in left join")
}

// TestJoinLeft_BothSides: right arrives first (stored), then left → result with both sides.
func TestJoinLeft_BothSides(t *testing.T) {
	svc := testEnv.Service
	svc.JoinLeft.Reset()

	require.NoError(t, post("/join-right-l", msg("jl3", 20, "right")))
	_, ok := svc.JoinLeft.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "right-only should not emit yet")

	require.NoError(t, post("/join-left-l", msg("jl3", 10, "left")))

	got, ok := svc.JoinLeft.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Left join result after both sides")
	assert.Equal(t, "jl3", got.Key)
	assert.Equal(t, 30, got.Value)
	assert.Equal(t, "left+right", got.Text)
}

// ── Right Join ────────────────────────────────────────────────────────────────

// TestJoinRight_RightOnly: right arrives alone → result emitted immediately (left slot empty).
func TestJoinRight_RightOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinRight.Reset()

	require.NoError(t, post("/join-right-r", msg("jr1", 20, "right")))

	got, ok := svc.JoinRight.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Right join result")
	assert.Equal(t, "jr1", got.Key)
	assert.Equal(t, 20, got.Value)
	assert.Equal(t, "right", got.Text)
}

// TestJoinRight_LeftOnly: left arrives alone → no result (handler not called when right is empty).
func TestJoinRight_LeftOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinRight.Reset()

	require.NoError(t, post("/join-left-r", msg("jr2", 10, "left")))

	_, ok := svc.JoinRight.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "left-only should produce no result in right join")
}

// TestJoinRight_BothSides: left arrives first (stored), then right → result with both sides.
func TestJoinRight_BothSides(t *testing.T) {
	svc := testEnv.Service
	svc.JoinRight.Reset()

	require.NoError(t, post("/join-left-r", msg("jr3", 10, "left")))
	_, ok := svc.JoinRight.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "left-only should not emit yet")

	require.NoError(t, post("/join-right-r", msg("jr3", 20, "right")))

	got, ok := svc.JoinRight.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Right join result after both sides")
	assert.Equal(t, "jr3", got.Key)
	assert.Equal(t, 30, got.Value)
	assert.Equal(t, "left+right", got.Text)
}

// ── Outer Join ────────────────────────────────────────────────────────────────

// TestJoinOuter_LeftOnly: left arrives → result emitted immediately (handler always fires).
func TestJoinOuter_LeftOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinOuter.Reset()

	require.NoError(t, post("/join-left-o", msg("jo1", 10, "left")))

	got, ok := svc.JoinOuter.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Outer join result from left-only")
	assert.Equal(t, "jo1", got.Key)
	assert.Equal(t, 10, got.Value)
	assert.Equal(t, "left", got.Text)
}

// TestJoinOuter_RightOnly: right arrives → result emitted immediately.
func TestJoinOuter_RightOnly(t *testing.T) {
	svc := testEnv.Service
	svc.JoinOuter.Reset()

	require.NoError(t, post("/join-right-o", msg("jo2", 20, "right")))

	got, ok := svc.JoinOuter.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Outer join result from right-only")
	assert.Equal(t, "jo2", got.Key)
	assert.Equal(t, 20, got.Value)
	assert.Equal(t, "right", got.Text)
}

// TestJoinOuter_BothSides: left and right for the same key → 2 independent results
// (each arrival fires the handler and clears storage, so they never combine).
func TestJoinOuter_BothSides(t *testing.T) {
	svc := testEnv.Service
	svc.JoinOuter.Reset()

	require.NoError(t, post("/join-left-o", msg("jo3", 10, "left")))
	require.NoError(t, post("/join-right-o", msg("jo3", 20, "right")))

	got, ok := svc.JoinOuter.WaitN(2, timeout)
	require.True(t, ok, "timeout waiting for both Outer join results")
	require.Len(t, got, 2)

	texts := []string{got[0].Text, got[1].Text}
	assert.ElementsMatch(t, []string{"left", "right"}, texts)
}

// ── LocalSource ───────────────────────────────────────────────────────────────

// TestLocalSource_Push verifies that a value pushed via ChannelProducer flows
// through the pipeline and arrives in the sink collector.
func TestLocalSource_Push(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()

	svc.LocalSourceProducer.Push(msg("ls1", 42, "hello"))

	got, ok := svc.LocalSource.Wait1(timeout)
	require.True(t, ok, "timeout waiting for LocalSource result")
	assert.Equal(t, "ls1", got.Key)
	assert.Equal(t, 42, got.Value)
	assert.Equal(t, "hello", got.Text)
}

// TestLocalSource_MultiPush verifies that N sequential pushes each arrive in order.
func TestLocalSource_MultiPush(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()

	msgs := []*operatorservice.Message{
		msg("ls2", 1, "one"),
		msg("ls2", 2, "two"),
		msg("ls2", 3, "three"),
	}
	for _, m := range msgs {
		svc.LocalSourceProducer.Push(m)
	}

	got, ok := svc.LocalSource.WaitN(3, timeout)
	require.True(t, ok, "timeout waiting for 3 LocalSource results")
	require.Len(t, got, 3)

	texts := []string{got[0].Text, got[1].Text, got[2].Text}
	assert.ElementsMatch(t, []string{"one", "two", "three"}, texts)
}

// TestLocalSource_BeginRequestError verifies that when BeginRequest returns an error
// neither ConsumeMessage nor EndRequest is called, and the collector stays empty.
func TestLocalSource_BeginRequestError(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()
	svc.LocalSourceHandler.SetBeginErr(operatorservice.ErrLocalSourceBeginRequest)
	defer svc.LocalSourceHandler.ClearErrors()

	svc.LocalSourceProducer.Push(msg("ls3", 0, "bad-begin"))

	_, ok := svc.LocalSource.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "no result expected when BeginRequest fails")

	// EndRequest must NOT be called (framework skips it on BeginRequest error)
	select {
	case <-svc.LocalSourceHandler.EndRequestCh:
		t.Fatal("EndRequest should not be called after BeginRequest error")
	default:
	}
}

// TestLocalSource_WithResult verifies the hasResult=true path: a value T is pushed
// through the pipeline (Map transforms it), and the result R flows back to the
// handler via SetResultCallback/consumeResult before Done() unblocks EndpointRequest.
func TestLocalSource_WithResult(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSourceResult.Reset()

	svc.LocalSourceResultProducer.Push(msg("lsr1", 5, "hello"))

	got, ok := svc.LocalSourceResult.Wait1(timeout)
	require.True(t, ok, "timeout waiting for LocalSourceResult")
	assert.Equal(t, "lsr1", got.Key)
	assert.Equal(t, 10, got.Value)       // Value * 2
	assert.Equal(t, "HELLO", got.Text)   // text uppercased by Map
}

// TestLocalSource_WithResult_Multi verifies multiple sequential request-response cycles.
func TestLocalSource_WithResult_Multi(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSourceResult.Reset()

	svc.LocalSourceResultProducer.Push(msg("lsr2", 3, "foo"))
	svc.LocalSourceResultProducer.Push(msg("lsr2", 7, "bar"))

	got, ok := svc.LocalSourceResult.WaitN(2, timeout)
	require.True(t, ok, "timeout waiting for 2 results")
	require.Len(t, got, 2)

	texts := []string{got[0].Text, got[1].Text}
	assert.ElementsMatch(t, []string{"FOO", "BAR"}, texts)
}

// TestLocalSource_ConsumeMessageError verifies that when ConsumeMessage returns an error
// the collector stays empty but EndRequest IS called with that error.
func TestLocalSource_ConsumeMessageError(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()
	svc.LocalSourceHandler.SetConsumeErr(operatorservice.ErrLocalSourceConsumeMessage)
	defer svc.LocalSourceHandler.ClearErrors()

	svc.LocalSourceProducer.Push(msg("ls4", 0, "bad-consume"))

	// EndRequest must be called with the error.
	select {
	case endErr := <-svc.LocalSourceHandler.EndRequestCh:
		assert.ErrorIs(t, endErr, operatorservice.ErrLocalSourceConsumeMessage)
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after ConsumeMessage error")
	}

	assert.Equal(t, 0, svc.LocalSource.Len(), "collector should be empty after ConsumeMessage error")
}

// ── MultiJoin ─────────────────────────────────────────────────────────────────

// TestMultiJoin_PartialNoEmit verifies that partial fills (< 3 slots) produce no output.
func TestMultiJoin_PartialNoEmit(t *testing.T) {
	svc := testEnv.Service
	svc.MultiJoin.Reset()

	require.NoError(t, post("/multijoin1", msg("mj-partial", 1, "first")))
	require.NoError(t, post("/multijoin2", msg("mj-partial", 2, "second")))

	_, ok := svc.MultiJoin.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "two slots filled: no result expected yet")

	// Third slot completes the join.
	require.NoError(t, post("/multijoin3", msg("mj-partial", 3, "third")))

	got, ok := svc.MultiJoin.Wait1(timeout)
	require.True(t, ok, "timeout waiting for MultiJoin result after third slot")
	assert.Equal(t, "mj-partial", got.Key)
	assert.Equal(t, "first+second+third", got.Text)

	// Exactly one result — the handler must not have fired prematurely.
	assert.Equal(t, 1, svc.MultiJoin.Len())
}

// TestMultiJoin_Slot0Last verifies that when slot 0 (the "left" stream) arrives
// last, the handler fires exactly once on that single event (not on each of the
// earlier slots, because the framework skips the handler while values[0] is empty).
func TestMultiJoin_Slot0Last(t *testing.T) {
	svc := testEnv.Service
	svc.MultiJoin.Reset()

	// Slots 1 and 2 arrive first — framework does not call the handler at all
	// (values[0] is empty), so no result yet.
	require.NoError(t, post("/multijoin2", msg("mj-s0last", 2, "second")))
	require.NoError(t, post("/multijoin3", msg("mj-s0last", 3, "third")))

	_, ok := svc.MultiJoin.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "slots 1+2 only: no result expected")

	// Slot 0 arrives last — handler fires once and immediately emits.
	require.NoError(t, post("/multijoin1", msg("mj-s0last", 1, "first")))

	got, ok := svc.MultiJoin.Wait1(timeout)
	require.True(t, ok, "timeout waiting for MultiJoin result after slot 0")
	assert.Equal(t, "mj-s0last", got.Key)
	assert.Equal(t, "first+second+third", got.Text)
	assert.Equal(t, 1, svc.MultiJoin.Len())
}

// ── HttpSink ──────────────────────────────────────────────────────────────────

// TestHttpSink_Happy verifies the full happy path: message is POST-ed via the
// HTTP sink, mock client receives the call, and EndRequest gets a nil error.
func TestHttpSink_Happy(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs1", 10, "hello")))

	assert.Equal(t, 1, svc.HttpSinkClient.CallCount(), "mock client must be called once")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		assert.NoError(t, endErr)
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest")
	}
}

// TestHttpSink_BeginRequestError verifies that when BeginRequest returns an error
// the HTTP client is never called and EndRequest is never called.
func TestHttpSink_BeginRequestError(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkEndpointHandler.SetBeginErr(fmt.Errorf("begin fail"))
	defer svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs-begin-err", 1, "x")))

	// Pipeline is synchronous — after post() returns, everything has run.
	assert.Equal(t, 0, svc.HttpSinkClient.CallCount(), "client must not be called when BeginRequest fails")
	assert.Empty(t, svc.HttpSinkEndpointHandler.EndRequestCh, "EndRequest must not be called when BeginRequest fails")
}

// TestHttpSink_ConsumeMessageError verifies that when ConsumeMessage returns an
// error the HTTP client is not called and EndRequest receives the error.
func TestHttpSink_ConsumeMessageError(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkEndpointHandler.SetConsumeErr(fmt.Errorf("consume fail"))
	defer svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs-consume-err", 2, "y")))

	assert.Equal(t, 0, svc.HttpSinkClient.CallCount(), "client must not be called when ConsumeMessage fails")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		require.Error(t, endErr)
		assert.Contains(t, endErr.Error(), "consume fail")
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after ConsumeMessage error")
	}
}

// TestHttpSink_NilRequest verifies that when ConsumeMessage does not call
// req.NewRequest the framework itself generates an error and calls EndRequest.
func TestHttpSink_NilRequest(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkEndpointHandler.SetSkipNewReq(true)
	defer svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs-nil-req", 3, "z")))

	assert.Equal(t, 0, svc.HttpSinkClient.CallCount(), "client must not be called when request is nil")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		require.Error(t, endErr)
		assert.Contains(t, endErr.Error(), "no HTTP request")
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after nil request")
	}
}

// TestHttpSink_ClientError verifies that when the HTTP client returns an error
// EndRequest receives that error.
func TestHttpSink_ClientError(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkClient.SetError(fmt.Errorf("network failure"))
	defer func() {
		svc.HttpSinkClient.SetError(nil)
		svc.HttpSinkEndpointHandler.ClearErrors()
	}()

	require.NoError(t, post("/http-sink-in", msg("hs-client-err", 4, "w")))

	assert.Equal(t, 1, svc.HttpSinkClient.CallCount(), "client.Do must be called even when it returns an error")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		require.Error(t, endErr)
		assert.Contains(t, endErr.Error(), "network failure")
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after client error")
	}
}

// TestHttpSink_NewRequestError verifies that when Requester.NewRequest returns an error
// (invalid HTTP method) the client is not called and EndRequest receives the error.
// This covers the error-return branch in Requester.NewRequest itself.
func TestHttpSink_NewRequestError(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkEndpointHandler.SetBadMethod(true)
	defer svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs-bad-method", 6, "u")))

	assert.Equal(t, 0, svc.HttpSinkClient.CallCount(), "client must not be called when NewRequest fails")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		require.Error(t, endErr)
		assert.Contains(t, endErr.Error(), "invalid method")
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after NewRequest error")
	}
}

// TestHttpSink_HandleResponseError verifies that when HandleResponse returns an
// error EndRequest receives that error.
func TestHttpSink_HandleResponseError(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()
	svc.HttpSinkEndpointHandler.SetHandleRespErr(fmt.Errorf("bad response"))
	defer svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("hs-resp-err", 5, "v")))

	assert.Equal(t, 1, svc.HttpSinkClient.CallCount(), "client.Do must be called on happy path up to HandleResponse")

	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		require.Error(t, endErr)
		assert.Contains(t, endErr.Error(), "bad response")
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest after HandleResponse error")
	}
}

// ── LocalSourceResult extra coverage ──────────────────────────────────────────────────────────────

// TestLocalSourceResult_SampledContext verifies the hasResult=true pipeline with sampling
// enabled on the push context. This causes a tracing span to be created in EndpointRequest,
// so when consumeResult is called the span != nil tracing branches are exercised.
func TestLocalSourceResult_SampledContext(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSourceResult.Reset()

	sampledCtx := tracing.EnableSampling(context.Background())
	svc.LocalSourceResultProducer.PushWithCtx(sampledCtx, msg("sampled-r", 4, "traced-result"))

	got, ok := svc.LocalSourceResult.Wait1(timeout)
	require.True(t, ok, "timeout waiting for sampled LocalSourceResult")
	assert.Equal(t, "sampled-r", got.Key)
	assert.Equal(t, 8, got.Value)
	assert.Equal(t, "TRACED-RESULT", got.Text)
}

// ── Coverage helpers ──────────────────────────────────────────────────────────────────────────────

// postWithHeaders sends a JSON-encoded value to path, adding the given extra headers.
func postWithHeaders(path string, v any, headers map[string]string) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, val := range headers {
		req.Header.Set(k, val)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// TestStreamIDHeader covers the x-stream-id header path in datasource/http serveHTTP
// (lines that read the header and set it on reqCtx, and the branch that recovers the
// stream ID from handlerCtx rather than generating a fresh one).
func TestStreamIDHeader(t *testing.T) {
	svc := testEnv.Service
	svc.Map.Reset()

	require.NoError(t, postWithHeaders("/map", msg("sid1", 2, "stream-id-test"),
		map[string]string{"x-stream-id": "test-stream-id-001"}))

	got, ok := svc.Map.Wait1(timeout)
	require.True(t, ok, "timeout waiting for Map result with stream ID header")
	assert.Equal(t, "STREAM-ID-TEST", got.Text)
}

// TestLocalSource_SampledContext covers the tracing span-creation path in
// datasource/localsource EndpointRequest (the branch gated on ec.tracer != nil &&
// tracing.SamplingEnabled(ctx)) and the "stream ID already in context" branch.
func TestLocalSource_SampledContext(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()

	// Build a context with both sampling enabled and a pre-set stream ID so that
	// EndpointRequest takes the "sid from context" branch (line 304) instead of
	// generating a new one, and creates a tracing span.
	sampledCtx := tracing.EnableSampling(runtime.WithStreamId(context.Background(), "sampled-stream-001"))
	svc.LocalSourceProducer.PushWithCtx(sampledCtx, msg("sampled1", 99, "traced"))

	got, ok := svc.LocalSource.Wait1(timeout)
	require.True(t, ok, "timeout waiting for LocalSource result with sampled context")
	assert.Equal(t, "sampled1", got.Key)
	assert.Equal(t, 99, got.Value)
}

// TestLocalSource_ErrorCollect covers StreamContext.ErrorCollect by configuring the handler to
// call sc.ErrorCollect instead of sc.Collect, which routes the call to the error stream consumer.
func TestLocalSource_ErrorCollect(t *testing.T) {
	svc := testEnv.Service
	svc.LocalSource.Reset()
	svc.LocalSourceHandler.ClearErrors()
	svc.LocalSourceHandler.SetUseErrorCollect(true)
	defer svc.LocalSourceHandler.ClearErrors()

	svc.LocalSourceProducer.Push(msg("ec1", 1, "error-collect"))

	_, ok := svc.LocalSource.Wait1(200 * time.Millisecond)
	assert.False(t, ok, "message should not reach sink when ErrorCollect is used instead of Collect")
}

// TestHttpSink_HandleResponseLog covers SinkStreamContext.Log and SinkStreamContext.Collect
// which are called in HandleResponse via sc.Log() and sc.Collect().
func TestHttpSink_HandleResponseLog(t *testing.T) {
	svc := testEnv.Service
	svc.HttpSinkClient.Reset()
	svc.HttpSinkEndpointHandler.ClearErrors()

	require.NoError(t, post("/http-sink-in", msg("log1", 1, "log-test")))

	assert.Equal(t, 1, svc.HttpSinkClient.CallCount(), "mock client must be called once")
	select {
	case endErr := <-svc.HttpSinkEndpointHandler.EndRequestCh:
		assert.NoError(t, endErr)
	case <-time.After(timeout):
		t.Fatal("timeout waiting for EndRequest")
	}
}

// ── HTTP source hasResult=true ────────────────────────────────────────────────────────────────────

// TestHTTPSource_WithResult calls the HTTP handler directly via httptest.NewRecorder so that
// the hasResult=true path in datasource/http/nethttp.go is covered without starting a real server.
// The pipeline doubles Value and uppercases Text; results are written as JSON to the ResponseWriter.
func TestHTTPSource_WithResult(t *testing.T) {
	svc := testEnv.Service

	m := &operatorservice.Message{Key: "hr1", Value: 5, Text: "hello"}
	body, err := json.Marshal(m)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/http-result", bytes.NewReader(body))
	svc.HttpResultHandler(w, r)

	resp := w.Result()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got operatorservice.Message
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, m.Key, got.Key)
	assert.Equal(t, m.Value*2, got.Value)
	assert.Equal(t, strings.ToUpper(m.Text), got.Text)
}

// ── Runtime graph ─────────────────────────────────────────────────────────────────────────────────

// TestRuntimeGraph exercises RuntimeToStreamApp, which walks the registered streams,
// data sources, data sinks, and endpoint consumers (calling FunctionImplementation on each).
// It also calls rarely-used accessor methods (WaitGroup, SetSinkCallback) via
// ExerciseSinkInternals to improve coverage.
func TestRuntimeGraph(t *testing.T) {
	streamApp := testEnv.Service.RuntimeToStreamApp()
	assert.NotNil(t, streamApp)
	assert.NotEmpty(t, streamApp.Streams)
	assert.NotEmpty(t, streamApp.DataConnectors)
	assert.NotEmpty(t, streamApp.Endpoints)

	// Register a no-op sink callback on the Map consumer, then send a message so
	// that sinkCallback.Done is called and that branch is covered.
	testEnv.Service.ExerciseSinkInternals()
	testEnv.Service.Map.Reset()
	require.NoError(t, post("/map", msg("graph", 1, "coverage")))
	_, ok := testEnv.Service.Map.Wait1(timeout)
	assert.True(t, ok, "message should arrive even with a no-op sink callback")
}

// ── Status web handlers ───────────────────────────────────────────────────────────────────────────

// getStatus sends a GET request to the given path and returns the status code.
func getStatus(path string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return 0, fmt.Errorf("new request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// TestStatusPage exercises the /status HTML handler (statusHandler).
func TestStatusPage(t *testing.T) {
	code, err := getStatus("/status")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code)
}

// TestStatusData exercises the /status/data JSON handler (dataHandler, makeEdges, makeNode,
// makeEdge, makeEdgeFromTo, makeErrorNode, svgDataURI, makeNodeImageURI, makeNodeImageSelectedURI).
// Because it iterates ALL registered streams, it also calls GetConsumers() on CaseStream and
// WhenStream, covering the methods that RuntimeToStreamApp never reaches.
func TestStatusData(t *testing.T) {
	code, err := getStatus("/status/data")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code)
}

// TestStatusGraph exercises the /status/graph YAML handler (graphHandler → AppToYaml and all
// of the config_to_api helpers it calls: LinkConfigToAPI, typeConfigToAPI, enumNameFromMap, etc.).
func TestStatusGraph(t *testing.T) {
	code, err := getStatus("/status/graph")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code)
}

// TestStatusVisJS and TestStatusVisCSS exercise the static-asset handlers.
func TestStatusVisJS(t *testing.T) {
	code, err := getStatus("/status/vis.min.js")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code)
}

func TestStatusVisCSS(t *testing.T) {
	code, err := getStatus("/status/vis.min.css")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code)
}

// TestStatusMethodNotAllowed covers the 405 branches in all status handlers.
func TestStatusMethodNotAllowed(t *testing.T) {
	for _, path := range []string{"/status", "/status/data", "/status/graph", "/status/vis.min.js", "/status/vis.min.css"} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		require.NoError(t, err, "path %s", path)
		resp.Body.Close()
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "path %s", path)
	}
}

// ── Shutdown ──────────────────────────────────────────────────────────────────────────────────────

// TestShutdown must run last. It shuts the service down while inside m.Run() so
// that Stop() code paths are captured by the coverage profiler (which flushes at
// m.Run() completion, not at process exit).
func TestShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	testEnv.Service.StopService(ctx)
}
