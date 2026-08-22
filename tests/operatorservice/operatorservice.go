/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operatorservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/datasink"
	"github.com/gorundebug/servicelib/datasource"
	httpds "github.com/gorundebug/servicelib/datasource/http"
	"github.com/gorundebug/servicelib/datasource/localsource"
	datasinkhttp "github.com/gorundebug/servicelib/datasink/http"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/datastruct"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/testlog"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/gorundebug/servicelib/runtime/testtracing"
	"github.com/gorundebug/servicelib/tests/operatorservice/config"
	"github.com/gorundebug/servicelib/transformation"
)

// Message is the universal data type for all operator test pipelines.
type Message struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
	Text  string `json:"text"`
}

// MessageSerde is a JSON serializer for *Message.
type MessageSerde struct{}

func (s *MessageSerde) IsStub() bool { return false }

func (s *MessageSerde) SerializeObj(value interface{}, _ []byte) ([]byte, error) {
	v, ok := value.(*Message)
	if !ok {
		return nil, fmt.Errorf("value is not *Message")
	}
	return json.Marshal(v)
}

func (s *MessageSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *MessageSerde) Serialize(value *Message, _ []byte) ([]byte, error) {
	return json.Marshal(value)
}

func (s *MessageSerde) Deserialize(data []byte) (*Message, error) {
	var m Message
	return &m, json.Unmarshal(data, &m)
}

// MessagesSerde is a JSON serializer for []*Message (used by FlatMapIterable).
type MessagesSerde struct{}

func (s *MessagesSerde) IsStub() bool { return false }

func (s *MessagesSerde) SerializeObj(value interface{}, _ []byte) ([]byte, error) {
	v, ok := value.([]*Message)
	if !ok {
		return nil, fmt.Errorf("value is not []*Message")
	}
	return json.Marshal(v)
}

func (s *MessagesSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *MessagesSerde) Serialize(value []*Message, _ []byte) ([]byte, error) {
	return json.Marshal(value)
}

func (s *MessagesSerde) Deserialize(data []byte) ([]*Message, error) {
	var m []*Message
	return m, json.Unmarshal(data, &m)
}

// ResultCollector accumulates values emitted by a sink for test assertions.
type ResultCollector[T any] struct {
	mu     sync.Mutex
	values []T
	ch     chan struct{}
}

func NewResultCollector[T any]() *ResultCollector[T] {
	return &ResultCollector[T]{ch: make(chan struct{}, 64)}
}

func (c *ResultCollector[T]) Store(v T) {
	c.mu.Lock()
	c.values = append(c.values, v)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

// WaitN blocks until at least n values are collected or timeout expires.
func (c *ResultCollector[T]) WaitN(n int, timeout time.Duration) ([]T, bool) {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		if len(c.values) >= n {
			result := make([]T, len(c.values))
			copy(result, c.values)
			c.mu.Unlock()
			return result, true
		}
		c.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, false
		}
		select {
		case <-c.ch:
		case <-time.After(remaining):
			return nil, false
		}
	}
}

// Wait1 is a convenience wrapper for WaitN(1, timeout).
func (c *ResultCollector[T]) Wait1(timeout time.Duration) (T, bool) {
	vals, ok := c.WaitN(1, timeout)
	if !ok {
		var zero T
		return zero, false
	}
	return vals[0], true
}

// Reset clears accumulated values (call between tests that share a collector).
func (c *ResultCollector[T]) Reset() {
	c.mu.Lock()
	c.values = nil
	c.mu.Unlock()
	for len(c.ch) > 0 {
		<-c.ch
	}
}

// Len returns the number of values collected so far (non-blocking).
func (c *ResultCollector[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.values)
}

// genericSinkEndpoint is a reusable sink endpoint backed by a ResultCollector.
type genericSinkEndpoint[T any] struct {
	collector *ResultCollector[T]
}

func (ep *genericSinkEndpoint[T]) GetStreamID(_ context.Context, _ T) string {
	return runtime.NewStreamID()
}

func (ep *genericSinkEndpoint[T]) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}

func (ep *genericSinkEndpoint[T]) ConsumeMessage(ctx context.Context, _ runtime.Stream, _ struct{}, value T, resultStream runtime.Collect[error]) error {
	ep.collector.Store(value)
	resultStream.Out(ctx, nil)
	return nil
}

func (ep *genericSinkEndpoint[T]) EndRequest(_ context.Context, _ runtime.Stream, _ error, _ struct{}) {}

// makeSink wires a sink stream to a ResultCollector via CustomEndpointConsumer.
func makeSink[T any](stream runtime.TypedSinkStream[T, error], col *ResultCollector[T]) error {
	ep := &genericSinkEndpoint[T]{collector: col}
	_, err := datasink.CustomEndpointConsumer[struct{}, T, error](stream, ep)
	return err
}

// makeSinkKeepConsumer is like makeSink but returns the Consumer so callers can
// exercise accessor methods (SetSinkCallback, WaitGroup) in tests.
func makeSinkKeepConsumer[T any](stream runtime.TypedSinkStream[T, error], col *ResultCollector[T]) (runtime.Consumer[T], error) {
	ep := &genericSinkEndpoint[T]{collector: col}
	return datasink.CustomEndpointConsumer[struct{}, T, error](stream, ep)
}

// sinkWG is a local interface for accessing the datasink WaitGroup.
type sinkWG interface {
	WaitGroup() *sync.WaitGroup
}

// sinkCallbackSetter is a local interface for setting a sink callback.
type sinkCallbackSetter[T any] interface {
	SetSinkCallback(runtime.SinkCallback[T])
}

// nopSinkCallback is a no-op SinkCallback used to exercise SetSinkCallback.
type nopSinkCallback[T any] struct{}

func (n *nopSinkCallback[T]) Done(_ context.Context, _ T, _ error) {}

// ExerciseSinkInternals calls accessor methods (WaitGroup, SetSinkCallback) that
// have no internal callers so they would otherwise be unreachable from tests.
func (s *OperatorService) ExerciseSinkInternals() {
	if ds, ok := s.GetDataSink(config.CustomSinkConnId).(sinkWG); ok {
		_ = ds.WaitGroup()
	}
	if ds, ok := s.GetDataSink(config.HttpSinkConnId).(sinkWG); ok {
		_ = ds.WaitGroup()
	}
	if setter, ok := s.firstMapConsumer.(sinkCallbackSetter[*Message]); ok {
		setter.SetSinkCallback(&nopSinkCallback[*Message]{})
	}
	if oec, ok := s.httpSinkConsumer.(runtime.OutputEndpointConsumer); ok {
		_ = oec.Endpoint()
	}
}

// msgHandler decodes a single *Message from an HTTP request body.
type msgHandler struct{}

func (h *msgHandler) BeginRequest(ctx context.Context, _ httpds.StreamContext[*Message, any, error], _ httpds.HandlerData) (context.Context, struct{}, error) {
	return context.WithoutCancel(ctx), struct{}{}, nil
}

func (h *msgHandler) ConsumeMessage(ctx context.Context, sc httpds.StreamContext[*Message, any, error], _ struct{}, data httpds.HandlerData, resultCtx httpds.ResultContext[struct{}, *Message, any, *Message, any, error]) error {
	defer resultCtx.Done()
	body, err := io.ReadAll(data.Request.Body)
	if err != nil {
		return nil
	}
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return nil
	}
	sc.Collect(ctx, &m)
	return nil
}

func (h *msgHandler) GetMessageID(_ context.Context, _ httpds.StreamContext[*Message, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *msgHandler) EndRequest(_ context.Context, _ httpds.StreamContext[*Message, any, error], _ error, _ struct{}, _ httpds.HandlerData) {
}

// msgsHandler decodes a JSON array []*Message from an HTTP request body.
type msgsHandler struct{}

func (h *msgsHandler) BeginRequest(ctx context.Context, _ httpds.StreamContext[[]*Message, any, error], _ httpds.HandlerData) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *msgsHandler) ConsumeMessage(ctx context.Context, sc httpds.StreamContext[[]*Message, any, error], _ struct{}, data httpds.HandlerData, resultCtx httpds.ResultContext[struct{}, []*Message, any, []*Message, any, error]) error {
	defer resultCtx.Done()
	body, err := io.ReadAll(data.Request.Body)
	if err != nil {
		return nil
	}
	var msgs []*Message
	if err := json.Unmarshal(body, &msgs); err != nil {
		return nil
	}
	sc.Collect(ctx, msgs)
	return nil
}

func (h *msgsHandler) GetMessageID(_ context.Context, _ httpds.StreamContext[[]*Message, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *msgsHandler) EndRequest(_ context.Context, _ httpds.StreamContext[[]*Message, any, error], _ error, _ struct{}, _ httpds.HandlerData) {
}

// constDelay implements transformation.DelayFunction with a configurable delay.
// Tests can change the delay at runtime via SetDuration.
type constDelay struct {
	mu sync.Mutex
	d  time.Duration
}

func newConstDelay(d time.Duration) *constDelay { return &constDelay{d: d} }

func (c *constDelay) SetDuration(d time.Duration) {
	c.mu.Lock()
	c.d = d
	c.mu.Unlock()
}

func (c *constDelay) Duration(_ context.Context, _ runtime.Stream, _ *Message) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.d
}

func (c *constDelay) DelayError(_ context.Context, _ runtime.Stream, _ *Message, _ error, _ runtime.Collect[*Message]) {
}

// ChannelProducer is a DataProducer that delivers values pushed via Push().
// Each value is delivered synchronously: Push blocks until the pipeline fully
// processes the message (BeginRequest → ConsumeMessage → EndRequest).
type ChannelProducer[T any] struct {
	ch chan ctxValue[T]
}

type ctxValue[T any] struct {
	ctx context.Context
	v   T
}

func NewChannelProducer[T any]() *ChannelProducer[T] {
	return &ChannelProducer[T]{ch: make(chan ctxValue[T], 16)}
}

// Push enqueues a value using a background context.
func (p *ChannelProducer[T]) Push(v T) {
	p.ch <- ctxValue[T]{ctx: context.Background(), v: v}
}

// PushWithCtx enqueues a value with an explicit context (e.g. with tracing sampling
// or a pre-set stream ID) so that coverage of context-sensitive code paths is possible.
func (p *ChannelProducer[T]) PushWithCtx(ctx context.Context, v T) {
	p.ch <- ctxValue[T]{ctx: ctx, v: v}
}

func (p *ChannelProducer[T]) Start(ctx context.Context, consumer runtime.Consumer[T]) error {
	for {
		select {
		case item := <-p.ch:
			consumer.Consume(item.ctx, item.v)
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *ChannelProducer[T]) Stop(_ context.Context) {}

// LocalSourceHandler is a configurable EndpointHandler[struct{}, *Message, any, error].
// Tests inject error behaviour via SetBeginErr / SetConsumeErr; EndRequest errors
// are sent to EndRequestCh for assertion.
type LocalSourceHandler struct {
	mu              sync.Mutex
	beginErr        error
	consumeErr      error
	useErrorCollect bool
	EndRequestCh    chan error
}

func newLocalSourceHandler() *LocalSourceHandler {
	return &LocalSourceHandler{EndRequestCh: make(chan error, 16)}
}

func (h *LocalSourceHandler) SetBeginErr(err error) {
	h.mu.Lock()
	h.beginErr = err
	h.mu.Unlock()
}

func (h *LocalSourceHandler) SetConsumeErr(err error) {
	h.mu.Lock()
	h.consumeErr = err
	h.mu.Unlock()
}

func (h *LocalSourceHandler) SetUseErrorCollect(v bool) {
	h.mu.Lock()
	h.useErrorCollect = v
	h.mu.Unlock()
}

func (h *LocalSourceHandler) ClearErrors() {
	h.mu.Lock()
	h.beginErr = nil
	h.consumeErr = nil
	h.useErrorCollect = false
	h.mu.Unlock()
	for len(h.EndRequestCh) > 0 {
		<-h.EndRequestCh
	}
}

func (h *LocalSourceHandler) Concurrency(_ localsource.StreamContext[*Message, any, error]) int {
	return 0
}

func (h *LocalSourceHandler) BeginRequest(ctx context.Context, _ localsource.StreamContext[*Message, any, error]) (context.Context, struct{}, error) {
	h.mu.Lock()
	err := h.beginErr
	h.mu.Unlock()
	return ctx, struct{}{}, err
}

func (h *LocalSourceHandler) ConsumeMessage(ctx context.Context, sc localsource.StreamContext[*Message, any, error], _ struct{}, value *Message, resultCtx localsource.ResultContext[struct{}, *Message, any, error]) error {
	defer resultCtx.Done()
	h.mu.Lock()
	err := h.consumeErr
	useEC := h.useErrorCollect
	h.mu.Unlock()
	if err != nil {
		return err
	}
	if useEC {
		sc.ErrorCollect(ctx, errors.New("injected error collect"))
		return nil
	}
	sc.Collect(ctx, value)
	return nil
}

func (h *LocalSourceHandler) GetMessageID(_ context.Context, _ localsource.StreamContext[*Message, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *LocalSourceHandler) EndRequest(_ context.Context, _ localsource.StreamContext[*Message, any, error], err error, _ struct{}) {
	select {
	case h.EndRequestCh <- err:
	default:
	}
}

// localSourceResultHandler implements EndpointHandler for the hasResult=true path.
// It pushes T into the pipeline and waits for the result R via SetResultCallback.
type localSourceResultHandler struct {
	collector *ResultCollector[*Message]
}

func (h *localSourceResultHandler) Concurrency(_ localsource.StreamContext[*Message, *Message, error]) int {
	return 0
}

func (h *localSourceResultHandler) BeginRequest(ctx context.Context, _ localsource.StreamContext[*Message, *Message, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *localSourceResultHandler) ConsumeMessage(ctx context.Context, sc localsource.StreamContext[*Message, *Message, error], _ struct{}, value *Message, resultCtx localsource.ResultContext[struct{}, *Message, *Message, error]) error {
	// Register result callback BEFORE collecting, because FunctionCall semantics
	// means the pipeline (and consumeResult) runs synchronously inside sc.Collect.
	resultCtx.SetResultCallback("msg", func(_ context.Context, _ localsource.StreamContext[*Message, *Message, error], _ struct{}, result *Message) bool {
		h.collector.Store(result)
		resultCtx.Done()
		return true
	})
	sc.Collect(ctx, value)
	return nil
}

func (h *localSourceResultHandler) GetMessageID(_ context.Context, _ localsource.StreamContext[*Message, *Message, error], _ struct{}, _ *Message) string {
	return "msg"
}

func (h *localSourceResultHandler) EndRequest(_ context.Context, _ localsource.StreamContext[*Message, *Message, error], _ error, _ struct{}) {
}

// httpSourceResultHandler implements EndpointHandler for the HTTP source hasResult=true path.
// It decodes the request body, registers a result callback that writes the JSON-encoded
// pipeline result back to the ResponseWriter, then collects the decoded message.
type httpSourceResultHandler struct{}

func (h *httpSourceResultHandler) BeginRequest(ctx context.Context, _ httpds.StreamContext[*Message, *Message, error], _ httpds.HandlerData) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *httpSourceResultHandler) ConsumeMessage(ctx context.Context, sc httpds.StreamContext[*Message, *Message, error], _ struct{}, data httpds.HandlerData, resultCtx httpds.ResultContext[struct{}, *Message, *Message, *Message, *Message, error]) error {
	body, err := io.ReadAll(data.Request.Body)
	if err != nil {
		resultCtx.Done()
		return nil
	}
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		resultCtx.Done()
		return nil
	}
	// Register before Collect: FunctionCall semantics runs pipeline synchronously,
	// so consumeResult fires inside Collect and must find the callback already set.
	resultCtx.SetResultCallback("msg", func(_ context.Context, _ httpds.StreamContext[*Message, *Message, error], _ struct{}, result *Message, d httpds.HandlerData) bool {
		b, _ := json.Marshal(result)
		d.Writer.Header().Set("Content-Type", "application/json")
		_, _ = d.Writer.Write(b)
		resultCtx.Done()
		return true
	})
	sc.Collect(ctx, &m)
	return nil
}

func (h *httpSourceResultHandler) GetMessageID(_ context.Context, _ httpds.StreamContext[*Message, *Message, error], _ struct{}, _ *Message) string {
	return "msg"
}

func (h *httpSourceResultHandler) EndRequest(_ context.Context, _ httpds.StreamContext[*Message, *Message, error], _ error, _ struct{}, _ httpds.HandlerData) {
}

// ErrLocalSourceBeginRequest is a sentinel error for BeginRequest injection.
var ErrLocalSourceBeginRequest = errors.New("begin request error")

// ErrLocalSourceConsumeMessage is a sentinel error for ConsumeMessage injection.
var ErrLocalSourceConsumeMessage = errors.New("consume message error")

// MockHTTPClient implements datasinkhttp.Client; records calls and returns configured responses.
type MockHTTPClient struct {
	mu       sync.Mutex
	calls    int
	doErr    error
	respCode int
	respBody []byte
	ReqCh    chan *http.Request
}

func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{ReqCh: make(chan *http.Request, 16), respCode: 200}
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.calls++
	doErr := m.doErr
	code := m.respCode
	body := m.respBody
	m.mu.Unlock()
	select {
	case m.ReqCh <- req:
	default:
	}
	if doErr != nil {
		return nil, doErr
	}
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}

func (m *MockHTTPClient) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *MockHTTPClient) SetError(err error) {
	m.mu.Lock()
	m.doErr = err
	m.mu.Unlock()
}

func (m *MockHTTPClient) SetResponse(code int, body []byte) {
	m.mu.Lock()
	m.respCode = code
	m.respBody = body
	m.mu.Unlock()
}

func (m *MockHTTPClient) Reset() {
	m.mu.Lock()
	m.calls = 0
	m.doErr = nil
	m.respCode = 200
	m.respBody = nil
	m.mu.Unlock()
	for len(m.ReqCh) > 0 {
		<-m.ReqCh
	}
}

// HttpSinkEndpointHandler is a configurable EndpointHandler for datasink/http tests.
type HttpSinkEndpointHandler struct {
	mu            sync.Mutex
	beginErr      error
	consumeErr    error
	skipNewReq    bool
	badMethod     bool
	handleRespErr error
	TargetURL     string
	EndRequestCh  chan error
}

func newHttpSinkEndpointHandler() *HttpSinkEndpointHandler {
	return &HttpSinkEndpointHandler{
		TargetURL:    "http://test-target/sink",
		EndRequestCh: make(chan error, 16),
	}
}

func (h *HttpSinkEndpointHandler) SetBeginErr(err error) {
	h.mu.Lock()
	h.beginErr = err
	h.mu.Unlock()
}

func (h *HttpSinkEndpointHandler) SetConsumeErr(err error) {
	h.mu.Lock()
	h.consumeErr = err
	h.mu.Unlock()
}

func (h *HttpSinkEndpointHandler) SetSkipNewReq(skip bool) {
	h.mu.Lock()
	h.skipNewReq = skip
	h.mu.Unlock()
}

func (h *HttpSinkEndpointHandler) SetBadMethod(bad bool) {
	h.mu.Lock()
	h.badMethod = bad
	h.mu.Unlock()
}

func (h *HttpSinkEndpointHandler) SetHandleRespErr(err error) {
	h.mu.Lock()
	h.handleRespErr = err
	h.mu.Unlock()
}

func (h *HttpSinkEndpointHandler) ClearErrors() {
	h.mu.Lock()
	h.beginErr = nil
	h.consumeErr = nil
	h.skipNewReq = false
	h.badMethod = false
	h.handleRespErr = nil
	h.mu.Unlock()
	for len(h.EndRequestCh) > 0 {
		<-h.EndRequestCh
	}
}

func (h *HttpSinkEndpointHandler) BeginRequest(ctx context.Context, _ datasinkhttp.StreamContext[*Message, struct{}, error]) (context.Context, struct{}, error) {
	h.mu.Lock()
	err := h.beginErr
	h.mu.Unlock()
	return ctx, struct{}{}, err
}

func (h *HttpSinkEndpointHandler) ConsumeMessage(ctx context.Context, _ datasinkhttp.StreamContext[*Message, struct{}, error], _ struct{}, value *Message, req *datasinkhttp.Requester) error {
	h.mu.Lock()
	consumeErr := h.consumeErr
	skipNewReq := h.skipNewReq
	badMethod := h.badMethod
	url := h.TargetURL
	h.mu.Unlock()
	if consumeErr != nil {
		return consumeErr
	}
	if skipNewReq {
		return nil
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	method := http.MethodPost
	if badMethod {
		method = "INVALID METHOD"
	}
	if _, err = req.NewRequest(ctx, method, url, bytes.NewReader(body)); err != nil {
		return err
	}
	return nil
}

func (h *HttpSinkEndpointHandler) HandleResponse(ctx context.Context, sc datasinkhttp.StreamContext[*Message, struct{}, error], _ struct{}, _ datasinkhttp.Response) error {
	_ = sc.Log()
	sc.Collect(ctx, struct{}{})
	h.mu.Lock()
	err := h.handleRespErr
	h.mu.Unlock()
	return err
}

func (h *HttpSinkEndpointHandler) EndRequest(_ context.Context, _ datasinkhttp.StreamContext[*Message, struct{}, error], err error, _ struct{}) {
	select {
	case h.EndRequestCh <- err:
	default:
	}
}

// OperatorService is the test service with all operator pipelines wired up.
type OperatorService struct {
	runtime.ServiceApp
	stopOnce sync.Once

	// Collectors — used by tests to read results.
	Map             *ResultCollector[*Message]
	Filter          *ResultCollector[*Message]
	FlatMap         *ResultCollector[*Message]
	FlatMapIterable *ResultCollector[*Message]
	Process         *ResultCollector[*Message]
	ProcessError    *ResultCollector[*Message]
	Split1          *ResultCollector[*Message]
	Split2          *ResultCollector[*Message]
	Merge           *ResultCollector[*Message]
	CaseA           *ResultCollector[*Message]
	CaseB           *ResultCollector[*Message]
	DelayResult     *ResultCollector[*Message]
	KeyBy           *ResultCollector[datastruct.KeyValue[string, *Message]]
	Join            *ResultCollector[*Message]
	JoinLeft        *ResultCollector[*Message]
	JoinRight       *ResultCollector[*Message]
	JoinOuter       *ResultCollector[*Message]
	MultiJoin       *ResultCollector[*Message]
	LocalSource       *ResultCollector[*Message]
	LocalSourceResult *ResultCollector[*Message]

	LocalSourceProducer       *ChannelProducer[*Message]
	LocalSourceHandler        *LocalSourceHandler
	LocalSourceResultProducer *ChannelProducer[*Message]

	HttpSinkClient          *MockHTTPClient
	HttpSinkEndpointHandler *HttpSinkEndpointHandler
	HttpResultHandler       httpds.HTTPHandler

	DelayFunc *constDelay

	firstMapConsumer  runtime.Consumer[*Message]
	httpSinkConsumer  runtime.Consumer[*Message]
}

func (s *OperatorService) GetConfig() *config.Config {
	return s.ServiceApp.GetConfig().(*config.Config)
}

func (s *OperatorService) RegisterGRPCHandler(_ runtime.Endpoint, _ any) {}

func (s *OperatorService) GetSerde(valueType reflect.Type) (serde.Serializer, error) {
	switch valueType {
	case serde.GetSerdeType[Message](), serde.GetSerdeType[*Message]():
		var ser serde.Serde[*Message] = &MessageSerde{}
		return ser, nil
	case serde.GetSerdeType[[]*Message]():
		var ser serde.Serde[[]*Message] = &MessagesSerde{}
		return ser, nil
	}
	return nil, nil
}

// registerHTTP registers one *Message HTTP endpoint consumer and wires the HTTP handler.
func (s *OperatorService) registerHTTP(
	stream runtime.TypedInputStream[*Message, any, error],
	h *msgHandler,
	path string,
) error {
	_, handler, err := datasource.NetHTTPEndpointConsumer[struct{}, *Message, any, *Message, any, error](stream, h)
	if err != nil {
		return err
	}
	s.RegisterHTTPHandler(path, http.HandlerFunc(handler))
	return nil
}

func (s *OperatorService) buildRuntime(_ context.Context) error {
	cfg := s.GetConfig()
	h := &msgHandler{}

	// ── Map ──────────────────────────────────────────────────────────────────
	inputMap, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMap, s)
	if err != nil {
		return err
	}
	mapStream, err := transformation.Map[*Message, *Message](&cfg.Streams.MapStream, inputMap,
		transformation.MapHandler[*Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message]) {
			out.Out(ctx, &Message{Key: m.Key, Value: m.Value, Text: strings.ToUpper(m.Text)})
		}),
	)
	if err != nil {
		return err
	}
	sinkMap, err := transformation.Sink[*Message, error](&cfg.Streams.SinkMap, mapStream)
	if err != nil {
		return err
	}
	if s.firstMapConsumer, err = makeSinkKeepConsumer(sinkMap, s.Map); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMap, h, cfg.Endpoints.Map.Path); err != nil {
		return err
	}

	// ── Filter ───────────────────────────────────────────────────────────────
	inputFilter, err := transformation.Input[*Message, any, error](&cfg.Streams.InputFilter, s)
	if err != nil {
		return err
	}
	filterStream, err := transformation.Filter[*Message](&cfg.Streams.FilterStream, inputFilter,
		transformation.FilterHandler[*Message](func(_ context.Context, _ runtime.Stream, m *Message) bool {
			return m.Value > 0
		}),
	)
	if err != nil {
		return err
	}
	sinkFilter, err := transformation.Sink[*Message, error](&cfg.Streams.SinkFilter, filterStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkFilter, s.Filter); err != nil {
		return err
	}
	if err = s.registerHTTP(inputFilter, h, cfg.Endpoints.Filter.Path); err != nil {
		return err
	}

	// ── FlatMap ───────────────────────────────────────────────────────────────
	inputFlatMap, err := transformation.Input[*Message, any, error](&cfg.Streams.InputFlatMap, s)
	if err != nil {
		return err
	}
	flatMapStream, err := transformation.FlatMap[*Message, *Message](&cfg.Streams.FlatMapStream, inputFlatMap,
		transformation.FlatMapHandler[*Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message]) {
			for _, word := range strings.Fields(m.Text) {
				out.Out(ctx, &Message{Key: m.Key, Value: m.Value, Text: word})
			}
		}),
	)
	if err != nil {
		return err
	}
	sinkFlatMap, err := transformation.Sink[*Message, error](&cfg.Streams.SinkFlatMap, flatMapStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkFlatMap, s.FlatMap); err != nil {
		return err
	}
	if err = s.registerHTTP(inputFlatMap, h, cfg.Endpoints.FlatMap.Path); err != nil {
		return err
	}

	// ── FlatMapIterable ───────────────────────────────────────────────────────
	inputFlatMapIterable, err := transformation.Input[[]*Message, any, error](&cfg.Streams.InputFlatMapIterable, s)
	if err != nil {
		return err
	}
	flatMapIterableStream, err := transformation.FlatMapIterable[[]*Message, *Message](&cfg.Streams.FlatMapIterableStream, inputFlatMapIterable)
	if err != nil {
		return err
	}
	sinkFlatMapIterable, err := transformation.Sink[*Message, error](&cfg.Streams.SinkFlatMapIterable, flatMapIterableStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkFlatMapIterable, s.FlatMapIterable); err != nil {
		return err
	}
	mh := &msgsHandler{}
	_, fmiHandler, err := datasource.NetHTTPEndpointConsumer[struct{}, []*Message, any, []*Message, any, error](inputFlatMapIterable, mh)
	if err != nil {
		return err
	}
	s.RegisterHTTPHandler(cfg.Endpoints.FlatMapIterable.Path, http.HandlerFunc(fmiHandler))

	// ── Process ───────────────────────────────────────────────────────────────
	inputProcess, err := transformation.Input[*Message, any, error](&cfg.Streams.InputProcess, s)
	if err != nil {
		return err
	}
	processStream, err := transformation.Process[*Message, *Message, *Message](&cfg.Streams.ProcessStream, inputProcess,
		transformation.ProcessHandler[*Message, *Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message], errOut runtime.Collect[*Message]) {
			if m.Value >= 0 {
				out.Out(ctx, &Message{Key: m.Key, Value: m.Value, Text: "ok:" + m.Text})
			} else {
				errOut.Out(ctx, &Message{Key: m.Key, Value: m.Value, Text: "err:" + m.Text})
			}
		}),
	)
	if err != nil {
		return err
	}
	sinkProcess, err := transformation.Sink[*Message, error](&cfg.Streams.SinkProcess, processStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkProcess, s.Process); err != nil {
		return err
	}
	sinkProcessError, err := transformation.Sink[*Message, error](&cfg.Streams.SinkProcessError, processStream.GetErrorStream())
	if err != nil {
		return err
	}
	if err = makeSink(sinkProcessError, s.ProcessError); err != nil {
		return err
	}
	if err = s.registerHTTP(inputProcess, h, cfg.Endpoints.Process.Path); err != nil {
		return err
	}

	// ── Split ─────────────────────────────────────────────────────────────────
	inputSplit, err := transformation.Input[*Message, any, error](&cfg.Streams.InputSplit, s)
	if err != nil {
		return err
	}
	splitStream, err := transformation.Split[*Message](&cfg.Streams.SplitStream, inputSplit)
	if err != nil {
		return err
	}
	splitLink1 := splitStream.AddStream()
	splitLink2 := splitStream.AddStream()
	sinkSplit1, err := transformation.Sink[*Message, error](&cfg.Streams.SinkSplit1, splitLink1)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSplit1, s.Split1); err != nil {
		return err
	}
	sinkSplit2, err := transformation.Sink[*Message, error](&cfg.Streams.SinkSplit2, splitLink2)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSplit2, s.Split2); err != nil {
		return err
	}
	if err = s.registerHTTP(inputSplit, h, cfg.Endpoints.Split.Path); err != nil {
		return err
	}

	// ── Merge ─────────────────────────────────────────────────────────────────
	inputMerge1, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMerge1, s)
	if err != nil {
		return err
	}
	inputMerge2, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMerge2, s)
	if err != nil {
		return err
	}
	mergeStream, err := transformation.Merge[*Message](&cfg.Streams.MergeStream, inputMerge1, inputMerge2)
	if err != nil {
		return err
	}
	sinkMerge, err := transformation.Sink[*Message, error](&cfg.Streams.SinkMerge, mergeStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkMerge, s.Merge); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMerge1, h, cfg.Endpoints.Merge1.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMerge2, h, cfg.Endpoints.Merge2.Path); err != nil {
		return err
	}

	// ── Case/When ─────────────────────────────────────────────────────────────
	inputCase, err := transformation.Input[*Message, any, error](&cfg.Streams.InputCase, s)
	if err != nil {
		return err
	}
	caseStream, err := transformation.CaseStream[*Message](&cfg.Streams.CaseStream, inputCase,
		transformation.BuildSwitchHandler[*Message](func(_ runtime.Stream, _ []transformation.When) (func(*Message) int, error) {
			return func(m *Message) int {
				if m.Value > 0 {
					return 0 // WhenA
				}
				return 1 // WhenB
			}, nil
		}),
	)
	if err != nil {
		return err
	}
	whenA, err := transformation.WhenStream[*Message, *Message](&cfg.Streams.WhenA, caseStream)
	if err != nil {
		return err
	}
	whenB, err := transformation.WhenStream[*Message, *Message](&cfg.Streams.WhenB, caseStream)
	if err != nil {
		return err
	}
	sinkCaseA, err := transformation.Sink[*Message, error](&cfg.Streams.SinkCaseA, whenA)
	if err != nil {
		return err
	}
	if err = makeSink(sinkCaseA, s.CaseA); err != nil {
		return err
	}
	sinkCaseB, err := transformation.Sink[*Message, error](&cfg.Streams.SinkCaseB, whenB)
	if err != nil {
		return err
	}
	if err = makeSink(sinkCaseB, s.CaseB); err != nil {
		return err
	}
	if err = s.registerHTTP(inputCase, h, cfg.Endpoints.Case.Path); err != nil {
		return err
	}

	// ── Delay ─────────────────────────────────────────────────────────────────
	inputDelay, err := transformation.Input[*Message, any, error](&cfg.Streams.InputDelay, s)
	if err != nil {
		return err
	}
	s.DelayFunc = newConstDelay(50 * time.Millisecond)
	delayStream, err := transformation.Delay[*Message](&cfg.Streams.DelayStream, inputDelay, s.DelayFunc)
	if err != nil {
		return err
	}
	sinkDelay, err := transformation.Sink[*Message, error](&cfg.Streams.SinkDelay, delayStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkDelay, s.DelayResult); err != nil {
		return err
	}
	if err = s.registerHTTP(inputDelay, h, cfg.Endpoints.Delay.Path); err != nil {
		return err
	}

	// ── KeyBy ─────────────────────────────────────────────────────────────────
	inputKeyBy, err := transformation.Input[*Message, any, error](&cfg.Streams.InputKeyBy, s)
	if err != nil {
		return err
	}
	keyByStream, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByStream, inputKeyBy,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	sinkKeyBy, err := transformation.Sink[datastruct.KeyValue[string, *Message], error](&cfg.Streams.SinkKeyBy, keyByStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkKeyBy, s.KeyBy); err != nil {
		return err
	}
	if err = s.registerHTTP(inputKeyBy, h, cfg.Endpoints.KeyBy.Path); err != nil {
		return err
	}

	// ── Join ──────────────────────────────────────────────────────────────────
	inputJoinLeft, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinLeft, s)
	if err != nil {
		return err
	}
	keyByJoinLeft, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinLeft, inputJoinLeft,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputJoinRight, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinRight, s)
	if err != nil {
		return err
	}
	keyByJoinRight, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinRight, inputJoinRight,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	joinStream, err := transformation.Join[string, *Message, *Message, *Message](&cfg.Streams.JoinStream, keyByJoinLeft, keyByJoinRight,
		transformation.JoinHandler[string, *Message, *Message, *Message](func(ctx context.Context, _ runtime.Stream, key string, left []*Message, right []*Message, out runtime.Collect[*Message]) bool {
			if len(left) > 0 && len(right) > 0 {
				out.Out(ctx, &Message{Key: key, Value: left[0].Value + right[0].Value, Text: left[0].Text + "+" + right[0].Text})
				return true
			}
			return false
		}),
	)
	if err != nil {
		return err
	}
	sinkJoin, err := transformation.Sink[*Message, error](&cfg.Streams.SinkJoin, joinStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkJoin, s.Join); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinLeft, h, cfg.Endpoints.JoinLeft.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinRight, h, cfg.Endpoints.JoinRight.Path); err != nil {
		return err
	}

	// ── Left Join ─────────────────────────────────────────────────────────────
	inputJoinLLeft, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinLLeft, s)
	if err != nil {
		return err
	}
	keyByJoinLLeft, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinLLeft, inputJoinLLeft,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputJoinRLeft, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinRLeft, s)
	if err != nil {
		return err
	}
	keyByJoinRLeft, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinRLeft, inputJoinRLeft,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	joinStreamLeft, err := transformation.Join[string, *Message, *Message, *Message](&cfg.Streams.JoinStreamLeft, keyByJoinLLeft, keyByJoinRLeft,
		transformation.JoinHandler[string, *Message, *Message, *Message](func(ctx context.Context, _ runtime.Stream, key string, left []*Message, right []*Message, out runtime.Collect[*Message]) bool {
			// Left join: handler fires when left is non-empty; right may be empty
			rightText := ""
			rightVal := 0
			if len(right) > 0 {
				rightText = "+" + right[0].Text
				rightVal = right[0].Value
			}
			out.Out(ctx, &Message{Key: key, Value: left[0].Value + rightVal, Text: left[0].Text + rightText})
			return true
		}),
	)
	if err != nil {
		return err
	}
	sinkJoinLeft, err := transformation.Sink[*Message, error](&cfg.Streams.SinkJoinLeft, joinStreamLeft)
	if err != nil {
		return err
	}
	if err = makeSink(sinkJoinLeft, s.JoinLeft); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinLLeft, h, cfg.Endpoints.JoinLLeft.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinRLeft, h, cfg.Endpoints.JoinRLeft.Path); err != nil {
		return err
	}

	// ── Right Join ────────────────────────────────────────────────────────────
	inputJoinLRight, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinLRight, s)
	if err != nil {
		return err
	}
	keyByJoinLRight, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinLRight, inputJoinLRight,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputJoinRRight, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinRRight, s)
	if err != nil {
		return err
	}
	keyByJoinRRight, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinRRight, inputJoinRRight,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	joinStreamRight, err := transformation.Join[string, *Message, *Message, *Message](&cfg.Streams.JoinStreamRight, keyByJoinLRight, keyByJoinRRight,
		transformation.JoinHandler[string, *Message, *Message, *Message](func(ctx context.Context, _ runtime.Stream, key string, left []*Message, right []*Message, out runtime.Collect[*Message]) bool {
			// Right join: handler fires when right is non-empty; left may be empty
			leftText := ""
			leftVal := 0
			if len(left) > 0 {
				leftText = left[0].Text + "+"
				leftVal = left[0].Value
			}
			out.Out(ctx, &Message{Key: key, Value: leftVal + right[0].Value, Text: leftText + right[0].Text})
			return true
		}),
	)
	if err != nil {
		return err
	}
	sinkJoinRight, err := transformation.Sink[*Message, error](&cfg.Streams.SinkJoinRight, joinStreamRight)
	if err != nil {
		return err
	}
	if err = makeSink(sinkJoinRight, s.JoinRight); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinLRight, h, cfg.Endpoints.JoinLRight.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinRRight, h, cfg.Endpoints.JoinRRight.Path); err != nil {
		return err
	}

	// ── Outer Join ────────────────────────────────────────────────────────────
	inputJoinLOuter, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinLOuter, s)
	if err != nil {
		return err
	}
	keyByJoinLOuter, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinLOuter, inputJoinLOuter,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputJoinROuter, err := transformation.Input[*Message, any, error](&cfg.Streams.InputJoinROuter, s)
	if err != nil {
		return err
	}
	keyByJoinROuter, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByJoinROuter, inputJoinROuter,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	joinStreamOuter, err := transformation.Join[string, *Message, *Message, *Message](&cfg.Streams.JoinStreamOuter, keyByJoinLOuter, keyByJoinROuter,
		transformation.JoinHandler[string, *Message, *Message, *Message](func(ctx context.Context, _ runtime.Stream, key string, left []*Message, right []*Message, out runtime.Collect[*Message]) bool {
			// Outer join: handler fires on every arrival regardless of which side
			var parts []string
			val := 0
			if len(left) > 0 {
				parts = append(parts, left[0].Text)
				val += left[0].Value
			}
			if len(right) > 0 {
				parts = append(parts, right[0].Text)
				val += right[0].Value
			}
			out.Out(ctx, &Message{Key: key, Value: val, Text: strings.Join(parts, "+")})
			return true
		}),
	)
	if err != nil {
		return err
	}
	sinkJoinOuter, err := transformation.Sink[*Message, error](&cfg.Streams.SinkJoinOuter, joinStreamOuter)
	if err != nil {
		return err
	}
	if err = makeSink(sinkJoinOuter, s.JoinOuter); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinLOuter, h, cfg.Endpoints.JoinLOuter.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputJoinROuter, h, cfg.Endpoints.JoinROuter.Path); err != nil {
		return err
	}

	// ── LocalSource ───────────────────────────────────────────────────────────
	inputLocalSource, err := transformation.Input[*Message, any, error](&cfg.Streams.InputLocalSource, s)
	if err != nil {
		return err
	}
	sinkLocalSource, err := transformation.Sink[*Message, error](&cfg.Streams.SinkLocalSource, inputLocalSource)
	if err != nil {
		return err
	}
	if err = makeSink(sinkLocalSource, s.LocalSource); err != nil {
		return err
	}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, any, error](inputLocalSource, s.LocalSourceProducer, s.LocalSourceHandler); err != nil {
		return err
	}

	// ── LocalSourceResult (hasResult=true: result flows back via callback) ────
	inputLocalSourceResult, err := transformation.Input[*Message, *Message, error](&cfg.Streams.InputLocalSourceResult, s)
	if err != nil {
		return err
	}
	mapLocalSourceResult, err := transformation.Map[*Message, *Message](&cfg.Streams.MapLocalSourceResult, inputLocalSourceResult,
		transformation.MapHandler[*Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message]) {
			out.Out(ctx, &Message{Key: m.Key, Value: m.Value * 2, Text: strings.ToUpper(m.Text)})
		}),
	)
	if err != nil {
		return err
	}
	// Wire map output back as the result stream so hasResult=true.
	if err = inputLocalSourceResult.SetSource(mapLocalSourceResult); err != nil {
		return err
	}
	resultHandler := &localSourceResultHandler{collector: s.LocalSourceResult}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, *Message, error](inputLocalSourceResult, s.LocalSourceResultProducer, resultHandler); err != nil {
		return err
	}

	// ── MultiJoin ─────────────────────────────────────────────────────────────
	inputMJ1, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMultiJoin1, s)
	if err != nil {
		return err
	}
	kbMJ1, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByMultiJoin1, inputMJ1,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputMJ2, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMultiJoin2, s)
	if err != nil {
		return err
	}
	kbMJ2, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByMultiJoin2, inputMJ2,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	inputMJ3, err := transformation.Input[*Message, any, error](&cfg.Streams.InputMultiJoin3, s)
	if err != nil {
		return err
	}
	kbMJ3, err := transformation.KeyBy[*Message, string, *Message](&cfg.Streams.KeyByMultiJoin3, inputMJ3,
		transformation.KeyByHandler[*Message, string, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[datastruct.KeyValue[string, *Message]]) {
			out.Out(ctx, datastruct.KeyValue[string, *Message]{Key: m.Key, Value: m})
		}),
	)
	if err != nil {
		return err
	}
	multiJoinStream, err := transformation.MultiJoin[string, *Message, *Message](&cfg.Streams.MultiJoinStream, kbMJ1,
		transformation.MultiJoinHandler[string, *Message, *Message](func(ctx context.Context, _ runtime.Stream, key string, values [][]interface{}, out runtime.Collect[*Message]) bool {
			if len(values) < 3 || len(values[0]) == 0 || len(values[1]) == 0 || len(values[2]) == 0 {
				return false
			}
			t1 := values[0][0].(*Message).Text
			t2 := values[1][0].(*Message).Text
			t3 := values[2][0].(*Message).Text
			out.Out(ctx, &Message{Key: key, Text: t1 + "+" + t2 + "+" + t3})
			return true
		}),
	)
	if err != nil {
		return err
	}
	if err = transformation.MultiJoinLink[string, *Message, *Message, *Message](multiJoinStream, kbMJ2); err != nil {
		return err
	}
	if err = transformation.MultiJoinLink[string, *Message, *Message, *Message](multiJoinStream, kbMJ3); err != nil {
		return err
	}
	sinkMultiJoin, err := transformation.Sink[*Message, error](&cfg.Streams.SinkMultiJoin, multiJoinStream)
	if err != nil {
		return err
	}
	if err = makeSink(sinkMultiJoin, s.MultiJoin); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMJ1, h, cfg.Endpoints.MultiJoin1.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMJ2, h, cfg.Endpoints.MultiJoin2.Path); err != nil {
		return err
	}
	if err = s.registerHTTP(inputMJ3, h, cfg.Endpoints.MultiJoin3.Path); err != nil {
		return err
	}

	// ── HttpSink ──────────────────────────────────────────────────────────────
	inputHttpSink, err := transformation.Input[*Message, any, error](&cfg.Streams.InputHttpSink, s)
	if err != nil {
		return err
	}
	sinkHttpSink, err := transformation.SinkWithResult[*Message, struct{}, error](&cfg.Streams.SinkHttpSink, inputHttpSink)
	if err != nil {
		return err
	}
	if s.httpSinkConsumer, err = datasinkhttp.MakeNetHTTPEndpointConsumer[struct{}, *Message, struct{}, *Message, struct{}, error](
		sinkHttpSink, s.HttpSinkClient, s.HttpSinkEndpointHandler,
	); err != nil {
		return err
	}
	if err = s.registerHTTP(inputHttpSink, h, cfg.Endpoints.HttpSinkIn.Path); err != nil {
		return err
	}

	// ── HttpResult (HTTP source hasResult=true path) ──────────────────────────
	inputHttpResult, err := transformation.Input[*Message, *Message, error](&cfg.Streams.InputHttpResult, s)
	if err != nil {
		return err
	}
	mapHttpResult, err := transformation.Map[*Message, *Message](&cfg.Streams.MapHttpResult, inputHttpResult,
		transformation.MapHandler[*Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message]) {
			out.Out(ctx, &Message{Key: m.Key, Value: m.Value * 2, Text: strings.ToUpper(m.Text)})
		}),
	)
	if err != nil {
		return err
	}
	if err = inputHttpResult.SetSource(mapHttpResult); err != nil {
		return err
	}
	_, s.HttpResultHandler, err = datasource.NetHTTPEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](inputHttpResult, &httpSourceResultHandler{})
	if err != nil {
		return err
	}

	return nil
}

func (s *OperatorService) StartService(ctx context.Context) error {
	if err := s.buildRuntime(ctx); err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}
	return s.Start(ctx)
}

func (s *OperatorService) StopService(ctx context.Context) {
	s.stopOnce.Do(func() {
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		done := make(chan struct{})
		go func() {
			s.Stop(timeoutCtx)
			close(done)
		}()
		select {
		case <-done:
		case <-timeoutCtx.Done():
		}
	})
}

// TestEnv holds test-scoped dependencies.
type TestEnv struct {
	Service *OperatorService
	Metrics *testmetrics.TestMetrics
	Tracing *testtracing.TestTracing
	Log     *testlog.TestLog
}

type testDeps struct {
	metrics *testmetrics.TestMetrics
	tracing *testtracing.TestTracing
	logs    *testlog.TestLog
}

func (d *testDeps) MetricsEngine(_ context.Context, _ environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
	return d.metrics, nil
}

func (d *testDeps) TracingEngine(_ context.Context, _ environment.ServiceEnvironment) (tracing.TracingEngine, error) {
	return d.tracing, nil
}

func (d *testDeps) LogsEngine(_ context.Context, _ environment.ServiceEnvironment) (log.LogsEngine, error) {
	return d.logs, nil
}

func (d *testDeps) DelayPool(_ context.Context, _ environment.ServiceEnvironment) (environment.DelayPool, error) {
	return nil, nil
}

// Main bootstraps the operator service, runs the test suite, and shuts down.
func Main(run func(*TestEnv) int) int {
	mainCtx := context.Background()

	env := &TestEnv{
		Metrics: testmetrics.New(),
		Tracing: testtracing.New(),
		Log:     testlog.New(),
	}
	deps := &testDeps{metrics: env.Metrics, tracing: env.Tracing, logs: env.Log}

	var err error
	env.Service, err = runtime.MakeService[*OperatorService, *config.Config](
		mainCtx,
		"OperatorService",
		deps,
		nil,
		nil,
		func() *OperatorService {
			return &OperatorService{
				Map:             NewResultCollector[*Message](),
				Filter:          NewResultCollector[*Message](),
				FlatMap:         NewResultCollector[*Message](),
				FlatMapIterable: NewResultCollector[*Message](),
				Process:         NewResultCollector[*Message](),
				ProcessError:    NewResultCollector[*Message](),
				Split1:          NewResultCollector[*Message](),
				Split2:          NewResultCollector[*Message](),
				Merge:           NewResultCollector[*Message](),
				CaseA:           NewResultCollector[*Message](),
				CaseB:           NewResultCollector[*Message](),
				DelayResult:     NewResultCollector[*Message](),
				KeyBy:           NewResultCollector[datastruct.KeyValue[string, *Message]](),
				Join:            NewResultCollector[*Message](),
				JoinLeft:        NewResultCollector[*Message](),
				JoinRight:       NewResultCollector[*Message](),
				JoinOuter:       NewResultCollector[*Message](),
				MultiJoin:       NewResultCollector[*Message](),
				LocalSource:       NewResultCollector[*Message](),
				LocalSourceResult: NewResultCollector[*Message](),

				LocalSourceProducer:       NewChannelProducer[*Message](),
				LocalSourceHandler:        newLocalSourceHandler(),
				LocalSourceResultProducer: NewChannelProducer[*Message](),

				HttpSinkClient:          NewMockHTTPClient(),
				HttpSinkEndpointHandler: newHttpSinkEndpointHandler(),
			}
		},
		func() *config.Config { return config.MakeConfig() },
	)
	if err != nil {
		panic(err)
	}

	if err := env.Service.StartService(mainCtx); err != nil {
		panic(err)
	}

	code := run(env)

	timeoutCtx, cancel := context.WithTimeout(mainCtx, 10*time.Second)
	defer cancel()
	env.Service.StopService(timeoutCtx)

	return code
}
