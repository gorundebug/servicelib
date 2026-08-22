/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grpcservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorundebug/servicelib/datasink"
	datasinkgrpc "github.com/gorundebug/servicelib/datasink/grpc"
	datasourcegrpc "github.com/gorundebug/servicelib/datasource/grpc"
	"github.com/gorundebug/servicelib/datasource/localsource"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/testlog"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/gorundebug/servicelib/runtime/testtracing"
	"github.com/gorundebug/servicelib/tests/grpcservice/config"
	"github.com/gorundebug/servicelib/transformation"
)

// Message is the shared data type for all gRPC test pipelines.
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

// ResultCollector accumulates values from a pipeline sink for test assertions.
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

func (c *ResultCollector[T]) Wait1(timeout time.Duration) (T, bool) {
	vals, ok := c.WaitN(1, timeout)
	if !ok {
		var zero T
		return zero, false
	}
	return vals[0], true
}

func (c *ResultCollector[T]) Reset() {
	c.mu.Lock()
	c.values = nil
	c.mu.Unlock()
	for len(c.ch) > 0 {
		<-c.ch
	}
}

func (c *ResultCollector[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.values)
}

// ChannelProducer delivers values pushed via Push() into a pipeline.
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

func (p *ChannelProducer[T]) Push(v T) {
	p.ch <- ctxValue[T]{ctx: context.Background(), v: v}
}

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

// genericSinkEndpoint wires a sink stream to a ResultCollector.
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

func makeSink[T any](stream runtime.TypedSinkStream[T, error], col *ResultCollector[T]) error {
	ep := &genericSinkEndpoint[T]{collector: col}
	_, err := datasink.CustomEndpointConsumer[struct{}, T, error](stream, ep)
	return err
}

// passthroughLocalSourceHandler passes values from producer straight through into the pipeline.
type passthroughLocalSourceHandler struct{}

func (h *passthroughLocalSourceHandler) Concurrency(_ localsource.StreamContext[*Message, any, error]) int {
	return 0
}

func (h *passthroughLocalSourceHandler) BeginRequest(ctx context.Context, _ localsource.StreamContext[*Message, any, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *passthroughLocalSourceHandler) ConsumeMessage(ctx context.Context, sc localsource.StreamContext[*Message, any, error], _ struct{}, value *Message, resultCtx localsource.ResultContext[struct{}, *Message, any, error]) error {
	defer resultCtx.Done()
	sc.Collect(ctx, value)
	return nil
}

func (h *passthroughLocalSourceHandler) GetMessageID(_ context.Context, _ localsource.StreamContext[*Message, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *passthroughLocalSourceHandler) EndRequest(_ context.Context, _ localsource.StreamContext[*Message, any, error], _ error, _ struct{}) {
}

// grpcSourceHandler processes incoming gRPC calls and feeds values into the pipeline.
// Used for all 4 source streaming modes (unary, server, client, bidi).
type grpcSourceHandler struct{}

func (h *grpcSourceHandler) BeginRequest(ctx context.Context, _ datasourcegrpc.StreamContext[*Message, any, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *grpcSourceHandler) ConsumeMessage(ctx context.Context, sc datasourcegrpc.StreamContext[*Message, any, error], _ struct{}, req *Message, _ datasourcegrpc.ResultContext[struct{}, *Message, *Message, any, error], _ datasourcegrpc.Sender[any, *Message]) (context.Context, error) {
	sc.Collect(ctx, req)
	return ctx, nil
}

func (h *grpcSourceHandler) GetMessageID(_ context.Context, _ datasourcegrpc.StreamContext[*Message, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *grpcSourceHandler) Eof(_ context.Context, _ datasourcegrpc.StreamContext[*Message, any, error], _ struct{}) {
}

func (h *grpcSourceHandler) EndRequest(_ context.Context, _ datasourcegrpc.StreamContext[*Message, any, error], _ error, _ struct{}) error {
	return nil
}

// grpcSourceResultHandler handles unary gRPC calls with hasResult=true:
// registers a SetResultCallback so the response flows back through the pipeline.
type grpcSourceResultHandler struct{}

func (h *grpcSourceResultHandler) BeginRequest(ctx context.Context, _ datasourcegrpc.StreamContext[*Message, *Message, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *grpcSourceResultHandler) ConsumeMessage(ctx context.Context, sc datasourcegrpc.StreamContext[*Message, *Message, error], _ struct{}, req *Message, resultCtx datasourcegrpc.ResultContext[struct{}, *Message, *Message, *Message, error], _ datasourcegrpc.Sender[*Message, *Message]) (context.Context, error) {
	// Register callback BEFORE sc.Collect: FunctionCall semantics runs the
	// pipeline (and consumeResult) synchronously inside Collect.
	resultCtx.SetResultCallback("msg", func(ctx context.Context, _ datasourcegrpc.StreamContext[*Message, *Message, error], _ struct{}, result *Message, sender datasourcegrpc.Sender[*Message, *Message]) bool {
		_ = sender.Send(ctx, result)
		return true
	})
	sc.Collect(ctx, req)
	return ctx, nil
}

func (h *grpcSourceResultHandler) GetMessageID(_ context.Context, _ datasourcegrpc.StreamContext[*Message, *Message, error], _ struct{}, _ *Message) string {
	return "msg"
}

func (h *grpcSourceResultHandler) Eof(_ context.Context, _ datasourcegrpc.StreamContext[*Message, *Message, error], _ struct{}) {
}

func (h *grpcSourceResultHandler) EndRequest(_ context.Context, _ datasourcegrpc.StreamContext[*Message, *Message, error], _ error, _ struct{}) error {
	return nil
}

// grpcSinkHandler sends the pipeline value as a gRPC request.
// Used for all 4 sink streaming modes.
type grpcSinkHandler struct {
	mu        sync.Mutex
	responses []*Message
	doneCh    chan struct{}
}

func newGrpcSinkHandler() *grpcSinkHandler {
	return &grpcSinkHandler{doneCh: make(chan struct{}, 64)}
}

func (h *grpcSinkHandler) BeginRequest(ctx context.Context, _ datasinkgrpc.StreamContext[*Message, *Message, error]) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *grpcSinkHandler) ConsumeMessage(ctx context.Context, _ datasinkgrpc.StreamContext[*Message, *Message, error], _ struct{}, value *Message, sender datasinkgrpc.Sender[*Message], resultCtx datasinkgrpc.ResultContext) error {
	err := sender.Send(ctx, value)
	resultCtx.Done()
	return err
}

func (h *grpcSinkHandler) HandleResponse(_ context.Context, _ datasinkgrpc.StreamContext[*Message, *Message, error], _ struct{}, response *Message) error {
	h.mu.Lock()
	h.responses = append(h.responses, response)
	h.mu.Unlock()
	select {
	case h.doneCh <- struct{}{}:
	default:
	}
	return nil
}

func (h *grpcSinkHandler) EndRequest(_ context.Context, _ datasinkgrpc.StreamContext[*Message, *Message, error], _ error, _ struct{}) {
}

func (h *grpcSinkHandler) Responses() []*Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]*Message, len(h.responses))
	copy(cp, h.responses)
	return cp
}

func (h *grpcSinkHandler) WaitResponse(timeout time.Duration) (*Message, bool) {
	select {
	case <-h.doneCh:
		h.mu.Lock()
		defer h.mu.Unlock()
		if len(h.responses) > 0 {
			return h.responses[len(h.responses)-1], true
		}
		return nil, false
	case <-time.After(timeout):
		return nil, false
	}
}

func (h *grpcSinkHandler) Reset() {
	h.mu.Lock()
	h.responses = nil
	h.mu.Unlock()
	for len(h.doneCh) > 0 {
		<-h.doneCh
	}
}

// MockUnaryClient captures unary gRPC calls from the datasink/grpc no-streaming consumer.
type MockUnaryClient struct {
	mu       sync.Mutex
	requests []*Message
	respErr  error
	ch       chan *Message
}

func NewMockUnaryClient() *MockUnaryClient {
	return &MockUnaryClient{ch: make(chan *Message, 16)}
}

func (m *MockUnaryClient) Call(_ context.Context, req *Message) (*Message, error) {
	m.mu.Lock()
	m.requests = append(m.requests, req)
	err := m.respErr
	m.mu.Unlock()
	select {
	case m.ch <- req:
	default:
	}
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (m *MockUnaryClient) WaitRequest(timeout time.Duration) (*Message, bool) {
	select {
	case req := <-m.ch:
		return req, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (m *MockUnaryClient) Requests() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Message, len(m.requests))
	copy(cp, m.requests)
	return cp
}

func (m *MockUnaryClient) Reset() {
	m.mu.Lock()
	m.requests = nil
	m.respErr = nil
	m.mu.Unlock()
	for len(m.ch) > 0 {
		<-m.ch
	}
}

// MockServerStreamClient provides server-streaming responses via Recv().
type MockServerStreamClient struct {
	mu        sync.Mutex
	responses []*Message
	idx       int
	reqCh     chan *Message
}

func NewMockServerStreamClient() *MockServerStreamClient {
	return &MockServerStreamClient{reqCh: make(chan *Message, 16)}
}

func (m *MockServerStreamClient) OpenStream(_ context.Context, req *Message) (datasinkgrpc.ServerStreamingGRPCStream[*Message], error) {
	m.mu.Lock()
	m.idx = 0
	m.mu.Unlock()
	select {
	case m.reqCh <- req:
	default:
	}
	return m, nil
}

func (m *MockServerStreamClient) Recv() (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.responses) {
		return nil, io.EOF
	}
	v := m.responses[m.idx]
	m.idx++
	return v, nil
}

func (m *MockServerStreamClient) SetResponses(responses []*Message) {
	m.mu.Lock()
	m.responses = responses
	m.mu.Unlock()
}

func (m *MockServerStreamClient) WaitRequest(timeout time.Duration) (*Message, bool) {
	select {
	case req := <-m.reqCh:
		return req, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (m *MockServerStreamClient) Reset() {
	m.mu.Lock()
	m.responses = nil
	m.idx = 0
	m.mu.Unlock()
	for len(m.reqCh) > 0 {
		<-m.reqCh
	}
}

// MockClientStreamClient captures Send calls and provides a response via CloseAndRecv.
type MockClientStreamClient struct {
	mu       sync.Mutex
	sent     []*Message
	response *Message
	closedCh chan struct{}
}

func NewMockClientStreamClient() *MockClientStreamClient {
	return &MockClientStreamClient{closedCh: make(chan struct{}, 16)}
}

func (m *MockClientStreamClient) OpenStream(_ context.Context) (datasinkgrpc.ClientStreamingGRPCStream[*Message, *Message], error) {
	m.mu.Lock()
	m.sent = nil
	if m.response == nil {
		m.response = &Message{Key: "close-recv", Value: 0, Text: "response"}
	}
	m.mu.Unlock()
	return m, nil
}

func (m *MockClientStreamClient) Send(req *Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, req)
	m.mu.Unlock()
	return nil
}

func (m *MockClientStreamClient) CloseAndRecv() (*Message, error) {
	m.mu.Lock()
	resp := m.response
	m.mu.Unlock()
	select {
	case m.closedCh <- struct{}{}:
	default:
	}
	return resp, nil
}

func (m *MockClientStreamClient) Sent() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func (m *MockClientStreamClient) WaitClose(timeout time.Duration) bool {
	select {
	case <-m.closedCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (m *MockClientStreamClient) SetResponse(resp *Message) {
	m.mu.Lock()
	m.response = resp
	m.mu.Unlock()
}

func (m *MockClientStreamClient) Reset() {
	m.mu.Lock()
	m.sent = nil
	m.response = &Message{Key: "close-recv", Value: 0, Text: "response"}
	m.mu.Unlock()
	for len(m.closedCh) > 0 {
		<-m.closedCh
	}
}

// MockBidiStreamClient captures Send calls and provides responses via Recv.
type MockBidiStreamClient struct {
	mu        sync.Mutex
	sent      []*Message
	responses []*Message
	recvIdx   int
	sendCh    chan *Message
}

func NewMockBidiStreamClient() *MockBidiStreamClient {
	return &MockBidiStreamClient{sendCh: make(chan *Message, 16)}
}

func (m *MockBidiStreamClient) OpenStream(_ context.Context) (datasinkgrpc.BidiStreamingGRPCStream[*Message, *Message], error) {
	m.mu.Lock()
	m.sent = nil
	m.recvIdx = 0
	m.mu.Unlock()
	return m, nil
}

func (m *MockBidiStreamClient) Send(req *Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, req)
	m.mu.Unlock()
	select {
	case m.sendCh <- req:
	default:
	}
	return nil
}

func (m *MockBidiStreamClient) Recv() (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recvIdx >= len(m.responses) {
		return nil, io.EOF
	}
	v := m.responses[m.recvIdx]
	m.recvIdx++
	return v, nil
}

func (m *MockBidiStreamClient) CloseSend() error { return nil }

func (m *MockBidiStreamClient) SetResponses(responses []*Message) {
	m.mu.Lock()
	m.responses = responses
	m.mu.Unlock()
}

func (m *MockBidiStreamClient) Sent() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func (m *MockBidiStreamClient) WaitSend(timeout time.Duration) (*Message, bool) {
	select {
	case req := <-m.sendCh:
		return req, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (m *MockBidiStreamClient) Reset() {
	m.mu.Lock()
	m.sent = nil
	m.responses = nil
	m.recvIdx = 0
	m.mu.Unlock()
	for len(m.sendCh) > 0 {
		<-m.sendCh
	}
}

// Mock gRPC source server implementations (used to call gRPC source handlers in tests).

// MockServerStreamingServer captures Send calls from a server-streaming source handler.
type MockServerStreamingServer struct {
	mu   sync.Mutex
	sent []*Message
}

func (m *MockServerStreamingServer) Send(msg *Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, msg)
	m.mu.Unlock()
	return nil
}

func (m *MockServerStreamingServer) Sent() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

// MockClientStreamingServer provides Recv() messages to a client-streaming source handler.
type MockClientStreamingServer struct {
	mu      sync.Mutex
	msgs    []*Message
	idx     int
	closed  *Message
}

func NewMockClientStreamingServer(msgs []*Message) *MockClientStreamingServer {
	return &MockClientStreamingServer{msgs: msgs}
}

func (m *MockClientStreamingServer) Recv() (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.msgs) {
		return nil, io.EOF
	}
	v := m.msgs[m.idx]
	m.idx++
	return v, nil
}

func (m *MockClientStreamingServer) SendAndClose(msg *Message) error {
	m.mu.Lock()
	m.closed = msg
	m.mu.Unlock()
	return nil
}

func (m *MockClientStreamingServer) Closed() *Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// MockBidiStreamingServer provides Recv() messages and captures Send() responses.
type MockBidiStreamingServer struct {
	mu       sync.Mutex
	recvMsgs []*Message
	recvIdx  int
	sent     []*Message
}

func NewMockBidiStreamingServer(recvMsgs []*Message) *MockBidiStreamingServer {
	return &MockBidiStreamingServer{recvMsgs: recvMsgs}
}

func (m *MockBidiStreamingServer) Recv() (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recvIdx >= len(m.recvMsgs) {
		return nil, io.EOF
	}
	v := m.recvMsgs[m.recvIdx]
	m.recvIdx++
	return v, nil
}

func (m *MockBidiStreamingServer) Send(msg *Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, msg)
	m.mu.Unlock()
	return nil
}

func (m *MockBidiStreamingServer) Sent() []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

// GrpcService is the test service for gRPC source and sink tests.
type GrpcService struct {
	runtime.ServiceApp
	stopOnce sync.Once

	// gRPC source: collectors for values delivered through the pipeline.
	GrpcSourceUnary        *ResultCollector[*Message]
	GrpcSourceServerStream *ResultCollector[*Message]
	GrpcSourceClientStream *ResultCollector[*Message]
	GrpcSourceBidiStream   *ResultCollector[*Message]

	// gRPC source: handler functions returned by Make* calls, callable in tests.
	UnaryHandler        datasourcegrpc.UnaryHandler[*Message, *Message]
	ServerStreamHandler datasourcegrpc.ServerStreamingHandler[*Message, *Message]
	ClientStreamHandler datasourcegrpc.ClientStreamingHandler[*Message, *Message]
	BidiStreamHandler   datasourcegrpc.BidiStreamingHandler[*Message, *Message]

	// gRPC source: unary with hasResult=true (result returned to caller via pipeline).
	UnaryResultHandler datasourcegrpc.UnaryHandler[*Message, *Message]

	// gRPC sink: mock clients for each streaming mode.
	SinkUnaryClient        *MockUnaryClient
	SinkServerStreamClient *MockServerStreamClient
	SinkClientStreamClient *MockClientStreamClient
	SinkBidiStreamClient   *MockBidiStreamClient

	// gRPC sink: handler to record HandleResponse calls.
	SinkUnaryHandler        *grpcSinkHandler
	SinkServerStreamHandler *grpcSinkHandler
	SinkClientStreamHandler *grpcSinkHandler
	SinkBidiStreamHandler   *grpcSinkHandler

	// gRPC sink: producers to push values into the pipelines.
	SinkUnaryProducer        *ChannelProducer[*Message]
	SinkServerStreamProducer *ChannelProducer[*Message]
	SinkClientStreamProducer *ChannelProducer[*Message]
	SinkBidiStreamProducer   *ChannelProducer[*Message]
}

func (s *GrpcService) GetConfig() *config.Config {
	return s.ServiceApp.GetConfig().(*config.Config)
}

func (s *GrpcService) GetSerde(valueType reflect.Type) (serde.Serializer, error) {
	switch valueType {
	case serde.GetSerdeType[Message](), serde.GetSerdeType[*Message]():
		var ser serde.Serde[*Message] = &MessageSerde{}
		return ser, nil
	}
	return nil, nil
}

func (s *GrpcService) buildRuntime(_ context.Context) error {
	cfg := s.GetConfig()
	srcHandler := &grpcSourceHandler{}

	// ── gRPC source: unary ───────────────────────────────────────────────────
	inputSrcUnary, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcSrcUnary, s)
	if err != nil {
		return err
	}
	sinkSrcUnary, err := transformation.Sink[*Message, error](&cfg.Streams.SinkGrpcSrcUnary, inputSrcUnary)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSrcUnary, s.GrpcSourceUnary); err != nil {
		return err
	}
	_, s.UnaryHandler, err = datasourcegrpc.MakeGRPCNoStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, any, error](inputSrcUnary, srcHandler)
	if err != nil {
		return err
	}

	// ── gRPC source: server-streaming ────────────────────────────────────────
	inputSrcServer, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcSrcServerStream, s)
	if err != nil {
		return err
	}
	sinkSrcServer, err := transformation.Sink[*Message, error](&cfg.Streams.SinkGrpcSrcServerStream, inputSrcServer)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSrcServer, s.GrpcSourceServerStream); err != nil {
		return err
	}
	_, s.ServerStreamHandler, err = datasourcegrpc.MakeGRPCServerStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, any, error](inputSrcServer, srcHandler)
	if err != nil {
		return err
	}

	// ── gRPC source: client-streaming ────────────────────────────────────────
	inputSrcClient, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcSrcClientStream, s)
	if err != nil {
		return err
	}
	sinkSrcClient, err := transformation.Sink[*Message, error](&cfg.Streams.SinkGrpcSrcClientStream, inputSrcClient)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSrcClient, s.GrpcSourceClientStream); err != nil {
		return err
	}
	_, s.ClientStreamHandler, err = datasourcegrpc.MakeGRPCClientStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, any, error](inputSrcClient, srcHandler)
	if err != nil {
		return err
	}

	// ── gRPC source: bidi-streaming ───────────────────────────────────────────
	inputSrcBidi, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcSrcBidiStream, s)
	if err != nil {
		return err
	}
	sinkSrcBidi, err := transformation.Sink[*Message, error](&cfg.Streams.SinkGrpcSrcBidiStream, inputSrcBidi)
	if err != nil {
		return err
	}
	if err = makeSink(sinkSrcBidi, s.GrpcSourceBidiStream); err != nil {
		return err
	}
	_, s.BidiStreamHandler, err = datasourcegrpc.MakeGRPCBidiStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, any, error](inputSrcBidi, srcHandler)
	if err != nil {
		return err
	}

	// ── gRPC source: unary with result (hasResult=true) ──────────────────────
	inputSrcUnaryResult, err := transformation.Input[*Message, *Message, error](&cfg.Streams.InputGrpcSrcUnaryResult, s)
	if err != nil {
		return err
	}
	mapSrcUnaryResult, err := transformation.Map[*Message, *Message](&cfg.Streams.MapGrpcSrcUnaryResult, inputSrcUnaryResult,
		transformation.MapHandler[*Message, *Message](func(ctx context.Context, _ runtime.Stream, m *Message, out runtime.Collect[*Message]) {
			out.Out(ctx, &Message{Key: m.Key, Value: m.Value * 2, Text: strings.ToUpper(m.Text)})
		}),
	)
	if err != nil {
		return err
	}
	if err = inputSrcUnaryResult.SetSource(mapSrcUnaryResult); err != nil {
		return err
	}
	_, s.UnaryResultHandler, err = datasourcegrpc.MakeGRPCNoStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](inputSrcUnaryResult, &grpcSourceResultHandler{})
	if err != nil {
		return err
	}

	// ── gRPC sink: unary ─────────────────────────────────────────────────────
	inputDstUnary, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcDstUnary, s)
	if err != nil {
		return err
	}
	sinkDstUnary, err := transformation.SinkWithResult[*Message, *Message, error](&cfg.Streams.SinkGrpcDstUnary, inputDstUnary)
	if err != nil {
		return err
	}
	uc := s.SinkUnaryClient
	if _, err = datasinkgrpc.MakeGRPCNoStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](
		sinkDstUnary, s.SinkUnaryHandler, uc.Call,
	); err != nil {
		return err
	}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, any, error](inputDstUnary, s.SinkUnaryProducer, &passthroughLocalSourceHandler{}); err != nil {
		return err
	}

	// ── gRPC sink: server-streaming ──────────────────────────────────────────
	inputDstServer, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcDstServerStream, s)
	if err != nil {
		return err
	}
	sinkDstServer, err := transformation.SinkWithResult[*Message, *Message, error](&cfg.Streams.SinkGrpcDstServerStream, inputDstServer)
	if err != nil {
		return err
	}
	sc := s.SinkServerStreamClient
	if _, err = datasinkgrpc.MakeGRPCServerStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](
		sinkDstServer, s.SinkServerStreamHandler, sc.OpenStream,
	); err != nil {
		return err
	}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, any, error](inputDstServer, s.SinkServerStreamProducer, &passthroughLocalSourceHandler{}); err != nil {
		return err
	}

	// ── gRPC sink: client-streaming ──────────────────────────────────────────
	inputDstClient, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcDstClientStream, s)
	if err != nil {
		return err
	}
	sinkDstClient, err := transformation.SinkWithResult[*Message, *Message, error](&cfg.Streams.SinkGrpcDstClientStream, inputDstClient)
	if err != nil {
		return err
	}
	cc := s.SinkClientStreamClient
	if _, err = datasinkgrpc.MakeGRPCClientStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](
		sinkDstClient, s.SinkClientStreamHandler, cc.OpenStream,
	); err != nil {
		return err
	}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, any, error](inputDstClient, s.SinkClientStreamProducer, &passthroughLocalSourceHandler{}); err != nil {
		return err
	}

	// ── gRPC sink: bidi-streaming ────────────────────────────────────────────
	inputDstBidi, err := transformation.Input[*Message, any, error](&cfg.Streams.InputGrpcDstBidiStream, s)
	if err != nil {
		return err
	}
	sinkDstBidi, err := transformation.SinkWithResult[*Message, *Message, error](&cfg.Streams.SinkGrpcDstBidiStream, inputDstBidi)
	if err != nil {
		return err
	}
	bc := s.SinkBidiStreamClient
	if _, err = datasinkgrpc.MakeGRPCBidiStreamingEndpointConsumer[struct{}, *Message, *Message, *Message, *Message, error](
		sinkDstBidi, s.SinkBidiStreamHandler, bc.OpenStream,
	); err != nil {
		return err
	}
	if _, err = localsource.MakeCustomEndpointConsumer[struct{}, *Message, any, error](inputDstBidi, s.SinkBidiStreamProducer, &passthroughLocalSourceHandler{}); err != nil {
		return err
	}

	return nil
}

func (s *GrpcService) StartService(ctx context.Context) error {
	if err := s.buildRuntime(ctx); err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}
	return s.Start(ctx)
}

func (s *GrpcService) StopService(ctx context.Context) {
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

// TestEnv holds test-scoped state for gRPC tests.
type TestEnv struct {
	Service *GrpcService
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

// Main bootstraps the gRPC test service, runs the test suite, and shuts down.
func Main(run func(*TestEnv) int) int {
	mainCtx := context.Background()

	env := &TestEnv{
		Metrics: testmetrics.New(),
		Tracing: testtracing.New(),
		Log:     testlog.New(),
	}
	deps := &testDeps{metrics: env.Metrics, tracing: env.Tracing, logs: env.Log}

	var err error
	env.Service, err = runtime.MakeService[*GrpcService, *config.Config](
		mainCtx,
		"GrpcService",
		deps,
		nil,
		nil,
		func() *GrpcService {
			return &GrpcService{
				GrpcSourceUnary:        NewResultCollector[*Message](),
				GrpcSourceServerStream: NewResultCollector[*Message](),
				GrpcSourceClientStream: NewResultCollector[*Message](),
				GrpcSourceBidiStream:   NewResultCollector[*Message](),

				SinkUnaryClient:        NewMockUnaryClient(),
				SinkServerStreamClient: NewMockServerStreamClient(),
				SinkClientStreamClient: NewMockClientStreamClient(),
				SinkBidiStreamClient:   NewMockBidiStreamClient(),

				SinkUnaryHandler:        newGrpcSinkHandler(),
				SinkServerStreamHandler: newGrpcSinkHandler(),
				SinkClientStreamHandler: newGrpcSinkHandler(),
				SinkBidiStreamHandler:   newGrpcSinkHandler(),

				SinkUnaryProducer:        NewChannelProducer[*Message](),
				SinkServerStreamProducer: NewChannelProducer[*Message](),
				SinkClientStreamProducer: NewChannelProducer[*Message](),
				SinkBidiStreamProducer:   NewChannelProducer[*Message](),
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
