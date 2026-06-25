/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grpc_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gorundebug/servicelib/tests/grpcservice"
)

const timeout = 500 * time.Millisecond

var testEnv *grpcservice.TestEnv

func TestMain(m *testing.M) {
	os.Exit(grpcservice.Main(func(env *grpcservice.TestEnv) int {
		testEnv = env
		return m.Run()
	}))
}

// ── gRPC source (datasource/grpc): framework acts as gRPC server ─────────────

// TestGRPCSource_Unary calls the unary handler directly and verifies the value
// flows through the pipeline to the sink collector.
func TestGRPCSource_Unary(t *testing.T) {
	svc := testEnv.Service
	svc.GrpcSourceUnary.Reset()

	req := &grpcservice.Message{Key: "u1", Value: 1, Text: "unary-req"}
	res, err := svc.UnaryHandler(context.Background(), req)
	require.NoError(t, err)
	_ = res

	got, ok := svc.GrpcSourceUnary.Wait1(timeout)
	require.True(t, ok, "expected message in unary source collector")
	assert.Equal(t, req.Key, got.Key)
	assert.Equal(t, req.Value, got.Value)
}

// TestGRPCSource_ServerStreaming calls the server-streaming handler with a mock
// ServerStreamingServer and verifies the request is processed by the pipeline.
func TestGRPCSource_ServerStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.GrpcSourceServerStream.Reset()

	req := &grpcservice.Message{Key: "ss1", Value: 2, Text: "server-stream-req"}
	mockServer := &grpcservice.MockServerStreamingServer{}
	err := svc.ServerStreamHandler(context.Background(), req, mockServer)
	require.NoError(t, err)

	got, ok := svc.GrpcSourceServerStream.Wait1(timeout)
	require.True(t, ok, "expected message in server-stream source collector")
	assert.Equal(t, req.Key, got.Key)
}

// TestGRPCSource_ClientStreaming calls the client-streaming handler with a mock
// ClientStreamingServer that provides two messages then EOF.
func TestGRPCSource_ClientStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.GrpcSourceClientStream.Reset()

	msgs := []*grpcservice.Message{
		{Key: "cs1", Value: 10, Text: "client-msg1"},
		{Key: "cs2", Value: 20, Text: "client-msg2"},
	}
	mockServer := grpcservice.NewMockClientStreamingServer(msgs)
	err := svc.ClientStreamHandler(context.Background(), mockServer)
	require.NoError(t, err)

	got, ok := svc.GrpcSourceClientStream.WaitN(2, timeout)
	require.True(t, ok, "expected 2 messages in client-stream source collector")
	assert.Equal(t, "cs1", got[0].Key)
	assert.Equal(t, "cs2", got[1].Key)
}

// TestGRPCSource_BidiStreaming calls the bidi-streaming handler with a mock
// BidiStreamingServer that provides one message then EOF.
func TestGRPCSource_BidiStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.GrpcSourceBidiStream.Reset()

	msgs := []*grpcservice.Message{
		{Key: "bd1", Value: 30, Text: "bidi-msg"},
	}
	mockServer := grpcservice.NewMockBidiStreamingServer(msgs)
	err := svc.BidiStreamHandler(context.Background(), mockServer)
	require.NoError(t, err)

	got, ok := svc.GrpcSourceBidiStream.Wait1(timeout)
	require.True(t, ok, "expected message in bidi-stream source collector")
	assert.Equal(t, "bd1", got.Key)
}

// TestGRPCSource_Unary_WithResult calls the unary handler that has a result
// stream connected (hasResult=true). The pipeline doubles Value and uppercases
// Text; the result is returned directly from the handler call.
func TestGRPCSource_Unary_WithResult(t *testing.T) {
	svc := testEnv.Service

	req := &grpcservice.Message{Key: "ur1", Value: 10, Text: "result-req"}
	resp, err := svc.UnaryResultHandler(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, req.Key, resp.Key)
	assert.Equal(t, req.Value*2, resp.Value)
	assert.Equal(t, strings.ToUpper(req.Text), resp.Text)
}

// ── gRPC sink (datasink/grpc): framework acts as gRPC client ─────────────────

// TestGRPCSink_Unary pushes a value into the unary sink pipeline and verifies
// the mock clientFn receives the expected request.
func TestGRPCSink_Unary(t *testing.T) {
	svc := testEnv.Service
	svc.SinkUnaryClient.Reset()

	msg := &grpcservice.Message{Key: "du1", Value: 100, Text: "dst-unary"}
	svc.SinkUnaryProducer.Push(msg)

	req, ok := svc.SinkUnaryClient.WaitRequest(timeout)
	require.True(t, ok, "expected mock unary clientFn to be called")
	assert.Equal(t, msg.Key, req.Key)
	assert.Equal(t, msg.Value, req.Value)
}

// TestGRPCSink_ServerStreaming pushes a value into the server-streaming sink
// pipeline and verifies the request is sent and responses are processed.
func TestGRPCSink_ServerStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.SinkServerStreamClient.Reset()
	svc.SinkServerStreamHandler.Reset()
	svc.SinkServerStreamClient.SetResponses([]*grpcservice.Message{
		{Key: "resp1", Value: 200, Text: "server-stream-resp"},
	})

	msg := &grpcservice.Message{Key: "ds1", Value: 101, Text: "dst-server"}
	svc.SinkServerStreamProducer.Push(msg)

	req, ok := svc.SinkServerStreamClient.WaitRequest(timeout)
	require.True(t, ok, "expected mock server-stream clientFn to be called")
	assert.Equal(t, msg.Key, req.Key)

	resp, ok := svc.SinkServerStreamHandler.WaitResponse(timeout)
	require.True(t, ok, "expected HandleResponse to be called with server response")
	assert.Equal(t, "resp1", resp.Key)
}

// TestGRPCSink_ClientStreaming pushes a value into the client-streaming sink
// pipeline. The handler sends the request and calls Done(), triggering CloseAndRecv.
func TestGRPCSink_ClientStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.SinkClientStreamClient.Reset()
	svc.SinkClientStreamHandler.Reset()

	msg := &grpcservice.Message{Key: "dc1", Value: 102, Text: "dst-client"}
	svc.SinkClientStreamProducer.Push(msg)

	closed := svc.SinkClientStreamClient.WaitClose(timeout)
	require.True(t, closed, "expected CloseAndRecv to be called")

	sent := svc.SinkClientStreamClient.Sent()
	require.Len(t, sent, 1)
	assert.Equal(t, msg.Key, sent[0].Key)
}

// TestGRPCSink_BidiStreaming pushes a value into the bidi-streaming sink pipeline.
// The handler sends the request and calls Done(), triggering CloseSend.
func TestGRPCSink_BidiStreaming(t *testing.T) {
	svc := testEnv.Service
	svc.SinkBidiStreamClient.Reset()
	svc.SinkBidiStreamHandler.Reset()

	msg := &grpcservice.Message{Key: "db1", Value: 103, Text: "dst-bidi"}
	svc.SinkBidiStreamProducer.Push(msg)

	sent, ok := svc.SinkBidiStreamClient.WaitSend(timeout)
	require.True(t, ok, "expected mock bidi-stream Send to be called")
	assert.Equal(t, msg.Key, sent.Key)
}
