package http

import (
	"context"
	"errors"
	"io"
	nethttp "net/http"
	"testing"
)

type statusOnlyBody struct {
	reads  int
	closed bool
}

func (b *statusOnlyBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("status-only handler does not need the response body")
}

func (b *statusOnlyBody) Close() error {
	b.closed = true
	return nil
}

type statusOnlyClient struct{ body *statusOnlyBody }

func (c statusOnlyClient) Do(*nethttp.Request) (*nethttp.Response, error) {
	return &nethttp.Response{StatusCode: nethttp.StatusAccepted, Body: c.body}, nil
}

type statusOnlyHandler struct {
	groupingHandler
	status int
	body   io.ReadCloser
	ended  bool
	err    error
}

func (h *statusOnlyHandler) HandleResponse(_ context.Context, _ StreamContext[int, int, error], _ int, response Response) error {
	h.status = response.StatusCode
	h.body = response.Body
	return nil
}

func (h *statusOnlyHandler) EndRequest(_ context.Context, _ StreamContext[int, int, error], err error, _ int) {
	h.ended = true
	h.err = err
}

func TestHTTPSinkDoesNotReadBodyBeforeUserHandler(t *testing.T) {
	body := &statusOnlyBody{}
	handler := &statusOnlyHandler{}
	endpoint := &groupingSinkEndpoint{}
	consumer := netHTTPSinkEndpointTypedConsumer[int, int, int, int, int, error]{
		stream:   &groupingSinkStream{},
		endpoint: endpoint,
		handler:  handler,
		client:   statusOnlyClient{body: body},
	}
	consumer.Consume(context.Background(), 42)
	if handler.status != nethttp.StatusAccepted || handler.body != body {
		t.Fatal("HandleResponse must receive the original response body and status")
	}
	if body.reads != 0 {
		t.Fatalf("runtime read the response body %d times without the handler asking", body.reads)
	}
	if !handler.ended || handler.err != nil || !body.closed {
		t.Fatalf("Consume did not finish lifecycle and close the body: ended=%v error=%v closed=%v", handler.ended, handler.err, body.closed)
	}
	if endpoint.started != 1 || endpoint.finished != 1 {
		t.Fatalf("unexpected request accounting: started=%d finished=%d", endpoint.started, endpoint.finished)
	}
}
