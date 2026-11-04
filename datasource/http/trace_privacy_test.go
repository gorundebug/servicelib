package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/testtracing"
)

type privatePathHandler struct {
	EndpointHandler[int, int, int, int, int, error]
	seen chan string
}

func (h *privatePathHandler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error], data HandlerData) (context.Context, int, error) {
	h.seen <- data.Request.URL.RequestURI()
	data.Writer.WriteHeader(nethttp.StatusBadRequest)
	return ctx, 0, errors.New("invalid tracking request")
}

func TestHTTPInputTraceUsesConfiguredRouteBeforeAdmission(t *testing.T) {
	recorder := testtracing.New()
	handler := &privatePathHandler{seen: make(chan string, 2)}
	consumer := &netHTTPEndpointTypedConsumer[int, int, int, int, int, error]{handler: handler, tracer: recorder.Tracer("privacy"), pathAttribute: tracing.StringAttr("path", "/notice/{id}")}
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		consumer.serveHTTP(w, r.WithContext(tracing.EnableSampling(r.Context())))
	}))
	defer server.Close()
	for _, uri := range []string{"/notice/private-subject?token=private-grant", "/notice/other-subject?token=other-grant"} {
		request, err := nethttp.NewRequestWithContext(t.Context(), "GET", server.URL+uri, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != nethttp.StatusBadRequest {
			t.Fatal("admission changed")
		}
		if got := <-handler.seen; got != uri {
			t.Fatal("request URI was changed")
		}
	}
	spans := recorder.Spans()
	if len(spans) != 2 {
		t.Fatalf("expected two completed spans, got %d", len(spans))
	}
	for _, span := range spans {
		found := false
		for _, attr := range span.Attributes {
			if attr.Key == "path" {
				found = true
				if attr.Value.AsString() != "/notice/{id}" {
					t.Fatal("trace exposed request path")
				}
			}
		}
		if !found {
			t.Fatal("route missing")
		}
		raw := fmt.Sprintf("%+v", span)
		for _, private := range []string{"private-subject", "private-grant", "other-subject", "other-grant"} {
			if strings.Contains(raw, private) {
				t.Fatal("private request data in span")
			}
		}
	}
}
