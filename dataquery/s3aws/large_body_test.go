package s3aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type observedBody struct {
	io.ReadCloser
	closed bool
}

func (b *observedBody) Close() error { b.closed = true; return b.ReadCloser.Close() }

// The server withholds most of the response until the mapper reads its first
// chunk. A whole-object buffer before mapping therefore cannot pass this test.
func TestLargeNativeBodyStreamingAndCleanup(t *testing.T) {
	const size = 16 * 1024 * 1024
	for _, mode := range []string{"success", "mapper-error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			release := make(chan struct{})
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Length", fmt.Sprint(size))
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, strings.Repeat("x", 4096))
				w.(http.Flusher).Flush()
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				chunk := strings.Repeat("x", 64*1024)
				for left := size - 4096; left > 0; {
					n := min(left, len(chunk))
					if _, err := io.WriteString(w, chunk[:n]); err != nil {
						return
					}
					left -= n
				}
			}))
			defer server.Close()
			client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
			stream := &queryStream{}
			failure := errors.New("mapper stopped")
			var body *observedBody
			var bytes int64
			h := &handler[s3.GetObjectInput, s3.GetObjectOutput]{
				input: &s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("large")},
				mapResult: func(o *s3.GetObjectOutput) error {
					body = &observedBody{ReadCloser: o.Body}
					o.Body = body
					first := make([]byte, 4096)
					n, err := io.ReadFull(body, first)
					bytes = int64(n)
					close(release)
					if err != nil {
						return err
					}
					if string(first) != strings.Repeat("x", len(first)) {
						return errors.New("incorrect first chunk")
					}
					if mode == "mapper-error" {
						return failure
					}
					if mode == "cancel" {
						cancel()
						return ctx.Err()
					}
					// Bounded mapper scratch space; never collect the complete object.
					scratch := make([]byte, 32*1024)
					for {
						n, err := body.Read(scratch)
						for _, value := range scratch[:n] {
							if value != 'x' {
								return errors.New("incorrect body byte")
							}
						}
						bytes += int64(n)
						if errors.Is(err, io.EOF) {
							return nil
						}
						if err != nil {
							return err
						}
					}
				},
				ended: func() {
					if body == nil || !body.closed {
						t.Error("native body not closed before EndRequest")
					}
				},
			}
			consumer, err := MakeGetObjectEndpointConsumer(stream, client, h)
			if err != nil {
				t.Fatal(err)
			}
			consumer.Consume(ctx, "large")
			if calls.Load() != 1 || h.ends != 1 {
				t.Fatalf("requests=%d ends=%d", calls.Load(), h.ends)
			}
			if mode == "success" {
				if h.err != nil || bytes != size || len(stream.values) != 1 {
					t.Fatalf("bytes=%d results=%v err=%v", bytes, stream.values, h.err)
				}
			} else {
				expected := failure
				if mode == "cancel" {
					expected = context.Canceled
				}
				if !errors.Is(h.err, expected) || len(stream.values) != 0 || len(stream.errors.errors) != 1 {
					t.Fatalf("outcome=%v results=%v errors=%v", h.err, stream.values, stream.errors.errors)
				}
			}
		})
	}
}
