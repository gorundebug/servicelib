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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type errorStream struct {
	runtime.TypedConsumedStream[error]
	errors []error
}

func (s *errorStream) Consume(_ context.Context, err error) { s.errors = append(s.errors, err) }

type queryStream struct {
	runtime.TypedQueryStream[string, int, error]
	consumer runtime.Consumer[string]
	values   []int
	errors   errorStream
}

func (s *queryStream) SetQueryConsumer(c runtime.Consumer[string])        { s.consumer = c }
func (s *queryStream) ConsumeResult(_ context.Context, value int)         { s.values = append(s.values, value) }
func (s *queryStream) GetErrorStream() runtime.TypedConsumedStream[error] { return &s.errors }

type handler[I, O any] struct {
	input        *I
	begins, ends int
	err          error
	mapResult    func(*O) error
	ended        func()
}

func (h *handler[I, O]) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, int, error) {
	h.begins++
	return ctx, 42, nil
}
func (h *handler[I, O]) BuildRequest(_ context.Context, state int, _ string) (*I, error) {
	if state != 42 {
		panic("state lost")
	}
	return h.input, nil
}
func (h *handler[I, O]) HandleResult(ctx context.Context, _ int, value *O, out runtime.Collect[int]) error {
	if h.mapResult != nil {
		if err := h.mapResult(value); err != nil {
			return err
		}
	}
	out.Out(ctx, 1)
	return nil
}
func (h *handler[I, O]) EndRequest(ctx context.Context, _ runtime.Stream, _ int, err error, _ runtime.Collect[int], errors runtime.Collect[error]) {
	h.ends++
	h.err = err
	if err != nil {
		errors.Out(ctx, err)
	}
	if h.ended != nil {
		h.ended()
	}
}
func check[I, O any](t *testing.T, stream *queryStream, h *handler[I, O], c runtime.Consumer[string], err error, results int) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	c.Consume(t.Context(), "key")
	if h.err != nil || h.begins != 1 || h.ends != 1 || len(stream.values) != results {
		t.Fatalf("begin=%d end=%d results=%v error=%v", h.begins, h.ends, stream.values, h.err)
	}
}

type closeReader struct {
	io.ReadSeeker
	closed bool
}

func (r *closeReader) Close() error { r.closed = true; return nil }

func TestNativeOperations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("continuation-token") == "next" {
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>second</Key></Contents></ListBucketResult>`)
			} else {
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>first</Key></Contents></ListBucketResult>`)
			}
		case r.Method == "GET":
			fmt.Fprint(w, "object contents")
		case r.Method == "HEAD":
			w.Header().Set("Content-Length", "15")
		case r.Method == "PUT":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"written"`)
		case r.Method == "DELETE":
			w.WriteHeader(204)
		default:
			http.Error(w, "unexpected operation", 400)
		}
	}))
	defer server.Close()
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
	t.Run("GetObject", func(t *testing.T) {
		stream := &queryStream{}
		h := &handler[s3.GetObjectInput, s3.GetObjectOutput]{input: &s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")}, mapResult: func(o *s3.GetObjectOutput) error {
			b, e := io.ReadAll(o.Body)
			if string(b) != "object contents" {
				t.Errorf("body=%q", b)
			}
			return e
		}}
		c, e := MakeGetObjectEndpointConsumer(stream, client, h)
		check(t, stream, h, c, e, 1)
	})
	t.Run("HeadObject", func(t *testing.T) {
		stream := &queryStream{}
		h := &handler[s3.HeadObjectInput, s3.HeadObjectOutput]{input: &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")}}
		c, e := MakeHeadObjectEndpointConsumer(stream, client, h)
		check(t, stream, h, c, e, 1)
	})
	t.Run("PutObject", func(t *testing.T) {
		body := &closeReader{ReadSeeker: strings.NewReader("upload")}
		stream := &queryStream{}
		h := &handler[s3.PutObjectInput, s3.PutObjectOutput]{input: &s3.PutObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key"), Body: body}, ended: func() {
			if !body.closed {
				t.Error("upload body open at EndRequest")
			}
		}}
		c, e := MakePutObjectEndpointConsumer(stream, client, h)
		check(t, stream, h, c, e, 1)
	})
	t.Run("DeleteObject", func(t *testing.T) {
		stream := &queryStream{}
		h := &handler[s3.DeleteObjectInput, s3.DeleteObjectOutput]{input: &s3.DeleteObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")}}
		c, e := MakeDeleteObjectEndpointConsumer(stream, client, h)
		check(t, stream, h, c, e, 1)
	})
	t.Run("ListObjects", func(t *testing.T) {
		stream := &queryStream{}
		h := &handler[s3.ListObjectsV2Input, s3.ListObjectsV2Output]{input: &s3.ListObjectsV2Input{Bucket: aws.String("bucket"), MaxKeys: aws.Int32(1)}}
		c, e := MakeListObjectsEndpointConsumer(stream, client, h)
		check(t, stream, h, c, e, 2)
	})
}

type getStub struct{ body *closeReader }

func (c getStub) GetObject(ctx context.Context, _ *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := s3.Options{}
	for _, option := range options {
		option(&opts)
	}
	if opts.Retryer == nil || opts.Retryer.MaxAttempts() != 1 {
		return nil, fmt.Errorf("native retry was not disabled")
	}
	return &s3.GetObjectOutput{Body: c.body}, nil
}
func TestGetClosesBeforeFailureFinalization(t *testing.T) {
	body := &closeReader{ReadSeeker: strings.NewReader("object")}
	failure := errors.New("mapping failed")
	stream := &queryStream{}
	h := &handler[s3.GetObjectInput, s3.GetObjectOutput]{input: &s3.GetObjectInput{}, mapResult: func(*s3.GetObjectOutput) error { return failure }, ended: func() {
		if !body.closed {
			t.Error("response body open at EndRequest")
		}
	}}
	c, err := MakeGetObjectEndpointConsumer(stream, getStub{body}, h)
	if err != nil {
		t.Fatal(err)
	}
	c.Consume(t.Context(), "key")
	if h.ends != 1 || !errors.Is(h.err, failure) || len(stream.values) != 0 || len(stream.errors.errors) != 1 {
		t.Fatalf("unexpected finalization: %+v", h)
	}
}
func TestNativeWriteFailureIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(503)
		fmt.Fprint(w, `<Error><Code>SlowDown</Code><Message>retryable native error</Message></Error>`)
	}))
	defer server.Close()
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
	stream := &queryStream{}
	h := &handler[s3.DeleteObjectInput, s3.DeleteObjectOutput]{input: &s3.DeleteObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")}}
	c, err := MakeDeleteObjectEndpointConsumer(stream, client, h)
	if err != nil {
		t.Fatal(err)
	}
	c.Consume(t.Context(), "key")
	if calls.Load() != 1 || h.ends != 1 || h.err == nil || len(stream.values) != 0 {
		t.Fatalf("calls=%d ends=%d error=%v", calls.Load(), h.ends, h.err)
	}
}

type noopQueryEnvironment struct{ runtime.RuntimeEnvironment }

func (noopQueryEnvironment) Metrics() metrics.Metrics {
	return metrics.NewNoopMetricsEngine().Metrics()
}
func (*queryStream) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return noopQueryEnvironment{} }

func TestNativeEmptyAndMissingObjects(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.URL.Query().Get("list-type") == "2":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>`)
		case strings.HasSuffix(r.URL.Path, "/missing"):
			w.WriteHeader(404)
			fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
		case strings.HasSuffix(r.URL.Path, "/forbidden"):
			w.WriteHeader(403)
			fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
		default:
			w.Header().Set("Content-Length", "0")
		}
	}))
	defer server.Close()
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
	for _, key := range []string{"empty", "missing", "forbidden"} {
		for _, operation := range []string{"get", "head"} {
			t.Run(operation+"/"+key, func(t *testing.T) {
				stream := &queryStream{}
				before := calls.Load()
				var c runtime.Consumer[string]
				var err, outcome error
				ends := 0
				if operation == "get" {
					h := &handler[s3.GetObjectInput, s3.GetObjectOutput]{input: &s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String(key)}, mapResult: func(o *s3.GetObjectOutput) error {
						b, e := io.ReadAll(o.Body)
						if len(b) != 0 {
							t.Errorf("expected empty body, got %q", b)
						}
						return e
					}}
					c, err = MakeGetObjectEndpointConsumer(stream, client, h)
					if err != nil {
						t.Fatal(err)
					}
					c.Consume(t.Context(), key)
					outcome, ends = h.err, h.ends
				} else {
					h := &handler[s3.HeadObjectInput, s3.HeadObjectOutput]{input: &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String(key)}, mapResult: func(o *s3.HeadObjectOutput) error {
						if o.ContentLength == nil || *o.ContentLength != 0 {
							t.Error("empty HEAD size lost")
						}
						return nil
					}}
					c, err = MakeHeadObjectEndpointConsumer(stream, client, h)
					if err != nil {
						t.Fatal(err)
					}
					c.Consume(t.Context(), key)
					outcome, ends = h.err, h.ends
				}
				if ends != 1 || calls.Load()-before != 1 {
					t.Fatalf("ends=%d native calls=%d", ends, calls.Load()-before)
				}
				if key == "empty" {
					if outcome != nil || len(stream.values) != 1 || len(stream.errors.errors) != 0 {
						t.Fatalf("empty must succeed: %v", outcome)
					}
				} else {
					var status interface{ HTTPStatusCode() int }
					expected := 404
					if key == "forbidden" {
						expected = 403
					}
					if !errors.As(outcome, &status) || status.HTTPStatusCode() != expected || len(stream.values) != 0 || len(stream.errors.errors) != 1 {
						t.Fatalf("error identity/results lost: %v", outcome)
					}
				}
			})
		}
	}
	stream := &queryStream{}
	h := &handler[s3.ListObjectsV2Input, s3.ListObjectsV2Output]{input: &s3.ListObjectsV2Input{Bucket: aws.String("bucket")}, mapResult: func(o *s3.ListObjectsV2Output) error {
		if len(o.Contents) != 0 {
			t.Error("expected empty list")
		}
		return nil
	}}
	c, err := MakeListObjectsEndpointConsumer(stream, client, h)
	check(t, stream, h, c, err, 1)
}
