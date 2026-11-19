// Package s3aws adapts the official AWS SDK v2 S3 client to independent Query streams.
package s3aws

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorundebug/servicelib/dataquery"
	"github.com/gorundebug/servicelib/runtime"
)

// Query never automatically repeats a write or replays partially emitted data.
// Disable native retries per operation, including when the client was supplied
// by a custom maker with a different default retry policy.
func noRetry(options *s3.Options) { options.Retryer = aws.NopRetryer{} }

// GetObjectClient is the narrow native client contract used by this operation.
type GetObjectClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type GetObjectHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildRequest(context.Context, HandlerState, T) (*s3.GetObjectInput, error)
	// HandleResult may emit zero or more mapped results synchronously. Do not
	// retain the native response or its body beyond this call.
	HandleResult(context.Context, HandlerState, *s3.GetObjectOutput, runtime.Collect[R]) error
}

func MakeGetObjectEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], client GetObjectClient, handler GetObjectHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
	if client == nil || handler == nil {
		return nil, fmt.Errorf("S3 Query requires client and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		request, err := handler.BuildRequest(ctx, state, value)
		if err != nil {
			return err
		}
		if request == nil {
			return fmt.Errorf("S3 Query builder returned no request")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := client.GetObject(ctx, request, noRetry)
		// Close before EndRequest even if mapping/cancellation fails. A custom
		// native client can return both a response and an error; close that body too.
		if response != nil && response.Body != nil {
			defer func() { err = errors.Join(err, response.Body.Close()) }()
		}
		if err != nil {
			return err
		}
		if response == nil {
			return fmt.Errorf("S3 Query returned no response")
		}
		if response.Body == nil {
			return fmt.Errorf("S3 GetObject returned no body")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.HandleResult(ctx, state, response, out); err != nil {
			return err
		}
		return ctx.Err()
	})
}

// HeadObjectClient is the narrow native client contract used by this operation.
type HeadObjectClient interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type HeadObjectHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildRequest(context.Context, HandlerState, T) (*s3.HeadObjectInput, error)
	// HandleResult may emit zero or more mapped results synchronously. Do not
	// retain the native response or its body beyond this call.
	HandleResult(context.Context, HandlerState, *s3.HeadObjectOutput, runtime.Collect[R]) error
}

func MakeHeadObjectEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], client HeadObjectClient, handler HeadObjectHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
	if client == nil || handler == nil {
		return nil, fmt.Errorf("S3 Query requires client and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		request, err := handler.BuildRequest(ctx, state, value)
		if err != nil {
			return err
		}
		if request == nil {
			return fmt.Errorf("S3 Query builder returned no request")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := client.HeadObject(ctx, request, noRetry)
		if err != nil {
			return err
		}
		if response == nil {
			return fmt.Errorf("S3 Query returned no response")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.HandleResult(ctx, state, response, out); err != nil {
			return err
		}
		return ctx.Err()
	})
}

// ListObjectsClient is the narrow native client contract used by this operation.
type ListObjectsClient interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type ListObjectsHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildRequest(context.Context, HandlerState, T) (*s3.ListObjectsV2Input, error)
	// HandleResult may emit zero or more mapped results synchronously. Do not
	// retain the native response or its body beyond this call.
	HandleResult(context.Context, HandlerState, *s3.ListObjectsV2Output, runtime.Collect[R]) error
}

func MakeListObjectsEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], client ListObjectsClient, handler ListObjectsHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
	if client == nil || handler == nil {
		return nil, fmt.Errorf("S3 Query requires client and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		request, err := handler.BuildRequest(ctx, state, value)
		if err != nil {
			return err
		}
		if request == nil {
			return fmt.Errorf("S3 Query builder returned no request")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// The native paginator retains only the current page, not the full listing.
		paginator := s3.NewListObjectsV2Paginator(client, request)
		previousToken := aws.ToString(request.ContinuationToken)
		for paginator.HasMorePages() {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, err := paginator.NextPage(ctx, noRetry)
			if err != nil {
				return err
			}
			if page == nil {
				return fmt.Errorf("S3 Query returned no listing page")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := handler.HandleResult(ctx, state, page, out); err != nil {
				return err
			}
			if aws.ToBool(page.IsTruncated) {
				token := aws.ToString(page.NextContinuationToken)
				if token == "" || token == previousToken {
					return fmt.Errorf("S3 listing did not advance its continuation token")
				}
				previousToken = token
			}
		}
		return ctx.Err()
	})
}

// PutObjectClient is the narrow native client contract used by this operation.
type PutObjectClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type PutObjectHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildRequest(context.Context, HandlerState, T) (*s3.PutObjectInput, error)
	// HandleResult may emit zero or more mapped results synchronously. Do not
	// retain the native response or its body beyond this call.
	HandleResult(context.Context, HandlerState, *s3.PutObjectOutput, runtime.Collect[R]) error
}

func MakePutObjectEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], client PutObjectClient, handler PutObjectHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
	if client == nil || handler == nil {
		return nil, fmt.Errorf("S3 Query requires client and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		request, err := handler.BuildRequest(ctx, state, value)
		if err != nil {
			return err
		}
		if request == nil {
			return fmt.Errorf("S3 Query builder returned no request")
		}
		// A successful builder transfers ownership of a closeable upload body.
		if body, ok := request.Body.(io.Closer); ok {
			defer func() { err = errors.Join(err, body.Close()) }()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := client.PutObject(ctx, request, noRetry)
		if err != nil {
			return err
		}
		if response == nil {
			return fmt.Errorf("S3 Query returned no response")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.HandleResult(ctx, state, response, out); err != nil {
			return err
		}
		return ctx.Err()
	})
}

// DeleteObjectClient is the narrow native client contract used by this operation.
type DeleteObjectClient interface {
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type DeleteObjectHandler[HandlerState, T, R, E any] interface {
	dataquery.EndpointHandler[HandlerState, R, E]
	BuildRequest(context.Context, HandlerState, T) (*s3.DeleteObjectInput, error)
	// HandleResult may emit zero or more mapped results synchronously. Do not
	// retain the native response or its body beyond this call.
	HandleResult(context.Context, HandlerState, *s3.DeleteObjectOutput, runtime.Collect[R]) error
}

func MakeDeleteObjectEndpointConsumer[HandlerState, T, R, E any](stream runtime.TypedQueryStream[T, R, E], client DeleteObjectClient, handler DeleteObjectHandler[HandlerState, T, R, E]) (runtime.Consumer[T], error) {
	if client == nil || handler == nil {
		return nil, fmt.Errorf("S3 Query requires client and handler")
	}
	return dataquery.MakeEndpointConsumer(stream, handler, func(ctx context.Context, state HandlerState, value T, out runtime.Collect[R]) (err error) {
		request, err := handler.BuildRequest(ctx, state, value)
		if err != nil {
			return err
		}
		if request == nil {
			return fmt.Errorf("S3 Query builder returned no request")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := client.DeleteObject(ctx, request, noRetry)
		if err != nil {
			return err
		}
		if response == nil {
			return fmt.Errorf("S3 Query returned no response")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.HandleResult(ctx, state, response, out); err != nil {
			return err
		}
		return ctx.Err()
	})
}
