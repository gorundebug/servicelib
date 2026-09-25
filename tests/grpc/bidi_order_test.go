package grpc_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/tests/grpcservice"
)

func TestGRPCSink_BidiResponseOrder(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		name := "success"
		if failFirst {
			name = "first_response_error"
		}
		t.Run(name, func(t *testing.T) {
			svc := testEnv.Service
			client := svc.SinkBidiStreamClient
			handler := svc.SinkBidiStreamHandler
			client.Reset()
			handler.Reset()
			firstEntered := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondEntered := make(chan struct{}, 1)
			ended := make(chan error, 1)
			firstError := errors.New("first response failed")
			release := sync.OnceFunc(func() { close(releaseFirst) })
			endObserved := false
			handler.SetLifecycleHooks(func(response *grpcservice.Message) error {
				if response.Key == "first" {
					close(firstEntered)
					<-releaseFirst
					if failFirst {
						return firstError
					}
				} else {
					secondEntered <- struct{}{}
				}
				return nil
			}, func(err error) { ended <- err })
			t.Cleanup(func() {
				release()
				if !endObserved {
					select {
					case <-ended:
					case <-time.After(5 * time.Second):
						t.Error("bidi session did not finish during cleanup")
					}
				}
				handler.SetLifecycleHooks(nil, nil)
			})
			client.SetResponses([]*grpcservice.Message{{Key: "first"}, {Key: "second"}})
			svc.SinkBidiStreamProducer.Push(&grpcservice.Message{Key: "ordered"})
			select {
			case <-firstEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("first response did not enter its handler")
			}
			select {
			case <-secondEntered:
				t.Error("second response overtook the suspended first handler")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			select {
			case err := <-ended:
				endObserved = true
				if failFirst && !errors.Is(err, firstError) {
					t.Errorf("EndRequest error = %v, want first response error", err)
				}
				if !failFirst && err != nil {
					t.Errorf("EndRequest error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("bidi session did not finish")
			}
			responses := handler.Responses()
			if failFirst {
				if len(responses) != 0 {
					t.Errorf("responses after first handler error: %v", responses)
				}
				select {
				case <-secondEntered:
					t.Error("second handler ran after the first handler failed")
				default:
				}
			} else if len(responses) != 2 || responses[0].Key != "first" || responses[1].Key != "second" {
				t.Errorf("response order = %v, want first then second", responses)
			}
		})
	}
}
