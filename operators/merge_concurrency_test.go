package operators

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

func TestMergeDoesNotSerializeIndependentParentCalls(t *testing.T) {
	firstCfg := &config.InputStreamConfig{ID: 1, Name: "first", IdService: 1}
	secondCfg := &config.InputStreamConfig{ID: 2, Name: "second", IdService: 1}
	mergeCfg := &config.MergeStreamConfig{ID: 3, Name: "merge", IdService: 1, IdSources: []int{1, 2}}
	observeCfg := &config.MapStreamConfig{ID: 4, Name: "observe", IdService: 1, IdSource: 3}
	rc, err := config.NewRuntimeConfig(&splitMergeSerdeConfig{streams: []config.StreamConfig{
		firstCfg, secondCfg, mergeCfg, observeCfg,
	}})
	require.NoError(t, err)
	env := &subStreamTestEnv{}
	require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
	first, err := MakeInputStream[int, any, any](firstCfg, env)
	require.NoError(t, err)
	second, err := MakeInputStream[int, any, any](secondCfg, env)
	require.NoError(t, err)
	merge, err := MakeMergeStream[int](mergeCfg, first, second)
	require.NoError(t, err)
	type delivery struct {
		context context.Context
		value   int
	}
	entered := make(chan delivery, 2)
	release := make(chan struct{})
	returned := make(chan int, 2)
	_, err = MakeMapStream[int, int](observeCfg, merge, MapHandler[int, int](
		func(ctx context.Context, _ runtime.Stream, value int, _ runtime.Collect[int]) {
			entered <- delivery{context: ctx, value: value}
			<-release
		}))
	require.NoError(t, err)
	type requestKey struct{}
	firstContext := context.WithValue(context.Background(), requestKey{}, "first")
	secondContext := context.WithValue(context.Background(), requestKey{}, "second")
	defer close(release)
	go func() {
		first.Consume(firstContext, 1)
		returned <- 1
	}()
	go func() {
		second.Consume(secondContext, 2)
		returned <- 2
	}()
	contexts := make(map[int]context.Context)
	for range 2 {
		select {
		case value := <-entered:
			contexts[value.value] = value.context
		case <-time.After(2 * time.Second):
			t.Fatal("Merge serialized independent direct calls")
		}
	}
	require.Same(t, firstContext, contexts[1])
	require.Same(t, secondContext, contexts[2])
	select {
	case <-returned:
		t.Fatal("direct Consume returned before its handler completed")
	default:
	}
	// Release both handlers without closing the channel twice on failure paths.
	release <- struct{}{}
	release <- struct{}{}
	var completed []int
	for range 2 {
		select {
		case value := <-returned:
			completed = append(completed, value)
		case <-time.After(2 * time.Second):
			t.Fatal("direct Consume did not return after its handler completed")
		}
	}
	require.ElementsMatch(t, []int{1, 2}, completed)
}
