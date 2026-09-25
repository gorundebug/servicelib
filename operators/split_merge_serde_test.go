package operators

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/stretchr/testify/require"
)

type splitMergeSerdeConfig struct {
	subStreamTestConfig
	streams []config.StreamConfig
}

func (c *splitMergeSerdeConfig) GetStreams() []config.StreamConfig { return c.streams }

type splitMergeSerdeParent struct {
	runtime.TypedStream[int]
	serializer serde.StreamSerde[int]
}

func (s *splitMergeSerdeParent) GetSerde() serde.StreamSerde[int] { return s.serializer }

func TestSplitMergeSerdeAndImmediateDelivery(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "LeftParentFirst"
		if reverse {
			name = "RightParentFirst"
		}
		t.Run(name, func(t *testing.T) {
			inputCfg := &config.InputStreamConfig{ID: 1, Name: "input", IdService: 1}
			splitCfg := &config.SplitStreamConfig{ID: 2, Name: "split", IdService: 1, IdSource: 1}
			leftCfg := &config.FilterStreamConfig{ID: 3, Name: "left", IdService: 1, IdSource: 2}
			rightCfg := &config.FilterStreamConfig{ID: 4, Name: "right", IdService: 1, IdSource: 2}
			mergeCfg := &config.MergeStreamConfig{ID: 5, Name: "merge", IdService: 1, IdSources: []int{3, 4}}
			if reverse {
				mergeCfg.IdSources = []int{4, 3}
			}
			observeCfg := &config.MapStreamConfig{ID: 6, Name: "observe", IdService: 1, IdSource: 5}
			rc, err := config.NewRuntimeConfig(&splitMergeSerdeConfig{streams: []config.StreamConfig{
				inputCfg, splitCfg, leftCfg, rightCfg, mergeCfg, observeCfg,
			}})
			require.NoError(t, err)
			env := &subStreamTestEnv{}
			require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
			input, err := MakeInputStream[int, any, any](inputCfg, env)
			require.NoError(t, err)
			rootSpy := &transformSerdeSpy[int]{}
			root := &splitMergeSerdeParent{TypedStream: input, serializer: serde.MakeStreamSerde[int](rootSpy)}
			split, err := MakeSplitStream[int](splitCfg, root)
			require.NoError(t, err)
			require.Same(t, root.GetSerde(), split.GetSerde())
			leftLink, rightLink := split.AddStream(), split.AddStream()
			require.Same(t, root.GetSerde(), leftLink.GetSerde())
			require.Same(t, root.GetSerde(), rightLink.GetSerde())
			var trace []string
			ctx := context.WithValue(context.Background(), struct{}{}, "parent")
			filter := func(label string) FilterHandler[int] {
				return func(got context.Context, _ runtime.Stream, _ int) bool {
					require.Same(t, ctx, got)
					trace = append(trace, label)
					return true
				}
			}
			left, err := MakeFilterStream[int](leftCfg, leftLink, filter("left"))
			require.NoError(t, err)
			right, err := MakeFilterStream[int](rightCfg, rightLink, filter("right"))
			require.NoError(t, err)
			leftSpy, rightSpy := &transformSerdeSpy[int]{}, &transformSerdeSpy[int]{}
			first := &splitMergeSerdeParent{TypedStream: left, serializer: serde.MakeStreamSerde[int](leftSpy)}
			second := &splitMergeSerdeParent{TypedStream: right, serializer: serde.MakeStreamSerde[int](rightSpy)}
			if reverse {
				first, second = second, first
			}
			merge, err := MakeMergeStream[int](mergeCfg, first, second)
			require.NoError(t, err)
			require.Same(t, first.GetSerde(), merge.GetSerde())
			require.NotSame(t, second.GetSerde(), merge.GetSerde())
			var values []int
			_, err = MakeMapStream[int, int](observeCfg, merge, MapHandler[int, int](
				func(got context.Context, _ runtime.Stream, value int, _ runtime.Collect[int]) {
					require.Same(t, ctx, got)
					trace = append(trace, "result")
					values = append(values, value)
				}))
			require.NoError(t, err)
			require.NoError(t, split.Build())
			input.Consume(ctx, 7)
			input.Consume(ctx, 8)
			require.Equal(t, []int{7, 7, 8, 8}, values)
			require.Equal(t, []string{"left", "result", "right", "result", "left", "result", "right", "result"}, trace)
			for _, spy := range []*transformSerdeSpy[int]{rootSpy, leftSpy, rightSpy} {
				require.Zero(t, spy.serialized)
				require.Zero(t, spy.deserialized)
			}
		})
	}
}
