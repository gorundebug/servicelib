package operators

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/stretchr/testify/require"
)

type transformSerdeSpy[T any] struct {
	serialized   int
	deserialized int
}

func (s *transformSerdeSpy[T]) Serialize(T, []byte) ([]byte, error) {
	s.serialized++
	return nil, errors.New("unexpected serialization on a direct edge")
}
func (s *transformSerdeSpy[T]) Deserialize([]byte) (T, error) {
	s.deserialized++
	var zero T
	return zero, errors.New("unexpected deserialization on a direct edge")
}
func (s *transformSerdeSpy[T]) SerializeObj(value interface{}, buf []byte) ([]byte, error) {
	return s.Serialize(value.(T), buf)
}
func (s *transformSerdeSpy[T]) DeserializeObj(buf []byte) (interface{}, error) {
	return s.Deserialize(buf)
}
func (*transformSerdeSpy[T]) IsStub() bool { return false }

type transformSerdeEnv struct {
	subStreamTestEnv
	integer transformSerdeSpy[int]
	text    transformSerdeSpy[string]
}

func (e *transformSerdeEnv) GetSerde(tp reflect.Type) (serde.Serializer, error) {
	switch tp {
	case serde.GetSerdeType[int]():
		return &e.integer, nil
	case serde.GetSerdeType[string]():
		return &e.text, nil
	default:
		return nil, errors.New("unused type in serde fixture")
	}
}

type transformSerdeConfig struct {
	subStreamTestConfig
	streams []config.StreamConfig
}

func (c *transformSerdeConfig) GetStreams() []config.StreamConfig { return c.streams }

// Override only the parent's published serde, not the environment's type cache.
// A filter inherits it; a transform must resolve its own output type instead.
type transformSerdeParent struct {
	runtime.TypedInputStream[int, any, any]
	parentSerde serde.StreamSerde[int]
}

func (s *transformSerdeParent) GetSerde() serde.StreamSerde[int] { return s.parentSerde }

func checkTransformSerde[R any](t *testing.T, flat bool, convert func(int) R) {
	t.Helper()
	inputCfg := config.InputStreamConfig{ID: 1, Name: "input", IdService: 1}
	beforeCfg := config.FilterStreamConfig{ID: 2, Name: "before", IdService: 1, IdSource: 1}
	mapCfg := config.MapStreamConfig{ID: 3, Name: "map", IdService: 1, IdSource: 2}
	flatCfg := config.FlatMapStreamConfig{ID: 3, Name: "flatmap", IdService: 1, IdSource: 2}
	afterCfg := config.FilterStreamConfig{ID: 4, Name: "after", IdService: 1, IdSource: 3}
	observeCfg := config.MapStreamConfig{ID: 5, Name: "observe", IdService: 1, IdSource: 4}
	var transformCfg config.StreamConfig = &mapCfg
	if flat {
		transformCfg = &flatCfg
	}
	cfg := &transformSerdeConfig{streams: []config.StreamConfig{
		&inputCfg, &beforeCfg, transformCfg, &afterCfg, &observeCfg,
	}}
	rc, err := config.NewRuntimeConfig(cfg)
	require.NoError(t, err)
	env := &transformSerdeEnv{}
	require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
	input, err := MakeInputStream[int, any, any](&inputCfg, env)
	require.NoError(t, err)
	parentSpy := &transformSerdeSpy[int]{}
	parent := &transformSerdeParent{
		TypedInputStream: input,
		parentSerde:      serde.MakeStreamSerde[int](parentSpy),
	}
	before, err := MakeFilterStream[int](&beforeCfg, parent,
		FilterHandler[int](func(context.Context, runtime.Stream, int) bool { return true }))
	require.NoError(t, err)
	require.Same(t, parent.GetSerde(), before.GetSerde())

	var transformed runtime.TypedTransformConsumedStream[int, R]
	if flat {
		transformed, err = MakeFlatMapStream[int, R](&flatCfg, before,
			FlatMapHandler[int, R](func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[R]) {
				out.Out(ctx, convert(value))
				out.Out(ctx, convert(value+1))
			}))
	} else {
		transformed, err = MakeMapStream[int, R](&mapCfg, before,
			MapHandler[int, R](func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[R]) {
				out.Out(ctx, convert(value))
			}))
	}
	require.NoError(t, err)
	require.NotSame(t, parent.GetSerde(), transformed.GetSerde())
	if serde.GetSerdeType[R]() == serde.GetSerdeType[int]() {
		require.Same(t, &env.integer, transformed.GetSerde().ValueSerializer())
	} else {
		require.Same(t, &env.text, transformed.GetSerde().ValueSerializer())
	}
	after, err := MakeFilterStream[R](&afterCfg, transformed,
		FilterHandler[R](func(context.Context, runtime.Stream, R) bool { return true }))
	require.NoError(t, err)
	require.Same(t, transformed.GetSerde(), after.GetSerde())

	ctx := context.WithValue(context.Background(), struct{}{}, "preserved")
	var received []R
	_, err = MakeMapStream[R, R](&observeCfg, after,
		MapHandler[R, R](func(actual context.Context, _ runtime.Stream, value R, _ runtime.Collect[R]) {
			require.Equal(t, ctx, actual)
			received = append(received, value)
		}))
	require.NoError(t, err)
	input.Consume(ctx, 7)
	expected := []R{convert(7)}
	if flat {
		expected = append(expected, convert(8))
	}
	require.Equal(t, expected, received, "results must be available in order when direct Consume returns")
	require.Zero(t, parentSpy.serialized+parentSpy.deserialized)
	require.Zero(t, env.integer.serialized+env.integer.deserialized)
	require.Zero(t, env.text.serialized+env.text.deserialized)
}

func TestTransformSerdeInheritanceAndDirectCalls(t *testing.T) {
	for _, flat := range []bool{false, true} {
		name := "Map"
		if flat {
			name = "FlatMap"
		}
		t.Run(name+"SameType", func(t *testing.T) {
			checkTransformSerde(t, flat, func(value int) int { return value * 2 })
		})
		t.Run(name+"ChangedType", func(t *testing.T) {
			checkTransformSerde(t, flat, strconv.Itoa)
		})
	}
}
