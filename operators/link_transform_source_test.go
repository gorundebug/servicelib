package operators

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

func TestLinkAcceptsTypeChangingMapOutputWithoutChangingDelivery(t *testing.T) {
	inputCfg := &config.InputStreamConfig{ID: 1, Name: "input", IdService: 1}
	mapCfg := &config.MapStreamConfig{ID: 2, Name: "convert", IdService: 1, IdSource: 1}
	linkCfg := &config.CycleLinkStreamConfig{ID: 3, Name: "link", IdService: 1, IdSource: 2}
	observeCfg := &config.MapStreamConfig{ID: 4, Name: "observe", IdService: 1, IdSource: 3}
	rc, err := config.NewRuntimeConfig(&transformSerdeConfig{streams: []config.StreamConfig{inputCfg, mapCfg, linkCfg, observeCfg}})
	require.NoError(t, err)
	env := &transformSerdeEnv{}
	require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
	input, err := MakeInputStream[int, any, any](inputCfg, env)
	require.NoError(t, err)
	converted, err := MakeMapStream[int, string](mapCfg, input, MapHandler[int, string](func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[string]) {
		out.Out(ctx, strconv.Itoa(value))
	}))
	require.NoError(t, err)
	link, err := MakeLinkStream[string](linkCfg, env)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), struct{}{}, "same invocation")
	var results []string
	_, err = MakeMapStream[string, string](observeCfg, link, MapHandler[string, string](func(got context.Context, _ runtime.Stream, value string, _ runtime.Collect[string]) {
		require.Same(t, ctx, got)
		results = append(results, value)
	}))
	require.NoError(t, err)
	// Late binding mirrors generated finish(), after other graph nodes exist.
	// Compile-time compatibility with the published SetSource method signature.
	var oldSetter func(runtime.TypedConsumedStream[string]) error = link.SetSource
	require.NotNil(t, oldSetter)
	require.NoError(t, runtime.BindLinkSource(link, converted))
	require.Same(t, converted.GetSerde(), link.GetSerde())
	input.Consume(ctx, 42)
	require.Equal(t, []string{"42"}, results)
	require.Zero(t, env.integer.serialized+env.integer.deserialized)
	require.Zero(t, env.text.serialized+env.text.deserialized)
}

// A custom implementation of the original public interface: no BindSource
// method is promoted through the embedded historical interface.
type legacyLink[T any] struct {
	runtime.TypedLinkStream[T]
	calls int
	want  error
}

func (link *legacyLink[T]) SetSource(runtime.TypedConsumedStream[T]) error {
	link.calls++
	return link.want
}

var _ runtime.TypedLinkStream[string] = (*legacyLink[string])(nil)

func TestLinkBindingPreservesLegacyImplementations(t *testing.T) {
	want := errors.New("original custom binding error")
	legacy := &legacyLink[string]{want: want}
	// The legacy SetSource implementation is still invoked and its error is
	// unchanged; no extra interface method is required to implement the SDK API.
	source := &legacyLink[string]{}
	require.Same(t, want, runtime.BindLinkSource[string](legacy, source))
	require.Equal(t, 1, legacy.calls)
	require.Error(t, runtime.BindLinkSource[string](nil, source))
	require.Error(t, runtime.BindLinkSource[string](legacy, nil))
}
