package runtime

import "errors"

// BindLinkSource binds a cycle's output source without requiring that source to
// also consume the output type (for example, Map[A,B] feeding Link[B]). Binding
// is startup-only and retains the normal SetSource semantics: serde, topology
// and delivery are still owned by the link. No adapter serializes/replays values.
//
// TypedLinkStream and its historical SetSource signature remain source-compatible
// for custom implementations. Older custom links keep their existing behavior
// for consumed sources; accepting other output sources requires BindSource.
func BindLinkSource[T any](link TypedLinkStream[T], source TypedStream[T]) error {
	if link == nil || source == nil {
		return errors.New("link binding requires a link and source")
	}
	if binding, ok := link.(interface{ BindSource(TypedStream[T]) error }); ok {
		return binding.BindSource(source)
	}
	if consumed, ok := source.(TypedConsumedStream[T]); ok {
		return link.SetSource(consumed)
	}
	return errors.New("custom link does not support binding a type-changing output source")
}
