package runtime

import "github.com/gorundebug/servicelib/runtime/config"

// streamGrouping contains definition names only. Concrete repetitions continue
// to be distinguished by existing stream/link identities, not new metric labels.
type streamGrouping struct {
	pipeline  string
	component string
}

// StreamGrouping returns cached definition labels without consulting runtime
// configuration. Every stream exposes grouping through its typed contract.
func StreamGrouping(stream Stream) (pipeline, component string) {
	return stream.GetPipelineName(), stream.GetComponentName()
}

func (s *ServiceStream[T]) GetPipelineName() string { return s.grouping.pipeline }

func (s *ServiceStream[T]) GetComponentName() string { return s.grouping.component }

func groupingForStream(cfg config.StreamConfig) streamGrouping {
	if cfg == nil {
		return streamGrouping{}
	}
	return streamGrouping{pipeline: cfg.GetPipeline(), component: cfg.GetComponent()}
}
