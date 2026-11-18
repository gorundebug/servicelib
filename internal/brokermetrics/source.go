package brokermetrics

import (
	"context"
	"fmt"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

// Source observes native delivery metadata, never inferred acknowledgements.
// All labels and observations are bounded and bound once per endpoint.
type Source struct {
	observations map[string]metrics.Int64Counter
}

func NewSource(m metrics.Metrics, protocol, connector, endpoint string) (*Source, error) {
	if metrics.IsNoop(m) {
		return nil, nil
	}
	scope := m.Scope("broker_endpoint", metrics.Labels{"protocol": protocol, "connector": connector, "endpoint": endpoint})
	names := []string{"received"}
	switch protocol {
	case "jetstream", "pulsar", "rabbitmq":
		names = append(names, "redelivered")
	case "redisstreams":
		names = append(names, "reclaimed")
	}
	s := &Source{observations: make(map[string]metrics.Int64Counter, len(names))}
	for _, name := range names {
		c, err := scope.Counter("source_observations_total", "Observed native source deliveries; not acknowledgements or business completion", metrics.Labels{"observation": name})
		if err != nil {
			return nil, fmt.Errorf("broker source metrics: %w", err)
		}
		s.observations[name] = c
	}
	return s, nil
}
func (s *Source) Observe(ctx context.Context, observation string) {
	if s == nil {
		return
	}
	if c := s.observations[observation]; c != nil {
		c.Inc(ctx)
	}
}
