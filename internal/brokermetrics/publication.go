// Package brokermetrics records bounded transport outcomes without changing
// admission, completion, retries, or user result routing.
package brokermetrics

import (
	"context"
	"fmt"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type Publication struct {
	outcomes map[string]metrics.Int64Counter
}

// NewPublication binds labels once per endpoint. Outcomes must be a static
// protocol vocabulary, never message identifiers, payloads, or error text.
func NewPublication(m metrics.Metrics, protocol, connector, endpoint string, outcomes ...string) (*Publication, error) {
	if metrics.IsNoop(m) {
		return nil, nil
	}
	scope := m.Scope("broker_endpoint", metrics.Labels{
		"protocol": protocol, "connector": connector, "endpoint": endpoint,
	})
	p := &Publication{outcomes: make(map[string]metrics.Int64Counter, len(outcomes))}
	for _, outcome := range outcomes {
		counter, err := scope.Counter("publication_outcomes_total", "Observed transport publication outcomes; not downstream business completion", metrics.Labels{"outcome": outcome})
		if err != nil {
			return nil, fmt.Errorf("broker publication metrics: %w", err)
		}
		p.outcomes[outcome] = counter
	}
	return p, nil
}

// Observe is safe for concurrent callbacks when the configured metrics backend
// is concurrency-safe, as required by the existing endpoint metrics contract.
func (p *Publication) Observe(ctx context.Context, outcome string) {
	if p == nil {
		return
	}
	if counter := p.outcomes[outcome]; counter != nil {
		counter.Inc(ctx)
	}
}
