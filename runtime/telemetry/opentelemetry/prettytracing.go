/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package opentelemetry

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// prettySpanExporter prints completed spans in a human-readable single-line format.
//
// Each span occupies one line so that spans from multiple services can be
// correlated by trace ID across separate log streams:
//
//	[1782d516][HotelSearch  ]  23:01:20.203    120ms  grpc.output   endpoint="Search Rooms"
//	[1782d516][HotelInventory]  23:01:20.280     85ms  grpc.input    endpoint="Search Rooms"
//
// Works correctly with both WithSyncer (one span per call) and WithBatcher
// (multiple spans per call, sorted by start time within a trace).
type prettySpanExporter struct {
	mu sync.Mutex
	w  io.Writer
}

func newPrettySpanExporter(w io.Writer) *prettySpanExporter {
	return &prettySpanExporter{w: w}
}

func (e *prettySpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// Sort all spans by start time so output is chronological.
	sorted := make([]sdktrace.ReadOnlySpan, len(spans))
	copy(sorted, spans)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartTime().Before(sorted[j].StartTime())
	})

	for _, s := range sorted {
		traceID := s.SpanContext().TraceID().String()
		if len(traceID) > 8 {
			traceID = traceID[:8]
		}

		svcName := ""
		for _, attr := range s.Resource().Attributes() {
			if string(attr.Key) == "service.name" {
				svcName = attr.Value.AsString()
				break
			}
		}

		dur := s.EndTime().Sub(s.StartTime())
		fmt.Fprintf(e.w, "[%s][%-16s]  %s  %8s  %-28s",
			traceID,
			svcName,
			s.StartTime().Format("15:04:05.000"),
			prettyDuration(dur),
			s.Name(),
		)
		for _, attr := range s.Attributes() {
			fmt.Fprintf(e.w, " %s=%q", string(attr.Key), attr.Value.AsString())
		}
		fmt.Fprintln(e.w)
	}
	return nil
}

func (e *prettySpanExporter) Shutdown(_ context.Context) error { return nil }

func prettyDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}
