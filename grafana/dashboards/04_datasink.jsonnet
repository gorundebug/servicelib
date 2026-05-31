// Dashboard: Data Sinks
//
// Metrics groups:
//
//   datasink_connector (labels: connector)
//     datasink_connector_events_total{connector, event}            counter  event=stop_timeout
//
//   datasink_endpoint (labels: connector, endpoint)
//     datasink_endpoint_events_total{connector, endpoint, event}   counter
//       event = begin_request_failed | late_result | request_error
//     datasink_endpoint_messages_total{connector, endpoint}        counter
//     datasink_endpoint_request_duration_seconds{connector, endpoint} histogram
//     datasink_endpoint_active_requests{connector, endpoint}       gauge

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local connFilter = 'connector=~"$connector"';
local epFilter   = '%s, endpoint=~"$endpoint"' % connFilter;

lib.dashboard(
  title='%s / Data Sinks' % lib.svc,
  uid='%s-datasink' % lib.svc,
  tags=['datasink'],
  variables=[
    lib.dsVar,
    lib.labelVar('connector', 'connector', 'datasink_endpoint_messages_total'),
    lib.labelVar('endpoint',  'endpoint',  'datasink_endpoint_messages_total', connFilter),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Connector Health
    // -------------------------------------------------------------------------
    lib.row('Connector Health'),

    lib.ts(
      title='Connector Stop Timeouts',
      targets=[
        lib.rate(
          'datasink_connector_events_total',
          '%s, event="stop_timeout"' % connFilter,
          '{{connector}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.stat(
      title='Connector Stop Timeouts (total)',
      targets=[
        lib.promQ(
          'sum(datasink_connector_events_total{%s, event="stop_timeout"}) by (connector)' % connFilter,
          '{{connector}}'
        ),
      ],
      w=12, h=6,
      unit='short',
    ),

    // -------------------------------------------------------------------------
    // Row: Endpoint Throughput
    // -------------------------------------------------------------------------
    lib.row('Endpoint Throughput'),

    lib.ts(
      title='Message Rate',
      targets=[
        lib.rate(
          'datasink_endpoint_messages_total',
          epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Active Requests',
      targets=[
        lib.promQ(
          'datasink_endpoint_active_requests{%s}' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    // -------------------------------------------------------------------------
    // Row: Request Latency
    // -------------------------------------------------------------------------
    lib.row('Request Latency'),

    lib.ts(
      title='Request Duration p50',
      targets=[
        lib.hQuantileBy(
          0.5,
          'datasink_endpoint_request_duration_seconds',
          'connector, endpoint',
          epFilter,
          'p50 {{connector}} / {{endpoint}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p95',
      targets=[
        lib.hQuantileBy(
          0.95,
          'datasink_endpoint_request_duration_seconds',
          'connector, endpoint',
          epFilter,
          'p95 {{connector}} / {{endpoint}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p99',
      targets=[
        lib.hQuantileBy(
          0.99,
          'datasink_endpoint_request_duration_seconds',
          'connector, endpoint',
          epFilter,
          'p99 {{connector}} / {{endpoint}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    // -------------------------------------------------------------------------
    // Row: Error Events
    // -------------------------------------------------------------------------
    lib.row('Error Events'),

    lib.ts(
      title='Request Errors Rate',
      targets=[
        lib.rate(
          'datasink_endpoint_events_total',
          '%s, event="request_error"' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Other Error Event Rates',
      targets=[
        lib.rate(
          'datasink_endpoint_events_total',
          '%s, event="begin_request_failed"' % epFilter,
          'begin_request_failed {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasink_endpoint_events_total',
          '%s, event="late_result"' % epFilter,
          'late_result {{connector}}/{{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.stat(
      title='Total Request Errors',
      targets=[
        lib.promQ(
          'sum(datasink_endpoint_events_total{%s, event="request_error"}) by (connector, endpoint)' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=4,
      unit='short',
    ),
  ]
)
