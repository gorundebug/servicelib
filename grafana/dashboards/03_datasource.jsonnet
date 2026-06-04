// Dashboard: Data Sources
//
// Metrics groups:
//
//   datasource_connector (labels: connector)
//     datasource_connector_events_total{connector, event}         counter  event=stop_timeout
//
//   datasource_endpoint (labels: connector, endpoint)
//     datasource_endpoint_events_total{connector, endpoint, event} counter
//       event = missing_stream_id | late_result | unknown_message_id
//             | duplicate_message_id | invalid_http_method
//             | begin_request_failed | request_error
//     datasource_endpoint_messages_total{connector, endpoint}      counter
//     datasource_endpoint_request_duration_seconds{connector, endpoint} histogram
//     datasource_endpoint_active_requests{connector, endpoint}     gauge
//     datasource_endpoint_pending_requests{connector, endpoint}    gauge
//     datasource_endpoint_pending_oldest_age_seconds{connector, endpoint} gauge

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local connFilter = 'connector=~"$connector"';
local epFilter   = '%s, endpoint=~"$endpoint"' % connFilter;

lib.dashboard(
  title='%s / Data Sources' % lib.svc,
  uid='%s-datasource' % lib.svc,
  tags=['datasource'],
  variables=[
    lib.dsVar,
    lib.labelVar('connector', 'connector', 'datasource_endpoint_messages_total'),
    lib.labelVar('endpoint',  'endpoint',  'datasource_endpoint_messages_total', connFilter),
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
          'datasource_connector_events_total',
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
          'sum(datasource_connector_events_total{%s, event="stop_timeout"}) by (connector)' % connFilter,
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
          'datasource_endpoint_messages_total',
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
          'datasource_endpoint_active_requests{%s}' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    // -------------------------------------------------------------------------
    // Row: Pending Requests
    // -------------------------------------------------------------------------
    lib.row('Pending Requests'),

    lib.ts(
      title='Pending Requests',
      targets=[
        lib.promQ(
          'datasource_endpoint_pending_requests{%s}' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Oldest Pending Request Age',
      targets=[
        lib.promQ(
          'datasource_endpoint_pending_oldest_age_seconds{%s}' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='s',
    ),

    lib.ts(
      title='Late Results Rate',
      targets=[
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="late_result"' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
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
          'datasource_endpoint_request_duration_seconds',
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
          'datasource_endpoint_request_duration_seconds',
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
          'datasource_endpoint_request_duration_seconds',
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
          'datasource_endpoint_events_total',
          '%s, event="request_error"' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Protocol / Logic Event Rates',
      targets=[
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="missing_stream_id"' % epFilter,
          'missing_stream_id {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="late_result"' % epFilter,
          'late_result {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="unknown_message_id"' % epFilter,
          'unknown_msg_id {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="duplicate_message_id"' % epFilter,
          'duplicate_msg_id {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="invalid_http_method"' % epFilter,
          'invalid_http_method {{connector}}/{{endpoint}}'
        ),
        lib.rate(
          'datasource_endpoint_events_total',
          '%s, event="begin_request_failed"' % epFilter,
          'begin_request_failed {{connector}}/{{endpoint}}'
        ),
      ],
      w=12, h=10,
      unit='ops',
    ),

    // Aggregated error counter stats
    lib.stat(
      title='Total Request Errors',
      targets=[
        lib.promQ(
          'sum(datasource_endpoint_events_total{%s, event="request_error"}) by (connector, endpoint)' % epFilter,
          '{{connector}} / {{endpoint}}'
        ),
      ],
      w=12, h=4,
      unit='short',
    ),
  ]
)
