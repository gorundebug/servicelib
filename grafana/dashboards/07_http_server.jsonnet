// Dashboard: HTTP Server
//
// Source: otelhttp.NewHandler (MetricsEngine.HTTPServerHandler)
// OTel semconv → Prometheus exporter v0.65:
//
//   http_server_request_duration_seconds{http_request_method, http_response_status_code,
//     http_route, url_scheme, server_address, server_port,
//     network_protocol_name, network_protocol_version, error_type}  histogram
//   http_server_request_body_size_bytes{...}                        histogram
//   http_server_response_body_size_bytes{...}                       histogram
//   http_server_active_requests{...}                                gauge

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local jobFilter   = 'job=~"$job"';
local srvFilter   = '%s, server_address=~"$server_address"' % jobFilter;
local routeFilter = '%s, http_route=~"$http_route"' % srvFilter;

lib.dashboard(
  title='%s / HTTP Server' % lib.svc,
  uid='%s-http-server' % lib.svc,
  tags=['http', 'server'],
  variables=[
    lib.dsVar,
    lib.jobVar('http_server_request_duration_seconds_bucket'),
    lib.labelVar('server_address', 'server_address', 'http_server_request_duration_seconds_bucket', jobFilter),
    lib.labelVar('http_route',     'http_route',     'http_server_request_duration_seconds_bucket', srvFilter),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Traffic
    // -------------------------------------------------------------------------
    lib.row('Traffic'),

    lib.ts(
      title='Active Requests',
      targets=[
        lib.promQ(
          'http_server_active_requests{%s}' % routeFilter,
          '{{http_route}} {{server_address}}'
        ),
      ],
      w=8, h=8,
      unit='short',
    ),

    lib.ts(
      title='Request Rate',
      targets=[
        lib.rate(
          'http_server_request_duration_seconds_count',
          routeFilter,
          '{{http_request_method}} {{http_route}} {{http_response_status_code}}'
        ),
      ],
      w=16, h=8,
      unit='ops',
    ),

    // -------------------------------------------------------------------------
    // Row: Latency
    // -------------------------------------------------------------------------
    lib.row('Latency'),

    lib.ts(
      title='Request Duration p50',
      targets=[
        lib.promQ(
          'histogram_quantile(0.50, sum(rate(http_server_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, http_route, http_request_method))' % routeFilter,
          'p50 {{http_request_method}} {{http_route}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(http_server_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, http_route, http_request_method))' % routeFilter,
          'p95 {{http_request_method}} {{http_route}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p99',
      targets=[
        lib.promQ(
          'histogram_quantile(0.99, sum(rate(http_server_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, http_route, http_request_method))' % routeFilter,
          'p99 {{http_request_method}} {{http_route}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    // -------------------------------------------------------------------------
    // Row: Latency Heatmap
    // -------------------------------------------------------------------------
    lib.row('Latency Heatmap'),

    lib.heatmap(
      title='Request Duration Heatmap',
      metric='http_server_request_duration_seconds',
      filters=routeFilter,
    ),

    // -------------------------------------------------------------------------
    // Row: Errors
    // -------------------------------------------------------------------------
    lib.row('Errors'),

    lib.ts(
      title='Error Rate (4xx + 5xx)',
      targets=[
        lib.rate(
          'http_server_request_duration_seconds_count',
          '%s, http_response_status_code=~"4..|5.."' % routeFilter,
          '{{http_response_status_code}} {{http_request_method}} {{http_route}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Transport Error Rate',
      targets=[
        lib.rate(
          'http_server_request_duration_seconds_count',
          '%s, error_type!=""' % routeFilter,
          '{{error_type}} {{http_request_method}} {{http_route}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    // -------------------------------------------------------------------------
    // Row: Payload Size
    // -------------------------------------------------------------------------
    lib.row('Payload Size'),

    lib.ts(
      title='Request Body Size p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(http_server_request_body_size_bytes_bucket{%s}[$__rate_interval])) by (le, http_route))' % routeFilter,
          'p95 req {{http_route}}'
        ),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Response Body Size p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(http_server_response_body_size_bytes_bucket{%s}[$__rate_interval])) by (le, http_route))' % routeFilter,
          'p95 resp {{http_route}}'
        ),
      ],
      w=12, h=8,
      unit='bytes',
    ),
  ]
)
