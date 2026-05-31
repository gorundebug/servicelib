// Dashboard: HTTP Client
//
// Source: otelhttp.NewTransport (MetricsEngine.HTTPClientTransport)
// OTel semconv → Prometheus exporter v0.65:
//
//   http_client_request_duration_seconds{http_request_method, http_response_status_code,
//     url_scheme, server_address, server_port,
//     network_protocol_name, network_protocol_version, error_type}  histogram
//   http_client_request_body_size_bytes{...}                        histogram
//   http_client_response_body_size_bytes{...}                       histogram

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local cliFilter = 'server_address=~"$server_address"';

lib.dashboard(
  title='%s / HTTP Client' % lib.svc,
  uid='%s-http-client' % lib.svc,
  tags=['http', 'client'],
  variables=[
    lib.dsVar,
    lib.labelVar('server_address', 'server_address', 'http_client_request_duration_seconds_bucket'),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Traffic
    // -------------------------------------------------------------------------
    lib.row('Traffic'),

    lib.ts(
      title='Request Rate',
      targets=[
        lib.rate(
          'http_client_request_duration_seconds_count',
          cliFilter,
          '{{http_request_method}} → {{server_address}} {{http_response_status_code}}'
        ),
      ],
      w=24, h=8,
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
          'histogram_quantile(0.50, sum(rate(http_client_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, server_address, http_request_method))' % cliFilter,
          'p50 {{http_request_method}} → {{server_address}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(http_client_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, server_address, http_request_method))' % cliFilter,
          'p95 {{http_request_method}} → {{server_address}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Request Duration p99',
      targets=[
        lib.promQ(
          'histogram_quantile(0.99, sum(rate(http_client_request_duration_seconds_bucket{%s}[$__rate_interval])) by (le, server_address, http_request_method))' % cliFilter,
          'p99 {{http_request_method}} → {{server_address}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    // -------------------------------------------------------------------------
    // Row: Errors
    // -------------------------------------------------------------------------
    lib.row('Errors'),

    lib.ts(
      title='Error Rate (4xx + 5xx)',
      targets=[
        lib.rate(
          'http_client_request_duration_seconds_count',
          '%s, http_response_status_code=~"4..|5.."' % cliFilter,
          '{{http_response_status_code}} {{http_request_method}} → {{server_address}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Transport Error Rate',
      targets=[
        lib.rate(
          'http_client_request_duration_seconds_count',
          '%s, error_type!=""' % cliFilter,
          '{{error_type}} {{http_request_method}} → {{server_address}}'
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
          'histogram_quantile(0.95, sum(rate(http_client_request_body_size_bytes_bucket{%s}[$__rate_interval])) by (le, server_address))' % cliFilter,
          'p95 req → {{server_address}}'
        ),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Response Body Size p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(http_client_response_body_size_bytes_bucket{%s}[$__rate_interval])) by (le, server_address))' % cliFilter,
          'p95 resp ← {{server_address}}'
        ),
      ],
      w=12, h=8,
      unit='bytes',
    ),
  ]
)
