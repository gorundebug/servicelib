// Dashboard: gRPC Client
//
// Source: otelgrpc.NewClientHandler (MetricsEngine.GRPCClientHandler)
// OTel semconv v1.41 → otelgrpc v0.69:
//
//   rpc_client_call_duration_seconds{rpc_system_name, rpc_method,
//     rpc_response_status_code}  histogram
//
// Note: rpc_service label removed in semconv v1.26+ (service is part of rpc_method).
//       per-message metrics removed in otelgrpc v0.68+.

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local jobFilter    = 'job=~"$job"';
local baseFilter   = '%s, rpc_system_name="grpc"' % jobFilter;
local methodFilter = '%s, rpc_method=~"$rpc_method"' % baseFilter;

lib.dashboard(
  title='%s / gRPC Client' % lib.svc,
  uid='%s-grpc-client' % lib.svc,
  tags=['grpc', 'client'],
  variables=[
    lib.dsVar,
    lib.jobVar('rpc_client_call_duration_seconds_bucket'),
    lib.labelVar('rpc_method', 'rpc_method', 'rpc_client_call_duration_seconds_bucket', baseFilter),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Traffic
    // -------------------------------------------------------------------------
    lib.row('Traffic'),

    lib.ts(
      title='RPC Rate',
      targets=[
        lib.rate(
          'rpc_client_call_duration_seconds_count',
          methodFilter,
          '{{rpc_method}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Status Code Distribution',
      targets=[
        lib.rate(
          'rpc_client_call_duration_seconds_count',
          methodFilter,
          '{{rpc_response_status_code}} {{rpc_method}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    // -------------------------------------------------------------------------
    // Row: Latency
    // -------------------------------------------------------------------------
    lib.row('Latency'),

    lib.ts(
      title='Duration p50',
      targets=[
        lib.promQ(
          'histogram_quantile(0.50, sum(rate(rpc_client_call_duration_seconds_bucket{%s}[$__rate_interval])) by (le, rpc_method))' % methodFilter,
          'p50 {{rpc_method}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Duration p95',
      targets=[
        lib.promQ(
          'histogram_quantile(0.95, sum(rate(rpc_client_call_duration_seconds_bucket{%s}[$__rate_interval])) by (le, rpc_method))' % methodFilter,
          'p95 {{rpc_method}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Duration p99',
      targets=[
        lib.promQ(
          'histogram_quantile(0.99, sum(rate(rpc_client_call_duration_seconds_bucket{%s}[$__rate_interval])) by (le, rpc_method))' % methodFilter,
          'p99 {{rpc_method}}'
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
      title='Call Duration Heatmap',
      metric='rpc_client_call_duration_seconds',
      filters=methodFilter,
    ),

    // -------------------------------------------------------------------------
    // Row: Errors
    // -------------------------------------------------------------------------
    lib.row('Errors'),

    lib.ts(
      title='Error Rate (non-OK)',
      targets=[
        lib.rate(
          'rpc_client_call_duration_seconds_count',
          '%s, rpc_response_status_code!="OK"' % methodFilter,
          '{{rpc_response_status_code}} {{rpc_method}}'
        ),
      ],
      w=24, h=8,
      unit='ops',
    ),
  ]
)
