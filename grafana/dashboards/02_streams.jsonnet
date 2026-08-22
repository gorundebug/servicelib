// Dashboard: Stream Pipeline
//
// Metrics group: stream
//   stream_messages_total{service, from, to}   counter  — messages passing through each link

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local serviceFilter = 'service=~"$service"';
local linkFilter = '%s, from=~"$from", to=~"$to"' % serviceFilter;

lib.dashboard(
  title='%s / Stream Pipeline' % lib.svc,
  uid='%s-streams' % lib.svc,
  tags=['streams'],
  variables=[
    lib.dsVar,
    lib.serviceVar,
    lib.labelVar('from', 'from', 'stream_messages_total', serviceFilter),
    lib.labelVar('to',   'to',   'stream_messages_total', serviceFilter),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Throughput
    // -------------------------------------------------------------------------
    lib.row('Stream Throughput'),

    // Total message rate across all links for the selected service
    lib.ts(
      title='Total Message Rate (all links)',
      targets=[
        lib.rate(
          'stream_messages_total',
          serviceFilter,
          'total'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    // Per-link message rate heatmap / stacked view
    lib.ts(
      title='Message Rate per Link',
      targets=[
        lib.rate(
          'stream_messages_total',
          linkFilter,
          '{{from}} → {{to}}'
        ),
      ],
      w=24, h=10,
      unit='ops',
    ),

    // -------------------------------------------------------------------------
    // Row: Totals
    // -------------------------------------------------------------------------
    lib.row('Cumulative Counters'),

    // Stat: total messages delivered (since start) per link
    lib.stat(
      title='Messages Delivered (total)',
      targets=[
        lib.promQ(
          'sum(stream_messages_total{%s}) by (from, to)' % serviceFilter,
          '{{from}} → {{to}}'
        ),
      ],
      w=12, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),

    // Top-N busiest links (bar gauge)
    g.panel.barGauge.new('Top Busiest Links (msg/s)')
    + g.panel.barGauge.queryOptions.withTargets([
        lib.rate(
          'stream_messages_total',
          serviceFilter,
          '{{from}} → {{to}}'
        ),
      ])
    + g.panel.barGauge.standardOptions.withUnit('ops')
    + g.panel.barGauge.options.withOrientation('horizontal')
    + g.panel.barGauge.gridPos.withW(12)
    + g.panel.barGauge.gridPos.withH(10),
  ]
)
