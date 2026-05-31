// Dashboard: Service Overview
//
// Metrics group: service
//   service_info{service, environment}               gauge   (always 1, used for metadata)
//   service_config_reloads_total{service, event}     counter (event=success|error)

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local serviceFilter = 'service=~"$service"';

lib.dashboard(
  title='%s / Service Overview' % lib.svc,
  uid='%s-service' % lib.svc,
  tags=['service'],
  variables=[
    lib.dsVar,
    lib.serviceVar,
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Service Info
    // -------------------------------------------------------------------------
    lib.row('Service Info'),

    // Stat: number of running service instances (count of series)
    lib.stat(
      title='Running Instances',
      targets=[
        lib.promQ(
          'count(service_info{%s}) by (service)' % serviceFilter,
          '{{service}}'
        ),
      ],
      w=6, h=4,
      unit='short',
    ),

    // Stat: service environment label (textual, shown as string via label_values)
    lib.stat(
      title='Environment',
      targets=[
        lib.promQ(
          'label_replace(service_info{%s}, "__tmp", "$1", "environment", "(.*)")' % serviceFilter,
          '{{environment}}'
        ),
      ],
      w=6, h=4,
      unit='short',
    ),

    // Time series: uptime expressed as kv (value stays 1 as long as service is up)
    lib.ts(
      title='Service Up',
      targets=[
        lib.promQ(
          'service_info{%s}' % serviceFilter,
          '{{service}} / {{environment}}'
        ),
      ],
      w=12, h=6,
      unit='short',
    ),

    // -------------------------------------------------------------------------
    // Row: Config Reloads
    // -------------------------------------------------------------------------
    lib.row('Config Hot-Reload'),

    // Stat: cumulative successful config reloads
    lib.stat(
      title='Successful Reloads (total)',
      targets=[
        lib.promQ(
          'sum(service_config_reloads_total{%s, event="success"}) by (service)' % serviceFilter,
          '{{service}}'
        ),
      ],
      w=6, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),

    // Stat: cumulative failed config reloads
    lib.stat(
      title='Failed Reloads (total)',
      targets=[
        lib.promQ(
          'sum(service_config_reloads_total{%s, event="error"}) by (service)' % serviceFilter,
          '{{service}}'
        ),
      ],
      w=6, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),

    // Time series: reload rate
    lib.ts(
      title='Config Reload Rate',
      targets=[
        lib.rate(
          'service_config_reloads_total',
          '%s, event="success"' % serviceFilter,
          '{{service}} success'
        ),
        lib.rate(
          'service_config_reloads_total',
          '%s, event="error"' % serviceFilter,
          '{{service}} error'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),
  ]
)
