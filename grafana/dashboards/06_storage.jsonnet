// Dashboard: Join Storage
//
// Metrics group: hashmap_join_storage (labels: service, name)
//   hashmap_join_storage_count{service, name}           gauge   — current element count
//   hashmap_join_storage_evictions_total{service, name} counter — items evicted by TTL rotation

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local svcFilter     = 'service=~"$service"';
local storageFilter = '%s, name=~"$storage"' % svcFilter;

lib.dashboard(
  title='%s / Join Storage' % lib.svc,
  uid='%s-storage' % lib.svc,
  tags=['storage'],
  variables=[
    lib.dsVar,
    lib.serviceVar,
    lib.labelVar('storage', 'name', 'hashmap_join_storage_count', svcFilter),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Storage Size
    // -------------------------------------------------------------------------
    lib.row('Storage Size'),

    lib.ts(
      title='Element Count',
      targets=[
        lib.promQ(
          'hashmap_join_storage_count{%s}' % storageFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.stat(
      title='Current Element Count',
      targets=[
        lib.promQ(
          'hashmap_join_storage_count{%s}' % storageFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),

    // -------------------------------------------------------------------------
    // Row: Evictions (TTL rotation)
    // -------------------------------------------------------------------------
    lib.row('TTL Evictions'),

    lib.ts(
      title='Eviction Rate',
      targets=[
        lib.rate(
          'hashmap_join_storage_evictions_total',
          storageFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.stat(
      title='Total Evictions',
      targets=[
        lib.promQ(
          'sum(hashmap_join_storage_evictions_total{%s}) by (service, name)' % storageFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=4,
      unit='short',
      reduceCalc='lastNotNull',
    ),

    // Combined: count vs eviction rate to spot memory pressure
    lib.ts(
      title='Count vs Eviction Rate (combined)',
      targets=[
        lib.promQ(
          'hashmap_join_storage_count{%s}' % storageFilter,
          'count {{service}}/{{name}}'
        ),
        lib.rate(
          'hashmap_join_storage_evictions_total',
          storageFilter,
          'eviction/s {{service}}/{{name}}'
        ),
      ],
      w=24, h=10,
      unit='short',
    ),
  ]
)
