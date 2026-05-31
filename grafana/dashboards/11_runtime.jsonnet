// Dashboard: Go Runtime & Process
//
// Source: prometheus/client_golang collectors in CreatePrometheusMetricsEngine:
//   collectors.NewGoCollector()      → go_* metrics
//   collectors.NewProcessCollector() → process_* metrics

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local jobFilter = 'job=~"$job"';

lib.dashboard(
  title='%s / Go Runtime & Process' % lib.svc,
  uid='%s-runtime' % lib.svc,
  tags=['runtime', 'go'],
  variables=[
    lib.dsVar,
    lib.labelVar('job', 'job', 'go_goroutines'),
  ],
  panels=[
    // -------------------------------------------------------------------------
    // Row: Goroutines & Threads
    // -------------------------------------------------------------------------
    lib.row('Goroutines & Threads'),

    lib.ts(
      title='Goroutines',
      targets=[
        lib.promQ('go_goroutines{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='OS Threads',
      targets=[
        lib.promQ('go_threads{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='short',
    ),

    // -------------------------------------------------------------------------
    // Row: Memory
    // -------------------------------------------------------------------------
    lib.row('Memory'),

    lib.ts(
      title='Heap Allocated',
      targets=[
        lib.promQ('go_memstats_heap_alloc_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Heap In Use vs Idle',
      targets=[
        lib.promQ('go_memstats_heap_inuse_bytes{%s}' % jobFilter, 'in-use {{job}}'),
        lib.promQ('go_memstats_heap_idle_bytes{%s}' % jobFilter,  'idle {{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Heap Reserved from OS (sys)',
      targets=[
        lib.promQ('go_memstats_heap_sys_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Stack In Use',
      targets=[
        lib.promQ('go_memstats_stack_inuse_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Heap Objects',
      targets=[
        lib.promQ('go_memstats_heap_objects{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Total Alloc Rate',
      targets=[
        lib.rate('go_memstats_alloc_bytes_total', jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='Bps',
    ),

    // -------------------------------------------------------------------------
    // Row: GC
    // -------------------------------------------------------------------------
    lib.row('Garbage Collection'),

    lib.ts(
      title='GC Pause Duration p50 / p75 / p99',
      targets=[
        lib.promQ(
          'histogram_quantile(0.50, rate(go_gc_duration_seconds_bucket{%s}[$__rate_interval]))' % jobFilter,
          'p50 {{job}}'
        ),
        lib.promQ(
          'histogram_quantile(0.75, rate(go_gc_duration_seconds_bucket{%s}[$__rate_interval]))' % jobFilter,
          'p75 {{job}}'
        ),
        lib.promQ(
          'histogram_quantile(0.99, rate(go_gc_duration_seconds_bucket{%s}[$__rate_interval]))' % jobFilter,
          'p99 {{job}}'
        ),
      ],
      w=12, h=8,
      unit='s',
    ),

    lib.ts(
      title='GC Runs per Second',
      targets=[
        lib.rate('go_gc_duration_seconds_count', jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Next GC Target',
      targets=[
        lib.promQ('go_memstats_next_gc_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='GC CPU Fraction',
      targets=[
        lib.promQ('go_memstats_gc_cpu_fraction{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='percentunit',
    ),

    // -------------------------------------------------------------------------
    // Row: Process
    // -------------------------------------------------------------------------
    lib.row('Process'),

    lib.ts(
      title='CPU Usage',
      targets=[
        lib.rate('process_cpu_seconds_total', jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='s/s',
    ),

    lib.ts(
      title='Resident Memory (RSS)',
      targets=[
        lib.promQ('process_resident_memory_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Virtual Memory',
      targets=[
        lib.promQ('process_virtual_memory_bytes{%s}' % jobFilter, '{{job}}'),
      ],
      w=12, h=8,
      unit='bytes',
    ),

    lib.ts(
      title='Open File Descriptors',
      targets=[
        lib.promQ('process_open_fds{%s}' % jobFilter, '{{job}}'),
        lib.promQ('process_max_fds{%s}' % jobFilter,  'max {{job}}'),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.stat(
      title='Process Start Time',
      targets=[
        lib.promQ('process_start_time_seconds{%s} * 1000' % jobFilter, '{{job}}'),
      ],
      w=12, h=4,
      unit='dateTimeAsLocal',
      reduceCalc='lastNotNull',
    ),
  ]
)
