// Dashboard: Task Pools
//
// Metrics groups:
//
//   task_pool (labels: service, name)
//     task_pool_queue_length{service, name}                       gauge
//     task_pool_executors_target{service, name}                   gauge
//     task_pool_executors_allocated{service, name}                gauge
//     task_pool_executors_busy{service, name}                     gauge
//     task_pool_tasks_total{service, name}                        counter
//     task_pool_task_execution_duration_seconds{service, name}    histogram
//     task_pool_events_total{service, name, event}                counter  event=stop_timeout | task_rejected | task_cancelled
//
//   priority_task_pool (labels: service, name)
//     priority_task_pool_queue_length{service, name}              gauge
//     priority_task_pool_executors_target{service, name}          gauge
//     priority_task_pool_executors_allocated{service, name}       gauge
//     priority_task_pool_executors_busy{service, name}            gauge
//     priority_task_pool_tasks_total{service, name}               counter
//     priority_task_pool_task_execution_duration_seconds{...}     histogram
//     priority_task_pool_events_total{service, name, event}       counter  event=stop_timeout | task_rejected | task_expired
//
//   delay_pool (labels: service)
//     delay_pool_wait_queue_length{service}                       gauge
//     delay_pool_tasks_total{service}                             counter
//     delay_pool_task_execution_duration_seconds{service}         histogram
//     delay_pool_events_total{service, event}                     counter  event=stop_timeout | task_cancelled

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local svcFilter      = 'service=~"$service"';
local poolFilter     = '%s, name=~"$pool"' % svcFilter;
local priPoolFilter  = '%s, name=~"$priority_pool"' % svcFilter;

lib.dashboard(
  title='%s / Task Pools' % lib.svc,
  uid='%s-pools' % lib.svc,
  tags=['pools'],
  variables=[
    lib.dsVar,
    lib.serviceVar,
    lib.labelVar('pool',          'name', 'task_pool_tasks_total',          svcFilter),
    lib.labelVar('priority_pool', 'name', 'priority_task_pool_tasks_total', svcFilter),
  ],
  panels=[
    // =========================================================================
    // Row: Task Pool
    // =========================================================================
    lib.row('Task Pool'),

    lib.ts(
      title='Task Pool — Queue Length',
      targets=[
        lib.promQ(
          'task_pool_queue_length{%s}' % poolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Task Pool — Executor State',
      targets=[
        lib.promQ('task_pool_executors_target{%s}' % poolFilter, 'target {{service}} / {{name}}'),
        lib.promQ('task_pool_executors_allocated{%s}' % poolFilter, 'allocated {{service}} / {{name}}'),
        lib.promQ('task_pool_executors_busy{%s}' % poolFilter, 'busy {{service}} / {{name}}'),
        lib.promQ(
          'clamp_min(task_pool_executors_allocated{%s} - task_pool_executors_busy{%s}, 0)' %
          [poolFilter, poolFilter],
          'free {{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Task Pool — Task Rate',
      targets=[
        lib.rate(
          'task_pool_tasks_total',
          poolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Task Pool — Execution Duration p50',
      targets=[
        lib.hQuantileBy(
          0.5,
          'task_pool_task_execution_duration_seconds',
          'service, name',
          poolFilter,
          'p50 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Task Pool — Execution Duration p95',
      targets=[
        lib.hQuantileBy(
          0.95,
          'task_pool_task_execution_duration_seconds',
          'service, name',
          poolFilter,
          'p95 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Task Pool — Execution Duration p99',
      targets=[
        lib.hQuantileBy(
          0.99,
          'task_pool_task_execution_duration_seconds',
          'service, name',
          poolFilter,
          'p99 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Task Pool — Stop Timeout Events',
      targets=[
        lib.rate(
          'task_pool_events_total',
          '%s, event="stop_timeout"' % poolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.ts(
      title='Task Pool — Rejected Tasks Rate',
      targets=[
        lib.rate(
          'task_pool_events_total',
          '%s, event="task_rejected"' % poolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.ts(
      title='Task Pool — Cancelled Tasks Rate',
      targets=[
        lib.rate(
          'task_pool_events_total',
          '%s, event="task_cancelled"' % poolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    // =========================================================================
    // Row: Priority Task Pool
    // =========================================================================
    lib.row('Priority Task Pool'),

    lib.ts(
      title='Priority Task Pool — Queue Length',
      targets=[
        lib.promQ(
          'priority_task_pool_queue_length{%s}' % priPoolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Priority Task Pool — Executor State',
      targets=[
        lib.promQ('priority_task_pool_executors_target{%s}' % priPoolFilter, 'target {{service}} / {{name}}'),
        lib.promQ('priority_task_pool_executors_allocated{%s}' % priPoolFilter, 'allocated {{service}} / {{name}}'),
        lib.promQ('priority_task_pool_executors_busy{%s}' % priPoolFilter, 'busy {{service}} / {{name}}'),
        lib.promQ(
          'clamp_min(priority_task_pool_executors_allocated{%s} - priority_task_pool_executors_busy{%s}, 0)' %
          [priPoolFilter, priPoolFilter],
          'free {{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Priority Task Pool — Task Rate',
      targets=[
        lib.rate(
          'priority_task_pool_tasks_total',
          priPoolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Priority Task Pool — Execution Duration p50',
      targets=[
        lib.hQuantileBy(
          0.5,
          'priority_task_pool_task_execution_duration_seconds',
          'service, name',
          priPoolFilter,
          'p50 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Priority Task Pool — Execution Duration p95',
      targets=[
        lib.hQuantileBy(
          0.95,
          'priority_task_pool_task_execution_duration_seconds',
          'service, name',
          priPoolFilter,
          'p95 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Priority Task Pool — Execution Duration p99',
      targets=[
        lib.hQuantileBy(
          0.99,
          'priority_task_pool_task_execution_duration_seconds',
          'service, name',
          priPoolFilter,
          'p99 {{service}}/{{name}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Priority Task Pool — Stop Timeout Events',
      targets=[
        lib.rate(
          'priority_task_pool_events_total',
          '%s, event="stop_timeout"' % priPoolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.ts(
      title='Priority Task Pool — Rejected Tasks Rate',
      targets=[
        lib.rate(
          'priority_task_pool_events_total',
          '%s, event="task_rejected"' % priPoolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.ts(
      title='Priority Task Pool — Expired Tasks Rate',
      targets=[
        lib.rate(
          'priority_task_pool_events_total',
          '%s, event="task_expired"' % priPoolFilter,
          '{{service}} / {{name}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    // =========================================================================
    // Row: Delay Pool
    // =========================================================================
    lib.row('Delay Pool'),

    lib.ts(
      title='Delay Pool — Wait Queue Length',
      targets=[
        lib.promQ(
          'delay_pool_wait_queue_length{%s}' % svcFilter,
          '{{service}}'
        ),
      ],
      w=12, h=8,
      unit='short',
    ),

    lib.ts(
      title='Delay Pool — Task Rate',
      targets=[
        lib.rate(
          'delay_pool_tasks_total',
          svcFilter,
          '{{service}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Delay Pool — Execution Duration p50',
      targets=[
        lib.hQuantileBy(
          0.5,
          'delay_pool_task_execution_duration_seconds',
          'service',
          svcFilter,
          'p50 {{service}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Delay Pool — Execution Duration p95',
      targets=[
        lib.hQuantileBy(
          0.95,
          'delay_pool_task_execution_duration_seconds',
          'service',
          svcFilter,
          'p95 {{service}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Delay Pool — Execution Duration p99',
      targets=[
        lib.hQuantileBy(
          0.99,
          'delay_pool_task_execution_duration_seconds',
          'service',
          svcFilter,
          'p99 {{service}}'
        ),
      ],
      w=8, h=8,
      unit='s',
    ),

    lib.ts(
      title='Delay Pool — Cancelled Tasks Rate',
      targets=[
        lib.rate(
          'delay_pool_events_total',
          '%s, event="task_cancelled"' % svcFilter,
          '{{service}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),

    lib.ts(
      title='Delay Pool — Stop Timeout Events',
      targets=[
        lib.rate(
          'delay_pool_events_total',
          '%s, event="stop_timeout"' % svcFilter,
          '{{service}}'
        ),
      ],
      w=12, h=6,
      unit='ops',
    ),
  ]
)
