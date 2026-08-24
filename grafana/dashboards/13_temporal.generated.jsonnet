// Dashboard: Temporal Server, official SDK/Worker, and ServiceLib graph context.
//
// Temporal owns Workflow, Activity, queue, retry, and SDK latency. ServiceLib
// contributes only the graph link throughput; it deliberately does not publish
// a second duration histogram for the same DurableCall operation.

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';
local lib = import '_lib.libsonnet';

local sdkFilter = 'telemetry_source="temporal-sdk", job=~"$sdk_job"';
local queueFilter = '%s, task_queue=~"$task_queue"' % sdkFilter;
local durableFilter = '%s, activity_type=~"servicegen\\.durable\\..*"' % queueFilter;
local serverFilter = 'telemetry_source="temporal-server", job=~"$server_job"';
local graphFilter = 'service=~"$service", from=~"$from", to=~"$to"';

lib.dashboard(
  title='%s / Temporal & DurableCall' % lib.svc,
  uid='%s-temporal' % lib.svc,
  tags=['temporal', 'durable-call'],
  variables=[
    lib.dsVar,
    lib.labelVar('server_job', 'job', 'service_requests', 'telemetry_source="temporal-server"'),
    lib.labelVar('sdk_job', 'job', 'temporal_worker_task_slots_available', 'telemetry_source="temporal-sdk"'),
    lib.labelVar('task_queue', 'task_queue', 'temporal_worker_task_slots_available', sdkFilter),
    lib.serviceVar,
    lib.labelVar('from', 'from', 'stream_messages_total', 'service=~"$service"'),
    lib.labelVar('to', 'to', 'stream_messages_total', 'service=~"$service", from=~"$from"'),
  ],
  panels=[
    lib.row('Temporal Server'),

    lib.ts(
      title='Frontend Request Rate',
      targets=[
        lib.promQ(
          'sum(rate(service_requests{%s}[$__rate_interval])) by (operation)' % serverFilter,
          '{{operation}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.ts(
      title='Server Error Rate',
      targets=[
        lib.promQ(
          'sum(rate({__name__=~"service_errors.*", %s}[$__rate_interval])) by (__name__, operation)' % serverFilter,
          '{{__name__}} {{operation}}'
        ),
      ],
      w=12, h=8,
      unit='ops',
    ),

    lib.row('Official SDK / Worker'),

    lib.ts(
      title='Worker Slots Used / Available',
      targets=[
        lib.promQ(
          'sum(temporal_worker_task_slots_used{%s}) by (worker_type, task_queue)' % queueFilter,
          'used {{worker_type}} / {{task_queue}}'
        ),
        lib.promQ(
          'sum(temporal_worker_task_slots_available{%s}) by (worker_type, task_queue)' % queueFilter,
          'available {{worker_type}} / {{task_queue}}'
        ),
      ],
      w=24, h=8,
      unit='short',
    ),

    lib.ts(
      title='Temporal Client Request Latency p50 / p95 / p99',
      targets=[
        lib.hQuantileBy(0.50, 'temporal_request_latency_seconds', 'operation', sdkFilter, 'p50 {{operation}}'),
        lib.hQuantileBy(0.95, 'temporal_request_latency_seconds', 'operation', sdkFilter, 'p95 {{operation}}'),
        lib.hQuantileBy(0.99, 'temporal_request_latency_seconds', 'operation', sdkFilter, 'p99 {{operation}}'),
      ],
      w=24, h=8,
      unit='s',
    ),

    lib.row('DurableCall — Temporal-owned latency'),

    lib.ts(
      title='Queue Wait p50 / p95 / p99',
      targets=[
        lib.hQuantileBy(0.50, 'temporal_activity_schedule_to_start_latency_seconds', 'activity_type, task_queue', durableFilter, 'p50 {{activity_type}}'),
        lib.hQuantileBy(0.95, 'temporal_activity_schedule_to_start_latency_seconds', 'activity_type, task_queue', durableFilter, 'p95 {{activity_type}}'),
        lib.hQuantileBy(0.99, 'temporal_activity_schedule_to_start_latency_seconds', 'activity_type, task_queue', durableFilter, 'p99 {{activity_type}}'),
      ],
      w=12, h=8,
      unit='s',
    ),

    lib.ts(
      title='Activity Execution p50 / p95 / p99',
      targets=[
        lib.hQuantileBy(0.50, 'temporal_activity_execution_latency_seconds', 'activity_type, task_queue', durableFilter, 'p50 {{activity_type}}'),
        lib.hQuantileBy(0.95, 'temporal_activity_execution_latency_seconds', 'activity_type, task_queue', durableFilter, 'p95 {{activity_type}}'),
        lib.hQuantileBy(0.99, 'temporal_activity_execution_latency_seconds', 'activity_type, task_queue', durableFilter, 'p99 {{activity_type}}'),
      ],
      w=12, h=8,
      unit='s',
    ),

    lib.ts(
      title='Activity Failures and Cancellations',
      targets=[
        lib.rate('temporal_activity_execution_failed_total', durableFilter, 'failed {{activity_type}}'),
        lib.rate('temporal_activity_execution_cancelled_total', durableFilter, 'cancelled {{activity_type}}'),
      ],
      w=24, h=8,
      unit='ops',
    ),

    lib.row('ServiceLib application context'),

    lib.ts(
      title='Graph Link Throughput',
      targets=[
        lib.rate('stream_messages_total', graphFilter, '{{from}} → {{to}}'),
      ],
      w=24, h=8,
      unit='ops',
    ),
  ]
)
