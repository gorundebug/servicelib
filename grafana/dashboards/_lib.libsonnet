// Shared helpers for servicelib Grafana dashboards.
// All metric name conventions follow runtime/environment/metrics usage in the codebase.

local g = import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet';

{
  // Service name injected at compile time: jsonnet --ext-str service=<name>
  svc:: std.extVar('service'),

  // ---------------------------------------------------------------------------
  // Query helpers
  // ---------------------------------------------------------------------------

  // Prometheus query with legend format.
  promQ(expr, legend='')::
    g.query.prometheus.new('$datasource', expr)
    + g.query.prometheus.withLegendFormat(legend)
    + g.query.prometheus.withIntervalFactor(2),

  // Rate over $__rate_interval.
  rate(metric, filters='', legend='')::
    self.promQ(
      'rate(%s{%s}[$__rate_interval])' % [metric, filters],
      legend
    ),

  // Irate for fast-moving counters.
  irate(metric, filters='', legend='')::
    self.promQ(
      'irate(%s{%s}[$__rate_interval])' % [metric, filters],
      legend
    ),

  // Histogram quantile.
  hQuantile(q, metric, filters='', legend='')::
    self.promQ(
      'histogram_quantile(%g, sum(rate(%s_bucket{%s}[$__rate_interval])) by (le))' % [q, metric, filters],
      legend
    ),

  hQuantileBy(q, metric, by, filters='', legend='')::
    self.promQ(
      'histogram_quantile(%g, sum(rate(%s_bucket{%s}[$__rate_interval])) by (le, %s))' % [q, metric, filters, by],
      legend
    ),

  // ---------------------------------------------------------------------------
  // Panel helpers
  // ---------------------------------------------------------------------------

  // Generic time series panel.
  ts(title, targets, w=12, h=8, unit='short')::
    g.panel.timeSeries.new(title)
    + g.panel.timeSeries.queryOptions.withTargets(targets)
    + g.panel.timeSeries.standardOptions.withUnit(unit)
    + g.panel.timeSeries.gridPos.withW(w)
    + g.panel.timeSeries.gridPos.withH(h),

  // Stat panel (single big value).
  stat(title, targets, w=6, h=4, unit='short', reduceCalc='lastNotNull')::
    g.panel.stat.new(title)
    + g.panel.stat.queryOptions.withTargets(targets)
    + g.panel.stat.standardOptions.withUnit(unit)
    + g.panel.stat.options.reduceOptions.withCalcs([reduceCalc])
    + g.panel.stat.gridPos.withW(w)
    + g.panel.stat.gridPos.withH(h),

  // Gauge panel (visual needle/bar).
  gaugePanel(title, targets, w=6, h=8, unit='short', min=0, max=100)::
    g.panel.gauge.new(title)
    + g.panel.gauge.queryOptions.withTargets(targets)
    + g.panel.gauge.standardOptions.withUnit(unit)
    + g.panel.gauge.standardOptions.withMin(min)
    + g.panel.gauge.standardOptions.withMax(max)
    + g.panel.gauge.gridPos.withW(w)
    + g.panel.gauge.gridPos.withH(h),

  // Row panel (collapsible section header).
  row(title, collapsed=false)::
    g.panel.row.new(title)
    + g.panel.row.withCollapsed(collapsed),

  // ---------------------------------------------------------------------------
  // Dashboard template variables
  // ---------------------------------------------------------------------------

  // Prometheus datasource variable.
  dsVar::
    g.dashboard.variable.datasource.new('datasource', 'prometheus')
    + g.dashboard.variable.datasource.generalOptions.withLabel('Datasource'),

  // Query variable for service name derived from service_info metric.
  serviceVar::
    g.dashboard.variable.query.new('service', 'label_values(service_info, service)')
    + g.dashboard.variable.query.withDatasource('prometheus', '$datasource')
    + g.dashboard.variable.query.generalOptions.withLabel('Service')
    + g.dashboard.variable.query.selectionOptions.withMulti(true)
    + g.dashboard.variable.query.selectionOptions.withIncludeAll(true),

  // Generic label-values variable.
  labelVar(name, label, metric, filter='')::
    local q = if filter != '' then
      'label_values(%s{%s}, %s)' % [metric, filter, label]
    else
      'label_values(%s, %s)' % [metric, label];
    g.dashboard.variable.query.new(name, q)
    + g.dashboard.variable.query.withDatasource('prometheus', '$datasource')
    + g.dashboard.variable.query.generalOptions.withLabel(std.asciiUpper(name[0]) + name[1:])
    + g.dashboard.variable.query.selectionOptions.withMulti(true)
    + g.dashboard.variable.query.selectionOptions.withIncludeAll(true),

  // ---------------------------------------------------------------------------
  // Dashboard builder
  // ---------------------------------------------------------------------------

  dashboard(title, uid, tags=[], variables=[], panels=[])::
    g.dashboard.new(title)
    + g.dashboard.withUid(uid)
    + g.dashboard.withTags(['servicelib'] + tags)
    + g.dashboard.withTimezone('browser')
    + g.dashboard.withRefresh('30s')
    + g.dashboard.time.withFrom('now-1h')
    + g.dashboard.time.withTo('now')
    + g.dashboard.withVariables(variables)
    + g.dashboard.withPanels(g.util.grid.wrapPanels(panels, panelWidth=12, panelHeight=8)),
}
