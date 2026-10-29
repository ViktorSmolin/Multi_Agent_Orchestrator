const DEFAULT_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
];

const metrics = new Map();

const escapeLabel = (value) =>
  String(value)
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('\n', '\\n');

const labelKey = (labels) =>
  Object.keys(labels || {})
    .sort()
    .map((key) => `${key}="${escapeLabel(labels[key])}"`)
    .join(',');

const seriesKey = (name, labels) => `${name}{${labelKey(labels)}}`;

const metric = (name, help, type) => {
  let entry = metrics.get(name);
  if (!entry) {
    entry = { name, help, type, values: new Map() };
    metrics.set(name, entry);
    return entry;
  }
  if (help) {
    entry.help = help;
  }
  if (type && entry.type === 'untyped') {
    entry.type = type;
  }
  return entry;
};

const inc = (name, labels = {}, value = 1) => {
  const entry = metric(name, '', 'counter');
  const key = labelKey(labels);
  const current = entry.values.get(key) || { labels, value: 0 };
  current.value += value;
  entry.values.set(key, current);
};

const set = (name, labels = {}, value) => {
  const entry = metric(name, '', 'gauge');
  entry.values.set(labelKey(labels), { labels, value });
};

const observe = (name, labels = {}, value, buckets = DEFAULT_BUCKETS) => {
  const entry = metric(name, '', 'histogram');
  const key = labelKey(labels);
  const current = entry.values.get(key) || {
    labels,
    buckets: buckets.map(() => 0),
    sum: 0,
    count: 0,
  };
  current.sum += value;
  current.count += 1;
  for (let index = 0; index < buckets.length; index += 1) {
    if (value <= buckets[index]) {
      current.buckets[index] += 1;
    }
  }
  entry.values.set(key, current);
};

const renderMetric = (entry) => {
  const lines = [];
  if (entry.help) {
    lines.push(`# HELP ${entry.name} ${entry.help}`);
  }
  lines.push(`# TYPE ${entry.name} ${entry.type}`);

  for (const { labels, value } of entry.values.values()) {
    lines.push(`${seriesKey(entry.name, labels)} ${value}`);
  }
  return lines;
};

const renderHistogram = (entry) => {
  const lines = [];
  lines.push(`# HELP ${entry.name} ${entry.help || ''}`.trimEnd());
  lines.push(`# TYPE ${entry.name} histogram`);

  for (const { labels, buckets, sum, count } of entry.values.values()) {
    let cumulative = 0;
    DEFAULT_BUCKETS.forEach((bucket, index) => {
      cumulative += buckets[index];
      lines.push(
        `${seriesKey(`${entry.name}_bucket`, { ...labels, le: bucket })} ${cumulative}`,
      );
    });
    lines.push(
      `${seriesKey(`${entry.name}_bucket`, { ...labels, le: '+Inf' })} ${count}`,
    );
    lines.push(`${seriesKey(`${entry.name}_sum`, labels)} ${sum}`);
    lines.push(`${seriesKey(`${entry.name}_count`, labels)} ${count}`);
  }
  return lines;
};

const render = () => {
  const lines = [];
  for (const entry of [...metrics.values()].sort((a, b) =>
    a.name.localeCompare(b.name),
  )) {
    if (entry.type === 'untyped') {
      continue;
    }
    if (entry.type === 'histogram') {
      lines.push(...renderHistogram(entry));
    } else {
      lines.push(...renderMetric(entry));
    }
  }

  lines.push('# HELP mao_process_uptime_seconds Process uptime');
  lines.push('# TYPE mao_process_uptime_seconds gauge');
  lines.push(`mao_process_uptime_seconds ${process.uptime()}`);

  lines.push('# HELP mao_process_resident_memory_bytes Resident memory');
  lines.push('# TYPE mao_process_resident_memory_bytes gauge');
  lines.push(`mao_process_resident_memory_bytes ${process.memoryUsage().rss}`);

  return `${lines.join('\n')}\n`;
};

const help = (name, text) => {
  let entry = metrics.get(name);
  if (!entry) {
    entry = { name, help: text, type: 'untyped', values: new Map() };
    metrics.set(name, entry);
    return;
  }
  entry.help = text;
};

module.exports = { inc, set, observe, render, help };
