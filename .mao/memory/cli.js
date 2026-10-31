#!/usr/bin/env node
'use strict';

/**
 * CLI промежуточной памяти триады.
 * Usage: node .mao/memory/cli.js <command> [args]
 */

const fs = require('node:fs');
const path = require('node:path');
const { createMemory, SEVERITIES } = require('./store');

const USAGE = `Промежуточная память L1/L2/L3

  post --from L1 --to L2 --kind ESCALATION --body "..." [--evidence PATH:N ...]
       [--severity CRITICAL|HIGH|MEDIUM|LOW] [--reply-to ID] [--correlation-id S]
       [--id UUID] [--body-file FILE | --body -] [--json]
  list [--from L1] [--to L2] [--kind DEBT] [--status open|acknowledged|closed]
       [--since ISO] [--limit N] [--json]
  show <id> [--json]
  ack <id> --from L2 [--body "..."]
  close <id> --from L2 [--body "..."]
  flow [--root DIRECTION|ESCALATION|DECISION] [--limit N] [--json]
  debt [--status open|acknowledged|closed]
  stats [--json]

Каталог памяти: переменная MAO_MEMORY_DIR или каталог этого файла.`;

const SEVERITY_RANK = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 };

const parseArgs = (argv) => {
  const positional = [];
  const flags = {};

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith('--')) {
      positional.push(arg);
      continue;
    }

    const eq = arg.indexOf('=');
    if (eq !== -1) {
      const key = arg.slice(2, eq);
      flags[key] = arg.slice(eq + 1);
      continue;
    }

    const key = arg.slice(2);
    const next = argv[index + 1];
    if (next === undefined || next.startsWith('--')) {
      flags[key] = true;
      continue;
    }

    if (Object.prototype.hasOwnProperty.call(flags, key)) {
      flags[key] = [].concat(flags[key], next);
    } else {
      flags[key] = next;
    }
    index += 1;
  }

  return { positional, flags };
};

const asArray = (value) => {
  if (value === undefined || value === true) {
    return undefined;
  }
  return [].concat(value);
};

const readBody = (flags) => {
  if (flags.body === '-') {
    return fs.readFileSync(0, 'utf8').trim();
  }
  if (typeof flags['body-file'] === 'string') {
    return fs.readFileSync(path.resolve(flags['body-file']), 'utf8').trim();
  }
  return typeof flags.body === 'string' ? flags.body : '';
};

const shortId = (id) => (typeof id === 'string' ? id.slice(0, 8) : '--------');

const firstLine = (body, max = 90) => {
  const line = String(body || '').split(/\r?\n/)[0].trim();
  return line.length > max ? `${line.slice(0, max - 1)}…` : line;
};

const formatEntry = (entry) => {
  const severity = entry.severity ? ` [${entry.severity}]` : '';
  const status = entry.status ? ` (${entry.status})` : '';
  const reply = entry.reply_to ? ` → ${shortId(entry.reply_to)}` : '';
  return [
    `${shortId(entry.id)}  ${entry.ts}`,
    `  ${entry.from} → ${entry.to}  ${entry.kind}${severity}${status}${reply}`,
    `  ${firstLine(entry.body)}`,
  ].join('\n');
};

const printJson = (payload) => {
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
};

const formatFlowEntry = (entry) => {
  const indent = '  '.repeat(entry.depth);
  const branch = entry.depth === 0 ? '' : '└ ';
  const severity = entry.severity ? ` [${entry.severity}]` : '';
  return `${indent}${branch}${entry.kind}${severity} ${shortId(entry.id)} ${entry.from}→${entry.to} (${entry.status}) ${firstLine(entry.body, 70)}`;
};

const fail = (message, code = 1) => {
  process.stderr.write(`${message}\n`);
  process.exit(code);
};

const post = (memory, flags) => {
  const result = memory.post({
    id: typeof flags.id === 'string' ? flags.id : undefined,
    from: flags.from,
    to: flags.to,
    kind: flags.kind,
    body: readBody(flags),
    severity: typeof flags.severity === 'string' ? flags.severity.toUpperCase() : undefined,
    evidence: asArray(flags.evidence),
    reply_to: typeof flags['reply-to'] === 'string' ? flags['reply-to'] : undefined,
    correlation_id:
      typeof flags['correlation-id'] === 'string' ? flags['correlation-id'] : undefined,
  });

  if (flags.json) {
    printJson(result);
    return;
  }

  if (result.duplicate) {
    process.stdout.write(`duplicate  ${result.entry.id}\n`);
    return;
  }

  process.stdout.write(`posted  ${result.entry.id}\n`);
};

const list = (memory, filters, flags) => {
  const entries = memory.list(filters);
  if (flags.json) {
    printJson(entries);
    return;
  }
  if (entries.length === 0) {
    process.stdout.write('пусто\n');
    return;
  }
  process.stdout.write(`${entries.map(formatEntry).join('\n\n')}\n`);
};

const main = () => {
  const [command, ...rest] = process.argv.slice(2);

  if (!command || command === 'help' || command === '--help' || command === '-h') {
    process.stdout.write(`${USAGE}\n`);
    return;
  }

  const memory = createMemory({ dir: process.env.MAO_MEMORY_DIR });
  const { positional, flags } = parseArgs(rest);

  switch (command) {
    case 'post': {
      post(memory, flags);
      return;
    }

    case 'list': {
      list(
        memory,
        {
          from: typeof flags.from === 'string' ? flags.from : undefined,
          to: typeof flags.to === 'string' ? flags.to : undefined,
          kind: typeof flags.kind === 'string' ? flags.kind : undefined,
          status: typeof flags.status === 'string' ? flags.status : undefined,
          since: typeof flags.since === 'string' ? flags.since : undefined,
          limit: flags.limit ? Number(flags.limit) : undefined,
        },
        flags,
      );
      return;
    }

    case 'show': {
      const id = positional[0];
      if (!id) {
        fail('show требует id', 2);
      }
      const entry = memory.get(id);
      if (!entry) {
        fail(`не найдено: ${id}`);
      }
      if (flags.json) {
        printJson(entry);
        return;
      }
      process.stdout.write(`${JSON.stringify(entry, null, 2)}\n`);
      return;
    }

    case 'ack':
    case 'close': {
      const id = positional[0];
      if (!id) {
        fail(`${command} требует id`, 2);
      }
      if (typeof flags.from !== 'string') {
        fail(`${command} требует --from L1|L2|L3`, 2);
      }
      const target = memory.get(id);
      if (!target) {
        fail(`не найдено: ${id}`);
      }
      const result = memory.post({
        from: flags.from,
        to: target.from,
        kind: command === 'ack' ? 'ACK' : 'CLOSE',
        body: readBody(flags) || `${command} ${id} by ${flags.from}`,
        reply_to: id,
      });
      if (flags.json) {
        printJson(result);
        return;
      }
      process.stdout.write(`${command}  ${id}  by ${flags.from}\n`);
      return;
    }

    case 'flow': {
      const rootKinds = asArray(flags.root);
      const entries = memory.flow({ rootKinds });

      if (flags.json) {
        printJson(entries);
        return;
      }

      if (entries.length === 0) {
        process.stdout.write('конвейер пуст\n');
        return;
      }

      const limit = flags.limit ? Number(flags.limit) : entries.length;
      const selected = limit > 0 ? entries.slice(-limit) : entries;
      process.stdout.write(`${selected.map(formatFlowEntry).join('\n')}\n`);
      return;
    }

    case 'debt': {
      const status = typeof flags.status === 'string' ? flags.status : undefined;
      const entries = memory
        .list({ kind: 'DEBT', status })
        .filter((entry) => status !== undefined || entry.status !== 'closed')
        .sort((left, right) => {
          const rankLeft = SEVERITY_RANK[left.severity] ?? 9;
          const rankRight = SEVERITY_RANK[right.severity] ?? 9;
          return rankLeft - rankRight;
        });

      if (flags.json) {
        printJson(entries);
        return;
      }

      if (entries.length === 0) {
        process.stdout.write('реестр долга пуст\n');
        return;
      }

      const counts = SEVERITIES.map((severity) => {
        const count = entries.filter((entry) => entry.severity === severity).length;
        return `${severity}=${count}`;
      }).join('  ');
      process.stdout.write(`Реестр техдолга (${entries.length})  ${counts}\n\n`);
      process.stdout.write(`${entries.map(formatEntry).join('\n\n')}\n`);
      return;
    }

    case 'stats': {
      const stats = memory.stats();
      if (flags.json) {
        printJson(stats);
        return;
      }
      process.stdout.write(
        [
          `записей: ${stats.total}   битых строк: ${stats.malformedLines}`,
          `по типу: ${JSON.stringify(stats.byKind)}`,
          `по статусу: ${JSON.stringify(stats.byStatus)}`,
          `долг: ${JSON.stringify(stats.debtBySeverity)}`,
          `файл: ${stats.path}`,
        ].join('\n') + '\n',
      );
      return;
    }

    default: {
      fail(`неизвестная команда: ${command}\n\n${USAGE}`, 2);
    }
  }
};

try {
  main();
} catch (error) {
  if (error.code === 'E_INVALID_ENTRY') {
    process.stderr.write(`отклонено: ${error.details.join('; ')}\n`);
    process.exit(1);
  }
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
}
