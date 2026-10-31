'use strict';

/**
 * Промежуточная память триады L1/L2/L3.
 *
 * Источник истины — append-only JSONL. Ничего не переписывается и не удаляется:
 * смена статуса — это тоже запись (ACK / CLOSE). Так сохраняется полный аудит
 * решений, а конфликт правок файла исключён по построению.
 */

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const LEVELS = Object.freeze(['L1', 'L2', 'L3']);
const TARGETS = Object.freeze([...LEVELS, 'ALL']);

const KINDS = Object.freeze([
  'FACT',
  'QUESTION',
  'ESCALATION',
  'DEBT',
  'DECISION',
  'ANSWER',
  'SANCTION',
  'DIRECTION',
  'RESULT',
  'ACK',
  'CLOSE',
]);

/**
 * Матрица полномочий: уровень может писать только те типы, которые входят
 * в его конституционную роль. Нарушение — не «стилистика», а отказ записи.
 */
const ALLOWED_KINDS = Object.freeze({
  L1: Object.freeze(['FACT', 'QUESTION', 'ESCALATION', 'DEBT', 'RESULT', 'ACK', 'CLOSE']),
  L2: Object.freeze([
    'FACT',
    'DECISION',
    'ANSWER',
    'ESCALATION',
    'DEBT',
    'ACK',
    'CLOSE',
  ]),
  L3: Object.freeze(['DIRECTION', 'SANCTION', 'ANSWER', 'ACK', 'CLOSE']),
});

const EVIDENCE_REQUIRED = Object.freeze([
  'FACT',
  'ESCALATION',
  'DEBT',
  'DECISION',
  'SANCTION',
  'RESULT',
]);

const REPLY_REQUIRED = Object.freeze([
  'ANSWER',
  'DECISION',
  'SANCTION',
  'RESULT',
  'ACK',
  'CLOSE',
]);

/**
 * Конвейер передачи: L3 ставит направление, L2 проектирует, L1 реализует.
 * Каждый шаг обязан ссылаться на предыдущий, иначе запись отклоняется.
 */
const PIPELINE_TARGETS = Object.freeze({
  DIRECTION: Object.freeze({ to: 'L2' }),
  DECISION: Object.freeze({
    to: 'L1',
    parents: Object.freeze(['DIRECTION', 'ESCALATION', 'DEBT']),
  }),
  RESULT: Object.freeze({ to: 'L2', parents: Object.freeze(['DECISION']) }),
  SANCTION: Object.freeze({ parents: Object.freeze(['DECISION']) }),
});

const SEVERITIES = Object.freeze(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']);
const STATUSES = Object.freeze(['open', 'acknowledged', 'closed']);

const BODY_MAX_LENGTH = 8000;
const EVIDENCE_MAX_ITEMS = 20;
const EVIDENCE_ITEM_MAX_LENGTH = 200;
const LOCK_TIMEOUT_MS = 5000;
const LOCK_STALE_MS = 30000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const sleepSync = (ms) => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
};

const isNonEmptyString = (value) =>
  typeof value === 'string' && value.trim().length > 0;

const isValidIsoTimestamp = (value) => {
  if (!isNonEmptyString(value)) {
    return false;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed);
};

const readEntries = (filePath) => {
  let raw;
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') {
      return { entries: [], malformed: 0 };
    }
    throw error;
  }

  const entries = [];
  let malformed = 0;

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed && typeof parsed === 'object' && typeof parsed.id === 'string') {
        entries.push(parsed);
      } else {
        malformed += 1;
      }
    } catch {
      malformed += 1;
    }
  }

  return { entries, malformed };
};

const deriveStatus = (entries) => {
  const statuses = new Map();

  for (const entry of entries) {
    statuses.set(entry.id, 'open');
  }

  for (const entry of entries) {
    if (entry.kind !== 'ACK' && entry.kind !== 'CLOSE') {
      continue;
    }
    if (!isNonEmptyString(entry.reply_to) || !statuses.has(entry.reply_to)) {
      continue;
    }
    if (entry.kind === 'CLOSE') {
      statuses.set(entry.reply_to, 'closed');
    } else if (statuses.get(entry.reply_to) !== 'closed') {
      statuses.set(entry.reply_to, 'acknowledged');
    }
  }

  return statuses;
};

const withLock = (lockPath, task) => {
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let fd = null;

  while (fd === null) {
    try {
      fd = fs.openSync(lockPath, 'wx');
      fs.writeSync(fd, String(process.pid));
    } catch (error) {
      if (error.code !== 'EEXIST') {
        throw error;
      }
      try {
        const stat = fs.statSync(lockPath);
        if (Date.now() - stat.mtimeMs > LOCK_STALE_MS) {
          fs.rmSync(lockPath, { force: true });
          continue;
        }
      } catch {
        continue;
      }
      if (Date.now() > deadline) {
        throw new Error(`memory write lock timeout: ${lockPath}`);
      }
      sleepSync(50);
    }
  }

  try {
    return task();
  } finally {
    try {
      fs.closeSync(fd);
    } catch {
      /* уже закрыт */
    }
    try {
      fs.rmSync(lockPath, { force: true });
    } catch {
      /* уже удалён */
    }
  }
};

const appendEntry = (filePath, entry) => {
  // Одна запись — один writeSync: строка не может быть склеена с соседней.
  const fd = fs.openSync(filePath, 'a');
  try {
    fs.writeSync(fd, `${JSON.stringify(entry)}\n`, null, 'utf8');
  } finally {
    fs.closeSync(fd);
  }
};

const validateEntry = (raw, { existingIds, statuses }) => {
  const errors = [];

  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return ['entry must be an object'];
  }

  const from = raw.from;
  const to = raw.to;
  const kind = raw.kind;

  if (!LEVELS.includes(from)) {
    errors.push(`from must be one of ${LEVELS.join(', ')}`);
  }
  if (!TARGETS.includes(to)) {
    errors.push(`to must be one of ${TARGETS.join(', ')}`);
  }
  if (!KINDS.includes(kind)) {
    errors.push(`kind must be one of ${KINDS.join(', ')}`);
  }
  if (!isNonEmptyString(raw.body)) {
    errors.push('body is required');
  } else if (raw.body.length > BODY_MAX_LENGTH) {
    errors.push(`body is longer than ${BODY_MAX_LENGTH} characters`);
  }

  if (LEVELS.includes(from) && KINDS.includes(kind)) {
    if (!ALLOWED_KINDS[from].includes(kind)) {
      errors.push(
        `${from} may not post ${kind}; allowed: ${ALLOWED_KINDS[from].join(', ')}`,
      );
    }
  }

  const evidence = Array.isArray(raw.evidence) ? raw.evidence : [];
  if (EVIDENCE_REQUIRED.includes(kind)) {
    if (evidence.length === 0) {
      errors.push(`${kind} requires at least one evidence item`);
    }
  }
  if (evidence.length > EVIDENCE_MAX_ITEMS) {
    errors.push(`evidence has more than ${EVIDENCE_MAX_ITEMS} items`);
  }
  for (const item of evidence) {
    if (!isNonEmptyString(item) || item.length > EVIDENCE_ITEM_MAX_LENGTH) {
      errors.push(
        `each evidence item must be 1..${EVIDENCE_ITEM_MAX_LENGTH} characters`,
      );
      break;
    }
  }

  if (kind === 'DEBT') {
    if (!SEVERITIES.includes(raw.severity)) {
      errors.push(`DEBT requires severity: ${SEVERITIES.join(', ')}`);
    }
  } else if (raw.severity !== undefined && raw.severity !== null) {
    errors.push('severity is only allowed for DEBT');
  }

  if (REPLY_REQUIRED.includes(kind)) {
    if (!isNonEmptyString(raw.reply_to)) {
      errors.push(`${kind} requires reply_to`);
    } else if (!existingIds.has(raw.reply_to)) {
      errors.push(`reply_to ${raw.reply_to} does not exist`);
    }
  }

  const targetEntry = raw.__targetEntry;
  const pipeline = PIPELINE_TARGETS[kind];

  if (pipeline) {
    if (pipeline.to && to !== pipeline.to) {
      errors.push(
        `${kind} must be addressed to ${pipeline.to} (chain: L3 DIRECTION -> L2 DECISION -> L1 RESULT)`,
      );
    }
    if (
      pipeline.parents &&
      targetEntry &&
      !pipeline.parents.includes(targetEntry.kind)
    ) {
      errors.push(
        `${kind} must reply to ${pipeline.parents.join(', ')}, got ${targetEntry.kind}`,
      );
    }
  }

  if (kind === 'ACK' || kind === 'CLOSE') {
    if (isNonEmptyString(raw.reply_to) && existingIds.has(raw.reply_to)) {
      const target = raw.reply_to;
      const owner = statuses.get(target);
      if (owner === 'closed') {
        errors.push(`entry ${target} is already closed`);
      }
      if (targetEntry) {
        const mayClose =
          targetEntry.to === from ||
          targetEntry.to === 'ALL' ||
          targetEntry.from === from;
        if (!mayClose) {
          errors.push(
            `${from} may not ${kind} entry ${target} addressed to ${targetEntry.to}`,
          );
        }
      }
    }
  }

  if (raw.id !== undefined) {
    if (!isNonEmptyString(raw.id) || !UUID_PATTERN.test(raw.id)) {
      errors.push('id must be a UUID');
    }
  }
  if (raw.ts !== undefined && !isValidIsoTimestamp(raw.ts)) {
    errors.push('ts must be an ISO-8601 timestamp');
  }

  if (raw.ref !== undefined) {
    const ref = raw.ref;
    if (!ref || typeof ref !== 'object' || Array.isArray(ref)) {
      errors.push('ref must be an object');
    } else {
      if (ref.files !== undefined) {
        if (!Array.isArray(ref.files) || ref.files.some((f) => !isNonEmptyString(f))) {
          errors.push('ref.files must be an array of non-empty strings');
        }
      }
      if (ref.ard !== undefined && !isNonEmptyString(ref.ard)) {
        errors.push('ref.ard must be a non-empty string');
      }
      if (ref.task !== undefined && !isNonEmptyString(ref.task)) {
        errors.push('ref.task must be a non-empty string');
      }
    }
  }

  if (raw.correlation_id !== undefined && !isNonEmptyString(raw.correlation_id)) {
    errors.push('correlation_id must be a non-empty string');
  }

  return errors;
};

const normalizeEntry = (raw) => {
  const entry = {
    id: isNonEmptyString(raw.id) ? raw.id : crypto.randomUUID(),
    ts: isValidIsoTimestamp(raw.ts) ? new Date(raw.ts).toISOString() : new Date().toISOString(),
    from: raw.from,
    to: raw.to,
    kind: raw.kind,
    body: String(raw.body),
  };

  if (raw.severity) {
    entry.severity = raw.severity;
  }
  if (Array.isArray(raw.evidence) && raw.evidence.length > 0) {
    entry.evidence = raw.evidence.map((item) => String(item));
  }
  if (isNonEmptyString(raw.reply_to)) {
    entry.reply_to = raw.reply_to;
  }
  if (isNonEmptyString(raw.correlation_id)) {
    entry.correlation_id = raw.correlation_id;
  }
  if (raw.ref && typeof raw.ref === 'object') {
    entry.ref = {
      ...(Array.isArray(raw.ref.files) ? { files: raw.ref.files.map(String) } : {}),
      ...(isNonEmptyString(raw.ref.ard) ? { ard: raw.ref.ard } : {}),
      ...(isNonEmptyString(raw.ref.task) ? { task: raw.ref.task } : {}),
    };
  }

  return entry;
};

const createMemory = ({ dir } = {}) => {
  const baseDir = path.resolve(dir || path.join(__dirname));
  const entriesPath = path.join(baseDir, 'entries.jsonl');
  const lockPath = path.join(baseDir, 'entries.lock');

  const readState = () => {
    const { entries, malformed } = readEntries(entriesPath);
    const statuses = deriveStatus(entries);
    const ids = new Set(entries.map((entry) => entry.id));
    return { entries, statuses, ids, malformed };
  };

  const post = (raw) => {
    const state = readState();
    const candidate = {
      ...raw,
      __targetEntry:
        isNonEmptyString(raw.reply_to)
          ? state.entries.find((entry) => entry.id === raw.reply_to)
          : undefined,
    };

    const errors = validateEntry(candidate, {
      existingIds: state.ids,
      statuses: state.statuses,
    });
    if (errors.length > 0) {
      const error = new Error(`invalid memory entry: ${errors.join('; ')}`);
      error.code = 'E_INVALID_ENTRY';
      error.details = errors;
      throw error;
    }

    const entry = normalizeEntry(raw);

    return withLock(lockPath, () => {
      const fresh = readState();
      if (fresh.ids.has(entry.id)) {
        return { entry: fresh.entries.find((item) => item.id === entry.id), duplicate: true };
      }
      fs.mkdirSync(baseDir, { recursive: true });
      appendEntry(entriesPath, entry);
      return { entry, duplicate: false };
    });
  };

  const list = (filters = {}) => {
    const state = readState();
    const limit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : undefined;
    const since = isValidIsoTimestamp(filters.since) ? Date.parse(filters.since) : undefined;

    const result = [];

    for (const entry of state.entries) {
      if (filters.from && entry.from !== filters.from) {
        continue;
      }
      if (filters.to && entry.to !== filters.to && !(filters.to !== 'ALL' && entry.to === 'ALL')) {
        continue;
      }
      if (filters.kind && entry.kind !== filters.kind) {
        continue;
      }
      if (filters.status) {
        const status = state.statuses.get(entry.id) || 'open';
        if (status !== filters.status) {
          continue;
        }
      }
      if (since !== undefined && Date.parse(entry.ts) < since) {
        continue;
      }
      result.push({ ...entry, status: state.statuses.get(entry.id) || 'open' });
    }

    return limit ? result.slice(-limit) : result;
  };

  const get = (id) => {
    const state = readState();
    const entry = state.entries.find((item) => item.id === id);
    if (!entry) {
      return null;
    }
    return { ...entry, status: state.statuses.get(id) || 'open' };
  };

  const stats = () => {
    const state = readState();
    const byKind = {};
    const byStatus = {};
    const debtBySeverity = {};

    for (const entry of state.entries) {
      byKind[entry.kind] = (byKind[entry.kind] || 0) + 1;
      const status = state.statuses.get(entry.id) || 'open';
      byStatus[status] = (byStatus[status] || 0) + 1;
      if (entry.kind === 'DEBT') {
        const severity = entry.severity || 'UNKNOWN';
        debtBySeverity[severity] = (debtBySeverity[severity] || 0) + 1;
      }
    }

    return {
      total: state.entries.length,
      malformedLines: state.malformed,
      byKind,
      byStatus,
      debtBySeverity,
      path: entriesPath,
    };
  };

  /**
   * Воронка передачи уровней: L3 DIRECTION -> L2 DECISION -> L1 RESULT.
   * Возвращает плоский список в порядке обхода с глубиной вложенности.
   * Служебные ACK/CLOSE в дерево не попадают — они меняют только статус.
   */
  const flow = ({ rootKinds } = {}) => {
    const state = readState();
    const tracked = state.entries.filter(
      (entry) => entry.kind !== 'ACK' && entry.kind !== 'CLOSE',
    );
    const byId = new Map(tracked.map((entry) => [entry.id, entry]));
    const allowedRoots = Array.isArray(rootKinds) ? new Set(rootKinds) : null;

    const childrenOf = new Map();
    const roots = [];

    for (const entry of tracked) {
      const parent = isNonEmptyString(entry.reply_to) ? byId.get(entry.reply_to) : undefined;
      if (parent) {
        if (!childrenOf.has(parent.id)) {
          childrenOf.set(parent.id, []);
        }
        childrenOf.get(parent.id).push(entry);
      } else {
        roots.push(entry);
      }
    }

    const result = [];
    const visit = (entry, depth) => {
      result.push({ ...entry, depth, status: state.statuses.get(entry.id) || 'open' });
      for (const child of childrenOf.get(entry.id) || []) {
        visit(child, depth + 1);
      }
    };

    for (const root of roots) {
      if (allowedRoots && !allowedRoots.has(root.kind)) {
        continue;
      }
      visit(root, 0);
    }

    return result;
  };

  return {
    paths: { dir: baseDir, entries: entriesPath },
    post,
    list,
    get,
    stats,
    flow,
    readState,
  };
};

module.exports = {
  createMemory,
  LEVELS,
  TARGETS,
  KINDS,
  STATUSES,
  SEVERITIES,
  ALLOWED_KINDS,
  readEntries,
  deriveStatus,
};
