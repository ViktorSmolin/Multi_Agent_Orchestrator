const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { describe, it, beforeEach, afterEach } = require('node:test');

const { createMemory, readEntries, deriveStatus } = require('../store');

const makeMemory = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mao-memory-'));
  return { memory: createMemory({ dir }), dir };
};

const cleanup = (dir) => {
  fs.rmSync(dir, { recursive: true, force: true });
};

describe('memory store', () => {
  let memory;
  let dir;

  beforeEach(() => {
    ({ memory, dir } = makeMemory());
  });

  afterEach(() => {
    cleanup(dir);
  });

  it('creates file on first post and stores a valid entry', () => {
    const { entry, duplicate } = memory.post({
      from: 'L1',
      to: 'L2',
      kind: 'FACT',
      body: 'engine has no tests',
      evidence: ['core-engine/src/server.js:1'],
    });

    assert.equal(duplicate, false);
    assert.match(entry.id, /^[0-9a-f-]{36}$/);
    assert.ok(fs.existsSync(path.join(dir, 'entries.jsonl')));

    const stored = memory.get(entry.id);
    assert.equal(stored.kind, 'FACT');
    assert.equal(stored.status, 'open');
    assert.deepEqual(stored.evidence, ['core-engine/src/server.js:1']);
  });

  it('is idempotent by id', () => {
    const id = '3f1c1a52-6d1e-4b7a-9c3a-2f7d0a1b2c3d';
    const first = memory.post({ id, from: 'L1', to: 'L2', kind: 'FACT', body: 'a', evidence: ['x:1'] });
    const second = memory.post({ id, from: 'L1', to: 'L2', kind: 'FACT', body: 'a', evidence: ['x:1'] });

    assert.equal(first.duplicate, false);
    assert.equal(second.duplicate, true);
    assert.equal(readEntries(path.join(dir, 'entries.jsonl')).entries.length, 1);
  });

  it('rejects facts without evidence', () => {
    assert.throws(
      () => memory.post({ from: 'L1', to: 'L2', kind: 'FACT', body: 'no evidence' }),
      /requires at least one evidence/,
    );
  });

  it('enforces the constitutional matrix', () => {
    assert.throws(
      () => memory.post({ from: 'L1', to: 'L3', kind: 'DECISION', body: 'x', evidence: ['a:1'] }),
      /L1 may not post DECISION/,
    );
    assert.throws(
      () => memory.post({ from: 'L3', to: 'L1', kind: 'ESCALATION', body: 'x', evidence: ['a:1'] }),
      /L3 may not post ESCALATION/,
    );
  });

  it('requires severity for debt and forbids it elsewhere', () => {
    assert.throws(
      () => memory.post({ from: 'L1', to: 'L2', kind: 'DEBT', body: 'd', evidence: ['a:1'] }),
      /DEBT requires severity/,
    );
    assert.throws(
      () =>
        memory.post({
          from: 'L1',
          to: 'L2',
          kind: 'FACT',
          body: 'f',
          evidence: ['a:1'],
          severity: 'HIGH',
        }),
      /only allowed for DEBT/,
    );
  });

  it('derives acknowledged and closed status from ACK/CLOSE entries', () => {
    const { entry } = memory.post({
      from: 'L1',
      to: 'L2',
      kind: 'ESCALATION',
      body: 'loss risk',
      evidence: ['taskStream.js:8'],
    });

    memory.post({ from: 'L2', to: 'L1', kind: 'ACK', body: 'accepted', reply_to: entry.id });
    assert.equal(memory.get(entry.id).status, 'acknowledged');

    memory.post({ from: 'L2', to: 'L1', kind: 'CLOSE', body: 'fixed', reply_to: entry.id });
    assert.equal(memory.get(entry.id).status, 'closed');

    assert.throws(
      () => memory.post({ from: 'L1', to: 'L2', kind: 'ACK', body: 'late', reply_to: entry.id }),
      /already closed/,
    );
  });

  it('rejects ack from a level the entry was not addressed to', () => {
    const { entry } = memory.post({
      from: 'L1',
      to: 'L2',
      kind: 'QUESTION',
      body: 'q',
    });

    assert.throws(
      () => memory.post({ from: 'L3', to: 'L1', kind: 'ACK', body: 'a', reply_to: entry.id }),
      /may not ACK/,
    );
  });

  it('requires reply_to to reference an existing entry', () => {
    assert.throws(
      () =>
        memory.post({
          from: 'L2',
          to: 'L1',
          kind: 'ANSWER',
          body: 'a',
          reply_to: '1b3e4d5f-6a7b-4c8d-9e0f-1a2b3c4d5e6f',
        }),
      /does not exist/,
    );
  });

  it('filters by recipient, kind and status', () => {
    const first = memory.post({ from: 'L1', to: 'L2', kind: 'DEBT', severity: 'HIGH', body: 'd1', evidence: ['a:1'] });
    memory.post({ from: 'L1', to: 'L3', kind: 'QUESTION', body: 'q1' });
    memory.post({ from: 'L1', to: 'ALL', kind: 'FACT', body: 'f1', evidence: ['a:2'] });

    assert.equal(memory.list({ to: 'L2' }).length, 2);
    assert.equal(memory.list({ kind: 'QUESTION' }).length, 1);
    assert.equal(memory.list({ status: 'open' }).length, 3);

    memory.post({ from: 'L2', to: 'L1', kind: 'ACK', body: 'ok', reply_to: first.entry.id });
    assert.equal(memory.list({ to: 'L2', status: 'open' }).length, 1);
  });

  it('tolerates malformed lines without losing valid ones', () => {
    memory.post({ from: 'L1', to: 'L2', kind: 'FACT', body: 'ok', evidence: ['a:1'] });
    fs.appendFileSync(path.join(dir, 'entries.jsonl'), '{ broken json\n');

    const { entries, malformed } = readEntries(path.join(dir, 'entries.jsonl'));
    assert.equal(entries.length, 1);
    assert.equal(malformed, 1);
    assert.equal(memory.stats().malformedLines, 1);
  });

  it('reports stats including debt by severity', () => {
    memory.post({ from: 'L1', to: 'L2', kind: 'DEBT', severity: 'CRITICAL', body: 'd', evidence: ['a:1'] });
    memory.post({ from: 'L1', to: 'L2', kind: 'DEBT', severity: 'LOW', body: 'd', evidence: ['a:2'] });

    const stats = memory.stats();
    assert.equal(stats.total, 2);
    assert.deepEqual(stats.debtBySeverity, { CRITICAL: 1, LOW: 1 });
  });

  it('survives concurrent writers without interleaved lines', async () => {
    const { execFile } = require('node:child_process');
    const { promisify } = require('node:util');
    const run = promisify(execFile);
    const cliPath = path.join(__dirname, '..', 'cli.js');

    const posts = Array.from({ length: 8 }, (_, index) =>
      run(
        process.execPath,
        [
          cliPath,
          'post',
          '--from',
          'L1',
          '--to',
          'L2',
          '--kind',
          'FACT',
          '--body',
          `parallel ${index}`,
          '--evidence',
          `f:${index}`,
        ],
        { env: { ...process.env, MAO_MEMORY_DIR: dir } },
      ),
    );

    await Promise.all(posts);

    const { entries, malformed } = readEntries(path.join(dir, 'entries.jsonl'));
    assert.equal(malformed, 0);
    assert.equal(entries.length, 8);
  });
});

describe('handover pipeline', () => {
  let memory;
  let dir;

  beforeEach(() => {
    ({ memory, dir } = makeMemory());
  });

  afterEach(() => {
    cleanup(dir);
  });

  const postDirection = () =>
    memory.post({
      from: 'L3',
      to: 'L2',
      kind: 'DIRECTION',
      body: 'Фаза целостности',
      evidence: ['PROJECT_GUIDE.md:1504'],
    }).entry;

  it('L3 cannot hand a direction straight to implementation', () => {
    assert.throws(
      () =>
        memory.post({
          from: 'L3',
          to: 'L1',
          kind: 'DIRECTION',
          body: 'сделай',
          evidence: ['a:1'],
        }),
      /DIRECTION must be addressed to L2/,
    );
  });

  it('L2 decision requires a design source and targets L1', () => {
    assert.throws(
      () =>
        memory.post({
          from: 'L2',
          to: 'L1',
          kind: 'DECISION',
          body: 'без источника',
          evidence: ['a:1'],
        }),
      /DECISION requires reply_to/,
    );

    const fact = memory.post({
      from: 'L1',
      to: 'L2',
      kind: 'FACT',
      body: 'факт',
      evidence: ['a:1'],
    }).entry;

    assert.throws(
      () =>
        memory.post({
          from: 'L2',
          to: 'L1',
          kind: 'DECISION',
          body: 'из факта',
          evidence: ['a:2'],
          reply_to: fact.id,
        }),
      /DECISION must reply to DIRECTION, ESCALATION, DEBT/,
    );
  });

  it('L1 cannot report implementation without a decision', () => {
    const direction = postDirection();

    assert.throws(
      () =>
        memory.post({
          from: 'L1',
          to: 'L2',
          kind: 'RESULT',
          body: 'сделал',
          evidence: ['a:1'],
          reply_to: direction.id,
        }),
      /RESULT must reply to DECISION/,
    );

    assert.throws(
      () =>
        memory.post({
          from: 'L1',
          to: 'L2',
          kind: 'RESULT',
          body: 'сделал',
          evidence: ['a:1'],
        }),
      /RESULT requires reply_to/,
    );
  });

  it('passes a full DIRECTION -> DECISION -> RESULT chain', () => {
    const direction = postDirection();
    const decision = memory.post({
      from: 'L2',
      to: 'L1',
      kind: 'DECISION',
      body: 'ARD-001: backpressure вместо trim',
      evidence: ['docs/ard/ARD-001.md'],
      reply_to: direction.id,
    }).entry;
    const result = memory.post({
      from: 'L1',
      to: 'L2',
      kind: 'RESULT',
      body: 'реализовано, тесты 13/13',
      evidence: ['.mao/memory/tests/store.test.js:1'],
      reply_to: decision.id,
    }).entry;

    assert.equal(memory.get(result.id).status, 'open');

    const chain = memory.flow();
    assert.deepEqual(
      chain.map((entry) => [entry.kind, entry.depth]),
      [
        ['DIRECTION', 0],
        ['DECISION', 1],
        ['RESULT', 2],
      ],
    );
  });

  it('sanction must approve a decision, not a direction', () => {
    const direction = postDirection();

    assert.throws(
      () =>
        memory.post({
          from: 'L3',
          to: 'L2',
          kind: 'SANCTION',
          body: 'одобряю',
          evidence: ['a:1'],
          reply_to: direction.id,
        }),
      /SANCTION must reply to DECISION/,
    );
  });

  it('keeps ACK/CLOSE out of the flow tree', () => {
    const direction = postDirection();
    const decision = memory.post({
      from: 'L2',
      to: 'L1',
      kind: 'DECISION',
      body: 'd',
      evidence: ['a:1'],
      reply_to: direction.id,
    }).entry;

    memory.post({ from: 'L1', to: 'L2', kind: 'ACK', body: 'принято', reply_to: decision.id });

    const chain = memory.flow();
    assert.equal(chain.length, 2);
    assert.equal(memory.get(decision.id).status, 'acknowledged');
  });
});

describe('deriveStatus', () => {
  it('keeps closed terminal', () => {
    const statuses = deriveStatus([
      { id: 'a', kind: 'FACT' },
      { id: 'b', kind: 'ACK', reply_to: 'a' },
      { id: 'c', kind: 'CLOSE', reply_to: 'a' },
      { id: 'd', kind: 'ACK', reply_to: 'a' },
    ]);
    assert.equal(statuses.get('a'), 'closed');
  });
});
