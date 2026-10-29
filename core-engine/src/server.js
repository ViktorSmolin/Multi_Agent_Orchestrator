const http = require('node:http');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const config = require('./config');
const { createPool, waitForDatabase } = require('./db');
const { runMigrations } = require('./migrations');
const { insertEvent } = require('./persistence');
const { createEventBridge, EVENT_CHANNEL } = require('./redis');
const { createTaskStream } = require('./taskStream');
const { createAuth } = require('./auth');
const metrics = require('./metrics');
const {
  PROTOCOL_VERSION,
  EVENT_TYPES,
  validateClientEnvelope,
  toSafeRequestId,
} = require('./protocol');

const pool = createPool(config.databaseUrl);
const auth = createAuth(config.apiKey);
const startedAt = new Date();
const migrationsDir =
  process.env.MIGRATIONS_DIR || path.join(__dirname, '..', 'migrations');
let shuttingDown = false;
let eventBridge = null;
let taskStream = null;

metrics.help('mao_events_total', 'Events processed by the engine');
metrics.help('mao_events_rejected_total', 'Events rejected before persistence');
metrics.help('mao_ws_connections', 'Open WebSocket connections');
metrics.help('mao_ws_messages_total', 'WebSocket messages received by type');
metrics.help('mao_redis_publish_total', 'Redis event fan-out results');
metrics.help('mao_task_claims_total', 'Task stream claim results');
metrics.help('mao_task_acks_total', 'Task stream acknowledgement results');
metrics.help('mao_task_stream_length', 'Approximate Redis task stream length');
metrics.help('mao_http_requests_total', 'HTTP requests by route and status');
metrics.help(
  'mao_http_request_duration_seconds',
  'HTTP request duration in seconds',
);
metrics.set('mao_ws_connections', {}, 0);

const log = (level, message, context = {}) => {
  process.stdout.write(
    `${JSON.stringify({
      timestamp: new Date().toISOString(),
      service: config.serviceName,
      level,
      message,
      ...context,
    })}\n`,
  );
};

const sendJson = (res, statusCode, payload) => {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    'cache-control': 'no-store',
  });
  res.end(body);
};

const readJsonBody = (req) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;

    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 1_000_000) {
        const error = new Error('Payload too large');
        error.statusCode = 413;
        reject(error);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });

    req.on('end', () => {
      if (chunks.length === 0) {
        resolve(undefined);
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        const error = new Error('Invalid JSON body');
        error.statusCode = 400;
        reject(error);
      }
    });

    req.on('error', reject);
  });

const stateKeyFromPath = (pathname) => {
  const match = pathname.match(/^\/state\/([A-Za-z0-9._:-]{1,128})$/);
  return match ? match[1] : null;
};

const routeLabel = (pathname) => {
  if (
    ['/health', '/metrics', '/events', '/events/cursor', '/tasks/claim', '/state'].includes(
      pathname,
    )
  ) {
    return pathname;
  }
  if (stateKeyFromPath(pathname)) {
    return '/state/:key';
  }
  return 'other';
};

const handleRequest = async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && url.pathname === '/health') {
    const probeStarted = process.hrtime.bigint();
    let database = 'up';
    let databaseLatencyMs;

    try {
      await pool.query('SELECT 1');
      databaseLatencyMs = Number(process.hrtime.bigint() - probeStarted) / 1e6;
    } catch (error) {
      database = 'down';
      log('error', 'health probe failed', { error: error.message });
    }

    const redis =
      eventBridge &&
      eventBridge.isReady() &&
      taskStream &&
      taskStream.isReady()
        ? 'up'
        : 'down';
    const healthy = database === 'up' && redis === 'up';

    sendJson(res, healthy ? 200 : 503, {
      status: healthy ? 'ok' : 'degraded',
      service: config.serviceName,
      instanceId: config.instanceId,
      protocolVersion: PROTOCOL_VERSION,
      authRequired: auth.enabled,
      database,
      databaseLatencyMs,
      redis,
      uptimeSeconds: Math.round(process.uptime()),
      startedAt: startedAt.toISOString(),
      timestamp: new Date().toISOString(),
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/metrics') {
    if (taskStream && taskStream.isReady()) {
      try {
        metrics.set('mao_task_stream_length', {}, await taskStream.length());
      } catch (error) {
        log('warn', 'task stream length failed', { error: error.message });
      }
    }
    const body = metrics.render();
    res.writeHead(200, {
      'content-type': 'text/plain; version=0.0.4; charset=utf-8',
      'cache-control': 'no-store',
      'content-length': Buffer.byteLength(body),
    });
    res.end(body);
    return;
  }

  if (!auth.isAuthorized(req)) {
    res.setHeader('www-authenticate', 'Bearer realm="mao"');
    sendJson(res, 401, { error: 'unauthorized' });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/tasks/claim') {
    const agentId = url.searchParams.get('agent_id') || '';
    if (!/^[A-Za-z0-9._:-]{1,128}$/.test(agentId)) {
      sendJson(res, 400, { error: 'invalid_agent_id' });
      return;
    }
    if (!taskStream || !taskStream.isReady()) {
      sendJson(res, 503, { error: 'task_stream_unavailable' });
      return;
    }

    const parsedBlock = Number.parseInt(
      url.searchParams.get('block_ms') ?? String(config.taskClaimBlockMs),
      10,
    );
    const blockMs = Number.isInteger(parsedBlock)
      ? Math.min(Math.max(parsedBlock, 100), 30000)
      : config.taskClaimBlockMs;
    const parsedIdle = Number.parseInt(
      url.searchParams.get('idle_ms') ?? String(config.taskClaimIdleMs),
      10,
    );
    const idleMs = Number.isInteger(parsedIdle)
      ? Math.min(Math.max(parsedIdle, 0), 300000)
      : config.taskClaimIdleMs;

    let claimed = null;
    try {
      for (let attempt = 0; attempt < 5; attempt += 1) {
        claimed = await taskStream.claim(agentId, { blockMs, idleMs });
        if (!claimed) {
          break;
        }

        const result = await pool.query(
          `SELECT 1 FROM agent_events
           WHERE event_type = 'TASK_UPDATED' AND correlation_id = $1
           LIMIT 1`,
          [claimed.event.correlation_id],
        );
        if (result.rowCount === 0) {
          break;
        }

        await taskStream.ackById(
          claimed.streamId,
          claimed.event.correlation_id,
        );
        metrics.inc('mao_task_claims_total', {
          result: 'skipped_completed',
        });
        claimed = null;
      }
      metrics.inc('mao_task_claims_total', {
        result: claimed ? 'claimed' : 'empty',
      });
    } catch (error) {
      metrics.inc('mao_task_claims_total', { result: 'error' });
      throw error;
    }
    if (!claimed) {
      res.writeHead(204, {
        'cache-control': 'no-store',
        'content-length': '0',
      });
      res.end();
      return;
    }

    sendJson(res, 200, {
      stream_id: claimed.streamId,
      event: claimed.event,
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/events/cursor') {
    const eventType = url.searchParams.get('event_type') ?? 'TASK_CREATED';
    if (!EVENT_TYPES.includes(eventType)) {
      sendJson(res, 400, {
        error: 'invalid_event_type',
        event_type: eventType,
      });
      return;
    }

    const { rows } = await pool.query(
      `SELECT COALESCE(MAX(sequence_id), 0) AS last_sequence_id
       FROM agent_events
       WHERE event_type = $1`,
      [eventType],
    );

    sendJson(res, 200, {
      event_type: eventType,
      last_sequence_id: Number(rows[0].last_sequence_id),
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/events') {
    const eventType = url.searchParams.get('event_type') ?? 'TASK_CREATED';
    if (!EVENT_TYPES.includes(eventType)) {
      sendJson(res, 400, {
        error: 'invalid_event_type',
        event_type: eventType,
      });
      return;
    }

    const parsedAfter = Number.parseInt(
      url.searchParams.get('after_sequence_id') ?? '0',
      10,
    );
    const afterSequenceId =
      Number.isInteger(parsedAfter) && parsedAfter >= 0 ? parsedAfter : 0;

    const parsedLimit = Number.parseInt(
      url.searchParams.get('limit') ?? '100',
      10,
    );
    const limit = Number.isInteger(parsedLimit)
      ? Math.min(Math.max(parsedLimit, 1), 500)
      : 100;

    const { rows } = await pool.query(
      `SELECT sequence_id, event_id, timestamp, sender_id, event_type,
              payload, correlation_id
       FROM agent_events
       WHERE event_type = $1 AND sequence_id > $2
       ORDER BY sequence_id
       LIMIT $3`,
      [eventType, afterSequenceId, limit],
    );

    const items = rows.map((row) => ({
      sequence_id: Number(row.sequence_id),
      event: {
        event_id: row.event_id,
        timestamp: new Date(row.timestamp).toISOString(),
        sender_id: row.sender_id,
        event_type: row.event_type,
        payload: row.payload,
        correlation_id: row.correlation_id,
      },
    }));

    sendJson(res, 200, {
      event_type: eventType,
      items,
      has_more: items.length === limit,
      next_after_sequence_id:
        items.length > 0 ? items[items.length - 1].sequence_id : afterSequenceId,
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/state') {
    const { rows } = await pool.query(
      'SELECT key, value, updated_at FROM engine_state ORDER BY updated_at DESC LIMIT 50',
    );
    sendJson(res, 200, { items: rows });
    return;
  }

  const stateKey = stateKeyFromPath(url.pathname);

  if (stateKey && req.method === 'GET') {
    const { rows } = await pool.query(
      'SELECT key, value, updated_at FROM engine_state WHERE key = $1',
      [stateKey],
    );
    if (rows.length === 0) {
      sendJson(res, 404, { error: 'not_found', key: stateKey });
      return;
    }
    sendJson(res, 200, rows[0]);
    return;
  }

  if (stateKey && req.method === 'PUT') {
    const body = await readJsonBody(req);
    if (!body || !Object.prototype.hasOwnProperty.call(body, 'value')) {
      sendJson(res, 400, { error: 'value_required' });
      return;
    }

    const { rows } = await pool.query(
      `INSERT INTO engine_state (key, value, updated_at)
       VALUES ($1, $2::jsonb, now())
       ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value, updated_at = now()
       RETURNING key, value, updated_at`,
      [stateKey, JSON.stringify(body.value)],
    );
    sendJson(res, 200, rows[0]);
    return;
  }

  sendJson(res, 404, { error: 'route_not_found', path: url.pathname });
};

const server = http.createServer((req, res) => {
  const started = process.hrtime.bigint();
  let route = 'other';
  try {
    route = routeLabel(new URL(req.url, 'http://localhost').pathname);
  } catch {
    route = 'invalid';
  }

  res.on('finish', () => {
    metrics.inc('mao_http_requests_total', {
      method: req.method,
      route,
      status: String(res.statusCode),
    });
    metrics.observe(
      'mao_http_request_duration_seconds',
      { method: req.method, route },
      Number(process.hrtime.bigint() - started) / 1e9,
    );
  });

  handleRequest(req, res).catch((error) => {
    log('error', 'request failed', {
      method: req.method,
      path: req.url,
      error: error.message,
    });
    if (!res.headersSent) {
      sendJson(res, error.statusCode || 500, {
        error: error.statusCode ? error.message : 'internal_error',
      });
    } else {
      res.destroy();
    }
  });
});

const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: config.maxPayloadBytes,
  verifyClient: (info, callback) => {
    if (auth.isAuthorized(info.req)) {
      callback(true);
      return;
    }
    log('warn', 'websocket unauthorized', {
      remoteAddress: info.req.socket.remoteAddress,
    });
    callback(false, 401, 'Unauthorized');
  },
});

const sendSerialized = (socket, payload) => {
  if (socket.readyState !== WebSocket.OPEN) {
    return;
  }
  if (socket.bufferedAmount > config.maxBufferedBytes) {
    log('warn', 'closing slow consumer', {
      connectionId: socket.connectionState?.id,
      bufferedAmount: socket.bufferedAmount,
    });
    socket.close(1013, 'slow_consumer');
    return;
  }
  try {
    socket.send(payload);
  } catch (error) {
    log('warn', 'websocket send failed', {
      connectionId: socket.connectionState?.id,
      error: error.message,
    });
  }
};

const sendEnvelope = (socket, envelope) => {
  sendSerialized(socket, JSON.stringify(envelope));
};

const errorEnvelope = (code, message, { requestId, details } = {}) => {
  const envelope = {
    v: PROTOCOL_VERSION,
    type: 'ERROR',
    data: { code, message },
  };
  if (requestId) {
    envelope.request_id = requestId;
  }
  if (details && details.length > 0) {
    envelope.data.details = details;
  }
  return envelope;
};

const pongEnvelope = (requestId) => {
  const envelope = {
    v: PROTOCOL_VERSION,
    type: 'PONG',
    data: { server_time: new Date().toISOString() },
  };
  if (requestId) {
    envelope.request_id = requestId;
  }
  return envelope;
};

const ackEnvelope = (requestId, data) => ({
  v: PROTOCOL_VERSION,
  type: 'ACK',
  request_id: requestId,
  data,
});

const welcomeEnvelope = (state) => ({
  v: PROTOCOL_VERSION,
  type: 'WELCOME',
  data: {
    connection_id: state.id,
    protocol_version: PROTOCOL_VERSION,
    heartbeat_interval_ms: config.heartbeatIntervalMs,
    heartbeat_timeout_ms: config.heartbeatTimeoutMs,
    max_payload_bytes: config.maxPayloadBytes,
    event_types: [...EVENT_TYPES],
    subscriptions: [],
    server_time: new Date().toISOString(),
  },
});

const subscriptionList = (subscriptions) => {
  if (subscriptions.has('*')) {
    return ['*'];
  }
  return EVENT_TYPES.filter((eventType) => subscriptions.has(eventType));
};

const subscribe = (subscriptions, eventTypes) => {
  if (eventTypes.includes('*')) {
    subscriptions.clear();
    subscriptions.add('*');
    return;
  }
  if (subscriptions.has('*')) {
    return;
  }
  for (const eventType of eventTypes) {
    subscriptions.add(eventType);
  }
};

const unsubscribe = (subscriptions, eventTypes) => {
  if (eventTypes.includes('*')) {
    subscriptions.clear();
    return;
  }
  if (subscriptions.has('*')) {
    subscriptions.clear();
    for (const eventType of EVENT_TYPES) {
      if (!eventTypes.includes(eventType)) {
        subscriptions.add(eventType);
      }
    }
    return;
  }
  for (const eventType of eventTypes) {
    subscriptions.delete(eventType);
  }
};

const broadcastEnvelope = (envelope) => {
  if (
    !envelope ||
    envelope.v !== PROTOCOL_VERSION ||
    envelope.type !== 'EVENT' ||
    !envelope.data ||
    !EVENT_TYPES.includes(envelope.data.event_type)
  ) {
    return;
  }

  const payload = JSON.stringify(envelope);
  const eventType = envelope.data.event_type;

  for (const client of wss.clients) {
    const state = client.connectionState;
    if (
      state &&
      (state.subscriptions.has('*') || state.subscriptions.has(eventType))
    ) {
      sendSerialized(client, payload);
    }
  }
};

const handleClientMessage = async (socket, state, raw) => {
  let parsed;
  try {
    parsed = JSON.parse(raw.toString('utf8'));
  } catch {
    metrics.inc('mao_ws_messages_total', { type: 'invalid_json' });
    sendEnvelope(
      socket,
      errorEnvelope('INVALID_JSON', 'Frame must contain valid JSON'),
    );
    return;
  }

  metrics.inc('mao_ws_messages_total', {
    type: ['EVENT', 'SUBSCRIBE', 'UNSUBSCRIBE', 'PING'].includes(parsed?.type)
      ? parsed.type
      : 'unknown',
  });

  if (
    parsed &&
    typeof parsed === 'object' &&
    parsed.v !== undefined &&
    parsed.v !== PROTOCOL_VERSION
  ) {
    sendEnvelope(
      socket,
      errorEnvelope(
        'UNSUPPORTED_PROTOCOL_VERSION',
        `Supported protocol version: ${PROTOCOL_VERSION}`,
        { requestId: toSafeRequestId(parsed.request_id) },
      ),
    );
    return;
  }

  const validation = validateClientEnvelope(parsed);
  if (!validation.ok) {
    metrics.inc('mao_events_rejected_total', {
      reason: 'validation_error',
    });
    sendEnvelope(
      socket,
      errorEnvelope(
        'VALIDATION_ERROR',
        'Message does not conform to protocol v1',
        {
          requestId: toSafeRequestId(parsed && parsed.request_id),
          details: validation.errors,
        },
      ),
    );
    return;
  }

  const message = validation.value;

  switch (message.type) {
    case 'PING': {
      sendEnvelope(socket, pongEnvelope(message.request_id));
      return;
    }
    case 'SUBSCRIBE': {
      subscribe(state.subscriptions, message.data.event_types);
      sendEnvelope(
        socket,
        ackEnvelope(message.request_id, {
          status: 'accepted',
          subscriptions: subscriptionList(state.subscriptions),
        }),
      );
      return;
    }
    case 'UNSUBSCRIBE': {
      unsubscribe(state.subscriptions, message.data.event_types);
      sendEnvelope(
        socket,
        ackEnvelope(message.request_id, {
          status: 'accepted',
          subscriptions: subscriptionList(state.subscriptions),
        }),
      );
      return;
    }
    case 'EVENT': {
      const event = message.data;
      const result = await insertEvent(pool, event);
      metrics.inc('mao_events_total', {
        event_type: event.event_type,
        result: result.duplicate ? 'duplicate' : 'accepted',
      });

      if (event.event_type === 'TASK_CREATED') {
        if (!taskStream || !taskStream.isReady()) {
          sendEnvelope(
            socket,
            errorEnvelope(
              'TASK_QUEUE_UNAVAILABLE',
              'Task stream is not ready; retry the event',
              { requestId: message.request_id },
            ),
          );
          return;
        }
        try {
          await taskStream.enqueue(event, event.correlation_id);
        } catch (error) {
          log('error', 'task enqueue failed', {
            eventId: event.event_id,
            correlationId: event.correlation_id,
            error: error.message,
          });
          sendEnvelope(
            socket,
            errorEnvelope(
              'TASK_QUEUE_UNAVAILABLE',
              'Task stream is not ready; retry the event',
              { requestId: message.request_id },
            ),
          );
          return;
        }
      }

      sendEnvelope(
        socket,
        ackEnvelope(message.request_id, {
          status: result.duplicate ? 'duplicate' : 'accepted',
          event_id: event.event_id,
          sequence_id: result.sequenceId,
          duplicate: result.duplicate,
        }),
      );

      log('info', result.duplicate ? 'event duplicate' : 'event accepted', {
        connectionId: state.id,
        eventId: event.event_id,
        eventType: event.event_type,
        correlationId: event.correlation_id,
        sequenceId: result.sequenceId,
      });

      if (event.event_type === 'TASK_UPDATED' && taskStream) {
        try {
          const acked = await taskStream.ack(event.correlation_id);
          metrics.inc('mao_task_acks_total', {
            result: acked ? 'acked' : 'missing',
          });
          log('info', 'task stream acknowledged', {
            correlationId: event.correlation_id,
            streamId: acked,
          });
        } catch (error) {
          metrics.inc('mao_task_acks_total', { result: 'error' });
          log('error', 'task stream ack failed', {
            correlationId: event.correlation_id,
            error: error.message,
          });
        }
      }

      if (!result.duplicate) {
        if (eventBridge) {
          try {
            await eventBridge.publish({
              v: PROTOCOL_VERSION,
              type: 'EVENT',
              sequence_id: result.sequenceId,
              data: event,
            });
            metrics.inc('mao_redis_publish_total', { result: 'ok' });
          } catch (error) {
            metrics.inc('mao_redis_publish_total', { result: 'error' });
            log('error', 'event publish failed', {
              eventId: event.event_id,
              error: error.message,
            });
          }
        } else {
          log('error', 'event bridge is not ready', {
            eventId: event.event_id,
          });
        }
      }
      return;
    }
    default: {
      sendEnvelope(
        socket,
        errorEnvelope('VALIDATION_ERROR', 'Unsupported message type', {
          requestId: message.request_id,
        }),
      );
    }
  }
};

wss.on('connection', (socket, req) => {
  const state = {
    id: randomUUID(),
    subscriptions: new Set(),
    remoteAddress: req.socket.remoteAddress,
  };
  socket.connectionState = state;
  socket.heartbeatTimeout = null;

  log('info', 'websocket connected', {
    connectionId: state.id,
    remoteAddress: state.remoteAddress,
  });
  metrics.set('mao_ws_connections', {}, wss.clients.size);

  const armHeartbeatTimeout = () => {
    if (socket.heartbeatTimeout) {
      clearTimeout(socket.heartbeatTimeout);
    }
    socket.heartbeatTimeout = setTimeout(() => {
      log('warn', 'heartbeat timeout', { connectionId: state.id });
      socket.terminate();
    }, config.heartbeatTimeoutMs);
    socket.heartbeatTimeout.unref();
  };

  armHeartbeatTimeout();
  sendEnvelope(socket, welcomeEnvelope(state));

  socket.on('pong', () => {
    armHeartbeatTimeout();
  });

  socket.on('message', (raw, isBinary) => {
    if (isBinary) {
      sendEnvelope(
        socket,
        errorEnvelope('VALIDATION_ERROR', 'Binary frames are not supported'),
      );
      socket.close(1003, 'binary_not_supported');
      return;
    }

    handleClientMessage(socket, state, raw).catch((error) => {
      log('error', 'message handling failed', {
        connectionId: state.id,
        error: error.message,
      });
      sendEnvelope(
        socket,
        errorEnvelope('INTERNAL_ERROR', 'Internal server error'),
      );
    });
  });

  socket.on('close', (code, reason) => {
    if (socket.heartbeatTimeout) {
      clearTimeout(socket.heartbeatTimeout);
      socket.heartbeatTimeout = null;
    }
    log('info', 'websocket closed', {
      connectionId: state.id,
      code,
      reason: reason.toString(),
    });
    metrics.set('mao_ws_connections', {}, wss.clients.size);
  });

  socket.on('error', (error) => {
    log('error', 'websocket error', {
      connectionId: state.id,
      error: error.message,
    });
  });
});

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.ping();
    }
  }
}, config.heartbeatIntervalMs);
heartbeat.unref();

const start = async () => {
  await waitForDatabase(pool, {
    attempts: config.dbConnectAttempts,
    delayMs: config.dbRetryDelayMs,
    onRetry: (attempt, attempts, error) => {
      log('warn', 'database not ready', {
        attempt,
        attempts,
        error: error.message,
      });
    },
  });

  await runMigrations(pool, {
    migrationsDir,
    onApplied: (version) => {
      log('info', 'migration applied', { version });
    },
  });

  await pool.query(
    `INSERT INTO engine_state (key, value, updated_at)
     VALUES ($1, $2::jsonb, now())
     ON CONFLICT (key) DO UPDATE
     SET value = EXCLUDED.value, updated_at = now()`,
    [
      'engine.last_boot',
      JSON.stringify({
        bootedAt: new Date().toISOString(),
        pid: process.pid,
        service: config.serviceName,
        protocolVersion: PROTOCOL_VERSION,
      }),
    ],
  );

  eventBridge = await createEventBridge({
    url: config.redisUrl,
    onEvent: broadcastEnvelope,
    onError: (source, error) => {
      log('error', 'redis bridge error', {
        source,
        error: error.message,
      });
    },
  });

  log('info', 'redis bridge ready', {
    channel: EVENT_CHANNEL,
    instanceId: config.instanceId,
  });

  taskStream = await createTaskStream({
    url: config.redisUrl,
    stream: config.taskStream,
    group: config.taskGroup,
    onError: (source, error) => {
      log('error', 'task stream error', {
        source,
        error: error.message,
      });
    },
    onMetric: () => {},
  });

  log('info', 'task stream ready', {
    stream: config.taskStream,
    group: config.taskGroup,
    instanceId: config.instanceId,
  });

  if (!auth.enabled) {
    log('warn', 'api key authentication is disabled', {
      hint: 'set MAO_API_KEY to require authentication',
    });
  }

  server.listen(config.port, '0.0.0.0', () => {
    log('info', 'core engine listening', {
      port: config.port,
      instanceId: config.instanceId,
      protocolVersion: PROTOCOL_VERSION,
    });
  });
};

const shutdown = async (signal) => {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  log('info', 'shutting down', { signal });

  clearInterval(heartbeat);
  for (const client of wss.clients) {
    client.close(1001, 'server_shutdown');
  }

  const forceExit = setTimeout(() => {
    log('error', 'graceful shutdown timed out');
    process.exit(1);
  }, 10000);
  forceExit.unref();

  await new Promise((resolve) => server.close(resolve));

  if (eventBridge) {
    await eventBridge.close().catch(() => {});
    eventBridge = null;
  }

  if (taskStream) {
    await taskStream.close().catch(() => {});
    taskStream = null;
  }

  await pool.end().catch(() => {});
  process.exit(0);
};

process.on('SIGTERM', () => {
  shutdown('SIGTERM').catch(() => process.exit(1));
});

process.on('SIGINT', () => {
  shutdown('SIGINT').catch(() => process.exit(1));
});

start().catch((error) => {
  log('error', 'startup failed', { error: error.message });
  process.exit(1);
});
