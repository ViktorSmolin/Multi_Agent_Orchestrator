const http = require('node:http');

const socketPath = process.env.DOCKER_SOCKET || '/var/run/docker.sock';
const intervalMs = positiveInt(process.env.AUTOHEAL_INTERVAL_MS, 15000, 1000);
const cooldownMs = positiveInt(process.env.AUTOHEAL_COOLDOWN_MS, 60000, 1000);
const healthPort = positiveInt(process.env.AUTOHEAL_HEALTH_PORT, 8092, 1);
const label = process.env.AUTOHEAL_LABEL || 'mao.autoheal=true';
const lastRestart = new Map();
let lastPollAt = 0;
let lastPollOk = false;

function positiveInt(value, fallback, minimum) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= minimum ? parsed : fallback;
}

function log(level, message, context = {}) {
  process.stdout.write(
    `${JSON.stringify({
      timestamp: new Date().toISOString(),
      service: 'mao-autoheal',
      level,
      message,
      ...context,
    })}\n`,
  );
}

function dockerRequest(method, requestPath) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath,
        path: requestPath,
        method,
        headers: { Host: 'docker' },
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf8');
          if (res.statusCode < 200 || res.statusCode >= 300) {
            reject(
              new Error(
                `docker ${method} ${requestPath} -> ${res.statusCode} ${body.slice(0, 200)}`,
              ),
            );
            return;
          }
          try {
            resolve(body ? JSON.parse(body) : null);
          } catch (error) {
            reject(error);
          }
        });
      },
    );
    req.setTimeout(10000, () => {
      req.destroy(new Error('docker socket timeout'));
    });
    req.on('error', reject);
    req.end();
  });
}

async function poll() {
  const filters = encodeURIComponent(JSON.stringify({ label: [label] }));
  const containers = await dockerRequest(
    'GET',
    `/containers/json?all=1&filters=${filters}`,
  );
  lastPollAt = Date.now();
  lastPollOk = true;

  for (const container of containers) {
    const info = await dockerRequest(
      'GET',
      `/containers/${encodeURIComponent(container.Id)}/json`,
    );
    const status = info.State && info.State.Status;
    const health = info.State && info.State.Health && info.State.Health.Status;
    if (status !== 'running' || health !== 'unhealthy') {
      continue;
    }

    const last = lastRestart.get(container.Id) || 0;
    if (Date.now() - last < cooldownMs) {
      log('info', 'restart skipped by cooldown', {
        container: container.Names,
        health,
      });
      continue;
    }

    lastRestart.set(container.Id, Date.now());
    log('warn', 'restarting unhealthy container', {
      container: container.Names,
      health,
    });
    await dockerRequest(
      'POST',
      `/containers/${encodeURIComponent(container.Id)}/restart?t=10`,
    );
  }
}

async function loop() {
  try {
    await poll();
  } catch (error) {
    lastPollOk = false;
    log('error', 'docker poll failed', { error: error.message });
  }
}

const healthServer = http.createServer((req, res) => {
  if (req.url !== '/health') {
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'not_found' }));
    return;
  }

  const pollAge = lastPollAt === 0 ? null : Date.now() - lastPollAt;
  const healthy =
    lastPollOk && pollAge !== null && pollAge <= intervalMs * 3;
  const body = JSON.stringify({
    status: healthy ? 'ok' : 'unhealthy',
    docker_socket: socketPath,
    label,
    last_poll_age_ms: pollAge,
    restart_cooldown_ms: cooldownMs,
  });
  res.writeHead(healthy ? 200 : 503, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
});

healthServer.listen(healthPort, '0.0.0.0', () => {
  log('info', 'autoheal listening', { port: healthPort, label });
});

loop();
const timer = setInterval(loop, intervalMs);

function shutdown(signal) {
  log('info', 'shutting down', { signal });
  clearInterval(timer);
  healthServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
