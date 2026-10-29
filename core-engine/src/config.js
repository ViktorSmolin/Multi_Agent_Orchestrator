const required = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
};

const asPositiveInt = (value, fallback) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
};

const heartbeatIntervalMs = asPositiveInt(process.env.HEARTBEAT_INTERVAL_MS, 30000);
const heartbeatTimeoutMs = Math.max(
  asPositiveInt(process.env.HEARTBEAT_TIMEOUT_MS, heartbeatIntervalMs * 2),
  heartbeatIntervalMs * 2,
);

module.exports = {
  serviceName: process.env.ENGINE_SERVICE_NAME || 'core-engine',
  instanceId: process.env.ENGINE_INSTANCE_ID || 'engine-1',
  port: asPositiveInt(process.env.ENGINE_PORT, 8090),
  databaseUrl: required('DATABASE_URL'),
  redisUrl: process.env.REDIS_URL || 'redis://redis:6379',
  apiKey: process.env.MAO_API_KEY || '',
  taskStream: process.env.TASK_STREAM || 'mao:tasks',
  taskGroup: process.env.TASK_GROUP || 'mao-agents',
  taskClaimIdleMs: asPositiveInt(process.env.TASK_CLAIM_IDLE_MS, 60000),
  taskClaimBlockMs: asPositiveInt(process.env.TASK_CLAIM_BLOCK_MS, 5000),
  dbRetryDelayMs: asPositiveInt(process.env.DB_RETRY_DELAY_MS, 2000),
  dbConnectAttempts: asPositiveInt(process.env.DB_CONNECT_ATTEMPTS, 30),
  heartbeatIntervalMs,
  heartbeatTimeoutMs,
  maxPayloadBytes: asPositiveInt(process.env.WS_MAX_PAYLOAD_BYTES, 65536),
  maxBufferedBytes: asPositiveInt(process.env.WS_MAX_BUFFERED_BYTES, 1048576),
};
