const { Pool } = require('pg');

const createPool = (connectionString) => {
  const pool = new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  pool.on('error', (error) => {
    process.stdout.write(
      `${JSON.stringify({
        timestamp: new Date().toISOString(),
        level: 'error',
        message: 'idle database client error',
        error: error.message,
      })}\n`,
    );
  });

  return pool;
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const waitForDatabase = async (
  pool,
  { attempts = 30, delayMs = 2000, onRetry = () => {} } = {},
) => {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await pool.query('SELECT 1');
      return;
    } catch (error) {
      lastError = error;
      if (attempt === attempts) {
        break;
      }
      onRetry(attempt, attempts, error);
      await delay(delayMs);
    }
  }
  throw lastError;
};

module.exports = { createPool, waitForDatabase };
