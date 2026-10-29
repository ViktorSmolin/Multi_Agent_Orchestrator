const fs = require('node:fs');
const path = require('node:path');

const MIGRATION_LOCK_ID = 742001;
const MIGRATION_FILE_PATTERN = /^\d+_[A-Za-z0-9_]+\.sql$/;

const runMigrations = async (pool, { migrationsDir, onApplied = () => {} }) => {
  const files = fs
    .readdirSync(migrationsDir)
    .filter((fileName) => MIGRATION_FILE_PATTERN.test(fileName))
    .sort();

  const client = await pool.connect();

  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    const appliedResult = await client.query('SELECT version FROM schema_migrations');
    const applied = new Set(appliedResult.rows.map((row) => row.version));

    for (const fileName of files) {
      const version = fileName.replace(/\.sql$/, '');
      if (applied.has(version)) {
        continue;
      }

      const sql = fs.readFileSync(path.join(migrationsDir, fileName), 'utf8');

      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [version]);
        await client.query('COMMIT');
        onApplied(version);
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(`Migration ${version} failed: ${error.message}`);
      }
    }
  } finally {
    await client
      .query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID])
      .catch(() => {});
    client.release();
  }
};

module.exports = { runMigrations };
