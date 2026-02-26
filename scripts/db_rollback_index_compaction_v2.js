import { pool } from "../db/connect.js";

const DEFAULT_MIGRATION_KEY = "20260226_index_compaction_v2";
const args = process.argv.slice(2);
const migrationKeyArg = args.find((arg) => arg.startsWith("--migration-key="));
const migrationKey = migrationKeyArg ? migrationKeyArg.split("=")[1] : DEFAULT_MIGRATION_KEY;

async function indexExists(table, indexName) {
  const [rows] = await pool.query(
    `
      SELECT 1
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = ?
        AND index_name = ?
      LIMIT 1
    `,
    [table, indexName]
  );
  return rows.length > 0;
}

async function run() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_index_compaction_backup (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      migration_key VARCHAR(96) NOT NULL,
      table_name VARCHAR(64) NOT NULL,
      index_name VARCHAR(128) NOT NULL,
      restore_sql TEXT NOT NULL,
      dropped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      restored_at DATETIME NULL,
      UNIQUE KEY uk_migration_table_index (migration_key, table_name, index_name)
    )
  `);

  const [rows] = await pool.query(
    `
      SELECT id, table_name, index_name, restore_sql
      FROM schema_index_compaction_backup
      WHERE migration_key = ?
      ORDER BY id DESC
    `,
    [migrationKey]
  );

  const summary = {
    migrationKey,
    restored: [],
    skipped: [],
    failed: [],
  };

  for (const row of rows) {
    const exists = await indexExists(row.table_name, row.index_name);
    if (exists) {
      summary.skipped.push({ table: row.table_name, index: row.index_name, reason: "already exists" });
      await pool.query(`UPDATE schema_index_compaction_backup SET restored_at = COALESCE(restored_at, NOW()) WHERE id = ?`, [
        row.id,
      ]);
      continue;
    }
    try {
      await pool.query(row.restore_sql);
      await pool.query(`UPDATE schema_index_compaction_backup SET restored_at = NOW() WHERE id = ?`, [row.id]);
      summary.restored.push({ table: row.table_name, index: row.index_name });
    } catch (err) {
      summary.failed.push({
        table: row.table_name,
        index: row.index_name,
        message: String(err?.message || err),
      });
    }
  }

  console.log(JSON.stringify(summary, null, 2));
  if (summary.failed.length > 0) process.exitCode = 1;
}

run()
  .catch((err) => {
    console.error("index compaction rollback failed:", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
