import { pool } from "../db/connect.js";

const DEFAULT_MIGRATION_KEY = "20260226_index_compaction_v2";

const targets = [
  {
    table: "post_comments",
    dropIndex: "idx_comments_post",
    requireAny: ["idx_post_comments_post_created"],
    reason: "covered by (post_id, created_at, user_id)",
  },
  {
    table: "post_favorites",
    dropIndex: "idx_post_fav_created",
    requireAny: ["idx_post_favorites_created"],
    reason: "duplicate index columns",
  },
  {
    table: "post_favorites",
    dropIndex: "idx_post_fav_owner_created",
    requireAny: ["idx_post_favorites_owner_created"],
    reason: "duplicate index columns",
  },
  {
    table: "user_follows",
    dropIndex: "idx_follows_follower",
    requireAny: ["idx_user_follows_follower_created", "idx_follows_follower_created"],
    reason: "covered by (follower_id, created_at, following_id)",
  },
  {
    table: "user_follows",
    dropIndex: "idx_follows_following",
    requireAny: ["idx_user_follows_following_created", "idx_follows_following_created"],
    reason: "covered by (following_id, created_at, follower_id)",
  },
  {
    table: "user_follows",
    dropIndex: "idx_follows_following_created",
    requireAny: ["idx_user_follows_following_created"],
    reason: "duplicate index columns",
  },
];

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const migrationKeyArg = args.find((arg) => arg.startsWith("--migration-key="));
const migrationKey = migrationKeyArg ? migrationKeyArg.split("=")[1] : DEFAULT_MIGRATION_KEY;

const quoteId = (value) => `\`${String(value || "").replace(/`/g, "``")}\``;

async function ensureMetaTables() {
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
}

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

async function buildRestoreSql(table, indexName) {
  const [rows] = await pool.query(
    `
      SELECT
        NON_UNIQUE,
        GROUP_CONCAT(
          CASE
            WHEN SUB_PART IS NOT NULL THEN CONCAT(CHAR(96), COLUMN_NAME, CHAR(96), '(', SUB_PART, ')')
            ELSE CONCAT(CHAR(96), COLUMN_NAME, CHAR(96))
          END
          ORDER BY SEQ_IN_INDEX
          SEPARATOR ', '
        ) AS cols
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = ?
        AND index_name = ?
      GROUP BY NON_UNIQUE
      LIMIT 1
    `,
    [table, indexName]
  );
  if (!rows.length || !rows[0].cols) return null;
  const uniquePart = Number(rows[0].NON_UNIQUE) === 0 ? "UNIQUE " : "";
  return `CREATE ${uniquePart}INDEX ${quoteId(indexName)} ON ${quoteId(table)} (${rows[0].cols})`;
}

async function run() {
  await ensureMetaTables();
  const summary = {
    migrationKey,
    dryRun,
    dropped: [],
    skipped: [],
    failed: [],
  };

  for (const target of targets) {
    const label = `${target.table}.${target.dropIndex}`;
    const dropExists = await indexExists(target.table, target.dropIndex);
    if (!dropExists) {
      summary.skipped.push({ label, reason: "index not found" });
      continue;
    }

    const requiredCandidates = Array.isArray(target.requireAny) ? target.requireAny : [];
    const existingRequired = [];
    for (const name of requiredCandidates) {
      if (await indexExists(target.table, name)) existingRequired.push(name);
    }
    const requiredExists = existingRequired.length > 0;
    if (!requiredExists) {
      summary.failed.push({ label, reason: `required index missing: ${requiredCandidates.join(" | ")}` });
      continue;
    }

    const restoreSql = await buildRestoreSql(target.table, target.dropIndex);
    if (!restoreSql) {
      summary.failed.push({ label, reason: "failed to build restore sql" });
      continue;
    }

    await pool.query(
      `
        INSERT INTO schema_index_compaction_backup (migration_key, table_name, index_name, restore_sql)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          restore_sql = VALUES(restore_sql),
          dropped_at = CURRENT_TIMESTAMP
      `,
      [migrationKey, target.table, target.dropIndex, restoreSql]
    );

    if (dryRun) {
      summary.dropped.push({ label, reason: target.reason, dryRun: true });
      continue;
    }

    await pool.query(`ALTER TABLE ${quoteId(target.table)} DROP INDEX ${quoteId(target.dropIndex)}`);
    summary.dropped.push({ label, reason: target.reason, dryRun: false });
  }

  console.log(JSON.stringify(summary, null, 2));
  if (summary.failed.length > 0) process.exitCode = 1;
}

run()
  .catch((err) => {
    console.error("index compaction migration failed:", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
