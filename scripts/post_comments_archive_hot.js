import { pool } from "../db/connect.js";

const args = process.argv.slice(2);

const getNumberArg = (name, fallback) => {
  const hit = args.find((arg) => arg.startsWith(`${name}=`));
  if (!hit) return fallback;
  const value = Number(hit.split("=")[1]);
  return Number.isFinite(value) && value >= 0 ? value : fallback;
};

const retainHotRows = Math.max(100000, getNumberArg("--retain-hot-rows", 10000000));
const batchSize = Math.max(1000, Math.min(getNumberArg("--batch-size", 50000), 200000));
const maxBatches = Math.max(1, Math.min(getNumberArg("--max-batches", 40), 500));
const olderThanDays = Math.max(0, getNumberArg("--older-than-days", 0));
const dryRun = args.includes("--dry-run");

async function ensureArchiveTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_comments_archive (
      id BIGINT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      parent_comment_id BIGINT NULL,
      type VARCHAR(20) DEFAULT 'COMMENT',
      content TEXT NOT NULL,
      like_count INT DEFAULT 0,
      reply_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'NORMAL',
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL,
      archive_batch_id BIGINT NULL,
      archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_post_comments_archive_post_created (post_id, created_at, id),
      INDEX idx_post_comments_archive_parent (parent_comment_id),
      INDEX idx_post_comments_archive_owner_created (post_owner_id, created_at, post_id, user_id),
      INDEX idx_post_comments_archive_batch (archive_batch_id, id)
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_comments_archive_batches (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      finished_at DATETIME NULL,
      retain_hot_rows BIGINT NOT NULL,
      cutoff_id BIGINT NOT NULL,
      batch_size INT NOT NULL,
      max_batches INT NOT NULL,
      older_than_days INT NOT NULL DEFAULT 0,
      scanned_rows BIGINT NOT NULL DEFAULT 0,
      moved_rows BIGINT NOT NULL DEFAULT 0,
      deleted_rows BIGINT NOT NULL DEFAULT 0,
      dry_run TINYINT(1) NOT NULL DEFAULT 0,
      status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
      note VARCHAR(255) NULL
    )
  `);
}

function buildFilter(cutoffId) {
  let where = "id <= ?";
  const params = [cutoffId];
  if (olderThanDays > 0) {
    where += " AND created_at < DATE_SUB(NOW(), INTERVAL ? DAY)";
    params.push(olderThanDays);
  }
  return { where, params };
}

async function runOneBatch(batchId, cutoffId) {
  const conn = await pool.getConnection();
  try {
    const { where, params } = buildFilter(cutoffId);
    await conn.beginTransaction();
    await conn.query(`CREATE TEMPORARY TABLE tmp_archive_ids (id BIGINT PRIMARY KEY) ENGINE=MEMORY`);
    await conn.query(
      `INSERT INTO tmp_archive_ids (id)
       SELECT pc.id
       FROM post_comments pc
       WHERE ${where}
         AND NOT EXISTS (
           SELECT 1
           FROM post_comments ch
           WHERE ch.parent_comment_id = pc.id
         )
       ORDER BY pc.id ASC
       LIMIT ?`,
      [...params, batchSize]
    );
    const [[pickedRow]] = await conn.query(`SELECT COUNT(*) AS c FROM tmp_archive_ids`);
    const picked = Number(pickedRow?.c || 0);
    if (picked <= 0) {
      await conn.rollback();
      await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_archive_ids`);
      return { picked: 0, copied: 0, deleted: 0 };
    }

    if (dryRun) {
      await conn.rollback();
      await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_archive_ids`);
      return { picked, copied: picked, deleted: 0 };
    }

    await conn.query(
      `
        INSERT IGNORE INTO post_comments_archive (
          id, post_id, user_id, post_owner_id, parent_comment_id, type, content,
          like_count, reply_count, status, created_at, updated_at, archive_batch_id, archived_at
        )
        SELECT
          pc.id, pc.post_id, pc.user_id, pc.post_owner_id, pc.parent_comment_id, pc.type, pc.content,
          pc.like_count, pc.reply_count, pc.status, pc.created_at, pc.updated_at, ?, NOW()
        FROM post_comments pc
        JOIN tmp_archive_ids t ON t.id = pc.id
      `,
      [batchId]
    );
    const [[copiedRow]] = await conn.query(
      `SELECT COUNT(*) AS c FROM post_comments_archive a JOIN tmp_archive_ids t ON t.id = a.id`
    );
    const copied = Number(copiedRow?.c || 0);
    if (copied < picked) {
      throw new Error(`copy mismatch: picked=${picked}, copied=${copied}`);
    }
    const [deleteRes] = await conn.query(
      `DELETE pc FROM post_comments pc JOIN tmp_archive_ids t ON t.id = pc.id`
    );
    const deleted = Number(deleteRes?.affectedRows || 0);
    await conn.commit();
    await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_archive_ids`);
    return { picked, copied, deleted };
  } catch (err) {
    try {
      await conn.rollback();
    } catch {
      // ignore rollback error
    }
    throw err;
  } finally {
    conn.release();
  }
}

async function run() {
  await ensureArchiveTables();
  const [[stats]] = await pool.query(`
    SELECT COUNT(*) AS total_rows, COALESCE(MAX(id), 0) AS max_id, COALESCE(MIN(id), 0) AS min_id
    FROM post_comments
  `);
  const totalRows = Number(stats?.total_rows || 0);
  const maxId = Number(stats?.max_id || 0);
  const minId = Number(stats?.min_id || 0);

  if (totalRows <= retainHotRows || maxId <= 0) {
    console.log(
      JSON.stringify(
        {
          message: "No archiving needed",
          totalRows,
          retainHotRows,
          maxId,
          minId,
          dryRun,
        },
        null,
        2
      )
    );
    return;
  }

  const cutoffId = Math.max(0, maxId - retainHotRows);
  if (cutoffId < minId) {
    console.log(
      JSON.stringify(
        {
          message: "Cutoff below min id; skip",
          totalRows,
          retainHotRows,
          cutoffId,
          minId,
          dryRun,
        },
        null,
        2
      )
    );
    return;
  }

  const [runInsert] = await pool.query(
    `
      INSERT INTO post_comments_archive_batches (
        retain_hot_rows, cutoff_id, batch_size, max_batches, older_than_days, dry_run, status, note
      )
      VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?)
    `,
    [
      retainHotRows,
      cutoffId,
      batchSize,
      maxBatches,
      olderThanDays,
      dryRun ? 1 : 0,
      dryRun ? "dry-run only" : "live archive",
    ]
  );
  const batchId = Number(runInsert?.insertId || 0);

  let scannedRows = 0;
  let movedRows = 0;
  let deletedRows = 0;
  let loops = 0;

  try {
    for (; loops < maxBatches; loops += 1) {
      const result = await runOneBatch(batchId, cutoffId);
      if (result.picked <= 0) break;
      scannedRows += result.picked;
      movedRows += result.copied;
      deletedRows += result.deleted;
      if (dryRun) continue;
    }

    await pool.query(
      `
        UPDATE post_comments_archive_batches
        SET
          finished_at = NOW(),
          scanned_rows = ?,
          moved_rows = ?,
          deleted_rows = ?,
          status = ?,
          note = ?
        WHERE id = ?
      `,
      [
        scannedRows,
        movedRows,
        deletedRows,
        dryRun ? "DRY_RUN" : "DONE",
        `${dryRun ? "dry-run" : "completed"} loops=${loops}`,
        batchId,
      ]
    );

    console.log(
      JSON.stringify(
        {
          batchId,
          dryRun,
          retainHotRows,
          cutoffId,
          loops,
          scannedRows,
          movedRows,
          deletedRows,
        },
        null,
        2
      )
    );
  } catch (err) {
    await pool.query(
      `
        UPDATE post_comments_archive_batches
        SET
          finished_at = NOW(),
          scanned_rows = ?,
          moved_rows = ?,
          deleted_rows = ?,
          status = 'FAILED',
          note = ?
        WHERE id = ?
      `,
      [scannedRows, movedRows, deletedRows, String(err?.message || err).slice(0, 250), batchId]
    );
    throw err;
  }
}

run()
  .catch((err) => {
    console.error("post_comments archive hot failed:", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
