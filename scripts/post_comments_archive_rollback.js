import { pool } from "../db/connect.js";

const args = process.argv.slice(2);

const getNumberArg = (name) => {
  const hit = args.find((arg) => arg.startsWith(`${name}=`));
  if (!hit) return null;
  const value = Number(hit.split("=")[1]);
  return Number.isFinite(value) && value > 0 ? value : null;
};

const batchSize = Math.max(1000, Math.min(getNumberArg("--batch-size") || 50000, 200000));
const rollbackLast = args.includes("--last");
const inputBatchId = getNumberArg("--batch-id");

async function resolveBatchId() {
  if (inputBatchId) return inputBatchId;
  if (!rollbackLast) return null;
  const [[row]] = await pool.query(
    `
      SELECT id
      FROM post_comments_archive_batches
      WHERE status IN ('DONE', 'DRY_RUN')
      ORDER BY id DESC
      LIMIT 1
    `
  );
  return Number(row?.id || 0) || null;
}

async function rollbackOneChunk(batchId) {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    await conn.query(`CREATE TEMPORARY TABLE tmp_restore_ids (id BIGINT PRIMARY KEY) ENGINE=MEMORY`);
    await conn.query(
      `
        INSERT INTO tmp_restore_ids (id)
        SELECT id
        FROM post_comments_archive
        WHERE archive_batch_id = ?
        ORDER BY id DESC
        LIMIT ?
      `,
      [batchId, batchSize]
    );
    const [[pickedRow]] = await conn.query(`SELECT COUNT(*) AS c FROM tmp_restore_ids`);
    const picked = Number(pickedRow?.c || 0);
    if (picked <= 0) {
      await conn.rollback();
      await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_restore_ids`);
      return { picked: 0, restored: 0, removedFromArchive: 0 };
    }

    const [insertRes] = await conn.query(
      `
        INSERT IGNORE INTO post_comments (
          id, post_id, user_id, post_owner_id, parent_comment_id, type, content,
          like_count, reply_count, status, created_at, updated_at
        )
        SELECT
          a.id, a.post_id, a.user_id, a.post_owner_id, a.parent_comment_id, a.type, a.content,
          a.like_count, a.reply_count, a.status, a.created_at, a.updated_at
        FROM post_comments_archive a
        JOIN tmp_restore_ids t ON t.id = a.id
      `
    );
    const restored = Number(insertRes?.affectedRows || 0);
    const [deleteRes] = await conn.query(
      `DELETE a FROM post_comments_archive a JOIN tmp_restore_ids t ON t.id = a.id`
    );
    const removedFromArchive = Number(deleteRes?.affectedRows || 0);

    await conn.commit();
    await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_restore_ids`);
    return { picked, restored, removedFromArchive };
  } catch (err) {
    try {
      await conn.rollback();
    } catch {
      // ignore rollback errors
    }
    throw err;
  } finally {
    conn.release();
  }
}

async function run() {
  const batchId = await resolveBatchId();
  if (!batchId) {
    throw new Error("batch id is required. Use --batch-id=123 or --last");
  }

  let loops = 0;
  let pickedTotal = 0;
  let restoredTotal = 0;
  let removedTotal = 0;

  while (true) {
    const result = await rollbackOneChunk(batchId);
    if (result.picked <= 0) break;
    loops += 1;
    pickedTotal += result.picked;
    restoredTotal += result.restored;
    removedTotal += result.removedFromArchive;
  }

  await pool.query(
    `
      UPDATE post_comments_archive_batches
      SET
        finished_at = NOW(),
        status = 'ROLLED_BACK',
        note = CONCAT('rollback loops=', ?, ', restored=', ?)
      WHERE id = ?
    `,
    [loops, restoredTotal, batchId]
  );

  console.log(
    JSON.stringify(
      {
        batchId,
        loops,
        pickedTotal,
        restoredTotal,
        removedTotal,
      },
      null,
      2
    )
  );
}

run()
  .catch((err) => {
    console.error("post_comments archive rollback failed:", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
