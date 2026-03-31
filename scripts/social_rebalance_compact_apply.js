import "dotenv/config";
import mysql from "mysql2/promise";
import { backupRoot, collectDistributions, qTable, writeManifest } from "./social_rebalance_common.js";
import fs from "fs/promises";
import path from "path";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const args = process.argv.slice(2);
const APPLY_BATCH_SIZE = Math.max(100, Math.min(1200, Number(process.env.SOCIAL_COMPACT_APPLY_BATCH || 300)));
const arg = (name, fallback = "") => {
  const hit = args.find((item) => item.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};
const COUNTS_ONLY = String(process.env.SOCIAL_COMPACT_COUNTS_ONLY || arg("counts-only", "0")) === "1";

const normalizeTag = (value) => String(value || "").trim().replace(/^compact_/, "");

async function loadManifest(tag) {
  const fullTag = `compact_${normalizeTag(tag)}`;
  const file = path.join(backupRoot(fullTag), "manifest.json");
  const raw = await fs.readFile(file, "utf8");
  return { fullTag, file, manifest: JSON.parse(raw) };
}

async function countRows(conn, tableName) {
  const [[row]] = await conn.query(`SELECT COUNT(*) AS c FROM ${qTable(tableName)}`);
  return Number(row?.c || 0);
}

async function fetchBatchIds(conn, targetTable, lastPostId) {
  const [rows] = await conn.query(
    `
      SELECT post_id
      FROM ${qTable(targetTable)}
      WHERE post_id > ?
      ORDER BY post_id ASC
      LIMIT ?
    `,
    [Number(lastPostId || 0), APPLY_BATCH_SIZE]
  );
  return rows.map((row) => Number(row.post_id)).filter((id) => Number.isFinite(id) && id > 0);
}

async function processTargetBatches(conn, targetTable, label, handler) {
  let lastPostId = 0;
  let processed = 0;
  let batchNo = 0;
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const ids = await fetchBatchIds(conn, targetTable, lastPostId);
    if (!ids.length) break;
    // eslint-disable-next-line no-await-in-loop
    await handler(ids);
    processed += ids.length;
    batchNo += 1;
    lastPostId = ids[ids.length - 1];
    if (batchNo % 20 === 0) {
      console.log(`[social-compact-apply] ${label}: processed ${processed} posts`);
    }
  }
}

async function applyCompactPatch(conn, targetTable, stageTables) {
  if (COUNTS_ONLY) {
    await processTargetBatches(conn, targetTable, "apply-counts", async (ids) => {
      await conn.query(
        `
          UPDATE posts p
          INNER JOIN ${qTable(targetTable)} t ON t.post_id = p.id
          SET
            p.like_count = t.target_like_count,
            p.favorite_count = t.target_favorite_count
          WHERE p.id IN (?)
        `,
        [ids]
      );
    });
    return;
  }

  await processTargetBatches(conn, targetTable, "apply", async (ids) => {
    await conn.query(`DELETE FROM post_likes WHERE post_id IN (?)`, [ids]);
    await conn.query(
      `
        INSERT INTO post_likes (post_id, user_id, post_owner_id, created_at)
        SELECT post_id, user_id, post_owner_id, created_at
        FROM ${qTable(stageTables.likes)}
        WHERE post_id IN (?)
      `,
      [ids]
    );

    await conn.query(`DELETE FROM post_favorites WHERE post_id IN (?)`, [ids]);
    await conn.query(
      `
        INSERT INTO post_favorites (post_id, user_id, post_owner_id, created_at)
        SELECT post_id, user_id, post_owner_id, created_at
        FROM ${qTable(stageTables.favorites)}
        WHERE post_id IN (?)
      `,
      [ids]
    );

    await conn.query(
      `
        UPDATE posts p
        INNER JOIN ${qTable(targetTable)} t ON t.post_id = p.id
        SET
          p.like_count = t.target_like_count,
          p.favorite_count = t.target_favorite_count
        WHERE p.id IN (?)
      `,
      [ids]
    );
  });

  if (stageTables.follows) {
    await conn.query(`DELETE FROM user_follows`);
    await conn.query(`
      INSERT INTO user_follows (follower_id, following_id, created_at, status)
      SELECT follower_id, following_id, created_at, status
      FROM ${qTable(stageTables.follows)}
    `);
  }

}

async function restoreCompactPatch(conn, targetTable, backupTables) {
  if (COUNTS_ONLY) {
    await processTargetBatches(conn, targetTable, "restore-counts", async (ids) => {
      await conn.query(
        `
          UPDATE posts p
          INNER JOIN ${qTable(targetTable)} t ON t.post_id = p.id
          SET
            p.like_count = t.old_like_count,
            p.favorite_count = t.old_favorite_count
          WHERE p.id IN (?)
        `,
        [ids]
      );
    });
    return;
  }

  await processTargetBatches(conn, targetTable, "restore", async (ids) => {
    await conn.query(`DELETE FROM post_likes WHERE post_id IN (?)`, [ids]);
    await conn.query(
      `
        INSERT INTO post_likes (post_id, user_id, post_owner_id, created_at)
        SELECT post_id, user_id, post_owner_id, created_at
        FROM ${qTable(backupTables.likes)}
        WHERE post_id IN (?)
      `,
      [ids]
    );

    await conn.query(`DELETE FROM post_favorites WHERE post_id IN (?)`, [ids]);
    await conn.query(
      `
        INSERT INTO post_favorites (post_id, user_id, post_owner_id, created_at)
        SELECT post_id, user_id, post_owner_id, created_at
        FROM ${qTable(backupTables.favorites)}
        WHERE post_id IN (?)
      `,
      [ids]
    );

    await conn.query(
      `
        UPDATE posts p
        INNER JOIN ${qTable(targetTable)} t ON t.post_id = p.id
        SET
          p.like_count = t.old_like_count,
          p.favorite_count = t.old_favorite_count
        WHERE p.id IN (?)
      `,
      [ids]
    );
  });

  if (backupTables.follows) {
    await conn.query(`DELETE FROM user_follows`);
    await conn.query(`
      INSERT INTO user_follows (follower_id, following_id, created_at, status)
      SELECT follower_id, following_id, created_at, status
      FROM ${qTable(backupTables.follows)}
    `);
  }

}

async function main() {
  const tagArg = arg("tag", "");
  if (!tagArg) {
    throw new Error("missing --tag=compact_YYYYMMDD_HHMMSS");
  }

  const { fullTag, manifest } = await loadManifest(tagArg);
  if (!manifest?.target_table || !manifest?.stage_tables?.likes || !manifest?.stage_tables?.favorites) {
    throw new Error(`manifest ${fullTag} is missing compact stage tables`);
  }

  const dir = backupRoot(fullTag);
  const conn = await mysql.createConnection(DB);

  try {
    await applyCompactPatch(conn, manifest.target_table, manifest.stage_tables);
    const after = await collectDistributions(conn);
    const liveCounts = {
      likes: await countRows(conn, "post_likes"),
      favorites: await countRows(conn, "post_favorites"),
      follows: manifest.stage_tables?.follows ? await countRows(conn, "user_follows") : null,
      counts_only: COUNTS_ONLY,
    };
    await writeManifest(dir, {
      ...manifest,
      phase: COUNTS_ONLY ? "counts_only_completed" : "completed",
      completed_at: new Date().toISOString(),
      after,
      live_counts: liveCounts,
    });
    console.log(JSON.stringify({ success: true, tag: fullTag, live_counts: liveCounts, after }, null, 2));
  } catch (err) {
    console.error("[social-compact-apply] failed", err);
    if (manifest?.target_table && manifest?.backup_tables?.likes && manifest?.backup_tables?.favorites) {
      try {
        await restoreCompactPatch(conn, manifest.target_table, manifest.backup_tables);
      } catch (restoreErr) {
        console.error("[social-compact-apply] restore failed", restoreErr);
      }
    }
    await writeManifest(dir, {
      ...manifest,
      phase: "apply_failed",
      failed_at: new Date().toISOString(),
      failure: String(err?.message || err),
    });
    process.exit(1);
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error("[social-compact-apply] fatal", err);
  process.exit(1);
});
