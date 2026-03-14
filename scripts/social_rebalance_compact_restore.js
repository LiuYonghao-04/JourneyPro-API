import "dotenv/config";
import mysql from "mysql2/promise";
import { backupRoot, qTable, writeManifest } from "./social_rebalance_common.js";
import fs from "fs/promises";
import path from "path";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

function parseArgs(argv) {
  const out = {};
  argv.forEach((arg) => {
    const hit = String(arg || "").match(/^--([^=]+)=(.+)$/);
    if (hit) out[hit[1]] = hit[2];
  });
  return out;
}

async function readManifest(tag) {
  const file = path.join(backupRoot(tag), "manifest.json");
  const raw = await fs.readFile(file, "utf8");
  return JSON.parse(raw);
}

async function restoreCompactPatch(conn, targetTable, backupTables) {
  await conn.query(`
    DELETE l
    FROM post_likes l
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = l.post_id
  `);
  await conn.query(`
    INSERT INTO post_likes (post_id, user_id, post_owner_id, created_at)
    SELECT post_id, user_id, post_owner_id, created_at
    FROM ${qTable(backupTables.likes)}
  `);

  await conn.query(`
    DELETE f
    FROM post_favorites f
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = f.post_id
  `);
  await conn.query(`
    INSERT INTO post_favorites (post_id, user_id, post_owner_id, created_at)
    SELECT post_id, user_id, post_owner_id, created_at
    FROM ${qTable(backupTables.favorites)}
  `);

  await conn.query(`DELETE FROM user_follows`);
  await conn.query(`
    INSERT INTO user_follows (follower_id, following_id, created_at, status)
    SELECT follower_id, following_id, created_at, status
    FROM ${qTable(backupTables.follows)}
  `);

  await conn.query(`
    UPDATE posts p
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = p.id
    SET
      p.like_count = t.old_like_count,
      p.favorite_count = t.old_favorite_count
  `);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const tag = String(args.tag || "").trim();
  if (!tag) {
    console.error("usage: node scripts/social_rebalance_compact_restore.js --tag=compact_YYYYMMDD_HHMMSS");
    process.exit(1);
  }

  const manifest = await readManifest(tag);
  const conn = await mysql.createConnection(DB);
  try {
    await restoreCompactPatch(conn, manifest.target_table, manifest.backup_tables);
    await writeManifest(backupRoot(tag), {
      ...manifest,
      phase: "restored",
      restored_at: new Date().toISOString(),
    });
    console.log(JSON.stringify({ success: true, restored_tag: tag }, null, 2));
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error("[social-compact-restore] fatal", err);
  process.exit(1);
});
