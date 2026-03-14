import "dotenv/config";
import fs from "fs/promises";
import path from "path";
import mysql from "mysql2/promise";

export const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

export const REBALANCE_TABLES = ["post_likes", "post_favorites", "user_follows"];

export function resolveRebalanceTables(overrides = {}) {
  return {
    post_likes: overrides.post_likes || "post_likes",
    post_favorites: overrides.post_favorites || "post_favorites",
    user_follows: overrides.user_follows || "user_follows",
  };
}

export function createTag() {
  const now = new Date();
  return `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}_${String(
    now.getHours()
  ).padStart(2, "0")}${String(now.getMinutes()).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
}

export function backupRoot(tag) {
  return path.resolve(process.cwd(), `backups/social_rebalance/${tag}`);
}

export async function connectDb() {
  return mysql.createConnection(DB);
}

export async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

export function qTable(name) {
  return `\`${String(name || "").replace(/`/g, "``")}\``;
}

export async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `
      SELECT 1 AS hit
      FROM information_schema.tables
      WHERE table_schema = DATABASE()
        AND table_name = ?
      LIMIT 1
    `,
    [tableName]
  );
  return !!rows.length;
}

export async function createBackupTables(conn, tag) {
  const map = {};
  for (const table of REBALANCE_TABLES) {
    const backupName = `${table}_rebak_${tag}`;
    const exists = await tableExists(conn, backupName);
    if (exists) {
      throw new Error(`backup table already exists: ${backupName}`);
    }
    // eslint-disable-next-line no-await-in-loop
    await conn.query(`CREATE TABLE ${qTable(backupName)} LIKE ${qTable(table)}`);
    // eslint-disable-next-line no-await-in-loop
    await conn.query(`INSERT INTO ${qTable(backupName)} SELECT * FROM ${qTable(table)}`);
    map[table] = backupName;
  }
  return map;
}

export async function restoreBackupTables(conn, backupTables) {
  await conn.query(`SET FOREIGN_KEY_CHECKS = 0`);
  try {
    for (const table of REBALANCE_TABLES) {
      const backupName = backupTables?.[table];
      if (!backupName) continue;
      // eslint-disable-next-line no-await-in-loop
      await conn.query(`DELETE FROM ${qTable(table)}`);
      // eslint-disable-next-line no-await-in-loop
      await conn.query(`INSERT INTO ${qTable(table)} SELECT * FROM ${qTable(backupName)}`);
    }
    await syncAllPostCounters(conn);
  } finally {
    await conn.query(`SET FOREIGN_KEY_CHECKS = 1`);
  }
}

export async function swapRestoreFromBackup(conn, backupTables, suffixTag = createTag()) {
  const failedTables = {};
  const renameParts = [];
  for (const table of REBALANCE_TABLES) {
    const backupName = backupTables?.[table];
    if (!backupName) continue;
    const failedName = `${table}_failed_${suffixTag}`;
    failedTables[table] = failedName;
    renameParts.push(`${qTable(table)} TO ${qTable(failedName)}`);
    renameParts.push(`${qTable(backupName)} TO ${qTable(table)}`);
  }
  if (!renameParts.length) {
    throw new Error("no backup tables available for swap restore");
  }
  await conn.query(`RENAME TABLE ${renameParts.join(", ")}`);
  return failedTables;
}

export async function createFastLiveBackup(conn, suffixTag = createTag()) {
  const backupTables = {};
  const renameParts = [];
  for (const table of REBALANCE_TABLES) {
    const backupName = `${table}_livebak_${suffixTag}`;
    backupTables[table] = backupName;
    renameParts.push(`${qTable(table)} TO ${qTable(backupName)}`);
  }
  await conn.query(`RENAME TABLE ${renameParts.join(", ")}`);
  for (const table of REBALANCE_TABLES) {
    const backupName = backupTables[table];
    // eslint-disable-next-line no-await-in-loop
    await conn.query(`CREATE TABLE ${qTable(table)} LIKE ${qTable(backupName)}`);
  }
  return backupTables;
}

export async function createShadowTables(conn, suffixTag = createTag()) {
  const shadowTables = {};
  for (const table of REBALANCE_TABLES) {
    const shadowName = `${table}_shadow_${suffixTag}`;
    if (await tableExists(conn, shadowName)) {
      throw new Error(`shadow table already exists: ${shadowName}`);
    }
    // eslint-disable-next-line no-await-in-loop
    await conn.query(`CREATE TABLE ${qTable(shadowName)} LIKE ${qTable(table)}`);
    shadowTables[table] = shadowName;
  }
  return shadowTables;
}

export async function restoreFromLiveBackup(conn, liveBackupTables, suffixTag = createTag()) {
  const failedTables = {};
  const renameParts = [];
  for (const table of REBALANCE_TABLES) {
    const backupName = liveBackupTables?.[table];
    if (!backupName) continue;
    const failedName = `${table}_seed_failed_${suffixTag}`;
    failedTables[table] = failedName;
    renameParts.push(`${qTable(table)} TO ${qTable(failedName)}`);
    renameParts.push(`${qTable(backupName)} TO ${qTable(table)}`);
  }
  if (!renameParts.length) {
    throw new Error("no live backup tables available for restore");
  }
  await conn.query(`RENAME TABLE ${renameParts.join(", ")}`);
  return failedTables;
}

export async function swapInShadowTables(conn, shadowTables, suffixTag = createTag()) {
  const liveBackupTables = {};
  const renameParts = [];
  for (const table of REBALANCE_TABLES) {
    const shadowName = shadowTables?.[table];
    if (!shadowName) continue;
    const backupName = `${table}_preswap_${suffixTag}`;
    liveBackupTables[table] = backupName;
    renameParts.push(`${qTable(table)} TO ${qTable(backupName)}`);
    renameParts.push(`${qTable(shadowName)} TO ${qTable(table)}`);
  }
  if (!renameParts.length) {
    throw new Error("no shadow tables available for swap");
  }
  await conn.query(`RENAME TABLE ${renameParts.join(", ")}`);
  return liveBackupTables;
}

export async function syncAllPostCounters(conn, tableOverrides = {}) {
  const tables = resolveRebalanceTables(tableOverrides);
  await conn.query(
    `
      UPDATE posts p
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(tables.post_likes)}
        GROUP BY post_id
      ) l ON l.post_id = p.id
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(tables.post_favorites)}
        GROUP BY post_id
      ) f ON f.post_id = p.id
      SET
        p.like_count = COALESCE(l.cnt, 0),
        p.favorite_count = COALESCE(f.cnt, 0)
    `
  );
}

export async function collectDistributions(conn, tableOverrides = {}) {
  const tables = resolveRebalanceTables(tableOverrides);
  const queries = {
    followers_summary: `
      SELECT COUNT(*) AS group_count, MIN(cnt) AS min_cnt, MAX(cnt) AS max_cnt, ROUND(AVG(cnt),2) AS avg_cnt
      FROM (
        SELECT following_id, COUNT(*) AS cnt
        FROM ${qTable(tables.user_follows)}
        WHERE COALESCE(status, 'NORMAL') = 'NORMAL'
        GROUP BY following_id
      ) t
    `,
    likes_summary: `
      SELECT COUNT(*) AS group_count, MIN(cnt) AS min_cnt, MAX(cnt) AS max_cnt, ROUND(AVG(cnt),2) AS avg_cnt
      FROM (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(tables.post_likes)}
        GROUP BY post_id
      ) t
    `,
    favorites_summary: `
      SELECT COUNT(*) AS group_count, MIN(cnt) AS min_cnt, MAX(cnt) AS max_cnt, ROUND(AVG(cnt),2) AS avg_cnt
      FROM (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(tables.post_favorites)}
        GROUP BY post_id
      ) t
    `,
    followers_top: `
      SELECT following_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.user_follows)}
      WHERE COALESCE(status, 'NORMAL') = 'NORMAL'
      GROUP BY following_id
      ORDER BY cnt DESC, following_id ASC
      LIMIT 10
    `,
    likes_top: `
      SELECT post_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.post_likes)}
      GROUP BY post_id
      ORDER BY cnt DESC, post_id ASC
      LIMIT 10
    `,
    favorites_top: `
      SELECT post_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.post_favorites)}
      GROUP BY post_id
      ORDER BY cnt DESC, post_id ASC
      LIMIT 10
    `,
    liker_users_top: `
      SELECT user_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.post_likes)}
      GROUP BY user_id
      ORDER BY cnt DESC, user_id ASC
      LIMIT 10
    `,
    favorite_users_top: `
      SELECT user_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.post_favorites)}
      GROUP BY user_id
      ORDER BY cnt DESC, user_id ASC
      LIMIT 10
    `,
    following_users_top: `
      SELECT follower_id AS entity_id, COUNT(*) AS cnt
      FROM ${qTable(tables.user_follows)}
      WHERE COALESCE(status, 'NORMAL') = 'NORMAL'
      GROUP BY follower_id
      ORDER BY cnt DESC, follower_id ASC
      LIMIT 10
    `,
  };

  const out = {};
  for (const [key, sql] of Object.entries(queries)) {
    // eslint-disable-next-line no-await-in-loop
    const [rows] = await conn.query(sql);
    out[key] = rows;
  }
  return out;
}

export async function writeManifest(dir, payload) {
  await ensureDir(dir);
  const file = path.join(dir, "manifest.json");
  await fs.writeFile(file, JSON.stringify(payload, null, 2), "utf8");
  return file;
}

export async function readManifestByTag(tag) {
  const file = path.join(backupRoot(tag), "manifest.json");
  const raw = await fs.readFile(file, "utf8");
  return JSON.parse(raw);
}
