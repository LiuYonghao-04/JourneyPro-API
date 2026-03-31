import "dotenv/config";
import mysql from "mysql2/promise";
import { DB, backupRoot, createTag, ensureDir, qTable, writeManifest } from "./social_rebalance_common.js";

const CFG = {
  batchSize: Math.max(500, Math.min(10000, Number(process.env.POST_COUNT_REBALANCE_BATCH || 4000))),
  minLikes: Math.max(1, Number(process.env.POST_COUNT_MIN_LIKES || 50)),
  maxLikes: Math.max(20, Number(process.env.POST_COUNT_MAX_LIKES || 170)),
  minFavorites: Math.max(0, Number(process.env.POST_COUNT_MIN_FAVORITES || 11)),
  maxFavorites: Math.max(5, Number(process.env.POST_COUNT_MAX_FAVORITES || 60)),
  recencyWindowDays: Math.max(30, Math.min(720, Number(process.env.POST_COUNT_RECENCY_DAYS || 240))),
};

const NOW = Date.now();
const DAY_MS = 24 * 3600 * 1000;

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function stableHash(input) {
  const text = String(input ?? "");
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function hashFloat(seed) {
  return stableHash(seed) / 4294967295;
}

function normalizeLog(value, min, max) {
  const lo = Math.log1p(Math.max(0, Number(min || 0)));
  const hi = Math.log1p(Math.max(Number(max || 1), Number(min || 0) + 1));
  const target = Math.log1p(Math.max(0, Number(value || 0)));
  if (hi <= lo) return 0.5;
  return clamp((target - lo) / (hi - lo), 0, 1);
}

function toAgeDays(value) {
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts) || ts <= 0) return 9999;
  return Math.max(0, (NOW - ts) / DAY_MS);
}

function buildTargets(row, stats) {
  const viewNorm = normalizeLog(row.view_count, stats.minView, stats.maxView);
  const ratingNorm = clamp((Number(row.rating || 4) - 4) / 1, 0, 1);
  const ageDays = toAgeDays(row.created_at);
  const recencyNorm = clamp(1 - ageDays / CFG.recencyWindowDays, 0, 1);
  const authorNorm = hashFloat(`author:${row.user_id}`);
  const poiNorm = hashFloat(`poi:${row.poi_id || 0}`);
  const engagement =
    viewNorm * 0.46 +
    recencyNorm * 0.2 +
    ratingNorm * 0.14 +
    authorNorm * 0.1 +
    poiNorm * 0.1;

  const score = clamp(engagement * 0.72 + hashFloat(`rank:${row.id}`) * 0.28, 0, 1);
  const tier = clamp(Math.floor(score * 10), 0, 9);

  const likeTierMin = [50, 56, 63, 71, 80, 91, 104, 118, 132, 146][tier];
  const likeTierMax = [68, 76, 86, 98, 111, 126, 142, 156, 165, 170][tier];
  let likeTarget = Math.round(likeTierMin + hashFloat(`like:${row.id}`) * (likeTierMax - likeTierMin));
  likeTarget = clamp(likeTarget, CFG.minLikes, CFG.maxLikes);

  const favoriteTierMin = [11, 13, 16, 20, 24, 29, 34, 40, 46, 52][tier];
  const favoriteTierMax = [20, 22, 26, 31, 36, 42, 48, 54, 58, 60][tier];
  let favoriteTarget = Math.round(
    favoriteTierMin + hashFloat(`favorite:${row.id}`) * (favoriteTierMax - favoriteTierMin)
  );

  const ratioCap = Math.floor(likeTarget * (0.26 + hashFloat(`ratio:${row.id}`) * 0.1));
  favoriteTarget = clamp(favoriteTarget, CFG.minFavorites, Math.min(CFG.maxFavorites, ratioCap));
  favoriteTarget = Math.min(favoriteTarget, likeTarget - 6);
  favoriteTarget = clamp(favoriteTarget, CFG.minFavorites, CFG.maxFavorites);

  const microBias = stableHash(`micro:${row.id}`) % 11;
  if (microBias <= 1) {
    likeTarget = clamp(likeTarget + 5, CFG.minLikes, CFG.maxLikes);
  } else if (microBias >= 9) {
    likeTarget = clamp(likeTarget - 4, CFG.minLikes, CFG.maxLikes);
  }
  if (microBias === 0) {
    favoriteTarget = clamp(favoriteTarget + 2, CFG.minFavorites, CFG.maxFavorites);
  } else if (microBias === 10) {
    favoriteTarget = clamp(favoriteTarget - 2, CFG.minFavorites, CFG.maxFavorites);
  }

  return {
    likeTarget,
    favoriteTarget,
  };
}

async function createTables(conn, backupTable, stageTable) {
  await conn.query(`DROP TABLE IF EXISTS ${qTable(stageTable)}`);
  await conn.query(`DROP TABLE IF EXISTS ${qTable(backupTable)}`);
  await conn.query(`
    CREATE TABLE ${qTable(backupTable)} (
      post_id BIGINT NOT NULL PRIMARY KEY,
      old_like_count INT NOT NULL,
      old_favorite_count INT NOT NULL
    )
  `);
  await conn.query(`
    INSERT INTO ${qTable(backupTable)} (post_id, old_like_count, old_favorite_count)
    SELECT id, like_count, favorite_count
    FROM posts
  `);
  await conn.query(`
    CREATE TABLE ${qTable(stageTable)} (
      post_id BIGINT NOT NULL PRIMARY KEY,
      target_like_count INT NOT NULL,
      target_favorite_count INT NOT NULL
    )
  `);
}

async function fetchStats(conn) {
  const [[row]] = await conn.query(`
    SELECT
      COUNT(*) AS totalPosts,
      MIN(view_count) AS minView,
      MAX(view_count) AS maxView,
      ROUND(AVG(view_count), 2) AS avgView
    FROM posts
  `);
  return {
    totalPosts: Number(row.totalPosts || 0),
    minView: Number(row.minView || 0),
    maxView: Number(row.maxView || 0),
    avgView: Number(row.avgView || 0),
  };
}

async function stageTargets(conn, stageTable, stats) {
  let processed = 0;
  let lastId = 0;
  while (true) {
    const [rows] = await conn.query(
      `
        SELECT id, user_id, poi_id, created_at, view_count, rating
        FROM posts
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
      `,
      [lastId, CFG.batchSize]
    );
    if (!rows.length) break;
    const values = rows.map((row) => {
      const targets = buildTargets(row, stats);
      return [Number(row.id), targets.likeTarget, targets.favoriteTarget];
    });
    await conn.query(
      `INSERT INTO ${qTable(stageTable)} (post_id, target_like_count, target_favorite_count) VALUES ?`,
      [values]
    );
    processed += rows.length;
    lastId = Number(rows[rows.length - 1].id);
    console.log(`staged ${processed}/${stats.totalPosts}`);
  }
}

async function applyTargets(conn, stageTable, totalPosts) {
  let processed = 0;
  let lastId = 0;
  while (true) {
    const [rows] = await conn.query(
      `
        SELECT post_id
        FROM ${qTable(stageTable)}
        WHERE post_id > ?
        ORDER BY post_id ASC
        LIMIT ?
      `,
      [lastId, CFG.batchSize]
    );
    if (!rows.length) break;
    const ids = rows.map((row) => Number(row.post_id));
    const placeholders = ids.map(() => "?").join(",");
    await conn.query(
      `
        UPDATE posts p
        JOIN ${qTable(stageTable)} s ON s.post_id = p.id
        SET
          p.like_count = s.target_like_count,
          p.favorite_count = s.target_favorite_count
        WHERE p.id IN (${placeholders})
      `,
      ids
    );
    processed += ids.length;
    lastId = ids[ids.length - 1];
    console.log(`applied ${processed}/${totalPosts}`);
  }
}

async function collectSummary(conn, stageTable) {
  const [[summary]] = await conn.query(`
    SELECT
      COUNT(*) AS rows_count,
      MIN(target_like_count) AS min_like,
      MAX(target_like_count) AS max_like,
      ROUND(AVG(target_like_count), 2) AS avg_like,
      MIN(target_favorite_count) AS min_favorite,
      MAX(target_favorite_count) AS max_favorite,
      ROUND(AVG(target_favorite_count), 2) AS avg_favorite
    FROM ${qTable(stageTable)}
  `);
  const [topLikes] = await conn.query(`
    SELECT target_like_count AS value, COUNT(*) AS c
    FROM ${qTable(stageTable)}
    GROUP BY target_like_count
    ORDER BY c DESC, target_like_count ASC
    LIMIT 12
  `);
  const [topFavorites] = await conn.query(`
    SELECT target_favorite_count AS value, COUNT(*) AS c
    FROM ${qTable(stageTable)}
    GROUP BY target_favorite_count
    ORDER BY c DESC, target_favorite_count ASC
    LIMIT 12
  `);
  return { summary, topLikes, topFavorites };
}

async function main() {
  const tag = `post_counts_${createTag()}`;
  const backupTable = `post_counts_backup_${tag}`;
  const stageTable = `post_counts_stage_${tag}`;
  const backupDir = backupRoot(tag);
  const conn = await mysql.createConnection(DB);
  try {
    console.log(`starting ${tag}`);
    const stats = await fetchStats(conn);
    await createTables(conn, backupTable, stageTable);
    await stageTargets(conn, stageTable, stats);
    const distribution = await collectSummary(conn, stageTable);
    await applyTargets(conn, stageTable, stats.totalPosts);
    const manifest = {
      tag,
      mode: "post_counts_only",
      config: CFG,
      stats,
      tables: {
        backupTable,
        stageTable,
      },
      distribution,
      createdAt: new Date().toISOString(),
    };
    await ensureDir(backupDir);
    const file = await writeManifest(backupDir, manifest);
    console.log(`manifest: ${file}`);
    console.log(JSON.stringify(distribution, null, 2));
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
