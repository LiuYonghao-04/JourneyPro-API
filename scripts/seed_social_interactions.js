import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CFG = {
  targetUsers: Math.max(28, Number(process.env.SOCIAL_TARGET_USERS || 420)),
  resetTables: String(process.env.SOCIAL_RESET_TABLES || "0") === "1",
  syncCounters: String(process.env.SOCIAL_SYNC_COUNTERS || "1") === "1",
  postLimit: Math.max(0, Number(process.env.SOCIAL_POST_LIMIT || 220000)),
  postBatch: Math.max(200, Math.min(5000, Number(process.env.SOCIAL_POST_BATCH || 1200))),
  eventInsertBatch: Math.max(500, Math.min(8000, Number(process.env.SOCIAL_EVENT_INSERT_BATCH || 3500))),
  followInsertBatch: Math.max(200, Math.min(6000, Number(process.env.SOCIAL_FOLLOW_INSERT_BATCH || 2000))),
  likeMin: Math.max(0, Number(process.env.SOCIAL_LIKE_MIN || 2)),
  likeMax: Math.max(1, Number(process.env.SOCIAL_LIKE_MAX || 18)),
  favMin: Math.max(0, Number(process.env.SOCIAL_FAV_MIN || 1)),
  favMax: Math.max(1, Number(process.env.SOCIAL_FAV_MAX || 10)),
  followMin: Math.max(1, Number(process.env.SOCIAL_FOLLOW_MIN || 26)),
  followMax: Math.max(2, Number(process.env.SOCIAL_FOLLOW_MAX || 120)),
  eventLookbackDays: Math.max(1, Number(process.env.SOCIAL_EVENT_LOOKBACK_DAYS || 45)),
  recentBoostHours: Math.max(1, Number(process.env.SOCIAL_RECENT_BOOST_HOURS || 24)),
};

const NOW = Date.now();
const DAY_MS = 24 * 3600 * 1000;

const normalize = (value) => String(value || "").trim();
const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const toTs = (value) => {
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts) || ts <= 0) return NOW - DAY_MS;
  return ts;
};

async function tryAlter(conn, sql) {
  try {
    await conn.query(sql);
  } catch (err) {
    const msg = String(err?.message || err);
    if (!msg.includes("Duplicate key") && !msg.includes("check that column/key exists")) {
      console.error("alter failed:", msg);
    }
  }
}

async function ensureTables(conn) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS post_likes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_like (post_id, user_id)
    );
  `);
  await conn.query(`
    CREATE TABLE IF NOT EXISTS post_favorites (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_fav (post_id, user_id)
    );
  `);
  await conn.query(`
    CREATE TABLE IF NOT EXISTS user_follows (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      follower_id BIGINT NOT NULL,
      following_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status VARCHAR(20) DEFAULT 'NORMAL',
      UNIQUE KEY uk_follows_pair (follower_id, following_id)
    );
  `);

  await tryAlter(conn, `ALTER TABLE post_likes ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(conn, `ALTER TABLE post_favorites ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(conn, `ALTER TABLE post_likes ADD INDEX idx_post_likes_created (created_at, post_id, user_id)`);
  await tryAlter(conn, `ALTER TABLE post_likes ADD INDEX idx_post_likes_user_created (user_id, created_at, post_id)`);
  await tryAlter(conn, `ALTER TABLE post_likes ADD INDEX idx_post_likes_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(conn, `ALTER TABLE post_favorites ADD INDEX idx_post_fav_created (created_at, post_id, user_id)`);
  await tryAlter(conn, `ALTER TABLE post_favorites ADD INDEX idx_post_fav_user_created (user_id, created_at, post_id)`);
  await tryAlter(conn, `ALTER TABLE post_favorites ADD INDEX idx_post_fav_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(conn, `ALTER TABLE user_follows ADD INDEX idx_follows_following_created (following_id, created_at, follower_id)`);
  await tryAlter(conn, `ALTER TABLE user_follows ADD INDEX idx_follows_follower_created (follower_id, created_at, following_id)`);
  await tryAlter(conn, `ALTER TABLE posts ADD INDEX idx_posts_user_id (user_id, id)`);
}

async function ensureUsers(conn) {
  const [existing] = await conn.query(`SELECT id, username FROM users ORDER BY id ASC`);
  const ids = existing.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
  if (ids.length >= CFG.targetUsers) return ids;

  const existingNames = new Set(existing.map((row) => normalize(row.username).toLowerCase()));
  const adjectives = ["Urban", "Smart", "Vector", "Atlas", "Swift", "Civic", "Pixel", "Orbit", "Signal", "Nova"];
  const nouns = ["Runner", "Mapper", "Planner", "Traveler", "Scout", "Guide", "Walker", "Rider", "Pilot", "Nomad"];

  let cursor = 1;
  const rows = [];
  while (ids.length + rows.length < CFG.targetUsers) {
    const a = adjectives[cursor % adjectives.length];
    const n = nouns[(cursor * 7) % nouns.length];
    const suffix = String(cursor).padStart(5, "0");
    const username = `social_bot_${suffix}`;
    if (!existingNames.has(username)) {
      const nickname = `${a} ${n} ${suffix.slice(-3)}`;
      const avatar = `https://api.dicebear.com/9.x/personas/svg?seed=${encodeURIComponent(username)}`;
      rows.push([username, "seed_social_bot_hash_v1", nickname, avatar]);
      existingNames.add(username);
    }
    cursor += 1;
  }

  if (rows.length) {
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      await conn.query(
        `INSERT INTO users (username, password_hash, nickname, avatar_url) VALUES ?`,
        [chunk]
      );
    }
  }

  const [after] = await conn.query(`SELECT id FROM users ORDER BY id ASC`);
  return after.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
}

function buildEventTime(postCreatedAt, seed) {
  const postTs = toTs(postCreatedAt);
  const recentCut = NOW - CFG.recentBoostHours * 3600 * 1000;
  const lookbackCut = NOW - CFG.eventLookbackDays * DAY_MS;
  const mode = stableHash(`${seed}:mode`) % 100;
  const minTs = mode < 26 ? Math.max(postTs, recentCut) : Math.max(postTs, lookbackCut);
  const maxTs = NOW - 60 * 1000;
  if (maxTs <= minTs) return new Date(maxTs);
  const span = maxTs - minTs;
  return new Date(minTs + (stableHash(`${seed}:at`) % span));
}

function pickDistinctUsers(userIds, authorId, count, seedText) {
  const seed = stableHash(seedText);
  const total = userIds.length;
  if (count <= 0 || total <= 1) return [];
  const maxCount = Math.min(count, total - 1);
  const selected = [];
  const seen = new Set();
  const stepRaw = (seed % (total - 1)) + 1;
  const step = stepRaw % 2 === 0 ? stepRaw + 1 : stepRaw;
  let idx = seed % total;
  let guard = 0;
  const maxGuard = total * 4;
  while (selected.length < maxCount && guard < maxGuard) {
    const uid = userIds[idx % total];
    if (uid !== authorId && !seen.has(uid)) {
      seen.add(uid);
      selected.push(uid);
    }
    idx += step;
    guard += 1;
  }
  return selected;
}

function computeLikeTarget(post, maxPerPost) {
  const like = Number(post.like_count) || 0;
  const fav = Number(post.favorite_count) || 0;
  const view = Number(post.view_count) || 0;
  const signal = like * 0.55 + fav * 0.8 + view * 0.02;
  if (signal <= 0) return 0;
  const base = Math.round(1 + Math.log1p(signal) * 1.25);
  const jitter = (stableHash(`lk:${post.id}`) % 3) - 1;
  const maxAllowed = Math.min(maxPerPost, CFG.likeMax);
  return clamp(base + jitter, CFG.likeMin, maxAllowed);
}

function computeFavTarget(post, likeTarget, maxPerPost) {
  if (likeTarget <= 0) return 0;
  const fav = Number(post.favorite_count) || 0;
  const view = Number(post.view_count) || 0;
  const signal = fav * 0.9 + view * 0.01;
  const base = Math.round(Math.max(0, likeTarget * 0.56) + Math.log1p(signal) * 0.45);
  const jitter = (stableHash(`fv:${post.id}`) % 3) - 1;
  const maxAllowed = Math.min(maxPerPost, likeTarget, CFG.favMax);
  return clamp(base + jitter, CFG.favMin, maxAllowed);
}

async function maybeResetTables(conn) {
  if (!CFG.resetTables) return;
  console.log("resetting post_likes / post_favorites / user_follows ...");
  await conn.query(`DELETE FROM post_likes`);
  await conn.query(`DELETE FROM post_favorites`);
  await conn.query(`DELETE FROM user_follows`);
}

async function fetchUserWeights(conn) {
  const [rows] = await conn.query(`
    SELECT
      u.id,
      COALESCE(s.post_cnt, 0) AS post_cnt,
      COALESCE(s.avg_view, 0) AS avg_view
    FROM users u
    LEFT JOIN (
      SELECT user_id, COUNT(*) AS post_cnt, AVG(view_count) AS avg_view
      FROM posts
      GROUP BY user_id
    ) s ON s.user_id = u.id
    ORDER BY u.id ASC
  `);
  const map = new Map();
  rows.forEach((row) => {
    const id = Number(row.id);
    const postCnt = Number(row.post_cnt) || 0;
    const avgView = Number(row.avg_view) || 0;
    const w = 1 + Math.log1p(postCnt * 3 + avgView * 0.07);
    map.set(id, Math.max(0.01, w));
  });
  return map;
}

async function seedFollowGraph(conn, userIds, userWeights) {
  if (userIds.length <= 2) return { inserted: 0, planned: 0 };

  const maxFollow = Math.min(CFG.followMax, userIds.length - 1);
  const minFollow = Math.min(CFG.followMin, maxFollow);
  const followRows = [];
  let inserted = 0;
  let planned = 0;

  const flush = async () => {
    if (!followRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO user_follows (follower_id, following_id, created_at) VALUES ?`,
      [followRows]
    );
    inserted += Number(res?.affectedRows || 0);
    followRows.length = 0;
  };

  for (let i = 0; i < userIds.length; i += 1) {
    const followerId = userIds[i];
    const targetCount = minFollow + (stableHash(`fc:${followerId}`) % (maxFollow - minFollow + 1));
    const ranked = userIds
      .filter((uid) => uid !== followerId)
      .map((uid) => {
        const base = userWeights.get(uid) || 1;
        const jitter = 0.68 + ((stableHash(`fw:${followerId}:${uid}`) % 1000) / 1000) * 0.7;
        return { uid, score: base * jitter };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, targetCount);

    ranked.forEach((row) => {
      planned += 1;
      followRows.push([
        followerId,
        row.uid,
        buildEventTime(NOW - 90 * DAY_MS, `follow:${followerId}:${row.uid}`),
      ]);
    });

    if (followRows.length >= CFG.followInsertBatch) {
      // eslint-disable-next-line no-await-in-loop
      await flush();
    }
  }

  await flush();
  return { inserted, planned };
}

async function syncPostCounters(conn, postIds) {
  const ids = [...new Set((postIds || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  if (!ids.length) return;
  await conn.query(
    `
      UPDATE posts p
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM post_likes
        WHERE post_id IN (?)
        GROUP BY post_id
      ) l ON l.post_id = p.id
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM post_favorites
        WHERE post_id IN (?)
        GROUP BY post_id
      ) f ON f.post_id = p.id
      SET
        p.like_count = COALESCE(l.cnt, 0),
        p.favorite_count = COALESCE(f.cnt, 0)
      WHERE p.id IN (?)
    `,
    [ids, ids, ids]
  );
}

async function seedReactions(conn, userIds) {
  let cursorId = 0;
  let processed = 0;
  let totalLikesPlanned = 0;
  let totalFavsPlanned = 0;
  let totalLikesInserted = 0;
  let totalFavsInserted = 0;
  const maxPerPost = userIds.length - 1;
  if (maxPerPost <= 0) {
    return { processed: 0, likes_planned: 0, favorites_planned: 0, likes_inserted: 0, favorites_inserted: 0 };
  }

  const likeRows = [];
  const favRows = [];

  const flushLikeRows = async () => {
    if (!likeRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO post_likes (post_id, user_id, post_owner_id, created_at) VALUES ?`,
      [likeRows]
    );
    totalLikesInserted += Number(res?.affectedRows || 0);
    likeRows.length = 0;
  };

  const flushFavRows = async () => {
    if (!favRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO post_favorites (post_id, user_id, post_owner_id, created_at) VALUES ?`,
      [favRows]
    );
    totalFavsInserted += Number(res?.affectedRows || 0);
    favRows.length = 0;
  };

  while (true) {
    const remain = CFG.postLimit > 0 ? CFG.postLimit - processed : CFG.postBatch;
    if (CFG.postLimit > 0 && remain <= 0) break;
    const qLimit = CFG.postLimit > 0 ? Math.min(CFG.postBatch, remain) : CFG.postBatch;
    const [rows] = await conn.query(
      `
        SELECT id, user_id, created_at, like_count, favorite_count, view_count
        FROM posts
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
      `,
      [cursorId, qLimit]
    );
    if (!rows.length) break;

    cursorId = Number(rows[rows.length - 1].id);
    const chunkPostIds = [];

    for (const post of rows) {
      const postId = Number(post.id);
      const authorId = Number(post.user_id);
      if (!Number.isFinite(postId) || !Number.isFinite(authorId)) continue;
      chunkPostIds.push(postId);

      const likeTarget = computeLikeTarget(post, maxPerPost);
      const favTarget = computeFavTarget(post, likeTarget, maxPerPost);
      const likeUsers = pickDistinctUsers(userIds, authorId, likeTarget, `lk:${postId}`);
      const favUsers = pickDistinctUsers(userIds, authorId, favTarget, `fv:${postId}`);

      totalLikesPlanned += likeUsers.length;
      totalFavsPlanned += favUsers.length;

      likeUsers.forEach((uid) => {
        likeRows.push([postId, uid, authorId, buildEventTime(post.created_at, `lk:${postId}:${uid}`)]);
      });
      favUsers.forEach((uid) => {
        favRows.push([postId, uid, authorId, buildEventTime(post.created_at, `fv:${postId}:${uid}`)]);
      });

      if (likeRows.length >= CFG.eventInsertBatch) {
        // eslint-disable-next-line no-await-in-loop
        await flushLikeRows();
      }
      if (favRows.length >= CFG.eventInsertBatch) {
        // eslint-disable-next-line no-await-in-loop
        await flushFavRows();
      }
    }

    // eslint-disable-next-line no-await-in-loop
    await flushLikeRows();
    // eslint-disable-next-line no-await-in-loop
    await flushFavRows();
    if (CFG.syncCounters) {
      // eslint-disable-next-line no-await-in-loop
      await syncPostCounters(conn, chunkPostIds);
    }

    processed += rows.length;
    if (processed % 12000 === 0 || (CFG.postLimit > 0 && processed >= CFG.postLimit)) {
      console.log(
        `progress posts=${processed} likes_inserted=${totalLikesInserted} favorites_inserted=${totalFavsInserted}`
      );
    }
  }

  return {
    processed,
    likes_planned: totalLikesPlanned,
    favorites_planned: totalFavsPlanned,
    likes_inserted: totalLikesInserted,
    favorites_inserted: totalFavsInserted,
  };
}

async function fetchSummary(conn) {
  const queries = {
    users: `SELECT COUNT(*) AS c FROM users`,
    posts: `SELECT COUNT(*) AS c FROM posts`,
    likes_table: `SELECT COUNT(*) AS c FROM post_likes`,
    favorites_table: `SELECT COUNT(*) AS c FROM post_favorites`,
    follows_table: `SELECT COUNT(*) AS c FROM user_follows`,
    post_like_sum: `SELECT COALESCE(SUM(like_count), 0) AS c FROM posts`,
    post_fav_sum: `SELECT COALESCE(SUM(favorite_count), 0) AS c FROM posts`,
  };
  const out = {};
  for (const [key, sql] of Object.entries(queries)) {
    const [[row]] = await conn.query(sql);
    out[key] = Number(row.c || 0);
  }
  return out;
}

async function main() {
  const pool = mysql.createPool({ ...DB, connectionLimit: 12, waitForConnections: true });
  const conn = await pool.getConnection();

  try {
    await ensureTables(conn);
    const before = await fetchSummary(conn);
    const userIds = await ensureUsers(conn);
    await maybeResetTables(conn);
    const userWeights = await fetchUserWeights(conn);
    const followResult = await seedFollowGraph(conn, userIds, userWeights);
    const reactionResult = await seedReactions(conn, userIds);
    const after = await fetchSummary(conn);

    console.log(
      JSON.stringify(
        {
          success: true,
          config: CFG,
          before,
          follow_seed: followResult,
          reaction_seed: reactionResult,
          after,
        },
        null,
        2
      )
    );
  } finally {
    conn.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
