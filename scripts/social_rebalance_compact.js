import "dotenv/config";
import mysql from "mysql2/promise";
import {
  backupRoot,
  collectDistributions,
  createTag,
  ensureDir,
  qTable,
  writeManifest,
} from "./social_rebalance_common.js";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CFG = {
  targetPostLimit: Math.max(2000, Math.min(40000, Number(process.env.SOCIAL_COMPACT_POST_LIMIT || 15000))),
  targetWindowDays: Math.max(7, Math.min(180, Number(process.env.SOCIAL_COMPACT_WINDOW_DAYS || 45))),
  postInsertBatch: Math.max(200, Math.min(4000, Number(process.env.SOCIAL_COMPACT_INSERT_BATCH || 1200))),
  followInsertBatch: Math.max(200, Math.min(4000, Number(process.env.SOCIAL_COMPACT_FOLLOW_BATCH || 1000))),
  minLikes: Math.max(1, Number(process.env.SOCIAL_COMPACT_MIN_LIKES || 4)),
  maxLikes: Math.max(8, Number(process.env.SOCIAL_COMPACT_MAX_LIKES || 24)),
  minFavorites: Math.max(0, Number(process.env.SOCIAL_COMPACT_MIN_FAVORITES || 2)),
  maxFavorites: Math.max(2, Number(process.env.SOCIAL_COMPACT_MAX_FAVORITES || 13)),
  followMin: Math.max(2, Number(process.env.SOCIAL_COMPACT_FOLLOW_MIN || 6)),
  followMax: Math.max(10, Number(process.env.SOCIAL_COMPACT_FOLLOW_MAX || 76)),
};

const DAY_MS = 24 * 3600 * 1000;
const NOW = Date.now();

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);
const normalize = (value) => String(value || "").trim();

function stableHash(input) {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function toTs(value) {
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts) || ts <= 0) return NOW - DAY_MS;
  return ts;
}

function buildUserTierProfile(userId, userWeight = 1) {
  const pct = stableHash(`compact:tier:${userId}`) % 100;
  const momentum = clamp(0.9 + Math.log1p(Number(userWeight) || 1) * 0.28, 0.9, 1.7);
  if (pct < 7) {
    return {
      label: "anchor",
      attraction: 1.82 * momentum,
      reactionBias: 1.32 * momentum,
      followMinFactor: 0.7,
      followMaxFactor: 1,
    };
  }
  if (pct < 24) {
    return {
      label: "rising",
      attraction: 1.34 * momentum,
      reactionBias: 1.12 * momentum,
      followMinFactor: 0.42,
      followMaxFactor: 0.8,
    };
  }
  if (pct < 70) {
    return {
      label: "steady",
      attraction: 1.0 * momentum,
      reactionBias: 0.95 * momentum,
      followMinFactor: 0.2,
      followMaxFactor: 0.54,
    };
  }
  return {
    label: "quiet",
    attraction: 0.68 * momentum,
    reactionBias: 0.7 * momentum,
    followMinFactor: 0.06,
    followMaxFactor: 0.24,
  };
}

function buildEventTime(postCreatedAt, seed) {
  const postTs = toTs(postCreatedAt);
  const minTs = Math.max(postTs, NOW - 60 * DAY_MS);
  const maxTs = NOW - 90 * 1000;
  if (maxTs <= minTs) return new Date(maxTs);
  const span = maxTs - minTs;
  return new Date(minTs + (stableHash(`${seed}:at`) % span));
}

function buildReactionUserPool(userIds, userWeights) {
  const pool = [];
  userIds.forEach((uid) => {
    const weight = Number(userWeights.get(uid) || 1);
    const tier = buildUserTierProfile(uid, weight);
    const repeats = clamp(Math.round(weight * tier.reactionBias * 2.6), 1, 16);
    for (let i = 0; i < repeats; i += 1) {
      pool.push(uid);
    }
  });
  return pool.length ? pool : [...userIds];
}

function pickDistinctUsers(userIds, authorId, count, seedText) {
  const seed = stableHash(seedText);
  const total = userIds.length;
  if (count <= 0 || total <= 1) return [];
  const maxCount = Math.min(count, total - 1);
  const selected = [];
  const seen = new Set();
  const stepRaw = (seed % Math.max(total - 1, 1)) + 1;
  const step = stepRaw % 2 === 0 ? stepRaw + 1 : stepRaw;
  let idx = seed % total;
  let guard = 0;
  const maxGuard = total * 5;
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

function pickDistinctUsersFromPool(pool, fallbackUserIds, authorId, count, seedText) {
  const total = pool.length;
  if (!total || count <= 0) return [];
  const maxCount = Math.min(count, Math.max(fallbackUserIds.length - 1, 0));
  if (maxCount <= 0) return [];
  const selected = [];
  const seen = new Set();
  const seed = stableHash(`pool:${seedText}`);
  const stepRaw = (seed % Math.max(total - 1, 1)) + 1;
  const step = stepRaw % 2 === 0 ? stepRaw + 1 : stepRaw;
  let idx = seed % total;
  let guard = 0;
  const maxGuard = total * 8;
  while (selected.length < maxCount && guard < maxGuard) {
    const uid = pool[idx % total];
    if (uid !== authorId && !seen.has(uid)) {
      seen.add(uid);
      selected.push(uid);
    }
    idx += step;
    guard += 1;
  }
  if (selected.length >= maxCount) return selected;
  const remainder = pickDistinctUsers(fallbackUserIds, authorId, maxCount - selected.length, `${seedText}:fallback`);
  remainder.forEach((uid) => {
    if (uid !== authorId && !seen.has(uid) && selected.length < maxCount) {
      seen.add(uid);
      selected.push(uid);
    }
  });
  return selected;
}

async function tryAlter(conn, sql) {
  try {
    await conn.query(sql);
  } catch (err) {
    const msg = String(err?.message || err);
    if (!msg.includes("Duplicate key") && !msg.includes("Duplicate column name") && !msg.includes("check that column/key exists")) {
      throw err;
    }
  }
}

async function ensureSupportIndexes(conn) {
  await tryAlter(conn, `ALTER TABLE posts ADD INDEX idx_posts_created_hot_compact (created_at, like_count, favorite_count, view_count, id)`);
  await tryAlter(conn, `ALTER TABLE post_likes ADD INDEX idx_post_likes_post_user_compact (post_id, user_id)`);
  await tryAlter(conn, `ALTER TABLE post_favorites ADD INDEX idx_post_favorites_post_user_compact (post_id, user_id)`);
}

async function createLikeTableLike(conn, targetTable, sourceTable) {
  await conn.query(`DROP TABLE IF EXISTS ${qTable(targetTable)}`);
  await conn.query(`CREATE TABLE ${qTable(targetTable)} LIKE ${qTable(sourceTable)}`);
}

async function fetchUsers(conn) {
  const [rows] = await conn.query(`SELECT id, username FROM users ORDER BY id ASC`);
  return rows.map((row) => ({ id: Number(row.id), username: row.username }));
}

async function fetchUserWeights(conn, userIds) {
  const ids = [...new Set((userIds || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  const cutoff = new Date(NOW - CFG.targetWindowDays * DAY_MS).toISOString().slice(0, 19).replace("T", " ");
  const [rows] = await conn.query(
    `
      SELECT
        u.id,
        COALESCE(s.post_cnt, 0) AS post_cnt,
        COALESCE(s.avg_view, 0) AS avg_view
      FROM users u
      LEFT JOIN (
        SELECT user_id, COUNT(*) AS post_cnt, AVG(view_count) AS avg_view
        FROM posts
        WHERE created_at >= ?
        GROUP BY user_id
      ) s ON s.user_id = u.id
      WHERE u.id IN (?)
      ORDER BY u.id ASC
    `,
    [cutoff, ids]
  );
  const map = new Map();
  rows.forEach((row) => {
    const id = Number(row.id);
    const postCnt = Number(row.post_cnt) || 0;
    const avgView = Number(row.avg_view) || 0;
    const weight = 1 + Math.log1p(postCnt * 2.4 + avgView * 0.03);
    map.set(id, Math.max(0.02, weight));
  });
  ids.forEach((id) => {
    if (!map.has(id)) map.set(id, 1);
  });
  return map;
}

async function selectTargetPosts(conn) {
  const cutoff = new Date(NOW - CFG.targetWindowDays * DAY_MS).toISOString().slice(0, 19).replace("T", " ");
  const [rows] = await conn.query(
    `
      SELECT
        p.id,
        p.user_id,
        p.created_at,
        p.like_count,
        p.favorite_count,
        p.view_count
      FROM posts p
      WHERE p.created_at >= ?
      ORDER BY
        (
          p.like_count * 5.4 +
          p.favorite_count * 6.2 +
          LEAST(p.view_count, 24000) / 150 +
          GREATEST(0, ? - TIMESTAMPDIFF(DAY, p.created_at, NOW())) * 0.55
        ) DESC,
        p.created_at DESC,
        p.id DESC
      LIMIT ?
    `,
    [cutoff, CFG.targetWindowDays, CFG.targetPostLimit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    user_id: Number(row.user_id),
    created_at: row.created_at,
    like_count: Number(row.like_count) || 0,
    favorite_count: Number(row.favorite_count) || 0,
    view_count: Number(row.view_count) || 0,
  }));
}

function buildTargetCounts(posts, userWeights) {
  const total = Math.max(posts.length - 1, 1);
  return posts.map((post, index) => {
    const freshnessDays = (NOW - toTs(post.created_at)) / DAY_MS;
    const recencyBoost = clamp(1 - freshnessDays / Math.max(CFG.targetWindowDays, 1), 0, 1);
    const percentile = 1 - index / total;
    const authorBias = buildUserTierProfile(post.user_id, userWeights.get(post.user_id) || 1).reactionBias;
    const signal = Math.log1p(post.view_count) * 1.5 + post.like_count * 1.2 + post.favorite_count * 1.6;
    const jitter = ((stableHash(`compact:like:${post.id}`) % 7) - 3) * 0.7;
    const desiredLikes = Math.round(
      CFG.minLikes +
        percentile * 8.4 +
        recencyBoost * 5.2 +
        signal * 0.42 +
        (authorBias - 1) * 4.2 +
        jitter
    );
    const targetLikes = clamp(desiredLikes, CFG.minLikes, CFG.maxLikes);

    const favRatio = 0.34 + percentile * 0.12 + recencyBoost * 0.08;
    const favJitter = ((stableHash(`compact:fav:${post.id}`) % 5) - 2) * 0.55;
    const desiredFavorites = Math.round(targetLikes * favRatio + favJitter);
    const targetFavorites = clamp(
      desiredFavorites,
      Math.min(CFG.minFavorites, targetLikes),
      Math.min(CFG.maxFavorites, targetLikes)
    );

    return {
      ...post,
      target_like_count: targetLikes,
      target_favorite_count: targetFavorites,
    };
  });
}

async function createTargetTable(conn, tag, targetPosts) {
  const tableName = `social_compact_targets_${tag}`;
  await conn.query(`DROP TABLE IF EXISTS ${qTable(tableName)}`);
  await conn.query(`
    CREATE TABLE ${qTable(tableName)} (
      post_id BIGINT PRIMARY KEY,
      author_id BIGINT NOT NULL,
      created_at DATETIME NOT NULL,
      old_like_count INT NOT NULL,
      old_favorite_count INT NOT NULL,
      target_like_count INT NOT NULL,
      target_favorite_count INT NOT NULL
    )
  `);
  const rows = targetPosts.map((post) => [
    post.id,
    post.user_id,
    new Date(post.created_at),
    post.like_count,
    post.favorite_count,
    post.target_like_count,
    post.target_favorite_count,
  ]);
  for (let i = 0; i < rows.length; i += 1000) {
    const chunk = rows.slice(i, i + 1000);
    // eslint-disable-next-line no-await-in-loop
    await conn.query(
      `INSERT INTO ${qTable(tableName)} (post_id, author_id, created_at, old_like_count, old_favorite_count, target_like_count, target_favorite_count) VALUES ?`,
      [chunk]
    );
  }
  return tableName;
}

async function backupTouchedData(conn, tag, targetTable) {
  const backupTables = {
    likes: `post_likes_compactbak_${tag}`,
    favorites: `post_favorites_compactbak_${tag}`,
    follows: `user_follows_compactbak_${tag}`,
  };
  await createLikeTableLike(conn, backupTables.likes, "post_likes");
  await createLikeTableLike(conn, backupTables.favorites, "post_favorites");
  await createLikeTableLike(conn, backupTables.follows, "user_follows");

  await conn.query(`
    INSERT INTO ${qTable(backupTables.likes)}
    SELECT l.*
    FROM post_likes l
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = l.post_id
  `);
  await conn.query(`
    INSERT INTO ${qTable(backupTables.favorites)}
    SELECT f.*
    FROM post_favorites f
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = f.post_id
  `);
  await conn.query(`
    INSERT INTO ${qTable(backupTables.follows)}
    SELECT *
    FROM user_follows
  `);

  return backupTables;
}

async function createStageTables(conn, tag) {
  const stageTables = {
    likes: `post_likes_compactnew_${tag}`,
    favorites: `post_favorites_compactnew_${tag}`,
    follows: `user_follows_compactnew_${tag}`,
  };
  await createLikeTableLike(conn, stageTables.likes, "post_likes");
  await createLikeTableLike(conn, stageTables.favorites, "post_favorites");
  await createLikeTableLike(conn, stageTables.follows, "user_follows");
  return stageTables;
}

async function seedStageFollows(conn, stageTable, userIds, userWeights) {
  const maxFollow = Math.min(CFG.followMax, userIds.length - 1);
  const minFollow = Math.min(CFG.followMin, maxFollow);
  const followRows = [];
  let inserted = 0;

  const flush = async () => {
    if (!followRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO ${qTable(stageTable)} (follower_id, following_id, created_at, status) VALUES ?`,
      [followRows]
    );
    inserted += Number(res?.affectedRows || 0);
    followRows.length = 0;
  };

  for (const followerId of userIds) {
    const followerWeight = userWeights.get(followerId) || 1;
    const followerTier = buildUserTierProfile(followerId, followerWeight);
    const localMin = clamp(Math.round(minFollow * followerTier.followMinFactor), 1, maxFollow);
    const localMax = clamp(Math.round(maxFollow * followerTier.followMaxFactor), localMin, maxFollow);
    const targetCount = localMin + (stableHash(`compact:follow:${followerId}`) % (localMax - localMin + 1));
    const ranked = userIds
      .filter((uid) => uid !== followerId)
      .map((uid) => {
        const targetWeight = userWeights.get(uid) || 1;
        const targetTier = buildUserTierProfile(uid, targetWeight);
        const jitter = 0.72 + ((stableHash(`compact:fw:${followerId}:${uid}`) % 1000) / 1000) * 0.72;
        return { uid, score: targetWeight * targetTier.attraction * jitter };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, targetCount);

    ranked.forEach((row) => {
      followRows.push([
        followerId,
        row.uid,
        buildEventTime(NOW - 90 * DAY_MS, `compact:follow:${followerId}:${row.uid}`),
        "NORMAL",
      ]);
    });

    if (followRows.length >= CFG.followInsertBatch) {
      // eslint-disable-next-line no-await-in-loop
      await flush();
    }
  }

  await flush();
  return inserted;
}

async function seedStageReactions(conn, stageTables, targetPosts, userIds, userWeights) {
  const reactionPool = buildReactionUserPool(userIds, userWeights);
  const likeRows = [];
  const favoriteRows = [];
  let likeInserted = 0;
  let favoriteInserted = 0;

  const flushLikes = async () => {
    if (!likeRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO ${qTable(stageTables.likes)} (post_id, user_id, post_owner_id, created_at) VALUES ?`,
      [likeRows]
    );
    likeInserted += Number(res?.affectedRows || 0);
    likeRows.length = 0;
  };

  const flushFavorites = async () => {
    if (!favoriteRows.length) return;
    const [res] = await conn.query(
      `INSERT IGNORE INTO ${qTable(stageTables.favorites)} (post_id, user_id, post_owner_id, created_at) VALUES ?`,
      [favoriteRows]
    );
    favoriteInserted += Number(res?.affectedRows || 0);
    favoriteRows.length = 0;
  };

  for (const post of targetPosts) {
    const likeUsers = pickDistinctUsersFromPool(
      reactionPool,
      userIds,
      post.user_id,
      post.target_like_count,
      `compact:like:${post.id}`
    );
    const favoriteUsers = pickDistinctUsersFromPool(
      reactionPool,
      userIds,
      post.user_id,
      post.target_favorite_count,
      `compact:fav:${post.id}`
    );

    likeUsers.forEach((uid) => {
      likeRows.push([post.id, uid, post.user_id, buildEventTime(post.created_at, `compact:lk:${post.id}:${uid}`)]);
    });
    favoriteUsers.forEach((uid) => {
      favoriteRows.push([post.id, uid, post.user_id, buildEventTime(post.created_at, `compact:fv:${post.id}:${uid}`)]);
    });

    if (likeRows.length >= CFG.postInsertBatch) {
      // eslint-disable-next-line no-await-in-loop
      await flushLikes();
    }
    if (favoriteRows.length >= CFG.postInsertBatch) {
      // eslint-disable-next-line no-await-in-loop
      await flushFavorites();
    }
  }

  await flushLikes();
  await flushFavorites();
  return { likeInserted, favoriteInserted };
}

async function applyCompactPatch(conn, targetTable, stageTables) {
  await conn.query(`
    DELETE l
    FROM post_likes l
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = l.post_id
  `);
  await conn.query(`
    INSERT INTO post_likes (post_id, user_id, post_owner_id, created_at)
    SELECT post_id, user_id, post_owner_id, created_at
    FROM ${qTable(stageTables.likes)}
  `);

  await conn.query(`
    DELETE f
    FROM post_favorites f
    INNER JOIN ${qTable(targetTable)} t ON t.post_id = f.post_id
  `);
  await conn.query(`
    INSERT INTO post_favorites (post_id, user_id, post_owner_id, created_at)
    SELECT post_id, user_id, post_owner_id, created_at
    FROM ${qTable(stageTables.favorites)}
  `);

  await conn.query(`DELETE FROM user_follows`);
  await conn.query(`
    INSERT INTO user_follows (follower_id, following_id, created_at, status)
    SELECT follower_id, following_id, created_at, status
    FROM ${qTable(stageTables.follows)}
  `);

  await conn.query(`
    UPDATE posts p
    INNER JOIN (
      SELECT
        t.post_id,
        COALESCE(l.cnt, 0) AS like_cnt,
        COALESCE(f.cnt, 0) AS favorite_cnt
      FROM ${qTable(targetTable)} t
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(stageTables.likes)}
        GROUP BY post_id
      ) l ON l.post_id = t.post_id
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM ${qTable(stageTables.favorites)}
        GROUP BY post_id
      ) f ON f.post_id = t.post_id
    ) s ON s.post_id = p.id
    SET
      p.like_count = s.like_cnt,
      p.favorite_count = s.favorite_cnt
  `);
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

async function countRows(conn, tableName) {
  const [[row]] = await conn.query(`SELECT COUNT(*) AS c FROM ${qTable(tableName)}`);
  return Number(row?.c || 0);
}

async function main() {
  const tag = createTag();
  const dir = backupRoot(`compact_${tag}`);
  await ensureDir(dir);
  const conn = await mysql.createConnection(DB);
  let manifest = null;
  let targetTable = null;
  let backupTables = null;
  let stageTables = null;

  try {
    await ensureSupportIndexes(conn);
    const before = await collectDistributions(conn);
    const users = await fetchUsers(conn);
    const userIds = users.map((row) => row.id).filter((id) => Number.isFinite(id) && id > 0);
    const userWeights = await fetchUserWeights(conn, userIds);
    const selectedPosts = await selectTargetPosts(conn);
    const targetPosts = buildTargetCounts(selectedPosts, userWeights);

    targetTable = await createTargetTable(conn, tag, targetPosts);
    backupTables = await backupTouchedData(conn, tag, targetTable);
    stageTables = await createStageTables(conn, tag);

    const stagedFollowCount = await seedStageFollows(conn, stageTables.follows, userIds, userWeights);
    const stagedReactions = await seedStageReactions(conn, stageTables, targetPosts, userIds, userWeights);

    manifest = {
      tag: `compact_${tag}`,
      generated_at: new Date().toISOString(),
      phase: "staged",
      config: CFG,
      target_table: targetTable,
      backup_tables: backupTables,
      stage_tables: stageTables,
      target_posts: targetPosts.length,
      staged_rows: {
        likes: stagedReactions.likeInserted,
        favorites: stagedReactions.favoriteInserted,
        follows: stagedFollowCount,
      },
      before,
    };
    await writeManifest(dir, manifest);

    await applyCompactPatch(conn, targetTable, stageTables);

    const after = await collectDistributions(conn);
    const liveCounts = {
      likes: await countRows(conn, "post_likes"),
      favorites: await countRows(conn, "post_favorites"),
      follows: await countRows(conn, "user_follows"),
    };

    await writeManifest(dir, {
      ...manifest,
      phase: "completed",
      completed_at: new Date().toISOString(),
      after,
      live_counts: liveCounts,
    });

    console.log(
      JSON.stringify(
        {
          success: true,
          tag: `compact_${tag}`,
          config: CFG,
          target_posts: targetPosts.length,
          staged_rows: manifest.staged_rows,
          live_counts: liveCounts,
          before,
          after,
        },
        null,
        2
      )
    );
  } catch (err) {
    console.error("[social-compact] failed", err);
    if (targetTable && backupTables) {
      try {
        await restoreCompactPatch(conn, targetTable, backupTables);
      } catch (restoreErr) {
        console.error("[social-compact] restore failed", restoreErr);
      }
    }
    await writeManifest(dir, {
      ...(manifest || {}),
      tag: manifest?.tag || `compact_${tag}`,
      phase: "failed",
      failed_at: new Date().toISOString(),
      target_table: targetTable,
      backup_tables: backupTables,
      stage_tables: stageTables,
      failure: String(err?.message || err),
    });
    process.exit(1);
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error("[social-compact] fatal", err);
  process.exit(1);
});
