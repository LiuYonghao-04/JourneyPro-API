import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();
const subscribers = new Map(); // userId -> Set(res)

router.use((req, res, next) => {
  res.setHeader("Cache-Control", "no-store");
  next();
});

async function tryAlter(sql) {
  try {
    await pool.query(sql);
  } catch (err) {
    const msg = String(err?.message || err);
    if (!msg.includes("Duplicate key") && !msg.includes("check that column/key exists")) {
      console.error("notifications alter error:", msg);
    }
  }
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      username VARCHAR(50) NOT NULL UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      nickname VARCHAR(50) NOT NULL,
      avatar_url VARCHAR(255) NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT NOT NULL,
      poi_id BIGINT NULL,
      title VARCHAR(100) NOT NULL,
      content TEXT NOT NULL,
      rating TINYINT NULL,
      cover_image VARCHAR(255) NULL,
      image_count INT DEFAULT 0,
      like_count INT DEFAULT 0,
      favorite_count INT DEFAULT 0,
      view_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'NORMAL',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_likes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_like (post_id, user_id),
      CONSTRAINT fk_post_likes_post FOREIGN KEY (post_id) REFERENCES posts(id),
      CONSTRAINT fk_post_likes_user FOREIGN KEY (user_id) REFERENCES users(id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_favorites (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_fav (post_id, user_id),
      CONSTRAINT fk_post_fav_post FOREIGN KEY (post_id) REFERENCES posts(id),
      CONSTRAINT fk_post_fav_user FOREIGN KEY (user_id) REFERENCES users(id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_comments (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      parent_comment_id BIGINT NULL,
      type VARCHAR(20) DEFAULT 'COMMENT',
      content TEXT NOT NULL,
      like_count INT DEFAULT 0,
      reply_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'NORMAL',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_comments_post FOREIGN KEY (post_id) REFERENCES posts(id),
      CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_follows (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      follower_id BIGINT NOT NULL,
      following_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status VARCHAR(20) DEFAULT 'NORMAL',
      CONSTRAINT fk_follows_follower FOREIGN KEY (follower_id) REFERENCES users(id),
      CONSTRAINT fk_follows_following FOREIGN KEY (following_id) REFERENCES users(id),
      CONSTRAINT uk_follows_pair UNIQUE (follower_id, following_id),
      CONSTRAINT chk_follows_not_self CHECK (follower_id <> following_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_notification_state (
      user_id BIGINT PRIMARY KEY,
      read_all_at DATETIME NULL,
      read_like_at DATETIME NULL,
      read_favorite_at DATETIME NULL,
      read_comment_at DATETIME NULL,
      read_follow_at DATETIME NULL,
      read_chat_at DATETIME NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  `);

  await tryAlter(`ALTER TABLE post_likes ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_favorites ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_comments ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_created (created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_favorites_created (created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_comments ADD INDEX idx_post_comments_created (created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_post_created (post_id, created_at, user_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_favorites_post_created (post_id, created_at, user_id)`);
  await tryAlter(`ALTER TABLE post_comments ADD INDEX idx_post_comments_post_created (post_id, created_at, user_id)`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_favorites_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_comments ADD INDEX idx_post_comments_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE user_follows ADD INDEX idx_user_follows_following_created (following_id, created_at, follower_id)`);
  await tryAlter(`ALTER TABLE posts ADD INDEX idx_posts_user_created (user_id, created_at, id)`);
  await tryAlter(`ALTER TABLE posts ADD INDEX idx_posts_user_id (user_id, id)`);
}

let ensureTablesPromise = null;
function ensureTablesReady() {
  if (!ensureTablesPromise) {
    ensureTablesPromise = ensureTables().catch((err) => {
      ensureTablesPromise = null;
      throw err;
    });
  }
  return ensureTablesPromise;
}

const EMPTY_STATE = {
  read_all_at: null,
  read_like_at: null,
  read_favorite_at: null,
  read_comment_at: null,
  read_follow_at: null,
  read_chat_at: null,
};
const FUTURE_SKEW_MS = 5 * 60 * 1000;

const mapState = (row) => ({
  read_all_at: row?.read_all_at || null,
  read_like_at: row?.read_like_at || null,
  read_favorite_at: row?.read_favorite_at || null,
  read_comment_at: row?.read_comment_at || null,
  read_follow_at: row?.read_follow_at || null,
  read_chat_at: row?.read_chat_at || null,
});

async function fetchNotificationState(userId) {
  const [[row]] = await pool.query(
    `SELECT read_all_at, read_like_at, read_favorite_at, read_comment_at, read_follow_at, read_chat_at
     FROM user_notification_state WHERE user_id = ? LIMIT 1`,
    [userId]
  );
  return row ? mapState(row) : { ...EMPTY_STATE };
}

function isUnread(item, state) {
  const type = String(item?.type || "");
  const allReadAtRaw = state?.read_all_at ? new Date(state.read_all_at).getTime() : 0;
  const perTypeKey = `read_${type}_at`;
  const typeReadAtRaw = state?.[perTypeKey] ? new Date(state[perTypeKey]).getTime() : 0;
  const nowMs = Date.now();
  const nowSec = Math.floor(nowMs / 1000);
  const readAnchorRaw = Math.max(allReadAtRaw || 0, typeReadAtRaw || 0);
  const createdAtRaw = item?.created_at ? new Date(item.created_at).getTime() : 0;
  const normalizedCreatedRaw =
    createdAtRaw > nowMs + FUTURE_SKEW_MS ? (readAnchorRaw || nowMs) : createdAtRaw;
  const createdAt = normalizedCreatedRaw ? Math.floor(normalizedCreatedRaw / 1000) : 0;
  const allReadAt = allReadAtRaw ? Math.min(Math.floor(allReadAtRaw / 1000), nowSec) : 0;
  if (allReadAt && createdAt <= allReadAt) return false;
  const typeReadAt = typeReadAtRaw ? Math.min(Math.floor(typeReadAtRaw / 1000), nowSec) : 0;
  if (typeReadAt && createdAt <= typeReadAt) return false;
  return true;
}

function toMysqlLocalDatetime(dateInput) {
  const d = dateInput instanceof Date ? dateInput : new Date(dateInput);
  if (Number.isNaN(d.getTime())) return null;
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function notificationKey(item) {
  return [
    item?.type || "x",
    item?.actor_id || 0,
    item?.post_id || 0,
    item?.comment_id || 0,
    item?.created_at || "",
  ].join(":");
}

function rebalanceAllNotifications(rows, limit) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length || limit <= 0) return [];

  const caps = {
    like: Math.max(8, Math.ceil(limit * 0.45)),
    favorite: Math.max(6, Math.ceil(limit * 0.3)),
    comment: Math.max(10, Math.ceil(limit * 0.55)),
    follow: Math.max(4, Math.ceil(limit * 0.22)),
  };
  const mins = {
    like: Math.min(8, caps.like),
    favorite: Math.min(6, caps.favorite),
    comment: Math.min(8, caps.comment),
    follow: Math.min(4, caps.follow),
  };
  const countByType = { like: 0, favorite: 0, comment: 0, follow: 0 };
  const selected = [];
  const seen = new Set();
  const pushRow = (row) => {
    const key = notificationKey(row);
    if (seen.has(key)) return false;
    seen.add(key);
    selected.push(row);
    const type = String(row?.type || "");
    if (Object.prototype.hasOwnProperty.call(countByType, type)) {
      countByType[type] += 1;
    }
    return true;
  };

  for (const row of list) {
    if (selected.length >= limit) break;
    const type = String(row?.type || "");
    if (!Object.prototype.hasOwnProperty.call(mins, type)) continue;
    if (countByType[type] < mins[type]) pushRow(row);
  }

  for (const row of list) {
    if (selected.length >= limit) break;
    const type = String(row?.type || "");
    if (!Object.prototype.hasOwnProperty.call(caps, type)) continue;
    if (countByType[type] < caps[type]) pushRow(row);
  }

  for (const row of list) {
    if (selected.length >= limit) break;
    pushRow(row);
  }

  return selected.slice(0, limit);
}

// GET /api/notifications/state?user_id=1
router.get("/state", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = parseInt(req.query.user_id || "0", 10);
    if (!userId) return res.json({ success: true, state: { ...EMPTY_STATE } });
    const state = await fetchNotificationState(userId);
    res.json({ success: true, state });
  } catch (err) {
    console.error("notification state error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// POST /api/notifications/read { user_id, type }
router.post("/read", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = parseInt(req.body?.user_id || "0", 10);
    const type = String(req.body?.type || "all").toLowerCase();
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    const when = toMysqlLocalDatetime(new Date());
    if (!when) {
      return res.status(500).json({ success: false, message: "server error" });
    }

    if (type === "all") {
      await pool.query(
        `INSERT INTO user_notification_state
         (user_id, read_all_at, read_like_at, read_favorite_at, read_comment_at, read_follow_at, read_chat_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           read_all_at = VALUES(read_all_at),
           read_like_at = VALUES(read_like_at),
           read_favorite_at = VALUES(read_favorite_at),
           read_comment_at = VALUES(read_comment_at),
           read_follow_at = VALUES(read_follow_at),
           read_chat_at = VALUES(read_chat_at)`,
        [userId, when, when, when, when, when, when]
      );
      const state = await fetchNotificationState(userId);
      return res.json({ success: true, state });
    }

    const map = {
      like: "read_like_at",
      favorite: "read_favorite_at",
      comment: "read_comment_at",
      follow: "read_follow_at",
      chat: "read_chat_at",
    };
    const col = map[type];
    if (!col) {
      return res.status(400).json({ success: false, message: "invalid type" });
    }

    await pool.query(
      `INSERT INTO user_notification_state (user_id, ${col})
       VALUES (?, ?)
       ON DUPLICATE KEY UPDATE ${col} = VALUES(${col})`,
      [userId, when]
    );
    const state = await fetchNotificationState(userId);
    res.json({ success: true, state });
  } catch (err) {
    console.error("mark notification read error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/notifications?user_id=1
router.get("/", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = parseInt(req.query.user_id || "0", 10);
    if (!userId) return res.json({ success: true, data: [] });
    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "50", 10), 200));
    const branchLimit = Math.max(40, Math.min(limit * 2 + 20, 120));
    const commentBranchLimit = Math.max(24, Math.min(limit + 10, 70));
    const type = String(req.query.type || "all").toLowerCase();
    const allowedTypes = new Set(["all", "like", "favorite", "comment", "follow"]);
    const filterType = allowedTypes.has(type) ? type : "all";
    const unionScanLimit = filterType === "all"
      ? Math.min(Math.max(branchLimit * 4, 180), 520)
      : limit;

    let sinceSql = "";
    let sinceValue = "";
    if (req.query.since) {
      const d = new Date(String(req.query.since));
      if (!Number.isNaN(d.getTime())) {
        sinceValue = d.toISOString().slice(0, 19).replace("T", " ");
        sinceSql = " AND __CREATED_AT__ > ? ";
      }
    }

    const include = (t) => filterType === "all" || filterType === t;
    const pieces = [];
    const params = [];

    if (include("like")) {
      pieces.push(`
        SELECT * FROM (
          SELECT 'like' AS type, pl.created_at, pl.user_id AS actor_id,
                 pl.post_id AS post_id, NULL AS comment_id
          FROM post_likes pl
          WHERE pl.post_owner_id = ? AND pl.user_id <> ? ${sinceSql.replace("__CREATED_AT__", "pl.created_at")}
          ORDER BY pl.created_at DESC
          LIMIT ${branchLimit}
        ) t_like
      `);
      params.push(userId, userId);
      if (sinceValue) params.push(sinceValue);
    }
    if (include("favorite")) {
      pieces.push(`
        SELECT * FROM (
          SELECT 'favorite' AS type, pf.created_at, pf.user_id AS actor_id,
                 pf.post_id AS post_id, NULL AS comment_id
          FROM post_favorites pf
          WHERE pf.post_owner_id = ? AND pf.user_id <> ? ${sinceSql.replace("__CREATED_AT__", "pf.created_at")}
          ORDER BY pf.created_at DESC
          LIMIT ${branchLimit}
        ) t_favorite
      `);
      params.push(userId, userId);
      if (sinceValue) params.push(sinceValue);
    }
    if (include("comment")) {
      pieces.push(`
        SELECT * FROM (
          SELECT 'comment' AS type, pc.created_at, pc.user_id AS actor_id,
                 pc.post_id AS post_id, pc.id AS comment_id
          FROM post_comments pc
          WHERE pc.post_owner_id = ? AND pc.user_id <> ? ${sinceSql.replace("__CREATED_AT__", "pc.created_at")}
          ORDER BY pc.created_at DESC
          LIMIT ${commentBranchLimit}
        ) t_comment
      `);
      params.push(userId, userId);
      if (sinceValue) params.push(sinceValue);
    }
    if (include("follow")) {
      pieces.push(`
        SELECT * FROM (
          SELECT 'follow' AS type, uf.created_at, uf.follower_id AS actor_id,
                 NULL AS post_id, NULL AS comment_id
          FROM user_follows uf
          WHERE uf.following_id = ? ${sinceSql.replace("__CREATED_AT__", "uf.created_at")}
          ORDER BY uf.created_at DESC
          LIMIT ${branchLimit}
        ) t_follow
      `);
      params.push(userId);
      if (sinceValue) params.push(sinceValue);
    }

    if (!pieces.length) {
      const state = await fetchNotificationState(userId);
      return res.json({ success: true, data: [], state });
    }

    const query = `
      SELECT
        n.type,
        n.created_at,
        n.actor_id,
        u.nickname,
        u.avatar_url,
        n.post_id,
        p.title,
        CASE
          WHEN n.type = 'comment' THEN LEFT(COALESCE(pc.content, ''), 260)
          ELSE NULL
        END AS content
      FROM (
        ${pieces.join("\nUNION ALL\n")}
      ) AS n
      LEFT JOIN users u ON u.id = n.actor_id
      LEFT JOIN posts p ON p.id = n.post_id
      LEFT JOIN post_comments pc ON pc.id = n.comment_id
      ORDER BY n.created_at DESC
      LIMIT ?
    `;

    const [rows] = await pool.query(query, [...params, unionScanLimit]);
    const rankedRows = filterType === "all" ? rebalanceAllNotifications(rows || [], limit) : (rows || []).slice(0, limit);

    const state = await fetchNotificationState(userId);
    const data = rankedRows.map((item) => ({
      ...item,
      unread: isUnread(item, state),
    }));
    const cursor = data[0]?.created_at || null;
    res.json({ success: true, data, state, cursor });
  } catch (err) {
    console.error("notifications error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;

export function pushNotification(targetUserId, payload) {
  const conns = subscribers.get(Number(targetUserId));
  if (!conns || conns.size === 0) return;
  const data = `data: ${JSON.stringify({ ...payload, created_at: payload.created_at || new Date() })}\n\n`;
  conns.forEach((res) => {
    try {
      res.write(data);
    } catch (e) {
      // ignore broken pipe; cleanup on close
    }
  });
}

router.get("/stream", async (req, res) => {
  const userId = parseInt(req.query.user_id || "0", 10);
  if (!userId) {
    return res.status(400).end();
  }
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
  res.write(`data: ${JSON.stringify({ connected: true })}\n\n`);
  const set = subscribers.get(userId) || new Set();
  set.add(res);
  subscribers.set(userId, set);
  req.on("close", () => {
    set.delete(res);
    if (set.size === 0) subscribers.delete(userId);
  });
});
