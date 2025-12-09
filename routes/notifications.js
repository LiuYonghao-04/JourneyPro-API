import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();
const subscribers = new Map(); // userId -> Set(res)

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
}

// GET /api/notifications?user_id=1
router.get("/", async (req, res) => {
  try {
    await ensureTables();
    const userId = parseInt(req.query.user_id || "0", 10);
    if (!userId) return res.json({ success: true, data: [] });

    const [rows] = await pool.query(
      `
      SELECT * FROM (
        SELECT 'like' AS type, pl.created_at, pl.user_id AS actor_id, u.nickname, u.avatar_url,
               p.id AS post_id, p.title, NULL AS content
        FROM post_likes pl
        JOIN posts p ON pl.post_id = p.id
        LEFT JOIN users u ON pl.user_id = u.id
        WHERE p.user_id = ? AND pl.user_id <> ?
        UNION ALL
        SELECT 'favorite' AS type, pf.created_at, pf.user_id AS actor_id, u.nickname, u.avatar_url,
               p.id AS post_id, p.title, NULL AS content
        FROM post_favorites pf
        JOIN posts p ON pf.post_id = p.id
        LEFT JOIN users u ON pf.user_id = u.id
        WHERE p.user_id = ? AND pf.user_id <> ?
        UNION ALL
        SELECT 'comment' AS type, pc.created_at, pc.user_id AS actor_id, u.nickname, u.avatar_url,
               p.id AS post_id, p.title, pc.content AS content
        FROM post_comments pc
        JOIN posts p ON pc.post_id = p.id
        LEFT JOIN users u ON pc.user_id = u.id
        WHERE p.user_id = ? AND pc.user_id <> ?
        UNION ALL
        SELECT 'follow' AS type, uf.created_at, uf.follower_id AS actor_id, u.nickname, u.avatar_url,
               NULL AS post_id, NULL AS title, NULL AS content
        FROM user_follows uf
        LEFT JOIN users u ON uf.follower_id = u.id
        WHERE uf.following_id = ?
      ) AS notifications
      ORDER BY created_at DESC
      LIMIT 50
      `,
      [userId, userId, userId, userId, userId, userId, userId]
    );

    res.json({ success: true, data: rows });
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
