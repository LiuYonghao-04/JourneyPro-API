import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();
const DEFAULT_USER_ID = 1; // 若未登录，允许匿名记录到用户1

async function ensureTables() {
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
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_images (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      image_url VARCHAR(255) NOT NULL,
      sort_order INT DEFAULT 0
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_likes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_like (post_id, user_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_favorites (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_fav (post_id, user_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tags (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(50) NOT NULL UNIQUE,
      type VARCHAR(30) DEFAULT 'CATEGORY',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_tags (
      post_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      PRIMARY KEY (post_id, tag_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_comments (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      parent_id BIGINT NULL,
      content TEXT NOT NULL,
      like_count INT DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS comment_likes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      comment_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_comment_user (comment_id, user_id)
    );
  `);
}

const normalize = (row, imagesMap, tagsMap, userMap = {}) => ({
  id: row.id,
  title: row.title,
  content: row.content,
  cover_image: row.cover_image,
  images: imagesMap.get(row.id) || [],
  tags: tagsMap.get(row.id) || [],
  like_count: row.like_count || 0,
  favorite_count: row.favorite_count || 0,
  view_count: row.view_count || 0,
  rating: row.rating,
  status: row.status,
  created_at: row.created_at,
  user:
    userMap[row.id] || {
      id: row.user_id,
      nickname: row.nickname || "旅人",
      avatar_url: row.avatar_url || null,
    },
});

async function fetchImages(postIds) {
  if (postIds.length === 0) return new Map();
  const [rows] = await pool.query(
    `SELECT post_id, image_url FROM post_images WHERE post_id IN (?) ORDER BY sort_order, id`,
    [postIds]
  );
  const map = new Map();
  rows.forEach((r) => {
    if (!map.has(r.post_id)) map.set(r.post_id, []);
    map.get(r.post_id).push(r.image_url);
  });
  return map;
}

async function fetchTags(postIds) {
  if (postIds.length === 0) return new Map();
  const [rows] = await pool.query(
    `SELECT pt.post_id, t.name FROM post_tags pt JOIN tags t ON pt.tag_id = t.id WHERE pt.post_id IN (?)`,
    [postIds]
  );
  const map = new Map();
  rows.forEach((r) => {
    if (!map.has(r.post_id)) map.set(r.post_id, []);
    map.get(r.post_id).push(r.name);
  });
  return map;
}

async function upsertTags(tagNames) {
  if (!tagNames || tagNames.length === 0) return [];
  const ids = [];
  for (const name of tagNames) {
    const [rows] = await pool.query(`SELECT id FROM tags WHERE name = ? LIMIT 1`, [name]);
    if (rows.length > 0) {
      ids.push(rows[0].id);
    } else {
      const [res] = await pool.query(`INSERT INTO tags (name) VALUES (?)`, [name]);
      ids.push(res.insertId);
    }
  }
  return ids;
}

router.get("/", async (req, res) => {
  try {
    await ensureTables();
    const limit = Math.min(parseInt(req.query.limit || "30", 10), 50);
    const offset = parseInt(req.query.offset || "0", 10);
    const sort = req.query.sort === "hot" ? "hot" : "latest";
    const orderBy =
      sort === "hot"
        ? "p.view_count DESC, p.like_count DESC, p.favorite_count DESC, p.created_at DESC"
        : "p.created_at DESC";

    const [rows] = await pool.query(
      `SELECT p.*, u.nickname, u.avatar_url
       FROM posts p
       LEFT JOIN users u ON p.user_id = u.id
       WHERE p.status = 'NORMAL'
       ORDER BY ${orderBy}
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    const ids = rows.map((r) => r.id);
    const imagesMap = await fetchImages(ids);
    const tagsMap = await fetchTags(ids);

    const data = rows.map((r) => normalize(r, imagesMap, tagsMap));
    res.json({ success: true, data });
  } catch (err) {
    console.error("list posts error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/:id", async (req, res) => {
  try {
    await ensureTables();
    const id = parseInt(req.params.id, 10);
    await pool.query(`UPDATE posts SET view_count = view_count + 1 WHERE id = ?`, [id]);
    const [[row]] = await pool.query(
      `SELECT p.*, u.nickname, u.avatar_url
       FROM posts p LEFT JOIN users u ON p.user_id = u.id WHERE p.id = ? LIMIT 1`,
      [id]
    );
    if (!row) return res.status(404).json({ success: false, message: "not found" });
    const imagesMap = await fetchImages([id]);
    const tagsMap = await fetchTags([id]);
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap) });
  } catch (err) {
    console.error("get post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/", async (req, res) => {
  try {
    await ensureTables();
    const {
      user_id,
      poi_id,
      title,
      content,
      rating,
      images = [],
      tags = [],
    } = req.body || {};
    if (!title || !content) {
      return res.status(400).json({ success: false, message: "title/content required" });
    }
    const authorId = user_id || DEFAULT_USER_ID;
    const cover = images[0] || null;
    const [result] = await pool.query(
      `INSERT INTO posts (user_id, poi_id, title, content, rating, cover_image, image_count)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [authorId, poi_id || null, title, content, rating || null, cover, images.length]
    );
    const postId = result.insertId;

    if (images.length > 0) {
      const rows = images.map((url, idx) => [postId, url, idx]);
      await pool.query(`INSERT INTO post_images (post_id, image_url, sort_order) VALUES ?`, [rows]);
    }

    const tagIds = await upsertTags(tags);
    if (tagIds.length > 0) {
      const rows = tagIds.map((tid) => [postId, tid]);
      await pool.query(`INSERT IGNORE INTO post_tags (post_id, tag_id) VALUES ?`, [rows]);
    }

    const [[row]] = await pool.query(
      `SELECT p.*, u.nickname, u.avatar_url FROM posts p LEFT JOIN users u ON p.user_id = u.id WHERE p.id = ?`,
      [postId]
    );
    const imagesMap = await fetchImages([postId]);
    const tagsMap = await fetchTags([postId]);
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap) });
  } catch (err) {
    console.error("create post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/:id/like", async (req, res) => {
  try {
    await ensureTables();
    const postId = parseInt(req.params.id, 10);
    const userId = req.body?.user_id || DEFAULT_USER_ID;
    const [[existing]] = await pool.query(
      `SELECT id FROM post_likes WHERE post_id = ? AND user_id = ? LIMIT 1`,
      [postId, userId]
    );
    if (existing) {
      await pool.query(`DELETE FROM post_likes WHERE id = ?`, [existing.id]);
      await pool.query(`UPDATE posts SET like_count = GREATEST(like_count - 1, 0) WHERE id = ?`, [postId]);
    } else {
      await pool.query(`INSERT INTO post_likes (post_id, user_id) VALUES (?, ?)`, [postId, userId]);
      await pool.query(`UPDATE posts SET like_count = like_count + 1 WHERE id = ?`, [postId]);
    }
    const [[row]] = await pool.query(
      `SELECT p.*, u.nickname, u.avatar_url FROM posts p LEFT JOIN users u ON p.user_id = u.id WHERE p.id = ?`,
      [postId]
    );
    if (!row) return res.status(404).json({ success: false, message: "not found" });
    const imagesMap = await fetchImages([postId]);
    const tagsMap = await fetchTags([postId]);
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap), liked: !existing });
  } catch (err) {
    console.error("like post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/:id/favorite", async (req, res) => {
  try {
    await ensureTables();
    const postId = parseInt(req.params.id, 10);
    const userId = req.body?.user_id || DEFAULT_USER_ID;
    const [[existing]] = await pool.query(
      `SELECT id FROM post_favorites WHERE post_id = ? AND user_id = ? LIMIT 1`,
      [postId, userId]
    );
    if (existing) {
      await pool.query(`DELETE FROM post_favorites WHERE id = ?`, [existing.id]);
      await pool.query(
        `UPDATE posts SET favorite_count = GREATEST(favorite_count - 1, 0) WHERE id = ?`,
        [postId]
      );
    } else {
      await pool.query(`INSERT INTO post_favorites (post_id, user_id) VALUES (?, ?)`, [postId, userId]);
      await pool.query(`UPDATE posts SET favorite_count = favorite_count + 1 WHERE id = ?`, [postId]);
    }
    res.json({ success: true, favorited: !existing });
  } catch (err) {
    console.error("fav post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// 获取标签列表
router.get("/tags/list", async (_req, res) => {
  try {
    await ensureTables();
    const [rows] = await pool.query(`SELECT id, name, type FROM tags ORDER BY id DESC LIMIT 50`);
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("list tags error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// 获取评论列表（含一级+二级）
router.get("/:id/comments", async (req, res) => {
  try {
    await ensureTables();
    const postId = parseInt(req.params.id, 10);
    const [rows] = await pool.query(
      `SELECT c.*, u.nickname, u.avatar_url
       FROM post_comments c
       LEFT JOIN users u ON c.user_id = u.id
       WHERE c.post_id = ?
       ORDER BY c.created_at ASC`,
      [postId]
    );
    const map = new Map();
    rows.forEach((r) => {
      const item = {
        id: r.id,
        post_id: r.post_id,
        user_id: r.user_id,
        parent_id: r.parent_id,
        content: r.content,
        like_count: r.like_count || 0,
        created_at: r.created_at,
        user: { id: r.user_id, nickname: r.nickname || "旅人", avatar_url: r.avatar_url || null },
        replies: [],
      };
      map.set(r.id, item);
    });
    const roots = [];
    map.forEach((c) => {
      if (c.parent_id && map.has(c.parent_id)) {
        map.get(c.parent_id).replies.push(c);
      } else {
        roots.push(c);
      }
    });
    res.json({ success: true, data: roots });
  } catch (err) {
    console.error("list comments error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// 新增评论 / 追评
router.post("/:id/comments", async (req, res) => {
  try {
    await ensureTables();
    const postId = parseInt(req.params.id, 10);
    const { content, parent_id, user_id } = req.body || {};
    if (!content) return res.status(400).json({ success: false, message: "content required" });
    const uid = user_id || DEFAULT_USER_ID;
    const [r] = await pool.query(
      `INSERT INTO post_comments (post_id, user_id, parent_id, content) VALUES (?, ?, ?, ?)`,
      [postId, uid, parent_id || null, content]
    );
    const commentId = r.insertId;
    const [[row]] = await pool.query(
      `SELECT c.*, u.nickname, u.avatar_url
       FROM post_comments c LEFT JOIN users u ON c.user_id = u.id WHERE c.id = ?`,
      [commentId]
    );
    const item = {
      id: row.id,
      post_id: row.post_id,
      user_id: row.user_id,
      parent_id: row.parent_id,
      content: row.content,
      like_count: row.like_count || 0,
      created_at: row.created_at,
      user: { id: row.user_id, nickname: row.nickname || "旅人", avatar_url: row.avatar_url || null },
      replies: [],
    };
    res.json({ success: true, data: item });
  } catch (err) {
    console.error("create comment error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// 评论点赞/取消
router.post("/comments/:cid/like", async (req, res) => {
  try {
    await ensureTables();
    const cid = parseInt(req.params.cid, 10);
    const userId = req.body?.user_id || DEFAULT_USER_ID;
    const [[existing]] = await pool.query(
      `SELECT id FROM comment_likes WHERE comment_id = ? AND user_id = ? LIMIT 1`,
      [cid, userId]
    );
    if (existing) {
      await pool.query(`DELETE FROM comment_likes WHERE id = ?`, [existing.id]);
      await pool.query(`UPDATE post_comments SET like_count = GREATEST(like_count - 1, 0) WHERE id = ?`, [cid]);
    } else {
      await pool.query(`INSERT INTO comment_likes (comment_id, user_id) VALUES (?, ?)`, [cid, userId]);
      await pool.query(`UPDATE post_comments SET like_count = like_count + 1 WHERE id = ?`, [cid]);
    }
    const [[row]] = await pool.query(
      `SELECT c.*, u.nickname, u.avatar_url
       FROM post_comments c LEFT JOIN users u ON c.user_id = u.id WHERE c.id = ?`,
      [cid]
    );
    const data = {
      id: row.id,
      post_id: row.post_id,
      user_id: row.user_id,
      parent_id: row.parent_id,
      content: row.content,
      like_count: row.like_count || 0,
      created_at: row.created_at,
      user: { id: row.user_id, nickname: row.nickname || "旅人", avatar_url: row.avatar_url || null },
    };
    res.json({ success: true, data, liked: !existing });
  } catch (err) {
    console.error("like comment error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
