import express from "express";
import { pool } from "../db/connect.js";
import { pushNotification } from "./notifications.js";

const router = express.Router();
const DEFAULT_USER_ID = 1; // 若未登录，允许匿名记录到用户1
const POST_SELECT_FIELDS = `
  p.*,
  u.nickname,
  u.avatar_url,
  poi.name AS poi_name,
  poi.category AS poi_category,
  poi.lat AS poi_lat,
  poi.lng AS poi_lng,
  poi.image_url AS poi_image_url,
  poi.address AS poi_address,
  poi.city AS poi_city
`;
const POST_FEED_SELECT_FIELDS = `
  p.id,
  p.user_id,
  p.poi_id,
  p.title,
  LEFT(p.content, 360) AS content,
  p.cover_image,
  p.like_count,
  p.favorite_count,
  p.view_count,
  p.created_at,
  u.nickname,
  u.avatar_url,
  poi.name AS poi_name,
  poi.category AS poi_category,
  poi.lat AS poi_lat,
  poi.lng AS poi_lng,
  poi.image_url AS poi_image_url
`;
const POST_FEED_LITE_SELECT_FIELDS = `
  p.id,
  p.user_id,
  p.poi_id,
  p.title,
  LEFT(p.content, 120) AS content,
  p.cover_image,
  p.like_count,
  p.favorite_count,
  p.view_count,
  p.created_at,
  u.nickname,
  u.avatar_url,
  poi.name AS poi_name,
  poi.lat AS poi_lat,
  poi.lng AS poi_lng
`;

async function tryAlter(sql) {
  try {
    await pool.query(sql);
  } catch (e) {
    const msg = String(e?.message || e);
    // swallow benign duplicate errors
    if (
      !msg.includes("Duplicate column") &&
      !msg.includes("check that column/key exists") &&
      !msg.includes("Unknown column 'parent_id'")
    ) {
      console.error("alter table failed:", msg);
    }
  }
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT NOT NULL,
      poi_id BIGINT NULL,
      title VARCHAR(100) NOT NULL,
      content TEXT NOT NULL,
      rating TINYINT NULL,
      cover_image VARCHAR(600) NULL,
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
      image_url VARCHAR(600) NOT NULL,
      sort_order INT DEFAULT 0
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS poi_photos (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      poi_id BIGINT NOT NULL,
      image_url VARCHAR(600) NOT NULL,
      source VARCHAR(40) NOT NULL DEFAULT 'AUTO',
      sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_poi_photo (poi_id, image_url(255)),
      INDEX idx_poi_sort (poi_id, sort_order, id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_likes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_like (post_id, user_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_favorites (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      post_owner_id BIGINT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_fav (post_id, user_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_views (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      user_id BIGINT NOT NULL,
      view_count INT DEFAULT 1,
      last_viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_post_user_view (post_id, user_id)
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
      post_owner_id BIGINT NULL,
      parent_comment_id BIGINT NULL,
      type VARCHAR(20) DEFAULT 'COMMENT',
      content TEXT NOT NULL,
      like_count INT DEFAULT 0,
      reply_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'NORMAL',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
  // lightweight migrations for older schemas
  await tryAlter(`ALTER TABLE post_comments CHANGE COLUMN parent_id parent_comment_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_comments ADD COLUMN type VARCHAR(20) DEFAULT 'COMMENT'`);
  await tryAlter(`ALTER TABLE post_comments ADD COLUMN reply_count INT DEFAULT 0`);
  await tryAlter(`ALTER TABLE post_comments ADD COLUMN status VARCHAR(20) DEFAULT 'NORMAL'`);
  await tryAlter(
    `ALTER TABLE post_comments ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`
  );
  await tryAlter(`ALTER TABLE post_likes ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_favorites ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE post_comments ADD COLUMN post_owner_id BIGINT NULL`);
  await tryAlter(`ALTER TABLE posts MODIFY poi_id BIGINT NULL DEFAULT NULL`);
  await tryAlter(`ALTER TABLE posts MODIFY cover_image VARCHAR(600) NULL`);
  await tryAlter(`ALTER TABLE post_images MODIFY image_url VARCHAR(600) NOT NULL`);
  await tryAlter(`ALTER TABLE post_images ADD INDEX idx_post_images_post_sort (post_id, sort_order, id)`);
  await tryAlter(`ALTER TABLE post_tags ADD INDEX idx_post_tags_tag_post (tag_id, post_id)`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_fav_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_user_post (user_id, post_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_favorites_user_post (user_id, post_id)`);
  await tryAlter(`ALTER TABLE post_likes ADD INDEX idx_post_likes_user_recent (user_id, id, post_id)`);
  await tryAlter(`ALTER TABLE post_favorites ADD INDEX idx_post_favorites_user_recent (user_id, id, post_id)`);
  await tryAlter(`ALTER TABLE post_comments ADD INDEX idx_post_comments_owner_created (post_owner_id, created_at, post_id, user_id)`);
  await tryAlter(`ALTER TABLE posts ADD INDEX idx_posts_status_created (status, created_at, id)`);
  await tryAlter(`ALTER TABLE posts ADD INDEX idx_posts_user_created (user_id, created_at, id)`);
  await tryAlter(`ALTER TABLE posts ADD INDEX idx_posts_status_poi_created (status, poi_id, created_at, id)`);
  await tryAlter(
    `ALTER TABLE posts ADD INDEX idx_posts_status_hot (status, view_count, like_count, favorite_count, created_at, id)`
  );
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

const normalize = (row, imagesMap, tagsMap, poiPhotosMap = new Map(), userMap = {}) => ({
  id: row.id,
  poi_id: row.poi_id,
  title: row.title,
  content: row.content,
  cover_image: row.cover_image,
  images: imagesMap.get(row.id) || [],
  tags: tagsMap.get(row.id) || [],
  like_count: row.like_count || 0,
  favorite_count: row.favorite_count || 0,
  view_count: row.view_count || 0,
  _liked: Number(row.liked_by_viewer || row._liked || 0) > 0,
  _fav: Number(row.favorited_by_viewer || row._fav || 0) > 0,
  rating: row.rating,
  status: row.status,
  created_at: row.created_at,
  poi: row.poi_id
    ? {
        id: row.poi_id,
        name: row.poi_name || null,
        category: row.poi_category || null,
        lat: Number.isFinite(Number(row.poi_lat)) ? Number(row.poi_lat) : null,
        lng: Number.isFinite(Number(row.poi_lng)) ? Number(row.poi_lng) : null,
        image_url: row.poi_image_url || null,
        address: row.poi_address || null,
        city: row.poi_city || null,
        photos: poiPhotosMap.get(Number(row.poi_id)) || (row.poi_image_url ? [row.poi_image_url] : []),
      }
    : null,
  user:
    userMap[row.id] || {
      id: row.user_id,
      nickname: row.nickname || "旅人",
      avatar_url: row.avatar_url || null,
    },
});

const normalizeCompact = (row, primaryImageMap, tagsMap, userMap = {}) => {
  const primaryImage = row.cover_image || primaryImageMap.get(row.id) || "";
  return {
    id: row.id,
    poi_id: row.poi_id,
    title: row.title,
    content: String(row.content || ""),
    cover_image: primaryImage || null,
    images: primaryImage ? [primaryImage] : [],
    tags: tagsMap.get(row.id) || [],
    like_count: row.like_count || 0,
    favorite_count: row.favorite_count || 0,
    view_count: row.view_count || 0,
    _liked: Number(row.liked_by_viewer || row._liked || 0) > 0,
    _fav: Number(row.favorited_by_viewer || row._fav || 0) > 0,
    created_at: row.created_at,
    poi: row.poi_id
      ? {
          id: row.poi_id,
          name: row.poi_name || null,
          category: row.poi_category || null,
          lat: Number.isFinite(Number(row.poi_lat)) ? Number(row.poi_lat) : null,
          lng: Number.isFinite(Number(row.poi_lng)) ? Number(row.poi_lng) : null,
          image_url: row.poi_image_url || null,
        }
      : null,
    user:
      userMap[row.id] || {
        id: row.user_id,
        nickname: row.nickname || "鏃呬汉",
        avatar_url: row.avatar_url || null,
      },
  };
};

async function fetchUserProfile(userId) {
  const [[u]] = await pool.query(`SELECT id, nickname, avatar_url FROM users WHERE id = ? LIMIT 1`, [userId]);
  return u || { id: userId, nickname: "旅人", avatar_url: null };
}

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

async function fetchPrimaryImages(postIds) {
  if (postIds.length === 0) return new Map();
  const [rows] = await pool.query(
    `
      SELECT post_id, image_url
      FROM (
        SELECT
          post_id,
          image_url,
          ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY sort_order ASC, id ASC) AS rn
        FROM post_images
        WHERE post_id IN (?)
      ) t
      WHERE rn = 1
    `,
    [postIds]
  );
  const map = new Map();
  rows.forEach((row) => {
    map.set(row.post_id, row.image_url);
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

async function fetchPoiPhotosByIds(poiIds, limitPerPoi = 6) {
  const ids = [...new Set((poiIds || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  if (!ids.length) return new Map();

  const [rows] = await pool.query(
    `
      SELECT poi_id, image_url
      FROM (
        SELECT
          poi_id,
          image_url,
          ROW_NUMBER() OVER (PARTITION BY poi_id ORDER BY sort_order ASC, id ASC) AS rn
        FROM poi_photos
        WHERE poi_id IN (?)
      ) t
      WHERE rn <= ?
      ORDER BY poi_id ASC, rn ASC
    `,
    [ids, Math.max(1, Math.min(limitPerPoi, 12))]
  );
  const map = new Map();
  rows.forEach((row) => {
    const poiId = Number(row.poi_id);
    if (!map.has(poiId)) map.set(poiId, []);
    map.get(poiId).push(row.image_url);
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
    await ensureTablesReady();
    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "30", 10) || 30, 50));
    const offset = Math.max(0, parseInt(req.query.offset || "0", 10) || 0);
    const sort = req.query.sort === "hot" ? "hot" : "latest";
    const compact = req.query.compact === "1";
    const lite = compact && req.query.lite === "1";
    const tag = typeof req.query.tag === "string" ? req.query.tag.trim() : "";
    const userId = req.query.user_id ? parseInt(req.query.user_id, 10) : null;
    const viewerIdRaw = req.query.viewer_id ? parseInt(req.query.viewer_id, 10) : null;
    const viewerId = Number.isFinite(viewerIdRaw) && viewerIdRaw > 0 ? viewerIdRaw : null;
    const likedBy = req.query.liked_by ? parseInt(req.query.liked_by, 10) : null;
    const favoritedBy = req.query.favorited_by ? parseInt(req.query.favorited_by, 10) : null;
    const poiId = req.query.poi_id ? parseInt(req.query.poi_id, 10) : null;
    const cursorCreatedAtRaw = req.query.cursor_created_at ? new Date(String(req.query.cursor_created_at)) : null;
    const cursorIdRaw = req.query.cursor_id ? parseInt(req.query.cursor_id, 10) : null;
    const hasCursor =
      sort === "latest" &&
      cursorCreatedAtRaw instanceof Date &&
      !Number.isNaN(cursorCreatedAtRaw.getTime()) &&
      Number.isFinite(cursorIdRaw) &&
      cursorIdRaw > 0;
    const isPublicFeedCacheable =
      compact && lite && !viewerId && !userId && !likedBy && !favoritedBy && !poiId && !tag && !hasCursor;
    if (isPublicFeedCacheable) {
      res.setHeader("Cache-Control", "public, max-age=20, stale-while-revalidate=40");
    } else {
      res.setHeader("Cache-Control", "no-store");
    }

    let where = "p.status = 'NORMAL'";
    const params = [];
    if (userId) {
      where += " AND p.user_id = ?";
      params.push(userId);
    }
    if (likedBy) {
      where +=
        " AND p.id IN (SELECT t.post_id FROM (SELECT pl.post_id FROM post_likes pl WHERE pl.user_id = ? ORDER BY pl.id DESC LIMIT 30000) t)";
      params.push(likedBy);
    }
    if (favoritedBy) {
      where +=
        " AND p.id IN (SELECT t.post_id FROM (SELECT pf.post_id FROM post_favorites pf WHERE pf.user_id = ? ORDER BY pf.id DESC LIMIT 30000) t)";
      params.push(favoritedBy);
    }
    if (poiId) {
      where += " AND p.poi_id = ?";
      params.push(poiId);
    }
    if (tag) {
      const [[tagRow]] = await pool.query(`SELECT id FROM tags WHERE name = ? LIMIT 1`, [tag]);
      if (!tagRow?.id) {
        return res.json({ success: true, data: [], next_cursor: null });
      }
      where += " AND EXISTS (SELECT 1 FROM post_tags pt WHERE pt.post_id = p.id AND pt.tag_id = ?)";
      params.push(Number(tagRow.id));
    }
    if (hasCursor) {
      where += " AND (p.created_at < ? OR (p.created_at = ? AND p.id < ?))";
      params.push(cursorCreatedAtRaw, cursorCreatedAtRaw, cursorIdRaw);
    }

    const orderBy =
      sort === "hot"
        ? "p.view_count DESC, p.like_count DESC, p.favorite_count DESC, p.created_at DESC"
        : "p.created_at DESC, p.id DESC";
    let postsFrom = "posts p";
    if (sort === "hot") {
      postsFrom = "posts p FORCE INDEX (idx_posts_status_hot)";
    } else if (userId) {
      postsFrom = "posts p FORCE INDEX (idx_posts_user_created)";
    } else if (poiId) {
      postsFrom = "posts p FORCE INDEX (idx_posts_status_poi_created)";
    } else {
      postsFrom = "posts p FORCE INDEX (idx_posts_status_created)";
    }

    const selectFields = compact
      ? lite
        ? POST_FEED_LITE_SELECT_FIELDS
        : POST_FEED_SELECT_FIELDS
      : POST_SELECT_FIELDS;
    const viewerSelect = viewerId
      ? `,
         IF(plv.id IS NULL, 0, 1) AS liked_by_viewer,
         IF(pfv.id IS NULL, 0, 1) AS favorited_by_viewer
        `
      : "";
    const viewerJoin = viewerId
      ? `
       LEFT JOIN post_likes plv ON plv.post_id = p.id AND plv.user_id = ?
       LEFT JOIN post_favorites pfv ON pfv.post_id = p.id AND pfv.user_id = ?
      `
      : "";
    const pagingSql = hasCursor ? "LIMIT ?" : "LIMIT ? OFFSET ?";
    const queryParams = viewerId
      ? [viewerId, viewerId, ...params, limit, ...(hasCursor ? [] : [offset])]
      : [...params, limit, ...(hasCursor ? [] : [offset])];
    const [rows] = await pool.query(
      `SELECT ${selectFields}${viewerSelect}
       FROM ${postsFrom}
       LEFT JOIN users u ON p.user_id = u.id
       LEFT JOIN poi ON p.poi_id = poi.id
       ${viewerJoin}
       WHERE ${where}
       ORDER BY ${orderBy}
       ${pagingSql}`,
      queryParams
    );
    const ids = rows.map((r) => r.id);
    const tagsMap = lite ? new Map() : await fetchTags(ids);
    let data = [];
    if (compact) {
      const needsPrimaryLookup = rows.some((row) => !row.cover_image);
      const primaryImageMap = needsPrimaryLookup ? await fetchPrimaryImages(ids) : new Map();
      data = rows.map((r) => normalizeCompact(r, primaryImageMap, tagsMap));
    } else {
      const imagesMap = await fetchImages(ids);
      const poiIds = rows.map((r) => r.poi_id).filter(Boolean);
      const poiPhotosMap = await fetchPoiPhotosByIds(poiIds, 6);
      data = rows.map((r) => normalize(r, imagesMap, tagsMap, poiPhotosMap));
    }
    const nextCursor =
      sort === "latest" && rows.length
        ? {
            created_at: rows[rows.length - 1].created_at,
            id: rows[rows.length - 1].id,
          }
        : null;
    res.json({ success: true, data, next_cursor: nextCursor });
  } catch (err) {
    console.error("list posts error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/reactions/summary", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = Number.parseInt(String(req.query.user_id || ""), 10);
    if (!Number.isFinite(userId) || userId <= 0) {
      return res.json({ success: true, data: { liked_ids: [], favorited_ids: [] } });
    }
    const limit = Math.max(20, Math.min(Number.parseInt(String(req.query.limit || "500"), 10) || 500, 5000));
    const [likedRows, favoritedRows] = await Promise.all([
      pool.query(`SELECT post_id FROM post_likes WHERE user_id = ? ORDER BY id DESC LIMIT ?`, [userId, limit]),
      pool.query(`SELECT post_id FROM post_favorites WHERE user_id = ? ORDER BY id DESC LIMIT ?`, [userId, limit]),
    ]);
    const likedIds = likedRows[0].map((row) => Number(row.post_id)).filter((id) => Number.isFinite(id) && id > 0);
    const favoritedIds = favoritedRows[0]
      .map((row) => Number(row.post_id))
      .filter((id) => Number.isFinite(id) && id > 0);
    res.json({ success: true, data: { liked_ids: likedIds, favorited_ids: favoritedIds } });
  } catch (err) {
    console.error("reactions summary error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/:id", async (req, res) => {
  try {
    await ensureTablesReady();
    const id = parseInt(req.params.id, 10);
    const viewerId = req.query.user_id ? parseInt(req.query.user_id, 10) : null;
    await pool.query(`UPDATE posts SET view_count = view_count + 1 WHERE id = ?`, [id]);
    if (viewerId) {
      await pool.query(
        `INSERT INTO post_views (post_id, user_id, view_count)
         VALUES (?, ?, 1)
         ON DUPLICATE KEY UPDATE view_count = view_count + 1, last_viewed_at = CURRENT_TIMESTAMP`,
        [id, viewerId]
      );
    }
    const [[row]] = await pool.query(
      `SELECT ${POST_SELECT_FIELDS}
       FROM posts p
       LEFT JOIN users u ON p.user_id = u.id
       LEFT JOIN poi ON p.poi_id = poi.id
       WHERE p.id = ? LIMIT 1`,
      [id]
    );
    if (!row) return res.status(404).json({ success: false, message: "not found" });
    const imagesMap = await fetchImages([id]);
    const tagsMap = await fetchTags([id]);
    const poiPhotosMap = await fetchPoiPhotosByIds([row.poi_id], 6);
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap, poiPhotosMap) });
  } catch (err) {
    console.error("get post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/", async (req, res) => {
  try {
    await ensureTablesReady();
    const {
      user_id,
      poi_id,
      title,
      content,
      rating,
      images = [],
      tags = [],
    } = req.body || {};
    const missing = [];
    if (!title) missing.push("title");
    if (!content) missing.push("content");
    if (!Array.isArray(images) || images.length === 0) missing.push("images");
    if (!Array.isArray(tags) || tags.length === 0) missing.push("tags");
    if (missing.length > 0) {
      return res
        .status(400)
        .json({ success: false, message: `Missing required field(s): ${missing.join(", ")}` });
    }
    const authorId = user_id || DEFAULT_USER_ID;
    let poiVal =
      poi_id === null || poi_id === undefined || poi_id === ""
        ? null
        : Number.isNaN(Number(poi_id))
        ? null
        : Number(poi_id);
    if (poiVal) {
      const [[poiRow]] = await pool.query(`SELECT id FROM poi WHERE id = ? LIMIT 1`, [poiVal]);
      if (!poiRow) {
        return res.status(400).json({ success: false, message: "poi_id does not exist" });
      }
      poiVal = poiRow.id;
    } else {
      poiVal = null;
    }
    const cover = images[0] || null;
    const [result] = await pool.query(
      `INSERT INTO posts (user_id, poi_id, title, content, rating, cover_image, image_count)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [authorId, poiVal, title, content, rating || null, cover, images.length]
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
      `SELECT ${POST_SELECT_FIELDS}
       FROM posts p
       LEFT JOIN users u ON p.user_id = u.id
       LEFT JOIN poi ON p.poi_id = poi.id
       WHERE p.id = ?`,
      [postId]
    );
    const imagesMap = await fetchImages([postId]);
    const tagsMap = await fetchTags([postId]);
    const poiPhotosMap = await fetchPoiPhotosByIds([row?.poi_id], 6);
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap, poiPhotosMap) });
  } catch (err) {
    console.error("create post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/:id/like", async (req, res) => {
  try {
    await ensureTablesReady();
    const postId = parseInt(req.params.id, 10);
    const userId = req.body?.user_id || DEFAULT_USER_ID;
    const [[existing]] = await pool.query(
      `SELECT id FROM post_likes WHERE post_id = ? AND user_id = ? LIMIT 1`,
      [postId, userId]
    );
    let liked = false;
    if (existing) {
      await pool.query(`DELETE FROM post_likes WHERE id = ?`, [existing.id]);
      await pool.query(`UPDATE posts SET like_count = GREATEST(like_count - 1, 0) WHERE id = ?`, [postId]);
    } else {
      await pool.query(
        `
          INSERT INTO post_likes (post_id, user_id, post_owner_id)
          SELECT ?, ?, p.user_id
          FROM posts p
          WHERE p.id = ?
          LIMIT 1
        `,
        [postId, userId, postId]
      );
      await pool.query(`UPDATE posts SET like_count = like_count + 1 WHERE id = ?`, [postId]);
      liked = true;
    }
    const [[row]] = await pool.query(
      `SELECT ${POST_SELECT_FIELDS}
       FROM posts p
       LEFT JOIN users u ON p.user_id = u.id
       LEFT JOIN poi ON p.poi_id = poi.id
       WHERE p.id = ?`,
      [postId]
    );
    if (!row) return res.status(404).json({ success: false, message: "not found" });
    const imagesMap = await fetchImages([postId]);
    const tagsMap = await fetchTags([postId]);
    const poiPhotosMap = await fetchPoiPhotosByIds([row.poi_id], 6);
    if (liked && row.user_id && row.user_id !== userId) {
      const actor = await fetchUserProfile(userId);
      pushNotification(row.user_id, {
        type: "like",
        actor_id: userId,
        actor_nickname: actor.nickname,
        actor_avatar: actor.avatar_url,
        post_id: postId,
        title: row.title,
      });
    }
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap, poiPhotosMap), liked: !existing });
  } catch (err) {
    console.error("like post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/:id/favorite", async (req, res) => {
  try {
    await ensureTablesReady();
    const postId = parseInt(req.params.id, 10);
    const userId = req.body?.user_id || DEFAULT_USER_ID;
    const [[existing]] = await pool.query(
      `SELECT id FROM post_favorites WHERE post_id = ? AND user_id = ? LIMIT 1`,
      [postId, userId]
    );
    let favored = false;
    if (existing) {
      await pool.query(`DELETE FROM post_favorites WHERE id = ?`, [existing.id]);
      await pool.query(
        `UPDATE posts SET favorite_count = GREATEST(favorite_count - 1, 0) WHERE id = ?`,
        [postId]
      );
    } else {
      await pool.query(
        `
          INSERT INTO post_favorites (post_id, user_id, post_owner_id)
          SELECT ?, ?, p.user_id
          FROM posts p
          WHERE p.id = ?
          LIMIT 1
        `,
        [postId, userId, postId]
      );
      await pool.query(`UPDATE posts SET favorite_count = favorite_count + 1 WHERE id = ?`, [postId]);
      favored = true;
    }
    const [[row]] = await pool.query(
      `SELECT ${POST_SELECT_FIELDS}
       FROM posts p
       LEFT JOIN users u ON p.user_id = u.id
       LEFT JOIN poi ON p.poi_id = poi.id
       WHERE p.id = ?`,
      [postId]
    );
    const imagesMap = await fetchImages([postId]);
    const tagsMap = await fetchTags([postId]);
    const poiPhotosMap = await fetchPoiPhotosByIds([row?.poi_id], 6);
    if (favored && row.user_id && row.user_id !== userId) {
      const actor = await fetchUserProfile(userId);
      pushNotification(row.user_id, {
        type: "favorite",
        actor_id: userId,
        actor_nickname: actor.nickname,
        actor_avatar: actor.avatar_url,
        post_id: postId,
        title: row.title,
      });
    }
    res.json({ success: true, data: normalize(row, imagesMap, tagsMap, poiPhotosMap), favorited: !existing });
  } catch (err) {
    console.error("fav post error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// 获取标签列表
router.get("/tags/list", async (_req, res) => {
  try {
    await ensureTablesReady();
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=900");
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
    await ensureTablesReady();
    const postId = parseInt(req.params.id, 10);
    const viewerId = req.query.user_id ? parseInt(req.query.user_id, 10) : null;
    const [rows] = await pool.query(
      `SELECT c.*, u.nickname, u.avatar_url, ${viewerId ? "IF(cl.id IS NULL, 0, 1)" : "0"} AS liked_by_user
       FROM post_comments c
       LEFT JOIN users u ON c.user_id = u.id
       ${viewerId ? "LEFT JOIN comment_likes cl ON cl.comment_id = c.id AND cl.user_id = ?" : ""}
       WHERE c.post_id = ?
       ORDER BY c.created_at ASC`,
      viewerId ? [viewerId, postId] : [postId]
    );
    const map = new Map();
    rows.forEach((r) => {
      const parentId = r.parent_comment_id ?? r.parent_id;
      const item = {
        id: r.id,
        post_id: r.post_id,
        user_id: r.user_id,
        parent_id: parentId,
        content: r.content,
        like_count: r.like_count || 0,
        created_at: r.created_at,
        liked_by_user: !!r.liked_by_user,
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
    await ensureTablesReady();
    const postId = parseInt(req.params.id, 10);
    const { content, parent_id, parent_comment_id, user_id } = req.body || {};
    if (!content) return res.status(400).json({ success: false, message: "content required" });
    const uid = user_id || DEFAULT_USER_ID;
    const parentId = parent_comment_id || parent_id || null;
    const type = parentId ? "REPLY" : "COMMENT";
    const [r] = await pool.query(
      `
        INSERT INTO post_comments (post_id, user_id, post_owner_id, parent_comment_id, type, content)
        SELECT ?, ?, p.user_id, ?, ?, ?
        FROM posts p
        WHERE p.id = ?
        LIMIT 1
      `,
      [postId, uid, parentId, type, content, postId]
    );
    if (!r?.insertId) {
      return res.status(404).json({ success: false, message: "post not found" });
    }
    const commentId = r.insertId;
    const [[postRow]] = await pool.query(`SELECT user_id, title FROM posts WHERE id = ? LIMIT 1`, [postId]);
    if (postRow?.user_id && postRow.user_id !== uid) {
      const actor = await fetchUserProfile(uid);
      pushNotification(postRow.user_id, {
        type: "comment",
        actor_id: uid,
        actor_nickname: actor.nickname,
        actor_avatar: actor.avatar_url,
        post_id: postId,
        title: postRow.title,
        content,
      });
    }
    if (parentId) {
      await pool.query(`UPDATE post_comments SET reply_count = reply_count + 1 WHERE id = ?`, [parentId]);
    }
    const [[row]] = await pool.query(
      `SELECT c.*, u.nickname, u.avatar_url
       FROM post_comments c LEFT JOIN users u ON c.user_id = u.id WHERE c.id = ?`,
      [commentId]
    );
    const item = {
      id: row.id,
      post_id: row.post_id,
      user_id: row.user_id,
      parent_id: row.parent_comment_id ?? row.parent_id,
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
    await ensureTablesReady();
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
    parent_id: row.parent_comment_id ?? row.parent_id,
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
