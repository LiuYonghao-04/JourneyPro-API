import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);
const round1 = (value) => Math.round(Number(value || 0) * 10) / 10;

let ensureViewsTablePromise = null;
const ensureViewsTable = async () => {
  if (!ensureViewsTablePromise) {
    ensureViewsTablePromise = pool
      .query(`
        CREATE TABLE IF NOT EXISTS post_views (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          post_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          view_count INT DEFAULT 1,
          last_viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_post_user_view (post_id, user_id)
        );
      `)
      .catch((err) => {
        ensureViewsTablePromise = null;
        throw err;
      });
  }
  await ensureViewsTablePromise;
};

let ensureSettingsTablePromise = null;
const ensureSettingsTable = async () => {
  if (!ensureSettingsTablePromise) {
    ensureSettingsTablePromise = pool
      .query(`
        CREATE TABLE IF NOT EXISTS user_recommendation_settings (
          user_id BIGINT PRIMARY KEY,
          interest_weight FLOAT NOT NULL DEFAULT 0.5,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
      `)
      .catch((err) => {
        ensureSettingsTablePromise = null;
        throw err;
      });
  }
  await ensureSettingsTablePromise;
};

const parseInterestWeight = (value) => {
  let weight = Number(value);
  if (!Number.isFinite(weight)) weight = 0.5;
  if (weight > 1) weight /= 100;
  return clamp(weight, 0, 1);
};

const safeRows = async (sql, params = []) => {
  try {
    const [rows] = await pool.query(sql, params);
    return rows;
  } catch (err) {
    if (err?.code === "ER_NO_SUCH_TABLE") return [];
    throw err;
  }
};

const safeScalar = async (sql, params = [], field = "count", defaultValue = 0) => {
  const rows = await safeRows(sql, params);
  const value = rows?.[0]?.[field];
  const num = Number(value);
  return Number.isFinite(num) ? num : defaultValue;
};

const buildPercentList = (rows, limit) => {
  const normalized = (rows || [])
    .map((r) => ({ name: r.name, weight: Number(r.weight) || 0 }))
    .filter((r) => r.name && r.weight > 0);

  const total = normalized.reduce((sum, r) => sum + r.weight, 0);
  const top = normalized.slice(0, limit).map((r) => ({
    name: r.name,
    weight: r.weight,
    percent: total ? round1((r.weight / total) * 100) : 0,
  }));
  const topWeight = top.reduce((sum, r) => sum + r.weight, 0);
  const otherWeight = Math.max(total - topWeight, 0);

  return {
    total_weight: total,
    items: top,
    other_percent: total ? round1((otherWeight / total) * 100) : 0,
  };
};

// GET /api/recommendation/settings?user_id=1
router.get("/settings", async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id || "0", 10);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    await ensureSettingsTable();

    const [[userRow]] = await pool.query(`SELECT id FROM users WHERE id = ? LIMIT 1`, [userId]);
    if (!userRow) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [[row]] = await pool.query(
      `SELECT interest_weight, updated_at FROM user_recommendation_settings WHERE user_id = ? LIMIT 1`,
      [userId]
    );
    const interestWeight = parseInterestWeight(row?.interest_weight);
    res.json({
      success: true,
      user_id: userId,
      interest_weight: interestWeight,
      distance_weight: 1 - interestWeight,
      updated_at: row?.updated_at || null,
      exists: !!row,
    });
  } catch (err) {
    console.error("recommendation settings get error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// POST /api/recommendation/settings  { user_id, interest_weight }
router.post("/settings", async (req, res) => {
  try {
    const userId = parseInt(req.body?.user_id || "0", 10);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    await ensureSettingsTable();

    const [[userRow]] = await pool.query(`SELECT id FROM users WHERE id = ? LIMIT 1`, [userId]);
    if (!userRow) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const interestWeight = parseInterestWeight(req.body?.interest_weight);

    await pool.query(
      `
        INSERT INTO user_recommendation_settings (user_id, interest_weight)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE interest_weight = VALUES(interest_weight), updated_at = CURRENT_TIMESTAMP
      `,
      [userId, interestWeight]
    );

    const [[row]] = await pool.query(
      `SELECT interest_weight, updated_at FROM user_recommendation_settings WHERE user_id = ? LIMIT 1`,
      [userId]
    );

    const saved = parseInterestWeight(row?.interest_weight);
    res.json({
      success: true,
      user_id: userId,
      interest_weight: saved,
      distance_weight: 1 - saved,
      updated_at: row?.updated_at || null,
    });
  } catch (err) {
    console.error("recommendation settings save error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/recommendation/profile?user_id=1&limit=6
router.get("/profile", async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id || "0", 10);
    const limit = clamp(parseInt(req.query.limit || "6", 10) || 6, 1, 20);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    try {
      await ensureViewsTable();
    } catch {
      // ignore
    }

    const params = [userId, userId, userId];
    const interactionSql = `
      SELECT post_id, SUM(weight) AS weight
      FROM (
        SELECT post_id, 3 AS weight FROM post_likes WHERE user_id = ?
        UNION ALL
        SELECT post_id, 5 AS weight FROM post_favorites WHERE user_id = ?
        UNION ALL
        SELECT post_id, LEAST(view_count, 6) AS weight FROM post_views WHERE user_id = ?
      ) t
      GROUP BY post_id
    `;

    const tagRows = await safeRows(
      `
        SELECT t.name AS name, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN post_tags pt ON pt.post_id = i.post_id
        JOIN tags t ON t.id = pt.tag_id
        GROUP BY t.name
        ORDER BY weight DESC
      `,
      params
    );

    const categoryRows = await safeRows(
      `
        SELECT poi.category AS name, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN posts p ON p.id = i.post_id
        JOIN poi ON poi.id = p.poi_id
        WHERE poi.category IS NOT NULL AND poi.category <> ''
        GROUP BY poi.category
        ORDER BY weight DESC
      `,
      params
    );

    const likes = await safeScalar(`SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ?`, [userId]);
    const favorites = await safeScalar(`SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ?`, [userId]);
    const views = await safeScalar(`SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ?`, [
      userId,
    ]);

    const tagProfile = buildPercentList(tagRows, limit);
    const categoryProfile = buildPercentList(categoryRows, limit);
    const personalized = tagProfile.total_weight > 0 || categoryProfile.total_weight > 0;

    res.json({
      success: true,
      user_id: userId,
      personalized,
      signals: { likes, favorites, views },
      tags: tagProfile,
      categories: categoryProfile,
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error("recommendation profile error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
