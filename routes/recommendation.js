import express from "express";
import { pool } from "../db/connect.js";
import { clamp, round } from "../services/reco/constants.js";
import {
  ensureUserExists,
  fetchUserRecommendationSettings,
  saveUserRecommendationSettings,
} from "../services/reco/profiles.js";
import { ensureRecoTables } from "../services/reco/schema.js";

const router = express.Router();

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
    .map((row) => ({ name: row.name, weight: Number(row.weight ?? row.score ?? 0) || 0 }))
    .filter((row) => row.name && row.weight > 0);

  const total = normalized.reduce((sum, row) => sum + row.weight, 0);
  const top = normalized.slice(0, limit).map((row) => ({
    name: row.name,
    weight: row.weight,
    percent: total ? round((row.weight / total) * 100, 1) : 0,
  }));
  const topWeight = top.reduce((sum, row) => sum + row.weight, 0);
  const otherWeight = Math.max(total - topWeight, 0);

  return {
    total_weight: total,
    items: top,
    other_percent: total ? round((otherWeight / total) * 100, 1) : 0,
  };
};

const fetchProfileFromAgg = async (userId, limit) => {
  const rows = await safeRows(
    `
      SELECT feature_type, feature_key, score
      FROM user_interest_agg
      WHERE user_id = ?
      ORDER BY score DESC
      LIMIT 400
    `,
    [userId]
  );

  if (!rows.length) return null;

  const tags = rows
    .filter((row) => row.feature_type === "tag")
    .map((row) => ({ name: row.feature_key, score: Number(row.score) || 0 }));
  const categories = rows
    .filter((row) => row.feature_type === "category")
    .map((row) => ({ name: row.feature_key, score: Number(row.score) || 0 }));

  if (!tags.length && !categories.length) return null;

  return {
    tags: buildPercentList(tags, limit),
    categories: buildPercentList(categories, limit),
    source: "user_interest_agg",
  };
};

// GET /api/recommendation/settings?user_id=1
router.get("/settings", async (req, res) => {
  try {
    const userId = Number.parseInt(req.query.user_id || "0", 10);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    await ensureRecoTables();

    const userExists = await ensureUserExists(userId);
    if (!userExists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const settings = await fetchUserRecommendationSettings(userId);

    res.json({
      success: true,
      user_id: userId,
      interest_weight: settings.interestWeight,
      distance_weight: settings.distanceWeight,
      explore_weight: settings.exploreWeight,
      mode_defaults: settings.modeDefaults,
      updated_at: settings.updatedAt,
      exists: settings.exists,
    });
  } catch (err) {
    console.error("recommendation settings get error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// POST /api/recommendation/settings
// { user_id, interest_weight, explore_weight, mode_defaults }
router.post("/settings", async (req, res) => {
  try {
    const userId = Number.parseInt(req.body?.user_id || "0", 10);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    await ensureRecoTables();

    const userExists = await ensureUserExists(userId);
    if (!userExists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const settings = await saveUserRecommendationSettings(userId, {
      interestWeight: req.body?.interest_weight,
      exploreWeight: req.body?.explore_weight,
      modeDefaults: req.body?.mode_defaults,
    });

    res.json({
      success: true,
      user_id: userId,
      interest_weight: settings.interestWeight,
      distance_weight: settings.distanceWeight,
      explore_weight: settings.exploreWeight,
      mode_defaults: settings.modeDefaults,
      updated_at: settings.updatedAt,
    });
  } catch (err) {
    console.error("recommendation settings save error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/recommendation/profile?user_id=1&limit=6
router.get("/profile", async (req, res) => {
  try {
    const userId = Number.parseInt(req.query.user_id || "0", 10);
    const limit = clamp(Number.parseInt(req.query.limit || "6", 10) || 6, 1, 20);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }

    await ensureRecoTables();

    const aggProfile = await fetchProfileFromAgg(userId, limit);

    const likes = await safeScalar(`SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ?`, [userId]);
    const favorites = await safeScalar(`SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ?`, [userId]);
    const views = await safeScalar(`SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ?`, [
      userId,
    ]);

    if (aggProfile) {
      const personalized = aggProfile.tags.total_weight > 0 || aggProfile.categories.total_weight > 0;
      return res.json({
        success: true,
        user_id: userId,
        personalized,
        source: aggProfile.source,
        signals: { likes, favorites, views },
        tags: aggProfile.tags,
        categories: aggProfile.categories,
        generated_at: new Date().toISOString(),
      });
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

    const tagProfile = buildPercentList(tagRows, limit);
    const categoryProfile = buildPercentList(categoryRows, limit);
    const personalized = tagProfile.total_weight > 0 || categoryProfile.total_weight > 0;

    res.json({
      success: true,
      user_id: userId,
      personalized,
      source: "fallback_posts",
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
