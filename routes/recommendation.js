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

const percentOf = (value, total, precision = 1) => {
  const base = Number(total) || 0;
  const part = Number(value) || 0;
  return base > 0 ? round((part / base) * 100, precision) : 0;
};

const buildSignalMix = ({ likes = 0, favorites = 0, views = 0 }) => {
  const items = [
    {
      key: "favorites",
      label: "Favorites",
      count: Number(favorites) || 0,
      weight: (Number(favorites) || 0) * 5,
      note: "Strongest intent signal for repeat preference.",
    },
    {
      key: "likes",
      label: "Likes",
      count: Number(likes) || 0,
      weight: (Number(likes) || 0) * 3,
      note: "Positive affinity across community content.",
    },
    {
      key: "views",
      label: "Reads",
      count: Number(views) || 0,
      weight: Math.sqrt(Math.max(Number(views) || 0, 0)) * 2,
      note: "Reading depth still influences long-tail interests.",
    },
  ];

  const totalWeight = items.reduce((sum, item) => sum + item.weight, 0);
  const normalized = items.map((item) => ({
    ...item,
    percent: percentOf(item.weight, totalWeight),
  }));
  const dominant = [...normalized].sort((a, b) => b.percent - a.percent)[0] || null;

  return {
    items: normalized,
    total_weight: round(totalWeight, 2),
    dominant,
  };
};

const buildDiversityScore = (...groups) => {
  const normalized = groups
    .flat()
    .map((item) => Number(item?.percent) || 0)
    .filter((value) => value > 0)
    .slice(0, 8);

  if (normalized.length < 2) return 0;
  const probabilities = normalized.map((value) => value / 100);
  const entropy = probabilities.reduce((sum, probability) => sum - probability * Math.log(probability), 0);
  return round((entropy / Math.log(probabilities.length)) * 100, 1);
};

const confidenceLabelFor = (confidence) => {
  if (confidence >= 80) return "Very stable";
  if (confidence >= 60) return "Learning well";
  if (confidence >= 40) return "Forming";
  return "Early";
};

const momentumLabelFor = (ratio) => {
  if (ratio >= 0.45) return "Hot this week";
  if (ratio >= 0.22) return "Steady";
  if (ratio > 0) return "Quiet";
  return "Cold start";
};

const buildArchetype = ({ topCategory, topTag, settings, diversityScore }) => {
  const interest = Number(settings?.interest_weight ?? settings?.interestWeight) || 0.5;
  const explore = Number(settings?.explore_weight ?? settings?.exploreWeight) || 0.15;
  const focusCategory = String(topCategory?.name || "").toLowerCase();
  const focusTag = String(topTag?.name || "").toLowerCase();

  if (focusCategory.includes("museum") || focusCategory.includes("history") || focusTag.includes("museum")) {
    return explore >= 0.5 ? "Culture Explorer" : "Culture Curator";
  }
  if (explore >= 0.62 && interest >= 0.55) return "Discovery Seeker";
  if (interest >= 0.64 && diversityScore < 45) return "Focused Specialist";
  if (interest <= 0.42 && explore <= 0.35) return "Route Optimizer";
  if (diversityScore >= 64) return "Broad City Sampler";
  return "Balanced City Planner";
};

const buildProfileSummary = ({ archetype, topCategory, topTag, dominantSignal, settings }) => {
  const interest = Number(settings?.interest_weight ?? settings?.interestWeight) || 0.5;
  const explore = Number(settings?.explore_weight ?? settings?.exploreWeight) || 0.15;
  const categoryPart = topCategory?.name ? `${topCategory.name} leads` : "Route context leads";
  const tagPart = topTag?.name ? `while #${topTag.name} keeps surfacing` : "with no single tag dominating";
  const orderingPart =
    interest >= 0.55 ? "recommendations lean slightly toward personal taste" : "recommendations still keep route efficiency in front";
  const explorePart =
    explore >= 0.5 ? "and exploration is intentionally elevated." : "and exploration stays controlled.";
  const signalPart = dominantSignal?.label ? `${dominantSignal.label} currently shapes the profile most.` : "";
  return `${archetype}: ${categoryPart}, ${tagPart}; ${orderingPart} ${explorePart} ${signalPart}`.trim();
};

const buildExplanationCards = ({ topCategory, topTag, dominantSignal, settings, diversityScore, confidence, recentActivity }) => {
  const cards = [];

  if (topCategory?.name) {
    cards.push({
      key: "focus-category",
      title: "Strongest category",
      value: `${topCategory.name} ${topCategory.percent}%`,
      detail: `This category absorbs the largest share of your weighted interactions and route matches.`,
    });
  }

  if (topTag?.name) {
    cards.push({
      key: "focus-tag",
      title: "Recurring tag",
      value: `#${topTag.name} ${topTag.percent}%`,
      detail: `Posts carrying this tag repeatedly receive stronger engagement from you.`,
    });
  }

  if (dominantSignal?.label) {
    cards.push({
      key: "dominant-signal",
      title: "Primary behavior signal",
      value: `${dominantSignal.label} ${dominantSignal.percent}%`,
      detail: dominantSignal.note,
    });
  }

  cards.push({
    key: "manual-bias",
    title: "Manual ranking bias",
    value: `${Math.round((Number(settings?.interest_weight) || 0.5) * 100)}% interest / ${Math.round(
      (Number(settings?.distance_weight) || 0.5) * 100
    )}% distance`,
    detail: `${Math.round((Number(settings?.safe_weight) || 0.85) * 100)}% safe / ${Math.round(
      (Number(settings?.explore_weight) || 0.15) * 100
    )}% explore. Sliders only reorder; they do not hide categories.`,
  });

  cards.push({
    key: "profile-shape",
    title: "Profile shape",
    value: `${confidence}% confidence / ${diversityScore}% diversity`,
    detail: `Confidence reflects evidence volume; diversity reflects how spread your interests are across tags and categories.`,
  });

  if (recentActivity?.last_30d > 0) {
    cards.push({
      key: "recent-momentum",
      title: "Recent momentum",
      value: `${recentActivity.momentum_label}`,
      detail: `${recentActivity.last_7d} weighted signals landed in the last 7 days, versus ${recentActivity.last_30d} over the last 30 days.`,
    });
  }

  return cards.slice(0, 6);
};

const fetchRecentActivity = async (userId) => {
  const likes7d = await safeScalar(
    `SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`,
    [userId]
  );
  const favorites7d = await safeScalar(
    `SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`,
    [userId]
  );
  const views7d = await safeScalar(
    `SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ? AND last_viewed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`,
    [userId]
  );
  const likes30d = await safeScalar(
    `SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)`,
    [userId]
  );
  const favorites30d = await safeScalar(
    `SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)`,
    [userId]
  );
  const views30d = await safeScalar(
    `SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ? AND last_viewed_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)`,
    [userId]
  );

  const weighted7d = round(likes7d * 3 + favorites7d * 5 + Math.sqrt(Math.max(views7d, 0)) * 2, 1);
  const weighted30d = round(likes30d * 3 + favorites30d * 5 + Math.sqrt(Math.max(views30d, 0)) * 2, 1);
  const ratio = weighted30d > 0 ? weighted7d / weighted30d : 0;

  return {
    last_7d: weighted7d,
    last_30d: weighted30d,
    momentum_ratio: round(ratio, 3),
    momentum_label: momentumLabelFor(ratio),
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
    const settings = await fetchUserRecommendationSettings(userId);

    const likes = await safeScalar(`SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ?`, [userId]);
    const favorites = await safeScalar(`SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ?`, [userId]);
    const views = await safeScalar(`SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ?`, [
      userId,
    ]);
    const recentActivity = await fetchRecentActivity(userId);
    const signalMix = buildSignalMix({ likes, favorites, views });

    if (aggProfile) {
      const personalized = aggProfile.tags.total_weight > 0 || aggProfile.categories.total_weight > 0;
      const topCategory = aggProfile.categories.items?.[0] || null;
      const topTag = aggProfile.tags.items?.[0] || null;
      const diversityScore = buildDiversityScore(aggProfile.tags.items, aggProfile.categories.items);
      const evidenceScore = Math.min(
        100,
        round((signalMix.total_weight / 42) * 100, 1)
      );
      const profileStrength = round(Math.min(100, ((aggProfile.tags.total_weight + aggProfile.categories.total_weight) / 28) * 100), 1);
      const confidence = round(evidenceScore * 0.55 + profileStrength * 0.45, 1);
      const archetype = buildArchetype({ topCategory, topTag, settings, diversityScore });
      const profileSummary = buildProfileSummary({
        archetype,
        topCategory,
        topTag,
        dominantSignal: signalMix.dominant,
        settings,
      });
      return res.json({
        success: true,
        user_id: userId,
        personalized,
        source: aggProfile.source,
        signals: {
          likes,
          favorites,
          views,
          mix: signalMix,
        },
        tags: aggProfile.tags,
        categories: aggProfile.categories,
        settings: {
          interest_weight: settings.interestWeight,
          distance_weight: settings.distanceWeight,
          explore_weight: settings.exploreWeight,
          safe_weight: round(1 - settings.exploreWeight, 6),
          updated_at: settings.updatedAt,
        },
        profile_story: {
          archetype,
          summary: profileSummary,
          confidence,
          confidence_label: confidenceLabelFor(confidence),
          diversity_score: diversityScore,
          evidence_score: evidenceScore,
          dominant_category: topCategory,
          dominant_tag: topTag,
          dominant_signal: signalMix.dominant,
        },
        recent_activity: recentActivity,
        explanations: buildExplanationCards({
          topCategory,
          topTag,
          dominantSignal: signalMix.dominant,
          settings: {
            interest_weight: settings.interestWeight,
            distance_weight: settings.distanceWeight,
            explore_weight: settings.exploreWeight,
            safe_weight: 1 - settings.exploreWeight,
          },
          diversityScore,
          confidence,
          recentActivity,
        }),
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
    const topCategory = categoryProfile.items?.[0] || null;
    const topTag = tagProfile.items?.[0] || null;
    const diversityScore = buildDiversityScore(tagProfile.items, categoryProfile.items);
    const evidenceScore = Math.min(100, round((signalMix.total_weight / 42) * 100, 1));
    const profileStrength = round(Math.min(100, ((tagProfile.total_weight + categoryProfile.total_weight) / 28) * 100), 1);
    const confidence = round(evidenceScore * 0.55 + profileStrength * 0.45, 1);
    const archetype = buildArchetype({ topCategory, topTag, settings, diversityScore });
    const profileSummary = buildProfileSummary({
      archetype,
      topCategory,
      topTag,
      dominantSignal: signalMix.dominant,
      settings,
    });

    res.json({
      success: true,
      user_id: userId,
      personalized,
      source: "fallback_posts",
      signals: {
        likes,
        favorites,
        views,
        mix: signalMix,
      },
      tags: tagProfile,
      categories: categoryProfile,
      settings: {
        interest_weight: settings.interestWeight,
        distance_weight: settings.distanceWeight,
        explore_weight: settings.exploreWeight,
        safe_weight: round(1 - settings.exploreWeight, 6),
        updated_at: settings.updatedAt,
      },
      profile_story: {
        archetype,
        summary: profileSummary,
        confidence,
        confidence_label: confidenceLabelFor(confidence),
        diversity_score: diversityScore,
        evidence_score: evidenceScore,
        dominant_category: topCategory,
        dominant_tag: topTag,
        dominant_signal: signalMix.dominant,
      },
      recent_activity: recentActivity,
      explanations: buildExplanationCards({
        topCategory,
        topTag,
        dominantSignal: signalMix.dominant,
        settings: {
          interest_weight: settings.interestWeight,
          distance_weight: settings.distanceWeight,
          explore_weight: settings.exploreWeight,
          safe_weight: 1 - settings.exploreWeight,
        },
        diversityScore,
        confidence,
        recentActivity,
      }),
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error("recommendation profile error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
