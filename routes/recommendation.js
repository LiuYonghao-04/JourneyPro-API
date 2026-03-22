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

const detourLabelFor = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return "Balanced";
  if (num <= 0.3) return "Tight route";
  if (num >= 0.7) return "Flexible detours";
  return "Balanced detours";
};

const EVOLUTION_WINDOWS = [
  { key: "days_61_90", label: "61-90d", short_label: "90d" },
  { key: "days_31_60", label: "31-60d", short_label: "60d" },
  { key: "last_30d", label: "Last 30d", short_label: "30d" },
];

const uniqueNames = (items) =>
  [...new Set((items || []).map((item) => String(item || "").trim()).filter(Boolean))];

const buildArchetype = ({ topCategory, topTag, settings, diversityScore }) => {
  const interest = Number(settings?.interest_weight ?? settings?.interestWeight) || 0.5;
  const explore = Number(settings?.explore_weight ?? settings?.exploreWeight) || 0.15;
  const detourTolerance = Number(settings?.detour_tolerance ?? settings?.detourTolerance) || 0.5;
  const focusCategory = String(topCategory?.name || "").toLowerCase();
  const focusTag = String(topTag?.name || "").toLowerCase();

  if (focusCategory.includes("museum") || focusCategory.includes("history") || focusTag.includes("museum")) {
    return explore >= 0.5 ? "Culture Explorer" : "Culture Curator";
  }
  if (explore >= 0.62 && interest >= 0.55) return "Discovery Seeker";
  if (interest >= 0.64 && diversityScore < 45) return "Focused Specialist";
  if (interest <= 0.42 && explore <= 0.35 && detourTolerance <= 0.38) return "Route Optimizer";
  if (detourTolerance >= 0.72 && explore >= 0.5) return "Flexible Route Explorer";
  if (diversityScore >= 64) return "Broad City Sampler";
  return "Balanced City Planner";
};

const buildProfileSummary = ({ archetype, topCategory, topTag, dominantSignal, settings }) => {
  const interest = Number(settings?.interest_weight ?? settings?.interestWeight) || 0.5;
  const explore = Number(settings?.explore_weight ?? settings?.exploreWeight) || 0.15;
  const detourTolerance = Number(settings?.detour_tolerance ?? settings?.detourTolerance) || 0.5;
  const categoryPart = topCategory?.name ? `${topCategory.name} leads` : "Route context leads";
  const tagPart = topTag?.name ? `while #${topTag.name} keeps surfacing` : "with no single tag dominating";
  const orderingPart =
    interest >= 0.55 ? "recommendations lean slightly toward personal taste" : "recommendations still keep route efficiency in front";
  const explorePart =
    explore >= 0.5 ? "and exploration is intentionally elevated." : "and exploration stays controlled.";
  const detourPart = `Detour tolerance stays ${detourLabelFor(detourTolerance).toLowerCase()}.`;
  const signalPart = dominantSignal?.label ? `${dominantSignal.label} currently shapes the profile most.` : "";
  return `${archetype}: ${categoryPart}, ${tagPart}; ${orderingPart} ${explorePart} ${detourPart} ${signalPart}`.trim();
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
    key: "detour-window",
    title: "Detour tolerance",
    value: `${Math.round((Number(settings?.detour_tolerance) || 0.5) * 100)}% ${detourLabelFor(
      settings?.detour_tolerance
    )}`,
    detail: "Lower values keep route deviation tighter; higher values allow more flexible stops before results are filtered out.",
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

const buildSettingsPayload = (settings) => ({
  interest_weight: settings.interestWeight,
  distance_weight: settings.distanceWeight,
  explore_weight: settings.exploreWeight,
  safe_weight: round(1 - settings.exploreWeight, 6),
  detour_tolerance: settings.detourTolerance,
  detour_label: detourLabelFor(settings.detourTolerance),
  updated_at: settings.updatedAt,
});

const buildEvolutionSeries = (rows, focusNames, kind, limit = 3) => {
  const totalsByBucket = new Map(EVOLUTION_WINDOWS.map((window) => [window.key, 0]));
  const weightsByName = new Map();

  (rows || []).forEach((row) => {
    const name = String(row?.name || "").trim();
    const bucket = String(row?.bucket || "").trim();
    const weight = Number(row?.weight) || 0;
    if (!name || !totalsByBucket.has(bucket) || weight <= 0) return;
    totalsByBucket.set(bucket, (totalsByBucket.get(bucket) || 0) + weight);
    const current = weightsByName.get(name) || {};
    current[bucket] = (Number(current[bucket]) || 0) + weight;
    weightsByName.set(name, current);
  });

  const rankedNames = [...weightsByName.entries()]
    .sort((a, b) => {
      const totalA = Object.values(a[1] || {}).reduce((sum, value) => sum + (Number(value) || 0), 0);
      const totalB = Object.values(b[1] || {}).reduce((sum, value) => sum + (Number(value) || 0), 0);
      return totalB - totalA;
    })
    .map(([name]) => name);
  const selectedNames = uniqueNames([...(focusNames || []), ...rankedNames]).slice(0, limit);

  return selectedNames.map((name) => {
    const weights = weightsByName.get(name) || {};
    const points = EVOLUTION_WINDOWS.map((window) => {
      const weight = Number(weights[window.key]) || 0;
      const total = Number(totalsByBucket.get(window.key)) || 0;
      return {
        key: window.key,
        label: window.label,
        short_label: window.short_label,
        weight: round(weight, 2),
        percent: total > 0 ? round((weight / total) * 100, 1) : 0,
      };
    });
    const recentPoint = points[points.length - 1] || { percent: 0 };
    const previousWeight = points.slice(0, -1).reduce((sum, point) => sum + (Number(point.weight) || 0), 0);
    const previousTotal = EVOLUTION_WINDOWS.slice(0, -1).reduce(
      (sum, window) => sum + (Number(totalsByBucket.get(window.key)) || 0),
      0
    );
    const previousPercent = previousTotal > 0 ? round((previousWeight / previousTotal) * 100, 1) : 0;
    const delta = round((Number(recentPoint.percent) || 0) - previousPercent, 1);
    return {
      key: `${kind}:${name}`,
      kind,
      name,
      points,
      recent_percent: Number(recentPoint.percent) || 0,
      previous_percent: previousPercent,
      delta,
      trend: delta >= 4 ? "up" : delta <= -4 ? "down" : "flat",
    };
  });
};

const buildPreferenceShiftSummary = ({ tags = [], categories = [] }) => {
  const decorate = (item) => ({
    ...item,
    label: item.kind === "tag" ? `#${item.name}` : item.name,
  });
  const combined = [...(categories || []), ...(tags || [])]
    .filter((item) => Math.abs(Number(item?.delta) || 0) >= 1)
    .sort((a, b) => Math.abs(Number(b?.delta) || 0) - Math.abs(Number(a?.delta) || 0))
    .map(decorate);
  const rising = combined
    .filter((item) => Number(item.delta) > 0)
    .sort((a, b) => Number(b.delta) - Number(a.delta))
    .slice(0, 3);
  const cooling = combined
    .filter((item) => Number(item.delta) < 0)
    .sort((a, b) => Number(a.delta) - Number(b.delta))
    .slice(0, 3);
  const dominant = rising[0] || cooling[0] || null;
  const summary = dominant
    ? Number(dominant.delta) > 0
      ? `${dominant.label} is gaining weight fastest in the most recent 30-day window.`
      : `${dominant.label} cooled off compared with the previous 60 days.`
    : "Recent behavior is stable; no major preference swing detected yet.";

  return {
    rising,
    cooling,
    dominant,
    summary,
  };
};

const fetchPreferenceEvolution = async (userId, { topTags = [], topCategories = [], limit = 3 } = {}) => {
  const params = [userId, userId, userId];
  const interactionSql = `
    SELECT
      post_id,
      CASE
        WHEN event_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 'last_30d'
        WHEN event_at >= DATE_SUB(NOW(), INTERVAL 60 DAY) THEN 'days_31_60'
        ELSE 'days_61_90'
      END AS bucket,
      SUM(weight) AS weight
    FROM (
      SELECT post_id, created_at AS event_at, 3 AS weight
      FROM post_likes
      WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
      UNION ALL
      SELECT post_id, created_at AS event_at, 5 AS weight
      FROM post_favorites
      WHERE user_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
      UNION ALL
      SELECT post_id, last_viewed_at AS event_at, LEAST(view_count, 6) AS weight
      FROM post_views
      WHERE user_id = ? AND last_viewed_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
    ) t
    GROUP BY post_id, bucket
  `;

  const [tagRows, categoryRows] = await Promise.all([
    safeRows(
      `
        SELECT t.name AS name, i.bucket AS bucket, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN post_tags pt ON pt.post_id = i.post_id
        JOIN tags t ON t.id = pt.tag_id
        GROUP BY t.name, i.bucket
        ORDER BY weight DESC
      `,
      params
    ),
    safeRows(
      `
        SELECT poi.category AS name, i.bucket AS bucket, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN posts p ON p.id = i.post_id
        JOIN poi ON poi.id = p.poi_id
        WHERE poi.category IS NOT NULL AND poi.category <> ''
        GROUP BY poi.category, i.bucket
        ORDER BY weight DESC
      `,
      params
    ),
  ]);

  const tags = buildEvolutionSeries(tagRows, topTags, "tag", limit);
  const categories = buildEvolutionSeries(categoryRows, topCategories, "category", limit);

  return {
    windows: EVOLUTION_WINDOWS,
    tags,
    categories,
    shifts: buildPreferenceShiftSummary({ tags, categories }),
  };
};

const buildProfilePayload = ({
  userId,
  source,
  tagProfile,
  categoryProfile,
  settings,
  likes,
  favorites,
  views,
  signalMix,
  recentActivity,
  evolution,
}) => {
  const personalized = tagProfile.total_weight > 0 || categoryProfile.total_weight > 0;
  const topCategory = categoryProfile.items?.[0] || null;
  const topTag = tagProfile.items?.[0] || null;
  const diversityScore = buildDiversityScore(tagProfile.items, categoryProfile.items);
  const evidenceScore = Math.min(100, round((signalMix.total_weight / 42) * 100, 1));
  const profileStrength = round(
    Math.min(100, ((tagProfile.total_weight + categoryProfile.total_weight) / 28) * 100),
    1
  );
  const confidence = round(evidenceScore * 0.55 + profileStrength * 0.45, 1);
  const archetype = buildArchetype({ topCategory, topTag, settings, diversityScore });
  const profileSummary = buildProfileSummary({
    archetype,
    topCategory,
    topTag,
    dominantSignal: signalMix.dominant,
    settings,
  });

  return {
    success: true,
    user_id: userId,
    personalized,
    source,
    signals: {
      likes,
      favorites,
      views,
      mix: signalMix,
    },
    tags: tagProfile,
    categories: categoryProfile,
    settings: buildSettingsPayload(settings),
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
    interest_evolution: evolution,
    preference_shifts: evolution?.shifts || buildPreferenceShiftSummary({}),
    explanations: buildExplanationCards({
      topCategory,
      topTag,
      dominantSignal: signalMix.dominant,
      settings: {
        interest_weight: settings.interestWeight,
        distance_weight: settings.distanceWeight,
        explore_weight: settings.exploreWeight,
        safe_weight: 1 - settings.exploreWeight,
        detour_tolerance: settings.detourTolerance,
      },
      diversityScore,
      confidence,
      recentActivity,
    }),
    generated_at: new Date().toISOString(),
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
      detour_tolerance: settings.detourTolerance,
      detour_label: detourLabelFor(settings.detourTolerance),
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
// { user_id, interest_weight, explore_weight, detour_tolerance, mode_defaults }
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
      detourTolerance: req.body?.detour_tolerance,
      modeDefaults: req.body?.mode_defaults,
    });

    res.json({
      success: true,
      user_id: userId,
      interest_weight: settings.interestWeight,
      distance_weight: settings.distanceWeight,
      explore_weight: settings.exploreWeight,
      detour_tolerance: settings.detourTolerance,
      detour_label: detourLabelFor(settings.detourTolerance),
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

    const [aggProfile, settings, likes, favorites, views, recentActivity] = await Promise.all([
      fetchProfileFromAgg(userId, limit),
      fetchUserRecommendationSettings(userId),
      safeScalar(`SELECT COUNT(*) AS count FROM post_likes WHERE user_id = ?`, [userId]),
      safeScalar(`SELECT COUNT(*) AS count FROM post_favorites WHERE user_id = ?`, [userId]),
      safeScalar(`SELECT COALESCE(SUM(view_count), 0) AS count FROM post_views WHERE user_id = ?`, [userId]),
      fetchRecentActivity(userId),
    ]);
    const signalMix = buildSignalMix({ likes, favorites, views });
    let source = aggProfile?.source || "fallback_posts";
    let tagProfile = aggProfile?.tags || null;
    let categoryProfile = aggProfile?.categories || null;

    if (!tagProfile || !categoryProfile) {
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

      const [tagRows, categoryRows] = await Promise.all([
        safeRows(
          `
            SELECT t.name AS name, SUM(i.weight) AS weight
            FROM (${interactionSql}) i
            JOIN post_tags pt ON pt.post_id = i.post_id
            JOIN tags t ON t.id = pt.tag_id
            GROUP BY t.name
            ORDER BY weight DESC
          `,
          params
        ),
        safeRows(
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
        ),
      ]);

      tagProfile = buildPercentList(tagRows, limit);
      categoryProfile = buildPercentList(categoryRows, limit);
      source = "fallback_posts";
    }

    const evolution = await fetchPreferenceEvolution(userId, {
      topTags: tagProfile.items?.map((item) => item.name),
      topCategories: categoryProfile.items?.map((item) => item.name),
      limit: 3,
    });

    res.json(
      buildProfilePayload({
        userId,
        source,
        tagProfile,
        categoryProfile,
        settings,
        likes,
        favorites,
        views,
        signalMix,
        recentActivity,
        evolution,
      })
    );
  } catch (err) {
    console.error("recommendation profile error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
