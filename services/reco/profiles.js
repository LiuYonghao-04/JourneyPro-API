import { pool } from "../../db/connect.js";
import {
  DEFAULT_EXPLORE_WEIGHT,
  DEFAULT_INTEREST_WEIGHT,
  FEATURE_TYPES,
  clamp,
  normalizeWeight,
  round,
} from "./constants.js";
import { ensureRecoTables } from "./schema.js";

const parseTagList = (value) => {
  if (!value) return [];
  return String(value)
    .split(/[,;|/]+/)
    .map((tag) => tag.trim())
    .filter(Boolean);
};

const toWeightMap = (rows, keyField, keyTransform = (value) => value) => {
  const map = new Map();
  (rows || []).forEach((row) => {
    const raw = row?.[keyField];
    if (raw === null || raw === undefined || raw === "") return;
    const key = keyTransform(raw);
    const weight = Number(row.score ?? row.weight ?? 0) || 0;
    map.set(key, weight);
  });
  return map;
};

const mapMax = (map) => {
  let max = 0;
  for (const value of map.values()) {
    if (value > max) max = value;
  }
  return max;
};

const mapTopKeys = (map, limit = 5) =>
  [...map.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([key]) => key);

const parseModeDefaults = (raw) => {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
};

export const ensureUserExists = async (userId) => {
  const uid = Number.parseInt(userId, 10);
  if (!uid) return false;
  const [[row]] = await pool.query(`SELECT id FROM users WHERE id = ? LIMIT 1`, [uid]);
  return !!row;
};

export const fetchUserRecommendationSettings = async (userId) => {
  const uid = Number.parseInt(userId, 10);
  if (!uid) {
    return {
      userId: null,
      exists: false,
      interestWeight: DEFAULT_INTEREST_WEIGHT,
      distanceWeight: 1 - DEFAULT_INTEREST_WEIGHT,
      exploreWeight: DEFAULT_EXPLORE_WEIGHT,
      modeDefaults: null,
      updatedAt: null,
    };
  }

  await ensureRecoTables();
  const [[row]] = await pool.query(
    `
      SELECT interest_weight, explore_weight, mode_defaults, updated_at
      FROM user_recommendation_settings
      WHERE user_id = ?
      LIMIT 1
    `,
    [uid]
  );

  const interestWeight = normalizeWeight(row?.interest_weight, DEFAULT_INTEREST_WEIGHT);
  const exploreWeight = normalizeWeight(row?.explore_weight, DEFAULT_EXPLORE_WEIGHT);

  return {
    userId: uid,
    exists: !!row,
    interestWeight,
    distanceWeight: round(1 - interestWeight, 6),
    exploreWeight,
    modeDefaults: parseModeDefaults(row?.mode_defaults),
    updatedAt: row?.updated_at || null,
  };
};

export const saveUserRecommendationSettings = async (
  userId,
  { interestWeight, exploreWeight, modeDefaults = null }
) => {
  const uid = Number.parseInt(userId, 10);
  if (!uid) {
    throw new Error("user_id required");
  }

  await ensureRecoTables();

  const normalizedInterest = normalizeWeight(interestWeight, DEFAULT_INTEREST_WEIGHT);
  const normalizedExplore = normalizeWeight(exploreWeight, DEFAULT_EXPLORE_WEIGHT);
  const modeDefaultsJson = modeDefaults && typeof modeDefaults === "object" ? JSON.stringify(modeDefaults) : null;

  await pool.query(
    `
      INSERT INTO user_recommendation_settings (user_id, interest_weight, explore_weight, mode_defaults)
      VALUES (?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        interest_weight = VALUES(interest_weight),
        explore_weight = VALUES(explore_weight),
        mode_defaults = VALUES(mode_defaults),
        updated_at = CURRENT_TIMESTAMP
    `,
    [uid, normalizedInterest, normalizedExplore, modeDefaultsJson]
  );

  return fetchUserRecommendationSettings(uid);
};

const fetchAggregatedProfile = async (userId) => {
  const uid = Number.parseInt(userId, 10);
  if (!uid) {
    return {
      tagWeights: new Map(),
      categoryWeights: new Map(),
      poiWeights: new Map(),
      maxTagWeight: 0,
      maxCategoryWeight: 0,
      maxPoiWeight: 0,
      topTags: [],
      topCategories: [],
      hasProfile: false,
      source: "none",
    };
  }

  await ensureRecoTables();

  const [rows] = await pool.query(
    `
      SELECT feature_type, feature_key, score
      FROM user_interest_agg
      WHERE user_id = ?
      ORDER BY score DESC
      LIMIT 500
    `,
    [uid]
  );

  const tagRows = rows.filter((row) => row.feature_type === FEATURE_TYPES.TAG);
  const categoryRows = rows.filter((row) => row.feature_type === FEATURE_TYPES.CATEGORY);
  const poiRows = rows.filter((row) => row.feature_type === FEATURE_TYPES.POI);

  const tagWeights = toWeightMap(tagRows, "feature_key", (value) => String(value));
  const categoryWeights = toWeightMap(categoryRows, "feature_key", (value) => String(value));
  const poiWeights = toWeightMap(poiRows, "feature_key", (value) => Number(value));

  const maxTagWeight = mapMax(tagWeights);
  const maxCategoryWeight = mapMax(categoryWeights);
  const maxPoiWeight = mapMax(poiWeights);

  const hasProfile = maxTagWeight > 0 || maxCategoryWeight > 0 || maxPoiWeight > 0;
  if (!hasProfile) {
    return {
      tagWeights,
      categoryWeights,
      poiWeights,
      maxTagWeight,
      maxCategoryWeight,
      maxPoiWeight,
      topTags: [],
      topCategories: [],
      hasProfile: false,
      source: "empty_agg",
    };
  }

  return {
    tagWeights,
    categoryWeights,
    poiWeights,
    maxTagWeight,
    maxCategoryWeight,
    maxPoiWeight,
    topTags: mapTopKeys(tagWeights, 6),
    topCategories: mapTopKeys(categoryWeights, 6),
    hasProfile,
    source: "user_interest_agg",
  };
};

const fetchFallbackProfile = async (userId) => {
  const uid = Number.parseInt(userId, 10);
  if (!uid) {
    return {
      tagWeights: new Map(),
      categoryWeights: new Map(),
      poiWeights: new Map(),
      maxTagWeight: 0,
      maxCategoryWeight: 0,
      maxPoiWeight: 0,
      topTags: [],
      topCategories: [],
      hasProfile: false,
      source: "none",
    };
  }

  const params = [uid, uid, uid];
  const interactionSql = `
    SELECT post_id, event_at, SUM(weight) AS weight
    FROM (
      SELECT post_id, created_at AS event_at, 3 AS weight FROM post_likes WHERE user_id = ?
      UNION ALL
      SELECT post_id, created_at AS event_at, 5 AS weight FROM post_favorites WHERE user_id = ?
      UNION ALL
      SELECT post_id, last_viewed_at AS event_at, LEAST(view_count, 6) AS weight FROM post_views WHERE user_id = ?
    ) t
    GROUP BY post_id, event_at
  `;

  let tagRows = [];
  let categoryRows = [];
  let poiRows = [];

  try {
    [tagRows] = await pool.query(
      `
        SELECT t.name AS tag, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN post_tags pt ON pt.post_id = i.post_id
        JOIN tags t ON t.id = pt.tag_id
        GROUP BY t.name
      `,
      params
    );

    [categoryRows] = await pool.query(
      `
        SELECT poi.category AS category, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN posts p ON p.id = i.post_id
        JOIN poi ON poi.id = p.poi_id
        WHERE poi.category IS NOT NULL AND poi.category <> ''
        GROUP BY poi.category
      `,
      params
    );

    [poiRows] = await pool.query(
      `
        SELECT p.poi_id AS poi_id, SUM(i.weight) AS weight
        FROM (${interactionSql}) i
        JOIN posts p ON p.id = i.post_id
        WHERE p.poi_id IS NOT NULL
        GROUP BY p.poi_id
      `,
      params
    );
  } catch {
    // keep defaults
  }

  const tagWeights = toWeightMap(tagRows, "tag", (value) => String(value));
  const categoryWeights = toWeightMap(categoryRows, "category", (value) => String(value));
  const poiWeights = toWeightMap(poiRows, "poi_id", (value) => Number(value));

  const maxTagWeight = mapMax(tagWeights);
  const maxCategoryWeight = mapMax(categoryWeights);
  const maxPoiWeight = mapMax(poiWeights);
  const hasProfile = maxTagWeight > 0 || maxCategoryWeight > 0 || maxPoiWeight > 0;

  return {
    tagWeights,
    categoryWeights,
    poiWeights,
    maxTagWeight,
    maxCategoryWeight,
    maxPoiWeight,
    topTags: mapTopKeys(tagWeights, 6),
    topCategories: mapTopKeys(categoryWeights, 6),
    hasProfile,
    source: "fallback_posts",
  };
};

export const fetchUserPreferenceProfile = async (userId) => {
  const agg = await fetchAggregatedProfile(userId);
  if (agg.hasProfile) return agg;
  return fetchFallbackProfile(userId);
};

export const computeInterestFit = (poi, profile) => {
  if (!poi) return { score: 0, topTag: null, matchTags: [] };
  const safeProfile =
    profile ||
    ({
      tagWeights: new Map(),
      categoryWeights: new Map(),
      poiWeights: new Map(),
      maxTagWeight: 0,
      maxCategoryWeight: 0,
      maxPoiWeight: 0,
      hasProfile: false,
    });

  const poiWeight = safeProfile.poiWeights.get(Number(poi.id)) || 0;
  const poiScore = safeProfile.maxPoiWeight ? poiWeight / safeProfile.maxPoiWeight : 0;

  const tagList = parseTagList(poi.tags);
  const uniqueTags = [...new Set(tagList)];
  const tagWeightSum = uniqueTags.reduce((sum, tag) => sum + (safeProfile.tagWeights.get(tag) || 0), 0);
  const tagScore = safeProfile.maxTagWeight ? tagWeightSum / safeProfile.maxTagWeight : 0;

  const categoryKey = String(poi.category || "").trim();
  const categoryWeight = categoryKey ? safeProfile.categoryWeights.get(categoryKey) || 0 : 0;
  const categoryScore = safeProfile.maxCategoryWeight ? categoryWeight / safeProfile.maxCategoryWeight : 0;

  const popularity = clamp((Number(poi.popularity) || 0) / 5, 0, 1);
  const score = safeProfile.hasProfile
    ? clamp(tagScore * 0.5 + categoryScore * 0.2 + poiScore * 0.3, 0, 1)
    : clamp(popularity * 0.5 + 0.25, 0, 1);

  let topTag = null;
  let topTagWeight = 0;
  uniqueTags.forEach((tag) => {
    const weight = safeProfile.tagWeights.get(tag) || 0;
    if (weight > topTagWeight) {
      topTag = tag;
      topTagWeight = weight;
    }
  });
  if (!topTag && categoryWeight > 0) {
    topTag = categoryKey;
  }

  const matchTags = uniqueTags.filter((tag) => (safeProfile.tagWeights.get(tag) || 0) > 0);
  if (categoryKey && categoryWeight > 0 && !matchTags.includes(categoryKey)) {
    matchTags.push(categoryKey);
  }

  return {
    score,
    poiScore,
    tagScore,
    categoryScore,
    topTag,
    matchTags,
  };
};
