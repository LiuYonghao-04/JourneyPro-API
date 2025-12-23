import express from "express";
import axios from "axios";
import { getNearbyPOIs } from "../models/poi.js";
import { pool } from "../db/connect.js";

const router = express.Router();

const OSRM_URL = process.env.OSRM_URL || "http://localhost:5000";

const toRad = (d) => (Number(d) * Math.PI) / 180;
const haversineMeters = (lat1, lng1, lat2, lng2) => {
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

const sampleRoutePoints = (coords, stepM, maxSamples) => {
  if (!Array.isArray(coords) || coords.length < 2) return [];
  const step = Math.max(Number(stepM) || 0, 50);
  const max = Math.max(parseInt(maxSamples || "0", 10) || 0, 5);

  const samples = [];
  const [lng0, lat0] = coords[0];
  samples.push({ lat: lat0, lng: lng0 });

  let traveled = 0;
  let nextAt = step;

  for (let i = 1; i < coords.length; i += 1) {
    const [lng1, lat1] = coords[i - 1];
    const [lng2, lat2] = coords[i];
    const seg = haversineMeters(lat1, lng1, lat2, lng2);
    if (!seg || !Number.isFinite(seg)) continue;

    while (traveled + seg >= nextAt) {
      const t = (nextAt - traveled) / seg;
      const lat = lat1 + (lat2 - lat1) * t;
      const lng = lng1 + (lng2 - lng1) * t;
      samples.push({ lat, lng });
      if (samples.length >= max) return samples;
      nextAt += step;
    }
    traveled += seg;
  }

  const [lngLast, latLast] = coords[coords.length - 1];
  const last = samples[samples.length - 1];
  if (samples.length < max && haversineMeters(last.lat, last.lng, latLast, lngLast) > 60) {
    samples.push({ lat: latLast, lng: lngLast });
  }

  return samples;
};

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const parseLngLat = (value) => {
  const [lng, lat] = String(value || "").split(",").map(Number);
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return null;
  return { lng, lat };
};

const parseViaPoints = (value) => {
  if (!value) return [];
  return String(value)
    .split(";")
    .map((pair) => parseLngLat(pair))
    .filter(Boolean);
};

const parseTagList = (value) => {
  if (!value) return [];
  return String(value)
    .split(/[,;|/]+/)
    .map((tag) => tag.trim())
    .filter(Boolean);
};

const toWeightMap = (rows, keyField, keyTransform = (v) => v) => {
  const map = new Map();
  rows.forEach((row) => {
    const raw = row?.[keyField];
    if (raw === null || raw === undefined || raw === "") return;
    const key = keyTransform(raw);
    const weight = Number(row.weight) || 0;
    map.set(key, weight);
  });
  return map;
};

const maxWeight = (map) => {
  let max = 0;
  for (const value of map.values()) {
    if (value > max) max = value;
  }
  return max;
};

let ensurePreferenceTablesPromise = null;
const ensurePreferenceTables = async () => {
  if (!ensurePreferenceTablesPromise) {
    ensurePreferenceTablesPromise = pool
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
        ensurePreferenceTablesPromise = null;
        throw err;
      });
  }
  await ensurePreferenceTablesPromise;
};

const fetchUserPreferences = async (userId) => {
  if (!userId) {
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
    };
  }

  try {
    await ensurePreferenceTables();
  } catch (err) {
    console.warn("preference tables unavailable", err);
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
    };
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
  } catch (err) {
    console.warn("preference query failed", err);
  }

  const tagWeights = toWeightMap(tagRows, "tag", (v) => String(v));
  const categoryWeights = toWeightMap(categoryRows, "category", (v) => String(v));
  const poiWeights = toWeightMap(poiRows, "poi_id", (v) => Number(v));

  const maxTagWeight = maxWeight(tagWeights);
  const maxCategoryWeight = maxWeight(categoryWeights);
  const maxPoiWeight = maxWeight(poiWeights);

  const topTags = [...tagWeights.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([tag]) => tag);
  const topCategories = [...categoryWeights.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([cat]) => cat);

  return {
    tagWeights,
    categoryWeights,
    poiWeights,
    maxTagWeight,
    maxCategoryWeight,
    maxPoiWeight,
    topTags,
    topCategories,
    hasProfile: maxTagWeight > 0 || maxCategoryWeight > 0 || maxPoiWeight > 0,
  };
};

const buildReason = ({ personalScore, bestDistanceKm, startKm, endKm, topTag, popScore }) => {
  if (personalScore >= 0.45 && topTag) {
    return `Matches your interests: ${topTag}`;
  }
  if (bestDistanceKm <= 0.35) {
    return "Right along your route";
  }
  if (startKm <= 0.6) {
    return "Near your start";
  }
  if (endKm <= 0.6) {
    return "Near your destination";
  }
  if (popScore >= 0.7) {
    return "Popular nearby";
  }
  return "Good fit for your route";
};

// GET /api/route/recommend?start=lng,lat&end=lng,lat&via=lng,lat;lng,lat&user_id=1&radius=700&limit=10&sample_m=350&category=museum
router.get("/recommend", async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) {
      return res.status(400).json({ error: "Missing start or end parameters" });
    }

    const startPoint = parseLngLat(start);
    const endPoint = parseLngLat(end);
    const userId = req.query.user_id ? parseInt(req.query.user_id, 10) : null;
    const viaPoints = parseViaPoints(req.query.via);
    if (!startPoint || !endPoint) {
      return res.status(400).json({ error: "Invalid start/end format" });
    }

    const radius = clamp(parseInt(req.query.radius || "700", 10) || 700, 50, 20000);
    const limit = clamp(parseInt(req.query.limit || "10", 10) || 10, 1, 50);
    const perSampleLimit = clamp(parseInt(req.query.per_sample_limit || "18", 10) || 18, 1, 60);
    const requestedSampleM = clamp(parseInt(req.query.sample_m || "350", 10) || 350, 150, 1200);
    const category = (req.query.category || "").toString().trim();

    const waypoints = [startPoint, ...viaPoints, endPoint];
    const coordString = waypoints.map((p) => `${p.lng},${p.lat}`).join(";");

    const osrmRes = await axios.get(
      `${OSRM_URL}/route/v1/driving/${coordString}?overview=full&geometries=geojson`
    );
    const route = osrmRes.data?.routes?.[0];
    if (!route || !route.geometry || !Array.isArray(route.geometry.coordinates)) {
      return res.status(404).json({ error: "No route found" });
    }

    const maxSamples = 60;
    const dynamicStep = route.distance ? Math.ceil(route.distance / maxSamples) : requestedSampleM;
    const sampleM = Math.max(requestedSampleM, dynamicStep);
    const samples = sampleRoutePoints(route.geometry.coordinates, sampleM, maxSamples);

    const byId = new Map(); // id -> { ...poi, hits, bestDistance }
    for (const s of samples) {
      const nearby = await getNearbyPOIs(s.lat, s.lng, radius, perSampleLimit, category || null);
      for (const p of nearby) {
        const id = p.id;
        if (!id) continue;
        const dist = Number(p.distance) || 0;
        const prev = byId.get(id);
        if (!prev) {
          byId.set(id, { ...p, hits: 1, bestDistance: dist });
        } else {
          prev.hits += 1;
          prev.bestDistance = Math.min(prev.bestDistance, dist || prev.bestDistance);
        }
      }
    }

    const preferences = await fetchUserPreferences(userId);

    const scored = Array.from(byId.values()).map((p) => {
      const popularity = Number(p.popularity) || 0;
      const km = (Number(p.bestDistance) || 0) / 1000;
      const startKm = haversineMeters(p.lat, p.lng, startPoint.lat, startPoint.lng) / 1000;
      const endKm = haversineMeters(p.lat, p.lng, endPoint.lat, endPoint.lng) / 1000;

      const popScore = clamp(popularity / 5, 0, 1);
      const distScore = 1 / (km + 1);
      const startScore = 1 / (startKm + 1);
      const endScore = 1 / (endKm + 1);
      const endpointScore = (startScore + endScore) / 2;
      const hitScore = clamp((Number(p.hits) || 0) / 3, 0, 1);

      const poiWeight = preferences.poiWeights.get(Number(p.id)) || 0;
      const poiScore = preferences.maxPoiWeight ? poiWeight / preferences.maxPoiWeight : 0;

      const tagList = parseTagList(p.tags);
      const uniqueTags = [...new Set(tagList)];
      const tagWeights = uniqueTags.map((tag) => preferences.tagWeights.get(tag) || 0);
      const tagScoreRaw = tagWeights.reduce((sum, v) => sum + v, 0);
      const tagScore = preferences.maxTagWeight ? tagScoreRaw / preferences.maxTagWeight : 0;

      const categoryLabel = p.category ? String(p.category) : "";
      const categoryWeight = categoryLabel ? preferences.categoryWeights.get(categoryLabel) || 0 : 0;
      const categoryScore = preferences.maxCategoryWeight ? categoryWeight / preferences.maxCategoryWeight : 0;

      const personalScore = preferences.hasProfile
        ? tagScore * 0.5 + categoryScore * 0.2 + poiScore * 0.3
        : 0;

      const score = preferences.hasProfile
        ? distScore * 0.3 + endpointScore * 0.15 + popScore * 0.15 + hitScore * 0.1 + personalScore * 0.3
        : distScore * 0.4 + endpointScore * 0.2 + popScore * 0.25 + hitScore * 0.15;

      let topTag = null;
      let topTagWeight = 0;
      uniqueTags.forEach((tag) => {
        const weight = preferences.tagWeights.get(tag) || 0;
        if (weight > topTagWeight) {
          topTag = tag;
          topTagWeight = weight;
        }
      });
      if (!topTag && categoryWeight > 0) {
        topTag = categoryLabel;
      }

      const matchTags = uniqueTags.filter((tag) => preferences.tagWeights.get(tag));
      if (categoryLabel && categoryWeight > 0 && !matchTags.includes(categoryLabel)) {
        matchTags.push(categoryLabel);
      }

      return {
        id: p.id,
        name: p.name,
        category: p.category,
        lat: p.lat,
        lng: p.lng,
        distance: Math.round(Number(p.bestDistance) || Number(p.distance) || 0),
        distance_to_start: Math.round(startKm * 1000),
        distance_to_end: Math.round(endKm * 1000),
        popularity: p.popularity,
        score,
        image_url: p.image_url,
        reason: buildReason({
          personalScore,
          bestDistanceKm: km,
          startKm,
          endKm,
          topTag,
          popScore,
        }),
        match_tags: matchTags,
        personal_score: personalScore,
      };
    });

    scored.sort((a, b) => b.score - a.score);
    const topPois = scored.slice(0, limit);

    res.json({
      base_route: route,
      recommended_pois: topPois,
      profile: {
        user_id: userId || null,
        tags: preferences.topTags,
        categories: preferences.topCategories,
        personalized: preferences.hasProfile,
      },
      debug: {
        osrm: OSRM_URL,
        sample_m: sampleM,
        samples: samples.length,
        radius_m: radius,
        candidates: scored.length,
      },
    });
  } catch (err) {
    console.error("Error in /recommend:", err);
    res.status(500).json({ error: "Server error" });
  }
});

// GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat
router.get("/with-poi", async (req, res) => {
  try {
    const { start, poi, end } = req.query;
    if (!start || !poi || !end) {
      return res.status(400).json({
        success: false,
        message: "Missing params: start / poi / end",
      });
    }

    const coordinates = `${start};${poi};${end}`;
    const url = `${OSRM_URL}/route/v1/driving/${coordinates}?overview=full&geometries=geojson&steps=true`;

    const osrmRes = await axios.get(url);
    const data = osrmRes.data;

    if (!data.routes || data.routes.length === 0) {
      return res.status(404).json({ success: false, message: "No route found" });
    }

    const route = data.routes[0];
    res.json({
      success: true,
      optimized_route: {
        geometry: route.geometry,
        distance: route.distance,
        duration: route.duration,
        legs: route.legs,
      },
    });
  } catch (err) {
    console.error("Error in /with-poi:", err);
    res.status(500).json({
      success: false,
      message: "Server error",
      error: err.message,
    });
  }
});

export default router;
