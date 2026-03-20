import express from "express";
import { pool } from "../db/connect.js";
import { getNearbyPOIs } from "../models/poi.js";
import { getPoiPhotoUrls } from "../services/poiPhotos.js";

const router = express.Router();
const ENABLE_RUNTIME_SCHEMA_MIGRATION = process.env.ENABLE_RUNTIME_SCHEMA_MIGRATION === "1";
let schemaMigrationNoticePrinted = false;

let ensurePoiSchemaPromise = null;
const poiPhotoFillInFlight = new Map();
const parkingNearbyCache = new Map();

const normalize = (value) => String(value || "").trim();
const isHttpUrl = (value) => /^https?:\/\//i.test(normalize(value));
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];
const EARTH_RADIUS_M = 6371000;
const PARKING_RADIUS_STEPS = [500, 1000];

const toRadians = (value) => (Number(value) * Math.PI) / 180;
const roundCoord = (value) => Number(Number(value || 0).toFixed(4));

const haversineMeters = (lat1, lng1, lat2, lng2) => {
  const dLat = toRadians(lat2 - lat1);
  const dLng = toRadians(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(a));
};

const buildParkingCacheKey = (lat, lng, radius, limit) =>
  `${roundCoord(lat)}:${roundCoord(lng)}:${Number(radius) || 0}:${Number(limit) || 0}`;

const formatParkingAddress = (tags = {}) => {
  const parts = [
    [tags["addr:housenumber"], tags["addr:street"]].filter(Boolean).join(" ").trim(),
    tags["addr:city"],
    tags["addr:postcode"],
  ]
    .map((item) => normalize(item))
    .filter(Boolean);
  return parts.join(", ");
};

const dedupeParkingItems = (items) => {
  const seen = new Set();
  return (items || []).filter((item) => {
    const key = `${item?.source || "x"}:${item?.osm_type || item?.id || ""}:${roundCoord(item?.lat)}:${roundCoord(item?.lng)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const fetchLocalParking = async (lat, lng, radius, limit = 8) => {
  const latDelta = radius / 111320;
  const lngDelta = radius / (111320 * Math.max(Math.cos(toRadians(lat)), 0.2));
  const [rows] = await pool.query(
    `
      SELECT
        id,
        name,
        category,
        lat,
        lng,
        address,
        city,
        image_url,
        (
          6371000 * 2 * ASIN(
            SQRT(
              POWER(SIN(RADIANS(lat - ?) / 2), 2) +
              COS(RADIANS(?)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lng - ?) / 2), 2)
            )
          )
        ) AS distance_m
      FROM poi
      WHERE
        lat BETWEEN ? AND ?
        AND lng BETWEEN ? AND ?
        AND (
          LOWER(COALESCE(category, '')) LIKE '%parking%'
          OR LOWER(COALESCE(tags, '')) LIKE '%parking%'
          OR LOWER(COALESCE(name, '')) LIKE '%parking%'
        )
      HAVING distance_m <= ?
      ORDER BY distance_m ASC, id ASC
      LIMIT ?
    `,
    [lat, lat, lng, lat - latDelta, lat + latDelta, lng - lngDelta, lng + lngDelta, radius, limit]
  );

  return rows.map((row) => ({
    id: row.id,
    name: row.name || "Parking",
    category: row.category || "parking",
    lat: Number(row.lat),
    lng: Number(row.lng),
    address: row.address || row.city || "",
    image_url: row.image_url || null,
    distance_m: Math.round(Number(row.distance_m) || 0),
    source: "local_poi",
  }));
};

const fetchOsmParking = async (lat, lng, radius, limit = 8) => {
  const cacheKey = buildParkingCacheKey(lat, lng, radius, limit);
  const cached = parkingNearbyCache.get(cacheKey);
  if (cached && cached.expires_at > Date.now()) {
    return cached.items;
  }

  const query = `[out:json][timeout:12];
(
  node["amenity"="parking"](around:${Math.round(radius)},${lat},${lng});
  way["amenity"="parking"](around:${Math.round(radius)},${lat},${lng});
  relation["amenity"="parking"](around:${Math.round(radius)},${lat},${lng});
);
out center tags;`;

  const res = await fetch("https://overpass-api.de/api/interpreter", {
    method: "POST",
    headers: {
      "Content-Type": "text/plain;charset=UTF-8",
      Accept: "application/json",
    },
    body: query,
  });
  if (!res.ok) {
    throw new Error(`overpass ${res.status}`);
  }

  const data = await res.json();
  const items = dedupeParkingItems(
    (data?.elements || [])
      .map((item) => {
        const itemLat = Number(item?.lat ?? item?.center?.lat);
        const itemLng = Number(item?.lon ?? item?.center?.lon);
        if (!Number.isFinite(itemLat) || !Number.isFinite(itemLng)) return null;
        return {
          id: item?.id || null,
          osm_type: item?.type || "node",
          name: item?.tags?.name || item?.tags?.operator || "Parking",
          category: "parking",
          lat: itemLat,
          lng: itemLng,
          address: formatParkingAddress(item?.tags || {}),
          capacity: item?.tags?.capacity || null,
          distance_m: Math.round(haversineMeters(lat, lng, itemLat, itemLng)),
          source: "osm_overpass",
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.distance_m - b.distance_m)
      .slice(0, limit)
  );

  parkingNearbyCache.set(cacheKey, {
    expires_at: Date.now() + 10 * 60 * 1000,
    items,
  });
  return items;
};

const isBenignAlterError = (err) => {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("Duplicate column") ||
    msg.includes("check that column/key exists") ||
    msg.includes("already exists")
  );
};

const safeAlter = async (sql) => {
  if (!ENABLE_RUNTIME_SCHEMA_MIGRATION) {
    if (!schemaMigrationNoticePrinted) {
      schemaMigrationNoticePrinted = true;
      console.warn("[poi] runtime schema migration disabled (set ENABLE_RUNTIME_SCHEMA_MIGRATION=1 to enable)");
    }
    return;
  }
  try {
    await pool.query(sql);
  } catch (err) {
    if (!isBenignAlterError(err)) throw err;
  }
};

const ensurePoiSchema = async () => {
  if (!ENABLE_RUNTIME_SCHEMA_MIGRATION) return;
  if (!ensurePoiSchemaPromise) {
    ensurePoiSchemaPromise = (async () => {
      await safeAlter(`ALTER TABLE poi MODIFY image_url VARCHAR(600) NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN description TEXT NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN opening_hours VARCHAR(160) NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN phone VARCHAR(64) NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN website VARCHAR(255) NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN rating_count INT NOT NULL DEFAULT 0`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN review_count INT NOT NULL DEFAULT 0`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN stay_minutes SMALLINT NOT NULL DEFAULT 60`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN best_visit_time VARCHAR(64) NULL`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN family_friendly TINYINT(1) NOT NULL DEFAULT 0`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN pet_friendly TINYINT(1) NOT NULL DEFAULT 0`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN indoor TINYINT(1) NOT NULL DEFAULT 0`);
      await safeAlter(`ALTER TABLE poi ADD COLUMN crowd_level TINYINT NOT NULL DEFAULT 2`);

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
        )
      `);
    })().catch((err) => {
      ensurePoiSchemaPromise = null;
      throw err;
    });
  }
  await ensurePoiSchemaPromise;
};

const POI_DETAIL_SELECT = `
  SELECT
    id, name, category, lat, lng, popularity, price, tags, image_url,
    address, city, country,
    description, opening_hours, phone, website,
    rating_count, review_count, stay_minutes, best_visit_time,
    family_friendly, pet_friendly, indoor, crowd_level
  FROM poi
  WHERE id = ? LIMIT 1
`;

const getPoiDetailRow = async (id) => {
  const [[row]] = await pool.query(POI_DETAIL_SELECT, [id]);
  return row || null;
};

const loadPoiPhotos = async (poiId, limit = 6) => {
  const photoLimit = Math.max(1, Math.min(parseInt(limit, 10) || 6, 12));
  const [rows] = await pool.query(
    `SELECT image_url FROM poi_photos WHERE poi_id = ? ORDER BY sort_order ASC, id ASC LIMIT ?`,
    [poiId, photoLimit]
  );
  return rows.map((row) => normalize(row.image_url)).filter(Boolean);
};

const savePoiPhotos = async (poiId, photos, source = "AUTO") => {
  const urls = unique(photos).filter(isHttpUrl).slice(0, 12);
  if (!urls.length) return;
  const rows = urls.map((url, index) => [poiId, url, source, index]);
  await pool.query(
    `
      INSERT INTO poi_photos (poi_id, image_url, source, sort_order)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        sort_order = LEAST(sort_order, VALUES(sort_order)),
        updated_at = CURRENT_TIMESTAMP
    `,
    [rows]
  );
};

const ensurePoiPhotos = async (poiRow, options = {}) => {
  if (!poiRow?.id) return [];
  const targetCount = Math.max(1, Math.min(parseInt(options.targetCount, 10) || 6, 12));
  const minCount = Math.max(1, Math.min(parseInt(options.minCount, 10) || 4, targetCount));

  let photos = await loadPoiPhotos(poiRow.id, targetCount);
  if (photos.length >= minCount) return photos.slice(0, targetCount);

  const key = String(poiRow.id);
  let task = poiPhotoFillInFlight.get(key);
  if (!task) {
    task = (async () => {
      const generated = await getPoiPhotoUrls(poiRow, {
        targetCount,
        resultCount: 30,
        verify: true,
        timeoutMs: 12000,
        categoryPoolSize: 42,
      });
      await savePoiPhotos(poiRow.id, generated, "AUTO_BAIDU");
    })().finally(() => {
      poiPhotoFillInFlight.delete(key);
    });
    poiPhotoFillInFlight.set(key, task);
  }

  try {
    await task;
  } catch (err) {
    console.error("ensurePoiPhotos populate error", err);
  }

  photos = await loadPoiPhotos(poiRow.id, targetCount);
  if (photos.length < minCount && isHttpUrl(poiRow.image_url) && !photos.includes(poiRow.image_url)) {
    photos.unshift(poiRow.image_url);
  }
  return unique(photos).slice(0, targetCount);
};

const fetchPostPrimaryImages = async (postIds) => {
  const ids = [...new Set((postIds || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  if (!ids.length) return new Map();
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
    [ids]
  );
  const map = new Map();
  rows.forEach((row) => {
    map.set(Number(row.post_id), normalize(row.image_url));
  });
  return map;
};

const fetchPostTags = async (postIds) => {
  const ids = [...new Set((postIds || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  if (!ids.length) return new Map();
  const [rows] = await pool.query(
    `
      SELECT pt.post_id, t.name
      FROM post_tags pt
      JOIN tags t ON t.id = pt.tag_id
      WHERE pt.post_id IN (?)
      ORDER BY t.name ASC
    `,
    [ids]
  );
  const map = new Map();
  rows.forEach((row) => {
    const postId = Number(row.post_id);
    if (!map.has(postId)) map.set(postId, []);
    map.get(postId).push(normalize(row.name));
  });
  return map;
};

const buildPoiBoundingBox = (lat, lng, radiusMeters) => {
  const latDelta = radiusMeters / 111320;
  const lngDelta = radiusMeters / (111320 * Math.max(Math.cos(toRadians(lat)), 0.2));
  return {
    minLat: lat - latDelta,
    maxLat: lat + latDelta,
    minLng: lng - lngDelta,
    maxLng: lng + lngDelta,
  };
};

const crowdLevelLabel = (value) => {
  const level = Number(value);
  if (!Number.isFinite(level) || level <= 0) return "";
  if (level <= 1) return "quiet";
  if (level <= 2) return "steady";
  if (level <= 3) return "busy";
  return "peak-hour busy";
};

const buildCommunitySummary = (poiRow, metrics, topTags) => {
  const postCount = Number(metrics?.post_count) || 0;
  const avgRating = Number(metrics?.avg_rating);
  const reviewCount = Number(metrics?.review_count) || 0;
  const likes = Number(metrics?.total_likes) || 0;
  const favorites = Number(metrics?.total_favorites) || 0;
  const views = Number(metrics?.total_views) || 0;
  const comments = Number(metrics?.comment_count) || 0;
  const topTagNames = (topTags || []).map((item) => normalize(item?.name)).filter(Boolean);

  const bestFor = [];
  const watchOutFor = [];
  const highlights = [];

  if (postCount) highlights.push(`${postCount} community posts linked to this place.`);
  if (Number.isFinite(avgRating) && avgRating > 0) {
    highlights.push(`${avgRating.toFixed(1)}/5 average rating across linked stories.`);
  }
  if (topTagNames.length) {
    highlights.push(`Travelers most often mention ${topTagNames.slice(0, 3).join(", ")}.`);
  }
  if (likes || favorites || views) {
    highlights.push(`${likes} likes, ${favorites} saves, and ${views} views across linked posts.`);
  }
  if (comments) {
    highlights.push(`${comments} comments give extra route context and visit tips.`);
  }

  if (poiRow?.category === "museum" || topTagNames.includes("museum") || topTagNames.includes("history")) {
    bestFor.push("culture-first routes");
  }
  if (poiRow?.category === "food" || topTagNames.includes("coffee") || topTagNames.includes("street-food")) {
    bestFor.push("food breaks");
  }
  if (poiRow?.family_friendly) bestFor.push("family-friendly plans");
  if (poiRow?.indoor) bestFor.push("rainy-day coverage");
  if (topTagNames.includes("photography") || topTagNames.includes("night-view")) {
    bestFor.push("photo-oriented stops");
  }
  if (Number(poiRow?.stay_minutes) >= 90) bestFor.push("longer anchor stops");

  if (!poiRow?.indoor && ["park", "nature", "viewpoint"].includes(normalize(poiRow?.category).toLowerCase())) {
    watchOutFor.push("weather-sensitive");
  }
  if (Number(poiRow?.stay_minutes) >= 120) watchOutFor.push("needs a larger time window");
  if (Number(poiRow?.crowd_level) >= 3) watchOutFor.push(`${crowdLevelLabel(poiRow?.crowd_level)} flow`);
  if (normalize(poiRow?.opening_hours)) watchOutFor.push("double-check opening hours");

  return {
    metrics: {
      post_count: postCount,
      avg_rating: Number.isFinite(avgRating) ? Number(avgRating.toFixed(1)) : null,
      review_count: reviewCount,
      total_likes: likes,
      total_favorites: favorites,
      total_views: views,
      comment_count: comments,
    },
    top_tags: topTagNames.slice(0, 6),
    highlights: unique(highlights).slice(0, 4),
    best_for: unique(bestFor).slice(0, 5),
    watch_out_for: unique(watchOutFor).slice(0, 4),
  };
};

const fetchPoiCommunitySummary = async (poiRow) => {
  const poiId = Number(poiRow?.id);
  if (!Number.isFinite(poiId) || poiId <= 0) return null;

  const [[metricRow]] = await pool.query(
    `
      SELECT
        COUNT(*) AS post_count,
        AVG(CASE WHEN p.rating BETWEEN 1 AND 5 THEN p.rating END) AS avg_rating,
        COALESCE(SUM(p.like_count), 0) AS total_likes,
        COALESCE(SUM(p.favorite_count), 0) AS total_favorites,
        COALESCE(SUM(p.view_count), 0) AS total_views,
        (
          SELECT COUNT(*)
          FROM post_comments c
          JOIN posts cp ON cp.id = c.post_id
          WHERE cp.poi_id = ? AND COALESCE(cp.status, 'NORMAL') = 'NORMAL' AND COALESCE(c.status, 'NORMAL') = 'NORMAL'
        ) AS comment_count,
        (
          SELECT COUNT(*)
          FROM posts rp
          WHERE rp.poi_id = ? AND COALESCE(rp.status, 'NORMAL') = 'NORMAL' AND rp.rating BETWEEN 1 AND 5
        ) AS review_count
      FROM posts p
      WHERE p.poi_id = ? AND COALESCE(p.status, 'NORMAL') = 'NORMAL'
    `,
    [poiId, poiId, poiId]
  );

  const [tagRows] = await pool.query(
    `
      SELECT t.name, COUNT(*) AS tag_count
      FROM post_tags pt
      JOIN tags t ON t.id = pt.tag_id
      JOIN posts p ON p.id = pt.post_id
      WHERE p.poi_id = ? AND COALESCE(p.status, 'NORMAL') = 'NORMAL'
      GROUP BY t.id, t.name
      ORDER BY tag_count DESC, t.name ASC
      LIMIT 6
    `,
    [poiId]
  );

  return buildCommunitySummary(poiRow, metricRow || {}, tagRows || []);
};

const fetchPoiRelatedPosts = async (poiId, limit = 4) => {
  const safePoiId = Number(poiId);
  if (!Number.isFinite(safePoiId) || safePoiId <= 0) return [];

  const [rows] = await pool.query(
    `
      SELECT
        p.id,
        p.title,
        LEFT(p.content, 220) AS excerpt,
        p.cover_image,
        p.like_count,
        p.favorite_count,
        p.view_count,
        p.rating,
        p.created_at,
        u.nickname,
        u.avatar_url
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      WHERE p.poi_id = ? AND COALESCE(p.status, 'NORMAL') = 'NORMAL'
      ORDER BY (p.like_count * 4 + p.favorite_count * 6 + p.view_count * 0.02) DESC, p.created_at DESC, p.id DESC
      LIMIT ?
    `,
    [safePoiId, Math.max(1, Math.min(Number(limit) || 4, 8))]
  );

  const postIds = rows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
  const [primaryImages, tagsMap] = await Promise.all([
    fetchPostPrimaryImages(postIds),
    fetchPostTags(postIds),
  ]);

  return rows.map((row) => ({
    id: Number(row.id),
    title: normalize(row.title),
    excerpt: normalize(row.excerpt),
    cover_image: normalize(row.cover_image) || primaryImages.get(Number(row.id)) || null,
    like_count: Number(row.like_count) || 0,
    favorite_count: Number(row.favorite_count) || 0,
    view_count: Number(row.view_count) || 0,
    rating: Number.isFinite(Number(row.rating)) ? Number(row.rating) : null,
    created_at: row.created_at,
    author_name: normalize(row.nickname) || "Traveler",
    author_avatar: normalize(row.avatar_url) || null,
    tags: (tagsMap.get(Number(row.id)) || []).slice(0, 4),
  }));
};

const fetchPoiNeighbors = async ({ poiRow, sameCategory = false, limit = 4, radiusMeters = 1600 }) => {
  const poiId = Number(poiRow?.id);
  const lat = Number(poiRow?.lat);
  const lng = Number(poiRow?.lng);
  if (!Number.isFinite(poiId) || !Number.isFinite(lat) || !Number.isFinite(lng)) return [];

  const bbox = buildPoiBoundingBox(lat, lng, radiusMeters);
  const category = normalize(poiRow?.category).toLowerCase();
  const categoryClause = sameCategory
    ? `AND LOWER(COALESCE(category, '')) = ?`
    : category
      ? `AND LOWER(COALESCE(category, '')) <> ?`
      : "";
  const params = [
    lat,
    lat,
    lng,
    poiId,
    bbox.minLat,
    bbox.maxLat,
    bbox.minLng,
    bbox.maxLng,
    ...(categoryClause ? [category] : []),
    radiusMeters,
    Math.max(1, Math.min(Number(limit) || 4, 8)),
  ];

  const [rows] = await pool.query(
    `
      SELECT
        id,
        name,
        category,
        lat,
        lng,
        address,
        city,
        image_url,
        popularity,
        (
          6371000 * 2 * ASIN(
            SQRT(
              POWER(SIN(RADIANS(lat - ?) / 2), 2) +
              COS(RADIANS(?)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lng - ?) / 2), 2)
            )
          )
        ) AS distance_m
      FROM poi
      WHERE id <> ?
        AND lat BETWEEN ? AND ?
        AND lng BETWEEN ? AND ?
        ${categoryClause}
      HAVING distance_m <= ?
      ORDER BY popularity DESC, distance_m ASC, id ASC
      LIMIT ?
    `,
    params
  );

  return rows.map((row) => ({
    id: Number(row.id),
    name: normalize(row.name) || "POI",
    category: normalize(row.category) || "poi",
    lat: Number(row.lat),
    lng: Number(row.lng),
    address: normalize(row.address) || normalize(row.city),
    image_url: normalize(row.image_url) || null,
    popularity: Number.isFinite(Number(row.popularity)) ? Number(row.popularity) : null,
    distance_m: Math.round(Number(row.distance_m) || 0),
  }));
};

// GET /api/poi/search?keyword=xx&limit=10
router.get("/search", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=60, stale-while-revalidate=120");
    const keyword = normalize(req.query.keyword);
    const limit = Math.min(parseInt(req.query.limit || "10", 10), 50);
    if (!keyword) return res.json({ success: true, data: [] });

    const like = `%${keyword}%`;
    const [rows] = await pool.query(
      `SELECT id, name, category, lat, lng, popularity, price, tags, image_url
       FROM poi
       WHERE name LIKE ?
       ORDER BY popularity DESC, id DESC
       LIMIT ?`,
      [like, limit]
    );
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("poi search error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/poi/nearby?lat=..&lng=..&radius=1000&limit=20&category=food
router.get("/nearby", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=15, stale-while-revalidate=30");
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    const radius = parseInt(req.query.radius || "1000", 10);
    const limit = parseInt(req.query.limit || "20", 10);
    const category = normalize(req.query.category);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ success: false, message: "lat/lng required" });
    }

    const rows = await getNearbyPOIs(lat, lng, radius, limit, category || null);
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("poi nearby error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/parking-nearby", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=30, stale-while-revalidate=120");
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "8", 10) || 8, 12));
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ success: false, message: "lat/lng required" });
    }

    for (const radius of PARKING_RADIUS_STEPS) {
      const localItems = await fetchLocalParking(lat, lng, radius, limit);
      let items = localItems;
      if (items.length < limit) {
        try {
          const osmItems = await fetchOsmParking(lat, lng, radius, limit);
          items = dedupeParkingItems([...localItems, ...osmItems]).sort(
            (a, b) => Number(a.distance_m || 0) - Number(b.distance_m || 0)
          );
        } catch (err) {
          console.warn("parking overpass fallback failed:", err?.message || err);
        }
      }
      if (items.length) {
        return res.json({
          success: true,
          data: {
            found: true,
            searched_radius_m: radius,
            expanded: radius > PARKING_RADIUS_STEPS[0],
            items: items.slice(0, limit),
          },
        });
      }
    }

    return res.json({
      success: true,
      data: {
        found: false,
        searched_radius_m: PARKING_RADIUS_STEPS[PARKING_RADIUS_STEPS.length - 1],
        expanded: true,
        items: [],
      },
    });
  } catch (err) {
    console.error("parking nearby error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/poi/:id/photos?limit=6
router.get("/:id/photos", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=120, stale-while-revalidate=300");
    await ensurePoiSchema();
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ success: false, message: "invalid id" });
    const row = await getPoiDetailRow(id);
    if (!row) return res.status(404).json({ success: false, message: "not found" });

    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "6", 10) || 6, 12));
    const photos = await ensurePoiPhotos(row, { targetCount: limit, minCount: Math.min(4, limit) });
    res.json({
      success: true,
      data: {
        poi_id: id,
        primary_image: row.image_url || null,
        photos,
        count: photos.length,
      },
    });
  } catch (err) {
    console.error("poi photos error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/poi/:id
router.get("/:id", async (req, res) => {
  try {
    res.setHeader("Cache-Control", "public, max-age=120, stale-while-revalidate=300");
    await ensurePoiSchema();
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ success: false, message: "invalid id" });
    const row = await getPoiDetailRow(id);
    if (!row) return res.status(404).json({ success: false, message: "not found" });

    const photoLimit = Math.max(1, Math.min(parseInt(req.query.photo_limit || "6", 10) || 6, 12));
    const photos = await ensurePoiPhotos(row, { targetCount: photoLimit, minCount: Math.min(4, photoLimit) });

    row.photos = photos;
    row.photo_count = photos.length;
    if ((!row.image_url || !isHttpUrl(row.image_url)) && photos.length) {
      row.image_url = photos[0];
    }

    const [communitySummary, relatedPosts, similarPlaces, pairedPlaces] = await Promise.all([
      fetchPoiCommunitySummary(row),
      fetchPoiRelatedPosts(id, 4),
      fetchPoiNeighbors({ poiRow: row, sameCategory: true, limit: 4, radiusMeters: 1800 }),
      fetchPoiNeighbors({ poiRow: row, sameCategory: false, limit: 4, radiusMeters: 1400 }),
    ]);

    row.community_summary = communitySummary;
    row.related_posts = relatedPosts;
    row.similar_places = similarPlaces;
    row.paired_places = pairedPlaces;

    res.json({ success: true, data: row });
  } catch (err) {
    console.error("poi detail error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
