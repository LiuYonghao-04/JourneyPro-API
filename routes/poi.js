import express from "express";
import { pool } from "../db/connect.js";
import { getNearbyPOIs } from "../models/poi.js";
import { getPoiPhotoUrls } from "../services/poiPhotos.js";

const router = express.Router();

let ensurePoiSchemaPromise = null;
const poiPhotoFillInFlight = new Map();

const normalize = (value) => String(value || "").trim();
const isHttpUrl = (value) => /^https?:\/\//i.test(normalize(value));
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];

const isBenignAlterError = (err) => {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("Duplicate column") ||
    msg.includes("check that column/key exists") ||
    msg.includes("already exists")
  );
};

const safeAlter = async (sql) => {
  try {
    await pool.query(sql);
  } catch (err) {
    if (!isBenignAlterError(err)) throw err;
  }
};

const ensurePoiSchema = async () => {
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

    res.json({ success: true, data: row });
  } catch (err) {
    console.error("poi detail error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
