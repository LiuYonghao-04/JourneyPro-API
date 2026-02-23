import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const TARGET_COUNT = Math.max(3, Math.min(12, Number(process.env.POI_GALLERY_TARGET || 6)));
const MIN_COUNT = Math.max(1, Math.min(TARGET_COUNT, Number(process.env.POI_GALLERY_MIN || 4)));
const LIMIT = Math.max(0, Number(process.env.POI_GALLERY_LIMIT || 0));
const CATEGORY_POOL_LIMIT = Math.max(80, Number(process.env.POI_GALLERY_CATEGORY_POOL || 360));

const normalize = (value) => String(value || "").trim();
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const ensureTable = async (pool) => {
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
};

const loadCategoryPool = async (pool, category, cache) => {
  const key = normalize(category).toLowerCase();
  if (cache.has(key)) return cache.get(key);
  const [rows] = await pool.query(
    `
      SELECT pp.image_url
      FROM poi_photos pp
      JOIN poi p ON p.id = pp.poi_id
      WHERE p.source IN ('OSM', 'NAPTAN', 'USER')
        AND LOWER(COALESCE(p.category,'')) = ?
      ORDER BY pp.id DESC
      LIMIT ?
    `,
    [key, CATEGORY_POOL_LIMIT]
  );
  const list = unique(rows.map((r) => r.image_url));
  cache.set(key, list);
  return list;
};

const main = async () => {
  const pool = mysql.createPool({ ...DB, connectionLimit: 10, waitForConnections: true });
  await ensureTable(pool);

  const [rows] = await pool.query(
    `
      SELECT
        p.id,
        p.name,
        p.category,
        p.image_url,
        COALESCE(pp.photo_count, 0) AS photo_count
      FROM poi p
      LEFT JOIN (
        SELECT poi_id, COUNT(*) AS photo_count
        FROM poi_photos
        GROUP BY poi_id
      ) pp ON pp.poi_id = p.id
      WHERE p.source IN ('OSM', 'NAPTAN', 'USER')
        AND COALESCE(pp.photo_count, 0) < ?
      ORDER BY p.popularity DESC, p.id ASC
      ${LIMIT > 0 ? "LIMIT ?" : ""}
    `,
    LIMIT > 0 ? [MIN_COUNT, LIMIT] : [MIN_COUNT]
  );

  if (!rows.length) {
    await pool.end();
    console.log("No POIs need category backfill.");
    return;
  }

  const categoryCache = new Map();
  let scanned = 0;
  let updated = 0;
  let stillLow = 0;

  for (const row of rows) {
    scanned += 1;

    const [existingRows] = await pool.query(
      `SELECT image_url FROM poi_photos WHERE poi_id = ? ORDER BY sort_order ASC, id ASC LIMIT ?`,
      [row.id, TARGET_COUNT]
    );
    const existing = unique([row.image_url, ...existingRows.map((r) => r.image_url)]);
    if (existing.length >= MIN_COUNT) continue;

    const poolByCategory = await loadCategoryPool(pool, row.category, categoryCache);
    const merged = [...existing];
    const seed = stableHash(`${row.id}|${row.name}|${row.category}`);
    for (let i = 0; i < poolByCategory.length && merged.length < TARGET_COUNT; i += 1) {
      const idx = (seed + i * 19) % poolByCategory.length;
      const url = poolByCategory[idx];
      if (!url || merged.includes(url)) continue;
      merged.push(url);
    }

    if (merged.length < MIN_COUNT) {
      stillLow += 1;
      continue;
    }

    const insertRows = merged.slice(0, TARGET_COUNT).map((url, i) => [row.id, url, "CATEGORY_BACKFILL", i]);
    await pool.query(
      `
        INSERT INTO poi_photos (poi_id, image_url, source, sort_order)
        VALUES ?
        ON DUPLICATE KEY UPDATE
          sort_order = LEAST(sort_order, VALUES(sort_order)),
          updated_at = CURRENT_TIMESTAMP
      `,
      [insertRows]
    );
    updated += 1;

    if (scanned % 150 === 0) {
      console.log(`progress scanned=${scanned} updated=${updated} still_low=${stillLow}`);
    }
  }

  const [[summary]] = await pool.query(
    `
      SELECT
        COUNT(*) AS poi_total,
        SUM(cnt >= ?) AS poi_ready
      FROM (
        SELECT p.id, COUNT(pp.id) AS cnt
        FROM poi p
        LEFT JOIN poi_photos pp ON pp.poi_id = p.id
        WHERE p.source IN ('OSM', 'NAPTAN', 'USER')
        GROUP BY p.id
      ) t
    `,
    [MIN_COUNT]
  );

  await pool.end();
  console.log(
    JSON.stringify(
      {
        scanned,
        updated,
        still_low: stillLow,
        target_count: TARGET_COUNT,
        min_count: MIN_COUNT,
        summary: {
          poi_total: Number(summary?.poi_total) || 0,
          poi_ready: Number(summary?.poi_ready) || 0,
        },
      },
      null,
      2
    )
  );
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
