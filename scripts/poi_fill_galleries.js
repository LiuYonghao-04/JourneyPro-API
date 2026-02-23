import mysql from "mysql2/promise";
import { getPoiPhotoUrls } from "../services/poiPhotos.js";

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
const OFFSET = Math.max(0, Number(process.env.POI_GALLERY_OFFSET || 0));
const CONCURRENCY = Math.max(2, Math.min(24, Number(process.env.POI_GALLERY_CONCURRENCY || 10)));
const VERIFY = String(process.env.POI_GALLERY_VERIFY || "0") === "1";
const QUERY_RESULT_COUNT = Math.max(12, Math.min(50, Number(process.env.POI_GALLERY_QUERY_RN || 30)));
const CATEGORY_POOL_SIZE = Math.max(12, Number(process.env.POI_GALLERY_POOL || 46));
const TIMEOUT_MS = Math.max(3000, Number(process.env.POI_GALLERY_TIMEOUT_MS || 12000));
const LOG_EVERY = Math.max(20, Number(process.env.POI_GALLERY_LOG_EVERY || 120));

const normalize = (value) => String(value || "").trim();
const normalizeLower = (value) => normalize(value).toLowerCase();
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];
const isHttp = (value) => /^https?:\/\//i.test(normalize(value));

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

const fetchRows = async (pool) => {
  const [rows] = await pool.query(
    `
      SELECT
        p.id,
        p.name,
        p.category,
        p.city,
        p.image_url,
        p.popularity,
        COALESCE(pp.photo_count, 0) AS photo_count
      FROM poi p
      LEFT JOIN (
        SELECT poi_id, COUNT(*) AS photo_count
        FROM poi_photos
        GROUP BY poi_id
      ) pp ON pp.poi_id = p.id
      WHERE p.source='OSM'
        AND COALESCE(pp.photo_count, 0) < ?
      ORDER BY p.popularity DESC, p.id ASC
      ${LIMIT > 0 ? "LIMIT ? OFFSET ?" : ""}
    `,
    LIMIT > 0 ? [MIN_COUNT, LIMIT, OFFSET] : [MIN_COUNT]
  );
  return rows;
};

const getGroupKey = (row) =>
  `${normalizeLower(row?.name)}|${normalizeLower(row?.category)}|${normalizeLower(row?.city || "")}`;

const savePoiPhotos = async (pool, poiId, urls, source = "AUTO_BAIDU_BATCH") => {
  const finalUrls = unique(urls).filter(isHttp).slice(0, TARGET_COUNT);
  if (!finalUrls.length) return 0;
  const rows = finalUrls.map((url, idx) => [poiId, url, source, idx]);
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
  return finalUrls.length;
};

const main = async () => {
  const pool = mysql.createPool({
    ...DB,
    connectionLimit: Math.max(8, CONCURRENCY + 2),
    waitForConnections: true,
  });

  await ensureTable(pool);
  const rows = await fetchRows(pool);
  if (!rows.length) {
    await pool.end();
    console.log("No POIs need gallery fill.");
    return;
  }

  const groupCache = new Map();
  let scanned = 0;
  let updatedPoi = 0;
  let addedUrls = 0;
  let emptyResult = 0;
  let usedGroupCache = 0;
  let fromPrimaryOnly = 0;

  const queue = [...rows];

  const worker = async () => {
    while (queue.length) {
      const row = queue.shift();
      if (!row) return;
      scanned += 1;

      const key = getGroupKey(row);
      let groupTask = groupCache.get(key);
      if (!groupTask) {
        groupTask = getPoiPhotoUrls(row, {
          targetCount: TARGET_COUNT,
          verify: VERIFY,
          resultCount: QUERY_RESULT_COUNT,
          categoryPoolSize: CATEGORY_POOL_SIZE,
          timeoutMs: TIMEOUT_MS,
        }).catch((err) => {
          console.error("group photo fetch error", { id: row.id, name: row.name, err: String(err) });
          return [];
        });
        groupCache.set(key, groupTask);
      } else {
        usedGroupCache += 1;
      }

      // eslint-disable-next-line no-await-in-loop
      const groupPhotos = await groupTask;
      const merged = unique([row.image_url, ...(groupPhotos || [])]).filter(isHttp).slice(0, TARGET_COUNT);
      if (!merged.length) {
        emptyResult += 1;
        if (scanned % LOG_EVERY === 0) {
          console.log(`progress scanned=${scanned} updated=${updatedPoi} empty=${emptyResult}`);
        }
        continue;
      }
      if (merged.length === 1 && merged[0] === row.image_url) fromPrimaryOnly += 1;

      // eslint-disable-next-line no-await-in-loop
      const savedCount = await savePoiPhotos(pool, row.id, merged);
      if (savedCount > 0) {
        updatedPoi += 1;
        addedUrls += savedCount;
      } else {
        emptyResult += 1;
      }

      if (scanned % LOG_EVERY === 0 || updatedPoi % LOG_EVERY === 0) {
        console.log(`progress scanned=${scanned} updated=${updatedPoi} empty=${emptyResult}`);
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length) }, () => worker()));

  const [[summary]] = await pool.query(
    `
      SELECT
        COUNT(*) AS poi_total,
        SUM(cnt >= ?) AS poi_ready
      FROM (
        SELECT p.id, COUNT(pp.id) AS cnt
        FROM poi p
        LEFT JOIN poi_photos pp ON pp.poi_id = p.id
        WHERE p.source='OSM'
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
        updated_poi: updatedPoi,
        added_urls: addedUrls,
        empty_result: emptyResult,
        used_group_cache: usedGroupCache,
        from_primary_only: fromPrimaryOnly,
        verify: VERIFY,
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
