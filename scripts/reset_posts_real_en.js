import mysql from "mysql2/promise";
import { getPoiPhotoUrls } from "../services/poiPhotos.js";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const normalize = (value) => String(value || "").trim();
const isHttp = (value) => /^https?:\/\//i.test(normalize(value));
const unique = (arr) => [...new Set((arr || []).map((x) => normalize(x)).filter(Boolean))];

const ensureTables = async (pool) => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT NOT NULL,
      poi_id BIGINT NULL,
      title VARCHAR(100) NOT NULL,
      content TEXT NOT NULL,
      rating TINYINT NULL,
      cover_image VARCHAR(600) NULL,
      image_count INT DEFAULT 0,
      like_count INT DEFAULT 0,
      favorite_count INT DEFAULT 0,
      view_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'NORMAL',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_images (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      image_url VARCHAR(600) NOT NULL,
      sort_order INT DEFAULT 0
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tags (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(50) NOT NULL UNIQUE,
      type VARCHAR(30) DEFAULT 'CATEGORY',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS post_tags (
      post_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      PRIMARY KEY (post_id, tag_id)
    );
  `);
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
    );
  `);
  await pool.query(`ALTER TABLE posts MODIFY cover_image VARCHAR(600) NULL`);
  await pool.query(`ALTER TABLE post_images MODIFY image_url VARCHAR(600) NOT NULL`);
};

const pickTargetPoi = async (conn) => {
  const preferredNames = [
    "Tate Modern",
    "Tower Bridge",
    "London Eye",
    "Natural History Museum",
    "Borough Market",
  ];
  const placeholders = preferredNames.map(() => "?").join(",");
  const [rows] = await conn.query(
    `
      SELECT id, name, category, lat, lng, city, address, image_url, popularity
      FROM poi
      WHERE source = 'OSM'
        AND image_url REGEXP '^(http|https)://'
        AND name IN (${placeholders})
      ORDER BY popularity DESC, id ASC
      LIMIT 1
    `,
    preferredNames
  );
  if (rows.length) return rows[0];

  const [fallback] = await conn.query(
    `
      SELECT id, name, category, lat, lng, city, address, image_url, popularity
      FROM poi
      WHERE source = 'OSM'
        AND image_url REGEXP '^(http|https)://'
      ORDER BY popularity DESC, id ASC
      LIMIT 1
    `
  );
  return fallback[0] || null;
};

const ensurePoiPhotos = async (conn, poi, targetCount = 6) => {
  const [existingRows] = await conn.query(
    `SELECT image_url FROM poi_photos WHERE poi_id = ? ORDER BY sort_order ASC, id ASC LIMIT ?`,
    [poi.id, targetCount]
  );
  let photos = unique(existingRows.map((row) => row.image_url)).filter(isHttp);

  if (photos.length < targetCount) {
    const generated = await getPoiPhotoUrls(poi, {
      targetCount,
      verify: true,
      resultCount: 30,
      timeoutMs: 12000,
      categoryPoolSize: 42,
    });
    const merged = unique([poi.image_url, ...photos, ...generated]).filter(isHttp).slice(0, targetCount);
    if (merged.length) {
      const values = merged.map((url, idx) => [poi.id, url, "RESET_SCRIPT", idx]);
      await conn.query(
        `
          INSERT INTO poi_photos (poi_id, image_url, source, sort_order)
          VALUES ?
          ON DUPLICATE KEY UPDATE
            sort_order = LEAST(sort_order, VALUES(sort_order)),
            updated_at = CURRENT_TIMESTAMP
        `,
        [values]
      );
    }
    photos = merged;
  }

  return photos.slice(0, targetCount);
};

const main = async () => {
  const pool = mysql.createPool({ ...DB, connectionLimit: 8, waitForConnections: true });
  await ensureTables(pool);

  const conn = await pool.getConnection();
  let foreignChecksDisabled = false;
  try {
    const [[user]] = await conn.query(`SELECT id FROM users ORDER BY id ASC LIMIT 1`);
    const userId = user?.id || 1;

    const poi = await pickTargetPoi(conn);
    if (!poi) throw new Error("No POI with real image found.");

    const poiPhotos = await ensurePoiPhotos(conn, poi, 6);
    const postImages = unique([poi.image_url, ...poiPhotos]).filter(isHttp).slice(0, 6);
    if (!postImages.length) throw new Error("Failed to prepare post images.");

    const tagNames = ["museum", "architecture", "citywalk", "photography", "riverfront"];

    await conn.query(`SET FOREIGN_KEY_CHECKS = 0`);
    foreignChecksDisabled = true;
    await conn.beginTransaction();

    await conn.query(`DELETE FROM comment_likes`);
    await conn.query(`DELETE FROM post_comments`);
    await conn.query(`DELETE FROM post_views`);
    await conn.query(`DELETE FROM post_favorites`);
    await conn.query(`DELETE FROM post_likes`);
    await conn.query(`DELETE FROM post_tags`);
    await conn.query(`DELETE FROM post_images`);
    await conn.query(`DELETE FROM posts`);
    await conn.query(`DELETE FROM tags`);

    const postTitle = `Golden Hour at ${poi.name}`;
    const postContent = [
      `I planned a short route around ${poi.name} and the nearby riverside.`,
      "The light was best around sunset, and the crowd was manageable after 5 PM.",
      "If you enjoy architecture and city photography, this stop is worth adding to your route.",
    ].join(" ");

    const [postResult] = await conn.query(
      `
        INSERT INTO posts (user_id, poi_id, title, content, rating, cover_image, image_count, like_count, favorite_count, view_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [userId, poi.id, postTitle, postContent, 5, postImages[0], postImages.length, 17, 9, 128]
    );
    const postId = postResult.insertId;

    const imageRows = postImages.map((url, idx) => [postId, url, idx]);
    await conn.query(`INSERT INTO post_images (post_id, image_url, sort_order) VALUES ?`, [imageRows]);

    for (const name of tagNames) {
      const [tagRes] = await conn.query(`INSERT INTO tags (name, type) VALUES (?, 'CATEGORY')`, [name]);
    await conn.query(`INSERT INTO post_tags (post_id, tag_id) VALUES (?, ?)`, [postId, tagRes.insertId]);
    }

    await conn.commit();
    await conn.query(`SET FOREIGN_KEY_CHECKS = 1`);
    foreignChecksDisabled = false;

    console.log(
      JSON.stringify(
        {
          success: true,
          user_id: userId,
          post_id: postId,
          poi_id: poi.id,
          poi_name: poi.name,
          image_count: postImages.length,
          tags: tagNames,
        },
        null,
        2
      )
    );
  } catch (err) {
    try {
      await conn.rollback();
    } catch {
      // ignore
    }
    if (foreignChecksDisabled) {
      try {
        await conn.query(`SET FOREIGN_KEY_CHECKS = 1`);
      } catch {
        // ignore
      }
    }
    throw err;
  } finally {
    conn.release();
    await pool.end();
  }
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
