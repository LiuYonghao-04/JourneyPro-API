import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CONCURRENCY = Math.max(8, Math.min(48, Number(process.env.IMG_REPAIR_CONCURRENCY || 28)));
const TIMEOUT_MS = Math.max(3000, Number(process.env.IMG_REPAIR_TIMEOUT_MS || 9000));
const TARGET_COUNT = Math.max(4, Math.min(8, Number(process.env.IMG_REPAIR_TARGET_COUNT || 6)));
const MIN_COUNT = Math.max(4, Math.min(TARGET_COUNT, Number(process.env.IMG_REPAIR_MIN_COUNT || 4)));
const VERIFY_RETRY = Math.max(0, Number(process.env.IMG_REPAIR_RETRY || 1));

const normalize = (value) => String(value || "").trim();
const normalizeLower = (value) => normalize(value).toLowerCase();
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];
const uniqueByDbPrefix = (items, prefixLen = 255) => {
  const seen = new Set();
  const output = [];
  for (const item of items || []) {
    const url = normalize(item);
    if (!url) continue;
    const key = url.slice(0, prefixLen);
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(url);
  }
  return output;
};
const isHttp = (value) => /^https?:\/\//i.test(normalize(value));

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const verifyCache = new Map();

const verifyImageUrl = async (url) => {
  const safeUrl = normalize(url);
  if (!safeUrl || !isHttp(safeUrl)) return false;
  if (verifyCache.has(safeUrl)) return verifyCache.get(safeUrl);

  const task = (async () => {
    for (let attempt = 0; attempt <= VERIFY_RETRY; attempt += 1) {
      try {
        const head = await fetch(safeUrl, {
          method: "HEAD",
          redirect: "follow",
          headers: { "User-Agent": "Mozilla/5.0 (JourneyPro image checker)" },
          signal: AbortSignal.timeout(TIMEOUT_MS),
        });
        if (head.ok) {
          const type = normalizeLower(head.headers.get("content-type"));
          if (type.startsWith("image/")) return true;
        }
      } catch {
        // fall through to GET
      }
      try {
        const get = await fetch(safeUrl, {
          method: "GET",
          redirect: "follow",
          headers: {
            "User-Agent": "Mozilla/5.0 (JourneyPro image checker)",
            Range: "bytes=0-1024",
          },
          signal: AbortSignal.timeout(TIMEOUT_MS),
        });
        if (get.ok) {
          const type = normalizeLower(get.headers.get("content-type"));
          if (type.startsWith("image/")) return true;
        }
      } catch {
        // retry if available
      }
      if (attempt < VERIFY_RETRY) {
        await sleep(180 + attempt * 260);
      }
    }
    return false;
  })();

  verifyCache.set(safeUrl, task);
  return task;
};

const mapWithConcurrency = async (items, concurrency, worker) => {
  const queue = [...items];
  await Promise.all(
    Array.from({ length: Math.min(concurrency, queue.length || 1) }, async () => {
      while (queue.length) {
        const item = queue.shift();
        if (item === undefined) return;
        // eslint-disable-next-line no-await-in-loop
        await worker(item);
      }
    })
  );
};

const pickFromPool = (seedKey, pool, count, existing = []) => {
  const result = [...existing];
  if (!pool.length) return result;
  const seed = stableHash(seedKey);
  for (let i = 0; i < pool.length && result.length < count; i += 1) {
    const idx = (seed + i * 17) % pool.length;
    const url = pool[idx];
    if (!url || result.includes(url)) continue;
    result.push(url);
  }
  return result;
};

const main = async () => {
  const pool = mysql.createPool({ ...DB, connectionLimit: Math.max(12, CONCURRENCY + 6), waitForConnections: true });

  const [urlRows] = await pool.query(
    `
      SELECT image_url AS url FROM poi_photos
      UNION
      SELECT image_url AS url FROM post_images
      UNION
      SELECT cover_image AS url FROM posts WHERE cover_image IS NOT NULL
      UNION
      SELECT avatar_url AS url FROM users WHERE avatar_url IS NOT NULL
    `
  );
  const allUrls = unique(urlRows.map((r) => r.url)).filter(isHttp);
  console.log(`verify_start total_urls=${allUrls.length} concurrency=${CONCURRENCY}`);

  const validSet = new Set();
  const invalidSet = new Set();
  let checked = 0;

  await mapWithConcurrency(allUrls, CONCURRENCY, async (url) => {
    const ok = await verifyImageUrl(url);
    checked += 1;
    if (ok) validSet.add(url);
    else invalidSet.add(url);
    if (checked % 250 === 0) {
      console.log(`verify_progress checked=${checked} valid=${validSet.size} invalid=${invalidSet.size}`);
    }
  });

  console.log(`verify_done checked=${checked} valid=${validSet.size} invalid=${invalidSet.size}`);

  // Build category/global pools from currently valid POI photos.
  const [validPhotoRows] = await pool.query(
    `
      SELECT p.id AS poi_id, p.name, p.category, pp.image_url
      FROM poi p
      JOIN poi_photos pp ON pp.poi_id = p.id
      WHERE p.source IN ('OSM', 'NAPTAN', 'USER')
      ORDER BY pp.sort_order ASC, pp.id ASC
    `
  );
  const categoryPool = new Map();
  const globalPool = [];
  validPhotoRows.forEach((row) => {
    const url = normalize(row.image_url);
    if (!validSet.has(url)) return;
    const cat = normalizeLower(row.category || "unknown");
    if (!categoryPool.has(cat)) categoryPool.set(cat, []);
    const list = categoryPool.get(cat);
    if (!list.includes(url)) list.push(url);
    if (!globalPool.includes(url)) globalPool.push(url);
  });

  // Repair poi_photos per POI and ensure 4-6 photos for all sources.
  const [poiRows] = await pool.query(
    `
      SELECT id, name, category, image_url
      FROM poi
      WHERE source IN ('OSM', 'NAPTAN', 'USER')
      ORDER BY id ASC
    `
  );
  let poiUpdated = 0;
  let poiImageUpdated = 0;

  for (const poi of poiRows) {
    const [rows] = await pool.query(
      `SELECT id, image_url FROM poi_photos WHERE poi_id = ? ORDER BY sort_order ASC, id ASC`,
      [poi.id]
    );
    const validExisting = unique(
      rows
        .map((r) => normalize(r.image_url))
        .filter((url) => validSet.has(url))
    );
    let merged = [...validExisting];

    const primary = normalize(poi.image_url);
    if (validSet.has(primary) && !merged.includes(primary)) merged.unshift(primary);

    const catPool = categoryPool.get(normalizeLower(poi.category || "unknown")) || [];
    merged = pickFromPool(`poi:${poi.id}:${poi.name}`, catPool, TARGET_COUNT, merged);
    merged = pickFromPool(`poi-global:${poi.id}:${poi.name}`, globalPool, TARGET_COUNT, merged);
    merged = uniqueByDbPrefix(merged).slice(0, TARGET_COUNT);

    if (merged.length < MIN_COUNT) {
      continue;
    }

    // Replace to exact repaired set.
    await pool.query(`DELETE FROM poi_photos WHERE poi_id = ?`, [poi.id]);
    const insertRows = uniqueByDbPrefix(merged).map((url, idx) => [poi.id, url, "REPAIRED", idx]);
    await pool.query(
      `
        INSERT INTO poi_photos (poi_id, image_url, source, sort_order)
        VALUES ?
      `,
      [insertRows]
    );
    poiUpdated += 1;

    if (!validSet.has(primary) || primary !== merged[0]) {
      await pool.query(`UPDATE poi SET image_url = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [merged[0], poi.id]);
      poiImageUpdated += 1;
    }
  }

  // Repair post images/covers.
  const [postRows] = await pool.query(`SELECT id, poi_id, cover_image FROM posts ORDER BY id ASC`);
  let postRowsUpdated = 0;
  let postImageRowsUpdated = 0;
  for (const post of postRows) {
    const [imgRows] = await pool.query(
      `SELECT id, image_url, sort_order FROM post_images WHERE post_id = ? ORDER BY sort_order ASC, id ASC`,
      [post.id]
    );
    const validImages = imgRows.map((r) => normalize(r.image_url)).filter((url) => validSet.has(url));
    let repaired = uniqueByDbPrefix(validImages);

    if (repaired.length < 4 && post.poi_id) {
      const [poiPhotoRows] = await pool.query(
        `SELECT image_url FROM poi_photos WHERE poi_id = ? ORDER BY sort_order ASC, id ASC LIMIT ?`,
        [post.poi_id, TARGET_COUNT]
      );
      const poiPhotos = unique(poiPhotoRows.map((r) => r.image_url)).filter((url) => validSet.has(url));
      repaired = uniqueByDbPrefix([...repaired, ...poiPhotos]);
    }
    if (repaired.length < 4) {
      repaired = pickFromPool(`post:${post.id}`, globalPool, TARGET_COUNT, repaired);
    }
    repaired = uniqueByDbPrefix(repaired).slice(0, TARGET_COUNT);
    if (!repaired.length) continue;

    await pool.query(`DELETE FROM post_images WHERE post_id = ?`, [post.id]);
    const insertRows = repaired.map((url, idx) => [post.id, url, idx]);
    await pool.query(`INSERT INTO post_images (post_id, image_url, sort_order) VALUES ?`, [insertRows]);
    postImageRowsUpdated += 1;

    const nextCover = repaired[0];
    if (normalize(post.cover_image) !== nextCover) {
      await pool.query(
        `UPDATE posts SET cover_image = ?, image_count = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [nextCover, repaired.length, post.id]
      );
      postRowsUpdated += 1;
    } else {
      await pool.query(`UPDATE posts SET image_count = ? WHERE id = ?`, [repaired.length, post.id]);
    }
  }

  // Repair avatars if invalid.
  const [users] = await pool.query(`SELECT id, nickname, avatar_url FROM users`);
  let avatarUpdated = 0;
  for (const user of users) {
    const avatar = normalize(user.avatar_url);
    if (avatar && validSet.has(avatar)) continue;
    const replacement = `https://api.dicebear.com/9.x/personas/svg?seed=${encodeURIComponent(
      normalize(user.nickname) || `user-${user.id}`
    )}`;
    await pool.query(`UPDATE users SET avatar_url = ? WHERE id = ?`, [replacement, user.id]);
    avatarUpdated += 1;
  }

  const [[summaryPoi]] = await pool.query(
    `
      SELECT
        COUNT(*) AS poi_total,
        SUM(cnt >= ?) AS poi_ready,
        SUM(cnt = 0) AS poi_zero
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
  const [[summaryPosts]] = await pool.query(
    `
      SELECT
        COUNT(*) AS posts_total,
        SUM(image_count >= 4) AS posts_image_ready
      FROM posts
    `
  );

  await pool.end();
  console.log(
    JSON.stringify(
      {
        verified: { checked, valid: validSet.size, invalid: invalidSet.size },
        repaired: {
          poi_rows_replaced: poiUpdated,
          poi_primary_updated: poiImageUpdated,
          post_rows_cover_updated: postRowsUpdated,
          post_rows_images_rebuilt: postImageRowsUpdated,
          users_avatar_updated: avatarUpdated,
        },
        summary: {
          poi_total: Number(summaryPoi?.poi_total) || 0,
          poi_ready: Number(summaryPoi?.poi_ready) || 0,
          poi_zero: Number(summaryPoi?.poi_zero) || 0,
          posts_total: Number(summaryPosts?.posts_total) || 0,
          posts_image_ready: Number(summaryPosts?.posts_image_ready) || 0,
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
