import "dotenv/config";
import { pool } from "../db/connect.js";
import { chunk } from "../services/reco/math.js";
import { ensureRecoTables } from "../services/reco/schema.js";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const raw = args.find((item) => item.startsWith(`${name}=`));
  if (!raw) return fallback;
  const value = Number.parseInt(raw.split("=")[1], 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
};

const historyDays = getArg("--days", 365);
const sampleLikes = getArg("--sample-likes", 120000);
const sampleFavorites = getArg("--sample-favorites", 120000);
const sampleViews = getArg("--sample-views", 60000);

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);
const round = (value, digits = 6) => {
  const factor = 10 ** digits;
  return Math.round(Number(value || 0) * factor) / factor;
};

const upsertUserInterest = async (conn, rows) => {
  if (!rows.length) return 0;
  const groups = chunk(rows, 1000);
  let written = 0;
  for (const batch of groups) {
    // eslint-disable-next-line no-await-in-loop
    await conn.query(
      `
        INSERT INTO user_interest_agg
          (user_id, feature_key, feature_type, score, last_event_at, updated_at)
        VALUES ?
        ON DUPLICATE KEY UPDATE
          score = VALUES(score),
          last_event_at = VALUES(last_event_at),
          updated_at = CURRENT_TIMESTAMP
      `,
      [batch]
    );
    written += batch.length;
  }
  return written;
};

const upsertPoiQuality = async (conn, rows) => {
  if (!rows.length) return 0;
  const groups = chunk(rows, 1000);
  let written = 0;
  for (const batch of groups) {
    // eslint-disable-next-line no-await-in-loop
    await conn.query(
      `
        INSERT INTO poi_quality_stats
          (poi_id, mode, impressions, interactions, add_via_count, save_count, quality_score, novelty_score, updated_at)
        VALUES ?
        ON DUPLICATE KEY UPDATE
          impressions = VALUES(impressions),
          interactions = VALUES(interactions),
          add_via_count = VALUES(add_via_count),
          save_count = VALUES(save_count),
          quality_score = VALUES(quality_score),
          novelty_score = VALUES(novelty_score),
          updated_at = CURRENT_TIMESTAMP
      `,
      [batch]
    );
    written += batch.length;
  }
  return written;
};

const main = async () => {
  const startedAt = Date.now();
  await ensureRecoTables();
  const conn = await pool.getConnection();

  try {
    console.log(
      `[reco:bootstrap-fast] start days=${historyDays} likes=${sampleLikes} favorites=${sampleFavorites} views=${sampleViews}`
    );

    await conn.query(`
      CREATE TEMPORARY TABLE tmp_reco_bootstrap_interactions (
        user_id BIGINT NOT NULL,
        post_id BIGINT NOT NULL,
        event_at DATETIME NOT NULL,
        weight FLOAT NOT NULL,
        event_type VARCHAR(20) NOT NULL,
        KEY idx_tmp_user (user_id),
        KEY idx_tmp_post (post_id),
        KEY idx_tmp_event (event_type, event_at)
      ) ENGINE=InnoDB
    `);

    const [likeInsert] = await conn.query(
      `
        INSERT INTO tmp_reco_bootstrap_interactions (user_id, post_id, event_at, weight, event_type)
        SELECT pl.user_id, pl.post_id, pl.created_at, 2, 'like_post'
        FROM post_likes pl FORCE INDEX (idx_post_likes_created)
        WHERE pl.created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        ORDER BY pl.created_at DESC
        LIMIT ?
      `,
      [historyDays, sampleLikes]
    );

    const [favInsert] = await conn.query(
      `
        INSERT INTO tmp_reco_bootstrap_interactions (user_id, post_id, event_at, weight, event_type)
        SELECT pf.user_id, pf.post_id, pf.created_at, 3, 'favorite_post'
        FROM post_favorites pf FORCE INDEX (idx_post_favorites_created)
        WHERE pf.created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        ORDER BY pf.created_at DESC
        LIMIT ?
      `,
      [historyDays, sampleFavorites]
    );

    const [viewInsert] = await conn.query(
      `
        INSERT INTO tmp_reco_bootstrap_interactions (user_id, post_id, event_at, weight, event_type)
        SELECT pv.user_id, pv.post_id, pv.last_viewed_at, LEAST(COALESCE(pv.view_count, 1), 6), 'detail_view'
        FROM post_views pv
        WHERE pv.last_viewed_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        ORDER BY pv.last_viewed_at DESC
        LIMIT ?
      `,
      [historyDays, sampleViews]
    );

    const [catRows] = await conn.query(
      `
        SELECT
          ti.user_id,
          LOWER(COALESCE(NULLIF(TRIM(poi.category), ''), 'unknown')) AS feature_key,
          SUM(ti.weight * EXP(-TIMESTAMPDIFF(DAY, ti.event_at, NOW()) / 30)) AS score,
          MAX(ti.event_at) AS last_event_at
        FROM tmp_reco_bootstrap_interactions ti
        JOIN posts p ON p.id = ti.post_id
        JOIN poi ON poi.id = p.poi_id
        GROUP BY ti.user_id, feature_key
        HAVING score <> 0
      `
    );

    const [poiRows] = await conn.query(
      `
        SELECT
          ti.user_id,
          CAST(p.poi_id AS CHAR(255)) AS feature_key,
          SUM(ti.weight * EXP(-TIMESTAMPDIFF(DAY, ti.event_at, NOW()) / 30)) AS score,
          MAX(ti.event_at) AS last_event_at
        FROM tmp_reco_bootstrap_interactions ti
        JOIN posts p ON p.id = ti.post_id
        WHERE p.poi_id IS NOT NULL
        GROUP BY ti.user_id, p.poi_id
        HAVING score <> 0
      `
    );

    const categoryUpsertRows = catRows.map((row) => [
      Number(row.user_id),
      String(row.feature_key || "unknown").slice(0, 255),
      "category",
      round(row.score, 6),
      row.last_event_at || new Date(),
      new Date(),
    ]);
    const poiUpsertRows = poiRows.map((row) => [
      Number(row.user_id),
      String(row.feature_key || "").slice(0, 255),
      "poi",
      round(row.score, 6),
      row.last_event_at || new Date(),
      new Date(),
    ]);
    const interestWritten =
      (await upsertUserInterest(conn, categoryUpsertRows)) + (await upsertUserInterest(conn, poiUpsertRows));

    const [qualityRows] = await conn.query(
      `
        SELECT
          p.poi_id AS poi_id,
          COUNT(*) AS interactions,
          SUM(CASE WHEN ti.event_type = 'favorite_post' THEN 1 ELSE 0 END) AS save_count
        FROM tmp_reco_bootstrap_interactions ti
        JOIN posts p ON p.id = ti.post_id
        WHERE p.poi_id IS NOT NULL
        GROUP BY p.poi_id
      `
    );

    const qualityUpsertRows = qualityRows.map((row) => {
      const interactions = Math.max(0, Number(row.interactions) || 0);
      const impressions = Math.max(interactions + 8, Math.round(interactions * 1.6));
      const saveCount = Math.max(0, Number(row.save_count) || 0);
      const qualityScore = clamp(Math.log(1 + interactions) / Math.log(201), 0, 1);
      const noveltyScore = clamp(1 / (1 + Math.log(1 + impressions)), 0, 1);
      return [
        Number(row.poi_id),
        "driving",
        impressions,
        interactions,
        0,
        saveCount,
        round(qualityScore, 6),
        round(noveltyScore, 6),
        new Date(),
      ];
    });
    const qualityWritten = await upsertPoiQuality(conn, qualityUpsertRows);

    await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_reco_bootstrap_interactions`);

    console.log(
      `[reco:bootstrap-fast] done likes=${Number(likeInsert?.affectedRows || 0)} favorites=${Number(
        favInsert?.affectedRows || 0
      )} views=${Number(viewInsert?.affectedRows || 0)} interest_rows=${interestWritten} quality_rows=${qualityWritten} elapsed_ms=${
        Date.now() - startedAt
      }`
    );
  } finally {
    conn.release();
    await pool.end();
  }
};

main().catch((err) => {
  console.error("[reco:bootstrap-fast] failed", err);
  process.exitCode = 1;
});
