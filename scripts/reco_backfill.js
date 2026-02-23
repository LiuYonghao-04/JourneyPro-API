import "dotenv/config";
import { pool } from "../db/connect.js";
import { ensureRecoTables } from "../services/reco/schema.js";

const args = process.argv.slice(2);
const sinceArg = args.find((arg) => arg.startsWith("--since-days="));
const sinceDays = sinceArg ? Number.parseInt(sinceArg.split("=")[1], 10) : 3650;
const safeSinceDays = Number.isFinite(sinceDays) && sinceDays > 0 ? sinceDays : 3650;

const runInsert = async (sql, params = []) => {
  const [result] = await pool.query(sql, params);
  return Number(result?.affectedRows) || 0;
};

const main = async () => {
  const startedAt = Date.now();
  console.log(`[reco:backfill] start since_days=${safeSinceDays}`);

  await ensureRecoTables();

  const insertedLikes = await runInsert(
    `
      INSERT INTO recommendation_events (
        user_id, session_id, request_id, algorithm_version, bucket,
        mode, route_hash, poi_id, rank_position, event_type, event_value, ts
      )
      SELECT
        pl.user_id,
        NULL,
        CONCAT('backfill:like:', pl.id) AS request_id,
        'v2_backfill',
        'backfill',
        'driving',
        NULL,
        p.poi_id,
        NULL,
        'like_post',
        1,
        COALESCE(pl.created_at, NOW())
      FROM post_likes pl
      JOIN posts p ON p.id = pl.post_id
      WHERE p.poi_id IS NOT NULL
        AND COALESCE(pl.created_at, NOW()) >= DATE_SUB(NOW(), INTERVAL ? DAY)
        AND NOT EXISTS (
          SELECT 1
          FROM recommendation_events re
          WHERE re.request_id = CONCAT('backfill:like:', pl.id)
            AND re.poi_id = p.poi_id
            AND re.event_type = 'like_post'
        )
    `,
    [safeSinceDays]
  );

  const insertedFavorites = await runInsert(
    `
      INSERT INTO recommendation_events (
        user_id, session_id, request_id, algorithm_version, bucket,
        mode, route_hash, poi_id, rank_position, event_type, event_value, ts
      )
      SELECT
        pf.user_id,
        NULL,
        CONCAT('backfill:fav:', pf.id) AS request_id,
        'v2_backfill',
        'backfill',
        'driving',
        NULL,
        p.poi_id,
        NULL,
        'favorite_post',
        1,
        COALESCE(pf.created_at, NOW())
      FROM post_favorites pf
      JOIN posts p ON p.id = pf.post_id
      WHERE p.poi_id IS NOT NULL
        AND COALESCE(pf.created_at, NOW()) >= DATE_SUB(NOW(), INTERVAL ? DAY)
        AND NOT EXISTS (
          SELECT 1
          FROM recommendation_events re
          WHERE re.request_id = CONCAT('backfill:fav:', pf.id)
            AND re.poi_id = p.poi_id
            AND re.event_type = 'favorite_post'
        )
    `,
    [safeSinceDays]
  );

  const insertedViews = await runInsert(
    `
      INSERT INTO recommendation_events (
        user_id, session_id, request_id, algorithm_version, bucket,
        mode, route_hash, poi_id, rank_position, event_type, event_value, ts
      )
      SELECT
        pv.user_id,
        NULL,
        CONCAT('backfill:view:', pv.id) AS request_id,
        'v2_backfill',
        'backfill',
        'driving',
        NULL,
        p.poi_id,
        NULL,
        'detail_view',
        LEAST(COALESCE(pv.view_count, 1), 6),
        COALESCE(pv.last_viewed_at, NOW())
      FROM post_views pv
      JOIN posts p ON p.id = pv.post_id
      WHERE p.poi_id IS NOT NULL
        AND COALESCE(pv.last_viewed_at, NOW()) >= DATE_SUB(NOW(), INTERVAL ? DAY)
        AND NOT EXISTS (
          SELECT 1
          FROM recommendation_events re
          WHERE re.request_id = CONCAT('backfill:view:', pv.id)
            AND re.poi_id = p.poi_id
            AND re.event_type = 'detail_view'
        )
    `,
    [safeSinceDays]
  );

  const totalInserted = insertedLikes + insertedFavorites + insertedViews;
  console.log(
    `[reco:backfill] done inserted_total=${totalInserted} likes=${insertedLikes} favorites=${insertedFavorites} views=${insertedViews} elapsed_ms=${Date.now() - startedAt}`
  );
};

main()
  .catch((err) => {
    console.error("[reco:backfill] failed", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch {
      // ignore
    }
  });
