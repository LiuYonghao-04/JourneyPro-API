import "dotenv/config";
import { pool } from "../db/connect.js";
import { EVENT_REWARD_WEIGHTS, FEATURE_TYPES, clamp, round } from "../services/reco/constants.js";
import { chunk } from "../services/reco/math.js";
import { ensureRecoTables } from "../services/reco/schema.js";

const args = process.argv.slice(2);
const daysArg = args.find((arg) => arg.startsWith("--days="));
const historyDays = daysArg ? Number.parseInt(daysArg.split("=")[1], 10) : 180;
const safeHistoryDays = Number.isFinite(historyDays) && historyDays > 0 ? historyDays : 180;
const JOB_LOCK_KEY = "journeypro_reco_offline_aggregate";

const rewardCaseSql = Object.entries(EVENT_REWARD_WEIGHTS)
  .map(([eventType, weight]) => `WHEN '${eventType}' THEN ${Number(weight)}`)
  .join(" ");

const upsertUserInterestAgg = async (rows) => {
  if (!rows.length) return;
  const grouped = chunk(rows, 1000);
  for (const batch of grouped) {
    // eslint-disable-next-line no-await-in-loop
    await pool.query(
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
  }
};

const upsertPoiQualityStats = async (rows) => {
  if (!rows.length) return;
  const grouped = chunk(rows, 1000);
  for (const batch of grouped) {
    // eslint-disable-next-line no-await-in-loop
    await pool.query(
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
  }
};

const mergeFeatureRows = (targetMap, rows, featureType, keyField) => {
  rows.forEach((row) => {
    const userId = Number(row.user_id);
    if (!userId) return;
    const rawKey = row[keyField];
    if (rawKey === null || rawKey === undefined || rawKey === "") return;
    const featureKey = String(rawKey).slice(0, 255);
    const score = Number(row.score) || 0;
    if (!Number.isFinite(score) || score === 0) return;

    const mapKey = `${userId}|${featureType}|${featureKey}`;
    const current = targetMap.get(mapKey);
    const lastEventAt = row.last_event_at || null;

    if (!current) {
      targetMap.set(mapKey, {
        user_id: userId,
        feature_type: featureType,
        feature_key: featureKey,
        score,
        last_event_at: lastEventAt,
      });
      return;
    }

    current.score += score;
    if (lastEventAt && (!current.last_event_at || new Date(lastEventAt) > new Date(current.last_event_at))) {
      current.last_event_at = lastEventAt;
    }
  });
};

const aggregateUserInterest = async () => {
  const target = new Map();

  const [eventCategoryRows] = await pool.query(
    `
      SELECT
        re.user_id,
        LOWER(COALESCE(NULLIF(TRIM(p.category), ''), 'unknown')) AS feature_key,
        SUM((CASE re.event_type ${rewardCaseSql} ELSE 0 END) * COALESCE(re.event_value, 1)
          * EXP(-TIMESTAMPDIFF(DAY, re.ts, NOW()) / 30)) AS score,
        MAX(re.ts) AS last_event_at
      FROM recommendation_events re
      JOIN poi p ON p.id = re.poi_id
      WHERE re.user_id IS NOT NULL
        AND re.ts >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY re.user_id, feature_key
      HAVING score <> 0
    `,
    [safeHistoryDays]
  );
  mergeFeatureRows(target, eventCategoryRows, FEATURE_TYPES.CATEGORY, "feature_key");

  const [eventPoiRows] = await pool.query(
    `
      SELECT
        re.user_id,
        CAST(re.poi_id AS CHAR(255)) AS feature_key,
        SUM((CASE re.event_type ${rewardCaseSql} ELSE 0 END) * COALESCE(re.event_value, 1)
          * EXP(-TIMESTAMPDIFF(DAY, re.ts, NOW()) / 30)) AS score,
        MAX(re.ts) AS last_event_at
      FROM recommendation_events re
      WHERE re.user_id IS NOT NULL
        AND re.ts >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY re.user_id, re.poi_id
      HAVING score <> 0
    `,
    [safeHistoryDays]
  );
  mergeFeatureRows(target, eventPoiRows, FEATURE_TYPES.POI, "feature_key");

  const interactionSql = `
    SELECT user_id, post_id, event_at, weight
    FROM (
      SELECT user_id, post_id, created_at AS event_at, 3 AS weight
      FROM post_likes
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ${safeHistoryDays} DAY)
      UNION ALL
      SELECT user_id, post_id, created_at AS event_at, 5 AS weight
      FROM post_favorites
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ${safeHistoryDays} DAY)
      UNION ALL
      SELECT user_id, post_id, last_viewed_at AS event_at, LEAST(view_count, 6) AS weight
      FROM post_views
      WHERE last_viewed_at >= DATE_SUB(NOW(), INTERVAL ${safeHistoryDays} DAY)
    ) interactions
  `;

  const [postCategoryRows] = await pool.query(
    `
      SELECT
        i.user_id,
        LOWER(COALESCE(NULLIF(TRIM(poi.category), ''), 'unknown')) AS feature_key,
        SUM(i.weight * EXP(-TIMESTAMPDIFF(DAY, i.event_at, NOW()) / 30)) AS score,
        MAX(i.event_at) AS last_event_at
      FROM (${interactionSql}) i
      JOIN posts p ON p.id = i.post_id
      JOIN poi ON poi.id = p.poi_id
      GROUP BY i.user_id, feature_key
      HAVING score <> 0
    `
  );
  mergeFeatureRows(target, postCategoryRows, FEATURE_TYPES.CATEGORY, "feature_key");

  const [postPoiRows] = await pool.query(
    `
      SELECT
        i.user_id,
        CAST(p.poi_id AS CHAR(255)) AS feature_key,
        SUM(i.weight * EXP(-TIMESTAMPDIFF(DAY, i.event_at, NOW()) / 30)) AS score,
        MAX(i.event_at) AS last_event_at
      FROM (${interactionSql}) i
      JOIN posts p ON p.id = i.post_id
      WHERE p.poi_id IS NOT NULL
      GROUP BY i.user_id, p.poi_id
      HAVING score <> 0
    `
  );
  mergeFeatureRows(target, postPoiRows, FEATURE_TYPES.POI, "feature_key");

  const [tagRows] = await pool.query(
    `
      SELECT
        i.user_id,
        t.name AS feature_key,
        SUM(i.weight * EXP(-TIMESTAMPDIFF(DAY, i.event_at, NOW()) / 30)) AS score,
        MAX(i.event_at) AS last_event_at
      FROM (${interactionSql}) i
      JOIN post_tags pt ON pt.post_id = i.post_id
      JOIN tags t ON t.id = pt.tag_id
      GROUP BY i.user_id, t.name
      HAVING score <> 0
    `
  );
  mergeFeatureRows(target, tagRows, FEATURE_TYPES.TAG, "feature_key");

  const upsertRows = [...target.values()].map((row) => [
    row.user_id,
    row.feature_key,
    row.feature_type,
    round(row.score, 6),
    row.last_event_at,
    new Date(),
  ]);

  await upsertUserInterestAgg(upsertRows);

  return {
    featureRows: upsertRows.length,
    users: new Set(upsertRows.map((row) => row[0])).size,
  };
};

const aggregatePoiQualityStats = async () => {
  const [rows] = await pool.query(
    `
      SELECT
        poi_id,
        mode,
        SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
        SUM(CASE WHEN event_type IN ('detail_view', 'open_posts', 'save', 'add_via', 'navigate', 'like_post', 'favorite_post') THEN COALESCE(event_value, 1) ELSE 0 END) AS interactions,
        SUM(CASE WHEN event_type = 'add_via' THEN COALESCE(event_value, 1) ELSE 0 END) AS add_via_count,
        SUM(CASE WHEN event_type = 'save' THEN COALESCE(event_value, 1) ELSE 0 END) AS save_count,
        SUM((CASE event_type ${rewardCaseSql} ELSE 0 END) * COALESCE(event_value, 1)) AS reward_sum
      FROM recommendation_events
      WHERE ts >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY poi_id, mode
    `,
    [safeHistoryDays]
  );

  const upsertRows = rows.map((row) => {
    const impressions = Math.max(0, Number(row.impressions) || 0);
    const interactions = Math.max(0, Number(row.interactions) || 0);
    const addViaCount = Math.max(0, Number(row.add_via_count) || 0);
    const saveCount = Math.max(0, Number(row.save_count) || 0);
    const rewardSum = Number(row.reward_sum) || 0;

    const qualitySmoothed = (interactions + 0.1 * 40) / (impressions + 40);
    const rewardBoost = clamp((rewardSum + 8) / 20, 0, 1);
    const qualityScore = clamp(qualitySmoothed * 0.75 + rewardBoost * 0.25, 0, 1);
    const noveltyScore = clamp(1 / (1 + Math.log(1 + impressions)), 0, 1);

    return [
      Number(row.poi_id),
      row.mode || "driving",
      Math.round(impressions),
      Math.round(interactions),
      Math.round(addViaCount),
      Math.round(saveCount),
      round(qualityScore, 6),
      round(noveltyScore, 6),
      new Date(),
    ];
  });

  await upsertPoiQualityStats(upsertRows);

  return {
    rows: upsertRows.length,
  };
};

const acquireJobLock = async (timeoutSeconds = 1) => {
  const [[row]] = await pool.query(`SELECT GET_LOCK(?, ?) AS locked`, [JOB_LOCK_KEY, timeoutSeconds]);
  return Number(row?.locked) === 1;
};

const releaseJobLock = async () => {
  try {
    await pool.query(`DO RELEASE_LOCK(?)`, [JOB_LOCK_KEY]);
  } catch {
    // ignore
  }
};

const main = async () => {
  console.log(`[reco:offline] start days=${safeHistoryDays}`);
  const locked = await acquireJobLock(1);
  if (!locked) {
    console.warn("[reco:offline] skip: another offline aggregate job is running");
    return;
  }

  const startTs = Date.now();
  try {
    await ensureRecoTables();

    const interest = await aggregateUserInterest();
    const quality = await aggregatePoiQualityStats();

    const elapsed = Date.now() - startTs;
    console.log(
      `[reco:offline] done feature_rows=${interest.featureRows} users=${interest.users} quality_rows=${quality.rows} elapsed_ms=${elapsed}`
    );
  } finally {
    await releaseJobLock();
  }
};

main()
  .catch((err) => {
    console.error("[reco:offline] failed", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch {
      // ignore
    }
  });
