import { pool } from "../../db/connect.js";

let ensureRecoTablesPromise = null;
const ENABLE_RUNTIME_SCHEMA_MIGRATION = process.env.ENABLE_RUNTIME_SCHEMA_MIGRATION === "1";
let schemaMigrationNoticePrinted = false;

const swallowDuplicateAlter = (err) => {
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
      console.warn("[reco] runtime schema migration disabled (set ENABLE_RUNTIME_SCHEMA_MIGRATION=1 to enable)");
    }
    return;
  }
  try {
    await pool.query(sql);
  } catch (err) {
    if (!swallowDuplicateAlter(err)) throw err;
  }
};

export const ensureRecoTables = async () => {
  if (!ENABLE_RUNTIME_SCHEMA_MIGRATION) return;
  if (!ensureRecoTablesPromise) {
    ensureRecoTablesPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS recommendation_events (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          user_id BIGINT NULL,
          session_id VARCHAR(128) NULL,
          request_id VARCHAR(128) NOT NULL,
          algorithm_version VARCHAR(32) NULL,
          bucket VARCHAR(32) NULL,
          mode VARCHAR(20) NOT NULL DEFAULT 'driving',
          route_hash VARCHAR(128) NULL,
          poi_id BIGINT NOT NULL,
          rank_position INT NULL,
          event_type VARCHAR(32) NOT NULL,
          event_value FLOAT NOT NULL DEFAULT 1,
          ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          KEY idx_reco_evt_user_ts (user_id, ts),
          KEY idx_reco_evt_request_poi (request_id, poi_id),
          KEY idx_reco_evt_poi_ts (poi_id, ts),
          KEY idx_reco_evt_type_ts (event_type, ts)
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS user_interest_agg (
          user_id BIGINT NOT NULL,
          feature_key VARCHAR(255) NOT NULL,
          feature_type VARCHAR(20) NOT NULL,
          score DOUBLE NOT NULL DEFAULT 0,
          last_event_at TIMESTAMP NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (user_id, feature_type, feature_key),
          KEY idx_user_interest_user_updated (user_id, updated_at)
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS poi_quality_stats (
          poi_id BIGINT NOT NULL,
          mode VARCHAR(20) NOT NULL DEFAULT 'driving',
          impressions BIGINT NOT NULL DEFAULT 0,
          interactions BIGINT NOT NULL DEFAULT 0,
          add_via_count BIGINT NOT NULL DEFAULT 0,
          save_count BIGINT NOT NULL DEFAULT 0,
          quality_score DOUBLE NOT NULL DEFAULT 0,
          novelty_score DOUBLE NOT NULL DEFAULT 0,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (poi_id, mode),
          KEY idx_poi_quality_updated (updated_at)
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS user_recommendation_settings (
          user_id BIGINT PRIMARY KEY,
          interest_weight FLOAT NOT NULL DEFAULT 0.5,
          explore_weight FLOAT NOT NULL DEFAULT 0.15,
          mode_defaults JSON NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
      `);
      await safeAlter(`ALTER TABLE user_recommendation_settings ADD COLUMN explore_weight FLOAT NOT NULL DEFAULT 0.15`);
      await safeAlter(`ALTER TABLE user_recommendation_settings ADD COLUMN mode_defaults JSON NULL`);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ab_assignments (
          subject_key VARCHAR(191) NOT NULL,
          experiment_key VARCHAR(128) NOT NULL,
          bucket VARCHAR(32) NOT NULL,
          assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (subject_key, experiment_key),
          KEY idx_ab_experiment_bucket (experiment_key, bucket)
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS post_views (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          post_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          view_count INT DEFAULT 1,
          last_viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_post_user_view (post_id, user_id)
        );
      `);
    })().catch((err) => {
      ensureRecoTablesPromise = null;
      throw err;
    });
  }

  await ensureRecoTablesPromise;
};
