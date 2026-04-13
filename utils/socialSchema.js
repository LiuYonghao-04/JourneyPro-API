import { pool } from "../db/connect.js";

const isBenignDdlError = (err) => {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("Duplicate column") ||
    msg.includes("Duplicate column name") ||
    msg.includes("Duplicate key") ||
    msg.includes("Duplicate key name") ||
    msg.includes("already exists") ||
    msg.includes("check that column/key exists")
  );
};

const tryAlter = async (sql) => {
  try {
    await pool.query(sql);
  } catch (err) {
    if (!isBenignDdlError(err)) throw err;
  }
};

let ensurePostModerationSchemaPromise = null;
let ensureNotificationSchemaPromise = null;

export async function ensurePostModerationSchema() {
  if (!ensurePostModerationSchemaPromise) {
    ensurePostModerationSchemaPromise = (async () => {
      await tryAlter(`ALTER TABLE posts ADD COLUMN is_featured TINYINT(1) NOT NULL DEFAULT 0`);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS post_reports (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          post_id BIGINT NOT NULL,
          reporter_user_id BIGINT NOT NULL,
          reason VARCHAR(40) NOT NULL,
          details VARCHAR(255) NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
          reviewed_by BIGINT NULL,
          reviewed_at DATETIME NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_post_report_user (post_id, reporter_user_id),
          KEY idx_post_reports_status_created (status, created_at),
          KEY idx_post_reports_post (post_id, created_at),
          KEY idx_post_reports_post_status (post_id, status, id),
          KEY idx_post_reports_reporter_created (reporter_user_id, created_at, id)
        );
      `);

      await tryAlter(`ALTER TABLE post_reports ADD COLUMN details VARCHAR(255) NULL`);
      await tryAlter(`ALTER TABLE post_reports ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'OPEN'`);
      await tryAlter(`ALTER TABLE post_reports ADD COLUMN reviewed_by BIGINT NULL`);
      await tryAlter(`ALTER TABLE post_reports ADD COLUMN reviewed_at DATETIME NULL`);
      await tryAlter(
        `ALTER TABLE post_reports ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`
      );
      await tryAlter(`ALTER TABLE post_reports ADD UNIQUE KEY uk_post_report_user (post_id, reporter_user_id)`);
      await tryAlter(`ALTER TABLE post_reports ADD INDEX idx_post_reports_status_created (status, created_at)`);
      await tryAlter(`ALTER TABLE post_reports ADD INDEX idx_post_reports_post (post_id, created_at)`);
      await tryAlter(`ALTER TABLE post_reports ADD INDEX idx_post_reports_post_status (post_id, status, id)`);
      await tryAlter(
        `ALTER TABLE post_reports ADD INDEX idx_post_reports_reporter_created (reporter_user_id, created_at, id)`
      );
    })().catch((err) => {
      ensurePostModerationSchemaPromise = null;
      throw err;
    });
  }

  return ensurePostModerationSchemaPromise;
}

export async function ensureNotificationSchema() {
  if (!ensureNotificationSchemaPromise) {
    ensureNotificationSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS user_notification_state (
          user_id BIGINT PRIMARY KEY,
          read_all_at DATETIME NULL,
          read_like_at DATETIME NULL,
          read_favorite_at DATETIME NULL,
          read_comment_at DATETIME NULL,
          read_report_at DATETIME NULL,
          read_follow_at DATETIME NULL,
          read_chat_at DATETIME NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS notification_events (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          user_id BIGINT NOT NULL,
          type VARCHAR(20) NOT NULL,
          actor_id BIGINT NULL,
          actor_name VARCHAR(80) NULL,
          actor_avatar_url VARCHAR(255) NULL,
          post_id BIGINT NULL,
          comment_id BIGINT NULL,
          title VARCHAR(160) NULL,
          content VARCHAR(500) NULL,
          meta_json LONGTEXT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          KEY idx_notification_events_user_created (user_id, created_at, id),
          KEY idx_notification_events_user_type_created (user_id, type, created_at, id)
        );
      `);

      await tryAlter(`ALTER TABLE user_notification_state ADD COLUMN read_report_at DATETIME NULL`);

      await tryAlter(`ALTER TABLE notification_events ADD COLUMN actor_id BIGINT NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN actor_name VARCHAR(80) NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN actor_avatar_url VARCHAR(255) NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN post_id BIGINT NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN comment_id BIGINT NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN title VARCHAR(160) NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN content VARCHAR(500) NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD COLUMN meta_json LONGTEXT NULL`);
      await tryAlter(`ALTER TABLE notification_events ADD INDEX idx_notification_events_user_created (user_id, created_at, id)`);
      await tryAlter(
        `ALTER TABLE notification_events ADD INDEX idx_notification_events_user_type_created (user_id, type, created_at, id)`
      );
    })().catch((err) => {
      ensureNotificationSchemaPromise = null;
      throw err;
    });
  }

  return ensureNotificationSchemaPromise;
}
