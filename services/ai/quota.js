import { pool } from "../../db/connect.js";
import { fetchUserAccessById, getRoleMeta, USER_ROLES } from "../../utils/userAccess.js";

const DEFAULT_GUEST_ROLE = USER_ROLES.USER;
const MONTH_REGEX = /^\d{4}-\d{2}$/;

let ensureAiQuotaSchemaPromise = null;

const tryAlter = async (sql) => {
  try {
    await pool.query(sql);
  } catch (err) {
    const msg = String(err?.message || err);
    if (
      !msg.includes("Duplicate column name") &&
      !msg.includes("Duplicate key name") &&
      !msg.includes("check that column/key exists")
    ) {
      throw err;
    }
  }
};

const toUsageMonth = (value = new Date()) => {
  const year = value.getUTCFullYear();
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
};

const buildSubjectKey = ({ userId, sessionKey }) => {
  const uid = Number.parseInt(String(userId || ""), 10);
  if (Number.isFinite(uid) && uid > 0) return `user:${uid}`;
  const normalized = String(sessionKey || "").trim().slice(0, 120);
  if (normalized) return `guest:${normalized}`;
  return "guest:anonymous";
};

const normalizeQuotaRow = ({ roleMeta, user, usageMonth, subjectKey, callCount }) => {
  const limit = roleMeta.aiLimit;
  const used = Math.max(0, Number(callCount) || 0);
  return {
    subject_key: subjectKey,
    usage_month: usageMonth,
    role: user?.role || roleMeta.role || DEFAULT_GUEST_ROLE,
    role_label: user?.role_label || roleMeta.label,
    user_id: user?.id || null,
    ai_monthly_limit: limit,
    ai_unlimited: !!roleMeta.aiUnlimited,
    used,
    remaining: limit === null ? null : Math.max(0, limit - used),
    allowed: limit === null ? true : used < limit,
  };
};

export async function ensureAiQuotaSchema() {
  if (!ensureAiQuotaSchemaPromise) {
    ensureAiQuotaSchemaPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS ai_usage_monthly (
          subject_key VARCHAR(140) NOT NULL,
          user_id BIGINT UNSIGNED NULL,
          usage_month CHAR(7) NOT NULL,
          call_count INT NOT NULL DEFAULT 0,
          last_called_at DATETIME NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (subject_key, usage_month),
          KEY idx_ai_usage_monthly_user (user_id, usage_month),
          KEY idx_ai_usage_monthly_month (usage_month, call_count)
        )
      `);
      await tryAlter(`ALTER TABLE ai_usage_monthly ADD COLUMN user_id BIGINT UNSIGNED NULL`);
    })().catch((err) => {
      ensureAiQuotaSchemaPromise = null;
      throw err;
    });
  }
  return ensureAiQuotaSchemaPromise;
}

async function resolveQuotaSubject({ userId, sessionKey }) {
  const user = await fetchUserAccessById(userId);
  const subjectKey = buildSubjectKey({ userId: user?.id, sessionKey });
  const roleMeta = getRoleMeta(user?.role || DEFAULT_GUEST_ROLE);
  return { user, roleMeta, subjectKey };
}

export async function fetchAiQuotaStatus({
  userId,
  sessionKey,
  usageMonth,
} = {}) {
  await ensureAiQuotaSchema();
  const month = MONTH_REGEX.test(String(usageMonth || "")) ? String(usageMonth) : toUsageMonth();
  const { user, roleMeta, subjectKey } = await resolveQuotaSubject({ userId, sessionKey });
  const [[row]] = await pool.query(
    `SELECT call_count FROM ai_usage_monthly WHERE subject_key = ? AND usage_month = ? LIMIT 1`,
    [subjectKey, month]
  );
  return normalizeQuotaRow({
    roleMeta,
    user,
    usageMonth: month,
    subjectKey,
    callCount: row?.call_count || 0,
  });
}

export async function consumeAiQuota({
  userId,
  sessionKey,
  usageMonth,
} = {}) {
  await ensureAiQuotaSchema();
  const month = MONTH_REGEX.test(String(usageMonth || "")) ? String(usageMonth) : toUsageMonth();
  const { user, roleMeta, subjectKey } = await resolveQuotaSubject({ userId, sessionKey });
  if (roleMeta.aiUnlimited) {
    return normalizeQuotaRow({
      roleMeta,
      user,
      usageMonth: month,
      subjectKey,
      callCount: 0,
    });
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [[row]] = await conn.query(
      `SELECT call_count FROM ai_usage_monthly WHERE subject_key = ? AND usage_month = ? FOR UPDATE`,
      [subjectKey, month]
    );
    const used = Math.max(0, Number(row?.call_count) || 0);
    const limit = Number(roleMeta.aiLimit) || 0;
    if (used >= limit) {
      await conn.rollback();
      return normalizeQuotaRow({
        roleMeta,
        user,
        usageMonth: month,
        subjectKey,
        callCount: used,
      });
    }

    if (row) {
      await conn.query(
        `UPDATE ai_usage_monthly SET call_count = call_count + 1, user_id = ?, last_called_at = NOW() WHERE subject_key = ? AND usage_month = ?`,
        [user?.id || null, subjectKey, month]
      );
    } else {
      await conn.query(
        `INSERT INTO ai_usage_monthly (subject_key, user_id, usage_month, call_count, last_called_at) VALUES (?, ?, ?, 1, NOW())`,
        [subjectKey, user?.id || null, month]
      );
    }
    await conn.commit();

    return normalizeQuotaRow({
      roleMeta,
      user,
      usageMonth: month,
      subjectKey,
      callCount: used + 1,
    });
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

