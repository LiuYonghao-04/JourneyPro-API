import { pool } from "../db/connect.js";

export const USER_ROLES = {
  USER: "USER",
  VIP: "VIP",
  SVIP: "SVIP",
  ADMIN: "ADMIN",
};

const VALID_ROLES = new Set(Object.values(USER_ROLES));

const ROLE_META = {
  [USER_ROLES.USER]: {
    label: "Normal",
    aiLimit: 10,
    adLimit: 0,
    canManageAds: false,
    isAdmin: false,
    aiUnlimited: false,
  },
  [USER_ROLES.VIP]: {
    label: "VIP",
    aiLimit: 30,
    adLimit: 0,
    canManageAds: false,
    isAdmin: false,
    aiUnlimited: false,
  },
  [USER_ROLES.SVIP]: {
    label: "SVIP",
    aiLimit: null,
    adLimit: 3,
    canManageAds: true,
    isAdmin: false,
    aiUnlimited: true,
  },
  [USER_ROLES.ADMIN]: {
    label: "Admin",
    aiLimit: null,
    adLimit: null,
    canManageAds: true,
    isAdmin: true,
    aiUnlimited: true,
  },
};

const normalizeRole = (value) => {
  const next = String(value || "").trim().toUpperCase();
  return VALID_ROLES.has(next) ? next : USER_ROLES.USER;
};

const metaForRole = (role) => ROLE_META[normalizeRole(role)] || ROLE_META[USER_ROLES.USER];

async function tryAlter(sql) {
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
}

let ensureSchemaPromise = null;

async function seedDefaultRoles() {
  await pool.query(`UPDATE users SET role = ? WHERE id = 1`, [USER_ROLES.ADMIN]);
  await pool.query(`UPDATE users SET role = ? WHERE id IN (2, 3)`, [USER_ROLES.SVIP]);
  await pool.query(`UPDATE users SET role = ? WHERE id IN (4, 5, 6, 7)`, [USER_ROLES.VIP]);
  await pool.query(
    `
      UPDATE users
      SET role = ?
      WHERE id NOT IN (1, 2, 3, 4, 5, 6, 7)
        AND (role IS NULL OR role = '' OR role NOT IN ('USER', 'VIP', 'SVIP', 'ADMIN'))
    `,
    [USER_ROLES.USER]
  );
}

export async function ensureUserAccessSchema() {
  if (!ensureSchemaPromise) {
    ensureSchemaPromise = (async () => {
      await tryAlter(`ALTER TABLE users ADD COLUMN role VARCHAR(20) NOT NULL DEFAULT 'USER'`);
      await tryAlter(`ALTER TABLE users ADD INDEX idx_users_role (role, id)`);
      await seedDefaultRoles();
    })().catch((err) => {
      ensureSchemaPromise = null;
      throw err;
    });
  }
  return ensureSchemaPromise;
}

export function appendUserAccess(user) {
  if (!user || typeof user !== "object") return user;
  const role = normalizeRole(user.role || (user.is_admin ? USER_ROLES.ADMIN : USER_ROLES.USER));
  const meta = metaForRole(role);
  return {
    ...user,
    role,
    role_label: meta.label,
    ai_monthly_limit: meta.aiLimit,
    ad_monthly_limit: meta.adLimit,
    can_manage_ads: meta.canManageAds,
    ai_unlimited: meta.aiUnlimited,
    is_admin: meta.isAdmin,
  };
}

export async function fetchUserAccessById(userId, conn = pool) {
  const uid = Number.parseInt(String(userId || ""), 10);
  if (!Number.isFinite(uid) || uid <= 0) return null;
  await ensureUserAccessSchema();
  const [rows] = await conn.query(
    `SELECT id, username, nickname, avatar_url, role, created_at FROM users WHERE id = ? LIMIT 1`,
    [uid]
  );
  if (!rows.length) return null;
  return appendUserAccess(rows[0]);
}

export function isAdminRole(role) {
  return normalizeRole(role) === USER_ROLES.ADMIN;
}

export function canManageAdsRole(role) {
  return !!metaForRole(role).canManageAds;
}

export function getRoleMeta(role) {
  const normalizedRole = normalizeRole(role);
  return {
    role: normalizedRole,
    ...metaForRole(normalizedRole),
  };
}
