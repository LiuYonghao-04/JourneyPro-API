import { pool } from "../db/connect.js";

export const USER_ROLES = {
  USER: "USER",
  VIP: "VIP",
  SVIP: "SVIP",
  ADMIN: "ADMIN",
};

const DEFAULT_MEMBERSHIP_EXPIRY = "2026-12-01 23:59:59";
const DEFAULT_BALANCE_CNY = 20;
const MEMBERSHIP_ROLES = new Set([USER_ROLES.VIP, USER_ROLES.SVIP]);

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

const normalizeDateTime = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  const ts = date.getTime();
  return Number.isFinite(ts) && ts > 0 ? date.toISOString() : null;
};

const computeMembershipState = (storedRole, expiresAt) => {
  const normalizedStoredRole = normalizeRole(storedRole);
  const normalizedExpiresAt = normalizeDateTime(expiresAt);

  if (normalizedStoredRole === USER_ROLES.ADMIN) {
    return {
      storedRole: normalizedStoredRole,
      effectiveRole: USER_ROLES.ADMIN,
      expiresAt: null,
      isActive: true,
      status: "permanent",
      daysLeft: null,
    };
  }

  if (!MEMBERSHIP_ROLES.has(normalizedStoredRole)) {
    return {
      storedRole: normalizedStoredRole,
      effectiveRole: USER_ROLES.USER,
      expiresAt: null,
      isActive: false,
      status: "none",
      daysLeft: null,
    };
  }

  if (!normalizedExpiresAt) {
    return {
      storedRole: normalizedStoredRole,
      effectiveRole: USER_ROLES.USER,
      expiresAt: null,
      isActive: false,
      status: "expired",
      daysLeft: 0,
    };
  }

  const now = Date.now();
  const expiryTs = new Date(normalizedExpiresAt).getTime();
  const isActive = Number.isFinite(expiryTs) && expiryTs > now;
  return {
    storedRole: normalizedStoredRole,
    effectiveRole: isActive ? normalizedStoredRole : USER_ROLES.USER,
    expiresAt: normalizedExpiresAt,
    isActive,
    status: isActive ? "active" : "expired",
    daysLeft: isActive ? Math.max(0, Math.ceil((expiryTs - now) / 86400000)) : 0,
  };
};

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
  await pool.query(
    `
      UPDATE users
      SET role_expires_at = ?, membership_updated_at = COALESCE(membership_updated_at, NOW())
      WHERE role IN ('VIP', 'SVIP')
        AND role_expires_at IS NULL
    `,
    [DEFAULT_MEMBERSHIP_EXPIRY]
  );
  await pool.query(`UPDATE users SET role_expires_at = NULL WHERE role = 'ADMIN'`);
  await pool.query(`UPDATE users SET balance_cny = ? WHERE balance_cny IS NULL`, [DEFAULT_BALANCE_CNY]);
}

export async function ensureUserAccessSchema() {
  if (!ensureSchemaPromise) {
    ensureSchemaPromise = (async () => {
      await tryAlter(`ALTER TABLE users ADD COLUMN role VARCHAR(20) NOT NULL DEFAULT 'USER'`);
      await tryAlter(`ALTER TABLE users ADD COLUMN role_expires_at DATETIME NULL`);
      await tryAlter(`ALTER TABLE users ADD COLUMN membership_updated_at DATETIME NULL`);
      await tryAlter(`ALTER TABLE users ADD COLUMN balance_cny DECIMAL(10,2) NOT NULL DEFAULT 20.00`);
      await tryAlter(`ALTER TABLE users ADD INDEX idx_users_role (role, id)`);
      await tryAlter(`ALTER TABLE users ADD INDEX idx_users_role_expiry (role, role_expires_at)`);
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
  const membership = computeMembershipState(
    user.role || (user.is_admin ? USER_ROLES.ADMIN : USER_ROLES.USER),
    user.role_expires_at
  );
  const meta = metaForRole(membership.effectiveRole);
  const storedMeta = metaForRole(membership.storedRole);
  return {
    ...user,
    stored_role: membership.storedRole,
    stored_role_label: storedMeta.label,
    role: membership.effectiveRole,
    role_label: meta.label,
    role_expires_at: membership.expiresAt,
    membership_expires_at: membership.expiresAt,
    membership_active: membership.isActive,
    membership_status: membership.status,
    membership_days_left: membership.daysLeft,
    membership_updated_at: normalizeDateTime(user.membership_updated_at),
    balance_cny: Number(user.balance_cny ?? DEFAULT_BALANCE_CNY),
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
    `
      SELECT id, username, nickname, avatar_url, role, role_expires_at, membership_updated_at, balance_cny, created_at
      FROM users
      WHERE id = ?
      LIMIT 1
    `,
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
