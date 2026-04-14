import { pool } from "../db/connect.js";
import { USER_ROLES } from "./userAccess.js";

export const BILLING_CYCLES = {
  MONTHLY: { code: "MONTHLY", label: "Monthly", months: 1 },
  YEARLY: { code: "YEARLY", label: "Yearly", months: 12 },
};

const PLAN_TEMPLATE = {
  [USER_ROLES.VIP]: {
    role: USER_ROLES.VIP,
    name: "VIP",
    tagline: "More AI planning room for regular travelers.",
    defaultPrices: {
      [BILLING_CYCLES.MONTHLY.code]: 19,
      [BILLING_CYCLES.YEARLY.code]: 188,
    },
    benefits: ["30 AI plans per month", "Priority membership badge", "Membership renewal history"],
  },
  [USER_ROLES.SVIP]: {
    role: USER_ROLES.SVIP,
    name: "SVIP",
    tagline: "Unlimited AI access and ad publishing tools.",
    defaultPrices: {
      [BILLING_CYCLES.MONTHLY.code]: 99,
      [BILLING_CYCLES.YEARLY.code]: 988,
    },
    benefits: ["Unlimited AI plans", "Up to 3 ad campaigns per month", "Premium membership badge"],
  },
};

const VALID_ROLES = [USER_ROLES.VIP, USER_ROLES.SVIP];
const VALID_CYCLES = Object.keys(BILLING_CYCLES);
let ensureMembershipPricingPromise = null;

const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const normalizeRole = (value) => String(value || "").trim().toUpperCase();
const normalizeCycle = (value) => String(value || "").trim().toUpperCase();

const ensureMoney = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return null;
  return Math.round(num * 100) / 100;
};

const buildPlanCatalog = (priceMap = new Map()) => {
  const catalog = {};
  VALID_ROLES.forEach((role) => {
    const template = PLAN_TEMPLATE[role];
    catalog[role] = {
      role: template.role,
      name: template.name,
      tagline: template.tagline,
      benefits: [...template.benefits],
      prices: {
        [BILLING_CYCLES.MONTHLY.code]: safeNumber(
          priceMap.get(`${role}:${BILLING_CYCLES.MONTHLY.code}`),
          template.defaultPrices[BILLING_CYCLES.MONTHLY.code]
        ),
        [BILLING_CYCLES.YEARLY.code]: safeNumber(
          priceMap.get(`${role}:${BILLING_CYCLES.YEARLY.code}`),
          template.defaultPrices[BILLING_CYCLES.YEARLY.code]
        ),
      },
    };
  });
  return catalog;
};

export async function ensureMembershipPricingSchema() {
  if (!ensureMembershipPricingPromise) {
    ensureMembershipPricingPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS membership_plan_prices (
          role VARCHAR(20) NOT NULL,
          billing_cycle VARCHAR(20) NOT NULL,
          price_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
          updated_by BIGINT NULL,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (role, billing_cycle),
          KEY idx_membership_plan_prices_updated (updated_at, role, billing_cycle)
        )
      `);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS membership_price_audit (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          role VARCHAR(20) NOT NULL,
          billing_cycle VARCHAR(20) NOT NULL,
          old_price_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
          new_price_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
          updated_by BIGINT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_membership_price_audit_created (created_at, id),
          KEY idx_membership_price_audit_role_cycle (role, billing_cycle, created_at)
        )
      `);

      const seedRows = [];
      VALID_ROLES.forEach((role) => {
        VALID_CYCLES.forEach((cycleCode) => {
          seedRows.push([role, cycleCode, PLAN_TEMPLATE[role].defaultPrices[cycleCode]]);
        });
      });
      if (seedRows.length) {
        await pool.query(
          `
            INSERT IGNORE INTO membership_plan_prices (role, billing_cycle, price_cny)
            VALUES ?
          `,
          [seedRows]
        );
      }
    })().catch((err) => {
      ensureMembershipPricingPromise = null;
      throw err;
    });
  }
  return ensureMembershipPricingPromise;
}

export async function fetchMembershipPlanCatalog(conn = pool) {
  await ensureMembershipPricingSchema();
  const [rows] = await conn.query(
    `
      SELECT role, billing_cycle, price_cny, updated_by, updated_at
      FROM membership_plan_prices
      WHERE role IN (?, ?)
        AND billing_cycle IN (?, ?)
    `,
    [USER_ROLES.VIP, USER_ROLES.SVIP, BILLING_CYCLES.MONTHLY.code, BILLING_CYCLES.YEARLY.code]
  );
  const priceMap = new Map();
  rows.forEach((row) => {
    priceMap.set(`${normalizeRole(row.role)}:${normalizeCycle(row.billing_cycle)}`, safeNumber(row.price_cny));
  });
  return buildPlanCatalog(priceMap);
}

export async function fetchMembershipPlanMatrix(conn = pool) {
  const catalog = await fetchMembershipPlanCatalog(conn);
  return VALID_ROLES.flatMap((role) =>
    VALID_CYCLES.map((cycleCode) => ({
      role,
      billing_cycle: cycleCode,
      billing_label: BILLING_CYCLES[cycleCode].label,
      months: BILLING_CYCLES[cycleCode].months,
      price_cny: safeNumber(catalog[role]?.prices?.[cycleCode]),
    }))
  );
}

export async function fetchMembershipPriceAudit(limit = 12, conn = pool) {
  await ensureMembershipPricingSchema();
  const safeLimit = Math.max(1, Math.min(Number(limit) || 12, 40));
  const [rows] = await conn.query(
    `
      SELECT
        a.id,
        a.role,
        a.billing_cycle,
        a.old_price_cny,
        a.new_price_cny,
        a.updated_by,
        a.created_at,
        u.nickname AS updated_by_nickname
      FROM membership_price_audit a
      LEFT JOIN users u ON u.id = a.updated_by
      ORDER BY a.id DESC
      LIMIT ?
    `,
    [safeLimit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    role: normalizeRole(row.role),
    billing_cycle: normalizeCycle(row.billing_cycle),
    old_price_cny: safeNumber(row.old_price_cny),
    new_price_cny: safeNumber(row.new_price_cny),
    updated_by: row.updated_by ? Number(row.updated_by) : null,
    updated_by_nickname: row.updated_by_nickname || null,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
  }));
}

export async function updateMembershipPlanPrices({ adminUserId, prices = [] }) {
  await ensureMembershipPricingSchema();
  const normalizedItems = Array.isArray(prices)
    ? prices
        .map((item) => ({
          role: normalizeRole(item?.role),
          billing_cycle: normalizeCycle(item?.billing_cycle),
          price_cny: ensureMoney(item?.price_cny),
        }))
        .filter(
          (item) =>
            VALID_ROLES.includes(item.role) &&
            VALID_CYCLES.includes(item.billing_cycle) &&
            Number.isFinite(item.price_cny) &&
            item.price_cny > 0
        )
    : [];

  if (normalizedItems.length !== VALID_ROLES.length * VALID_CYCLES.length) {
    throw new Error("all VIP/SVIP monthly and yearly prices are required");
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [currentRows] = await conn.query(
      `
        SELECT role, billing_cycle, price_cny
        FROM membership_plan_prices
        WHERE role IN (?, ?)
          AND billing_cycle IN (?, ?)
        FOR UPDATE
      `,
      [USER_ROLES.VIP, USER_ROLES.SVIP, BILLING_CYCLES.MONTHLY.code, BILLING_CYCLES.YEARLY.code]
    );
    const currentMap = new Map();
    currentRows.forEach((row) => {
      currentMap.set(`${normalizeRole(row.role)}:${normalizeCycle(row.billing_cycle)}`, safeNumber(row.price_cny));
    });

    const changes = [];
    for (const item of normalizedItems) {
      const key = `${item.role}:${item.billing_cycle}`;
      const oldPrice = safeNumber(currentMap.get(key), PLAN_TEMPLATE[item.role].defaultPrices[item.billing_cycle]);
      if (Math.abs(oldPrice - item.price_cny) < 0.001) continue;
      await conn.query(
        `
          INSERT INTO membership_plan_prices (role, billing_cycle, price_cny, updated_by)
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            price_cny = VALUES(price_cny),
            updated_by = VALUES(updated_by),
            updated_at = CURRENT_TIMESTAMP
        `,
        [item.role, item.billing_cycle, item.price_cny, adminUserId || null]
      );
      await conn.query(
        `
          INSERT INTO membership_price_audit (
            role, billing_cycle, old_price_cny, new_price_cny, updated_by
          )
          VALUES (?, ?, ?, ?, ?)
        `,
        [item.role, item.billing_cycle, oldPrice, item.price_cny, adminUserId || null]
      );
      changes.push({
        role: item.role,
        billing_cycle: item.billing_cycle,
        old_price_cny: oldPrice,
        new_price_cny: item.price_cny,
      });
    }
    await conn.commit();
    return {
      changes,
      plans: await fetchMembershipPlanCatalog(),
      matrix: await fetchMembershipPlanMatrix(),
      audit: await fetchMembershipPriceAudit(12),
    };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}
