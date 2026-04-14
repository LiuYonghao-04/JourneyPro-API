import express from "express";
import { pool } from "../db/connect.js";
import { ensureUserAccessSchema, fetchUserAccessById, USER_ROLES, getRoleMeta } from "../utils/userAccess.js";
import { fetchAiQuotaHistory, fetchAiQuotaStatus } from "../services/ai/quota.js";
import {
  BILLING_CYCLES,
  ensureMembershipPricingSchema,
  fetchMembershipPlanCatalog,
} from "../utils/membershipPricing.js";

const router = express.Router();

let ensureMembershipSchemaPromise = null;

const parseUserId = (value) => {
  const uid = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(uid) && uid > 0 ? uid : 0;
};

const normalizeRole = (value) => {
  const next = String(value || "").trim().toUpperCase();
  return [USER_ROLES.USER, USER_ROLES.VIP, USER_ROLES.SVIP, USER_ROLES.ADMIN].includes(next) ? next : USER_ROLES.USER;
};

const normalizeCycle = (value) => {
  const next = String(value || "").trim().toUpperCase();
  return BILLING_CYCLES[next] ? next : "";
};

const toIso = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  const ts = date.getTime();
  return Number.isFinite(ts) && ts > 0 ? date.toISOString() : null;
};

const addUtcMonths = (input, months) => {
  const base = input instanceof Date ? new Date(input.getTime()) : new Date(input);
  const day = base.getUTCDate();
  base.setUTCMonth(base.getUTCMonth() + months);
  if (base.getUTCDate() !== day) {
    base.setUTCDate(0);
  }
  return base;
};

const decoratePlanPrice = (plan, cycleCode) => {
  const monthlyPrice = Number(plan.prices[BILLING_CYCLES.MONTHLY.code] || 0);
  const actualPrice = Number(plan.prices[cycleCode] || 0);
  const months = BILLING_CYCLES[cycleCode].months;
  const originalPrice = cycleCode === BILLING_CYCLES.YEARLY.code ? monthlyPrice * months : actualPrice;
  const savings = Math.max(0, originalPrice - actualPrice);
  const savingsPct = originalPrice > 0 ? Math.round((savings / originalPrice) * 100) : 0;
  return {
    cycle: cycleCode,
    label: BILLING_CYCLES[cycleCode].label,
    months,
    price_cny: actualPrice,
    original_price_cny: originalPrice,
    savings_cny: savings,
    savings_pct: savingsPct,
  };
};

const formatPlans = (catalog) =>
  Object.values(catalog || {}).map((plan) => ({
    ...plan,
    pricing: [
      decoratePlanPrice(plan, BILLING_CYCLES.MONTHLY.code),
      decoratePlanPrice(plan, BILLING_CYCLES.YEARLY.code),
    ],
  }));

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

async function ensureMembershipSchema() {
  await ensureUserAccessSchema();
  await ensureMembershipPricingSchema();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS membership_orders (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id BIGINT UNSIGNED NOT NULL,
      role_before VARCHAR(20) NOT NULL,
      role_after VARCHAR(20) NOT NULL,
      billing_cycle VARCHAR(20) NOT NULL,
      months_paid INT NOT NULL,
      amount_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      balance_before_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      balance_after_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      expires_before DATETIME NULL,
      expires_after DATETIME NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'PAID',
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_membership_orders_user_created (user_id, created_at),
      KEY idx_membership_orders_role_after (role_after, created_at)
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wallet_ledger (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id BIGINT UNSIGNED NOT NULL,
      direction VARCHAR(20) NOT NULL DEFAULT 'DEBIT',
      amount_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      balance_before_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      balance_after_cny DECIMAL(10,2) NOT NULL DEFAULT 0,
      entry_type VARCHAR(40) NOT NULL DEFAULT 'MEMBERSHIP_PURCHASE',
      reference_type VARCHAR(40) NULL,
      reference_id BIGINT UNSIGNED NULL,
      note VARCHAR(255) NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_wallet_ledger_user_created (user_id, created_at),
      KEY idx_wallet_ledger_entry_type (entry_type, created_at)
    )
  `);
  await tryAlter(`ALTER TABLE membership_orders ADD COLUMN balance_before_cny DECIMAL(10,2) NOT NULL DEFAULT 0`);
  await tryAlter(`ALTER TABLE membership_orders ADD COLUMN balance_after_cny DECIMAL(10,2) NOT NULL DEFAULT 0`);
  await tryAlter(`ALTER TABLE users ADD COLUMN role_expires_at DATETIME NULL`);
  await tryAlter(`ALTER TABLE users ADD COLUMN membership_updated_at DATETIME NULL`);
  await tryAlter(`ALTER TABLE users ADD COLUMN balance_cny DECIMAL(10,2) NOT NULL DEFAULT 20.00`);
  await pool.query(`UPDATE users SET balance_cny = 20.00 WHERE balance_cny IS NULL`);
}

function ensureMembershipSchemaReady() {
  if (!ensureMembershipSchemaPromise) {
    ensureMembershipSchemaPromise = ensureMembershipSchema().catch((err) => {
      ensureMembershipSchemaPromise = null;
      throw err;
    });
  }
  return ensureMembershipSchemaPromise;
}

async function fetchMembershipOrders(userId, limit = 12) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 12, 40));
  const [rows] = await pool.query(
    `
      SELECT id, role_before, role_after, billing_cycle, months_paid, amount_cny,
             balance_before_cny, balance_after_cny,
             expires_before, expires_after, status, created_at
      FROM membership_orders
      WHERE user_id = ?
      ORDER BY id DESC
      LIMIT ?
    `,
    [userId, safeLimit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    role_before: normalizeRole(row.role_before),
    role_after: normalizeRole(row.role_after),
    billing_cycle: normalizeCycle(row.billing_cycle),
    months_paid: Number(row.months_paid) || 0,
    amount_cny: Number(row.amount_cny) || 0,
    balance_before_cny: Number(row.balance_before_cny) || 0,
    balance_after_cny: Number(row.balance_after_cny) || 0,
    expires_before: toIso(row.expires_before),
    expires_after: toIso(row.expires_after),
    status: String(row.status || "PAID").trim().toUpperCase(),
    created_at: toIso(row.created_at),
  }));
}

async function fetchWalletLedger(userId, limit = 16) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 16, 60));
  const [rows] = await pool.query(
    `
      SELECT
        id,
        direction,
        amount_cny,
        balance_before_cny,
        balance_after_cny,
        entry_type,
        reference_type,
        reference_id,
        note,
        created_at
      FROM wallet_ledger
      WHERE user_id = ?
      ORDER BY id DESC
      LIMIT ?
    `,
    [userId, safeLimit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    direction: String(row.direction || "DEBIT").trim().toUpperCase(),
    amount_cny: Number(row.amount_cny) || 0,
    balance_before_cny: Number(row.balance_before_cny) || 0,
    balance_after_cny: Number(row.balance_after_cny) || 0,
    entry_type: String(row.entry_type || "MEMBERSHIP_PURCHASE").trim().toUpperCase(),
    reference_type: String(row.reference_type || "").trim().toUpperCase(),
    reference_id: row.reference_id ? Number(row.reference_id) : null,
    note: String(row.note || "").trim(),
    created_at: toIso(row.created_at),
  }));
}

router.get("/summary", async (req, res) => {
  try {
    await ensureMembershipSchemaReady();
    const userId = parseUserId(req.query.user_id);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    const user = await fetchUserAccessById(userId);
    if (!user) {
      return res.status(404).json({ success: false, message: "user not found" });
    }
    const [orders, walletLedger, aiQuota, aiUsageHistory, planCatalog] = await Promise.all([
      fetchMembershipOrders(user.id, 10),
      fetchWalletLedger(user.id, 12),
      fetchAiQuotaStatus({ userId: user.id }),
      fetchAiQuotaHistory({ userId: user.id, limit: 6 }),
      fetchMembershipPlanCatalog(),
    ]);
    res.json({
      success: true,
      user,
      plans: formatPlans(planCatalog),
      orders,
      wallet_ledger: walletLedger,
      ai_quota: aiQuota,
      ai_usage_history: aiUsageHistory,
      reminders: {
        membership_expiring_soon:
          !!user.membership_active &&
          Number.isFinite(Number(user.membership_days_left)) &&
          Number(user.membership_days_left) <= 14,
        wallet_low_balance: Number(user.balance_cny || 0) <= 5,
      },
    });
  } catch (err) {
    console.error("membership summary error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/purchase", async (req, res) => {
  const userId = parseUserId(req.body?.user_id);
  const targetRole = normalizeRole(req.body?.target_role);
  const billingCycle = normalizeCycle(req.body?.billing_cycle);
  if (!userId) {
    return res.status(400).json({ success: false, message: "user_id required" });
  }
  if (![USER_ROLES.VIP, USER_ROLES.SVIP].includes(targetRole)) {
    return res.status(400).json({ success: false, message: "target_role must be VIP or SVIP" });
  }
  if (!billingCycle) {
    return res.status(400).json({ success: false, message: "billing_cycle must be MONTHLY or YEARLY" });
  }

  const conn = await pool.getConnection();
  try {
    await ensureMembershipSchemaReady();
    const planCatalog = await fetchMembershipPlanCatalog(conn);
    const cycle = BILLING_CYCLES[billingCycle];
    const plan = planCatalog[targetRole];
    const amount = Number(plan?.prices?.[billingCycle] || 0);
    if (!plan || !Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ success: false, message: "pricing is not configured for this plan" });
    }
    await conn.beginTransaction();
    const [rows] = await conn.query(
      `
        SELECT id, role, role_expires_at, membership_updated_at, balance_cny
        FROM users
        WHERE id = ?
        LIMIT 1
        FOR UPDATE
      `,
      [userId]
    );
    if (!rows.length) {
      await conn.rollback();
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const rawUser = rows[0];
    const user = await fetchUserAccessById(userId, conn);
    if (!user) {
      await conn.rollback();
      return res.status(404).json({ success: false, message: "user not found" });
    }
    if (user.role === USER_ROLES.ADMIN || user.stored_role === USER_ROLES.ADMIN) {
      await conn.rollback();
      return res.status(403).json({ success: false, message: "Admin account does not require membership purchase" });
    }
    if (user.role === USER_ROLES.SVIP && targetRole === USER_ROLES.VIP) {
      await conn.rollback();
      return res.status(400).json({ success: false, message: "Current membership is already higher than VIP" });
    }

    const balanceBefore = Number(rawUser.balance_cny) || 0;
    if (balanceBefore < amount) {
      await conn.rollback();
      return res.status(400).json({
        success: false,
        message: `Insufficient balance. Current balance is ¥${balanceBefore.toFixed(2)}.`,
        balance_cny: balanceBefore,
        required_cny: amount,
      });
    }

    const now = new Date();
    const currentExpiry = rawUser.role_expires_at ? new Date(rawUser.role_expires_at) : null;
    const anchorDate =
      currentExpiry && Number.isFinite(currentExpiry.getTime()) && currentExpiry.getTime() > now.getTime()
        ? currentExpiry
        : now;
    const nextExpiry = addUtcMonths(anchorDate, cycle.months);
    const balanceAfter = Math.max(0, Number((balanceBefore - amount).toFixed(2)));

    await conn.query(
      `
        UPDATE users
        SET role = ?, role_expires_at = ?, membership_updated_at = NOW(), balance_cny = ?
        WHERE id = ?
      `,
      [targetRole, nextExpiry, balanceAfter, userId]
    );

    const [orderResult] = await conn.query(
      `
        INSERT INTO membership_orders (
          user_id, role_before, role_after, billing_cycle, months_paid, amount_cny,
          balance_before_cny, balance_after_cny, expires_before, expires_after, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PAID')
      `,
      [
        userId,
        user.stored_role || USER_ROLES.USER,
        targetRole,
        billingCycle,
        cycle.months,
        amount,
        balanceBefore,
        balanceAfter,
        currentExpiry && Number.isFinite(currentExpiry.getTime()) ? currentExpiry : null,
        nextExpiry,
      ]
    );
    await conn.query(
      `
        INSERT INTO wallet_ledger (
          user_id,
          direction,
          amount_cny,
          balance_before_cny,
          balance_after_cny,
          entry_type,
          reference_type,
          reference_id,
          note
        )
        VALUES (?, 'DEBIT', ?, ?, ?, 'MEMBERSHIP_PURCHASE', 'MEMBERSHIP_ORDER', ?, ?)
      `,
      [
        userId,
        amount,
        balanceBefore,
        balanceAfter,
        orderResult.insertId,
        `${getRoleMeta(targetRole).label} ${cycle.label} membership purchase`,
      ]
    );

    await conn.commit();

    const refreshedUser = await fetchUserAccessById(userId);
    const [orders, walletLedger] = await Promise.all([
      fetchMembershipOrders(userId, 10),
      fetchWalletLedger(userId, 12),
    ]);
    const [latestRows] = await pool.query(
      `
        SELECT id, role_before, role_after, billing_cycle, months_paid, amount_cny,
               balance_before_cny, balance_after_cny,
               expires_before, expires_after, status, created_at
        FROM membership_orders
        WHERE id = ?
        LIMIT 1
      `,
      [orderResult.insertId]
    );
    const latestOrder = latestRows.length
      ? {
          id: Number(latestRows[0].id),
          role_before: normalizeRole(latestRows[0].role_before),
          role_after: normalizeRole(latestRows[0].role_after),
          billing_cycle: normalizeCycle(latestRows[0].billing_cycle),
          months_paid: Number(latestRows[0].months_paid) || 0,
          amount_cny: Number(latestRows[0].amount_cny) || 0,
          balance_before_cny: Number(latestRows[0].balance_before_cny) || 0,
          balance_after_cny: Number(latestRows[0].balance_after_cny) || 0,
          expires_before: toIso(latestRows[0].expires_before),
          expires_after: toIso(latestRows[0].expires_after),
          status: String(latestRows[0].status || "PAID").trim().toUpperCase(),
          created_at: toIso(latestRows[0].created_at),
        }
      : null;

    return res.json({
      success: true,
      user: refreshedUser,
      order: latestOrder,
      plans: formatPlans(planCatalog),
      orders,
      wallet_ledger: walletLedger,
      purchased_plan: {
        role: targetRole,
        role_label: getRoleMeta(targetRole).label,
        cycle: billingCycle,
        cycle_label: cycle.label,
        amount_cny: amount,
        balance_after_cny: balanceAfter,
        expires_at: toIso(nextExpiry),
      },
    });
  } catch (err) {
    await conn.rollback();
    console.error("membership purchase error", err);
    return res.status(500).json({ success: false, message: "server error" });
  } finally {
    conn.release();
  }
});

export default router;
