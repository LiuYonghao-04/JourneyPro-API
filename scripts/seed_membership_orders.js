import { pool } from "../db/connect.js";
import { ensureUserAccessSchema, USER_ROLES } from "../utils/userAccess.js";
import { pathToFileURL } from "url";

const BILLING_CYCLES = {
  MONTHLY: { code: "MONTHLY", months: 1, prices: { VIP: 19, SVIP: 99 } },
  YEARLY: { code: "YEARLY", months: 12, prices: { VIP: 188, SVIP: 988 } },
};
const MIN_STANDARD_USER_RATIO = 0.4;

const ORDER_PLAN = [
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
  { role: USER_ROLES.SVIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.MONTHLY.code },
  { role: USER_ROLES.VIP, cycle: BILLING_CYCLES.YEARLY.code },
];

const BUFFER_PATTERN = [24, 36, 48, 72, 96, 120, 160, 220];

const toMysqlDateTime = (value) => {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes()
  )}:${pad(date.getSeconds())}`;
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

async function ensureMembershipSchema() {
  await ensureUserAccessSchema();
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
}

async function fetchCandidateUsers(limit) {
  const [rows] = await pool.query(
    `
      SELECT u.id, u.nickname, u.role, u.role_expires_at, u.balance_cny
      FROM users u
      LEFT JOIN membership_orders mo ON mo.user_id = u.id
      WHERE u.id <> 1
        AND COALESCE(u.role, 'USER') = 'USER'
        AND mo.id IS NULL
      ORDER BY u.id ASC
      LIMIT ?
    `,
    [limit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    nickname: row.nickname || `User ${row.id}`,
    role: String(row.role || USER_ROLES.USER).trim().toUpperCase() || USER_ROLES.USER,
    role_expires_at: row.role_expires_at ? new Date(row.role_expires_at) : null,
    balance_cny: Number(row.balance_cny) || 0,
  }));
}

async function fetchPopulationSnapshot(conn = pool) {
  const [rows] = await conn.query(`
    SELECT
      COUNT(*) AS total_users,
      SUM(CASE WHEN role = 'VIP' AND role_expires_at > NOW() THEN 1 ELSE 0 END) AS vip_active,
      SUM(CASE WHEN role = 'SVIP' AND role_expires_at > NOW() THEN 1 ELSE 0 END) AS svip_active,
      SUM(CASE WHEN role = 'ADMIN' THEN 1 ELSE 0 END) AS admin_total,
      SUM(
        CASE
          WHEN role = 'USER' OR role IS NULL OR role = '' THEN 1
          WHEN role IN ('VIP', 'SVIP') AND (role_expires_at IS NULL OR role_expires_at <= NOW()) THEN 1
          ELSE 0
        END
      ) AS standard_total
    FROM users
  `);
  const row = rows?.[0] || {};
  const totalUsers = Number(row.total_users) || 0;
  const standardTotal = Number(row.standard_total) || 0;
  const vipActive = Number(row.vip_active) || 0;
  const svipActive = Number(row.svip_active) || 0;
  const adminTotal = Number(row.admin_total) || 0;
  return {
    total_users: totalUsers,
    standard_total: standardTotal,
    vip_active: vipActive,
    svip_active: svipActive,
    admin_total: adminTotal,
    standard_pct: totalUsers > 0 ? Number(((standardTotal / totalUsers) * 100).toFixed(2)) : 0,
    paying_pct: totalUsers > 0 ? Number((((vipActive + svipActive) / totalUsers) * 100).toFixed(2)) : 0,
  };
}

export async function main() {
  await ensureMembershipSchema();

  const candidates = await fetchCandidateUsers(ORDER_PLAN.length);
  const beforeSnapshot = await fetchPopulationSnapshot();
  if (candidates.length < ORDER_PLAN.length) {
    throw new Error(`Not enough candidate users without membership orders. Need ${ORDER_PLAN.length}, got ${candidates.length}.`);
  }
  const projectedStandardUsers = beforeSnapshot.standard_total - ORDER_PLAN.length;
  const projectedStandardRatio =
    beforeSnapshot.total_users > 0 ? projectedStandardUsers / beforeSnapshot.total_users : 0;
  if (projectedStandardRatio <= MIN_STANDARD_USER_RATIO) {
    throw new Error(
      `Projected standard-user ratio ${(projectedStandardRatio * 100).toFixed(2)}% would fall below the ${(MIN_STANDARD_USER_RATIO * 100).toFixed(0)}% floor.`
    );
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const startAt = new Date(Date.now() - (ORDER_PLAN.length - 1) * 6 * 60 * 60 * 1000);
    const inserted = [];

    for (let index = 0; index < ORDER_PLAN.length; index += 1) {
      const user = candidates[index];
      const spec = ORDER_PLAN[index];
      const cycleMeta = BILLING_CYCLES[spec.cycle];
      const amount = Number(cycleMeta.prices[spec.role] || 0);
      const monthsPaid = cycleMeta.months;
      const createdAt = new Date(startAt.getTime() + index * 6 * 60 * 60 * 1000);
      const expiresAfter = addUtcMonths(createdAt, monthsPaid);
      const balanceAfter = BUFFER_PATTERN[index % BUFFER_PATTERN.length];
      const balanceBefore = Number((amount + balanceAfter).toFixed(2));

      await conn.query(
        `
          UPDATE users
          SET role = ?, role_expires_at = ?, membership_updated_at = ?, balance_cny = ?
          WHERE id = ?
        `,
        [spec.role, toMysqlDateTime(expiresAfter), toMysqlDateTime(createdAt), balanceAfter, user.id]
      );

      const [orderResult] = await conn.query(
        `
          INSERT INTO membership_orders (
            user_id, role_before, role_after, billing_cycle, months_paid, amount_cny,
            balance_before_cny, balance_after_cny, expires_before, expires_after, status, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PAID', ?)
        `,
        [
          user.id,
          user.role || USER_ROLES.USER,
          spec.role,
          spec.cycle,
          monthsPaid,
          amount.toFixed(2),
          balanceBefore.toFixed(2),
          balanceAfter.toFixed(2),
          user.role_expires_at ? toMysqlDateTime(user.role_expires_at) : null,
          toMysqlDateTime(expiresAfter),
          toMysqlDateTime(createdAt),
        ]
      );

      await conn.query(
        `
          INSERT INTO wallet_ledger (
            user_id, direction, amount_cny, balance_before_cny, balance_after_cny,
            entry_type, reference_type, reference_id, note, created_at
          )
          VALUES (?, 'DEBIT', ?, ?, ?, 'MEMBERSHIP_PURCHASE', 'MEMBERSHIP_ORDER', ?, ?, ?)
        `,
        [
          user.id,
          amount.toFixed(2),
          balanceBefore.toFixed(2),
          balanceAfter.toFixed(2),
          Number(orderResult.insertId),
          `Seeded ${spec.role} ${spec.cycle} membership order for admin dashboard`,
          toMysqlDateTime(createdAt),
        ]
      );

      inserted.push({
        id: Number(orderResult.insertId),
        user_id: user.id,
        nickname: user.nickname,
        role_after: spec.role,
        billing_cycle: spec.cycle,
        amount_cny: amount,
        created_at: toMysqlDateTime(createdAt),
      });
    }

    await conn.commit();

    const [recentRows] = await pool.query(
      `
        SELECT id, user_id, role_after, billing_cycle, amount_cny, created_at
        FROM membership_orders
        WHERE status = 'PAID'
        ORDER BY id DESC
        LIMIT 8
      `
    );
    const recentTotal = recentRows.length || 1;
    const recentVip = recentRows.filter((row) => String(row.role_after || "").toUpperCase() === USER_ROLES.VIP).length;
    const recentSvip = recentRows.filter((row) => String(row.role_after || "").toUpperCase() === USER_ROLES.SVIP).length;
    const afterSnapshot = await fetchPopulationSnapshot(conn);

    console.log(
      JSON.stringify(
        {
          inserted_orders: inserted.length,
          platform_distribution_before: beforeSnapshot,
          platform_distribution_after: afterSnapshot,
          recent_paid_orders: recentRows.map((row) => ({
            id: Number(row.id),
            user_id: Number(row.user_id),
            role_after: row.role_after,
            billing_cycle: row.billing_cycle,
            amount_cny: Number(row.amount_cny),
            created_at: row.created_at,
          })),
          recent_paid_distribution: {
            vip: recentVip,
            svip: recentSvip,
            vip_pct: Number(((recentVip / recentTotal) * 100).toFixed(2)),
            svip_pct: Number(((recentSvip / recentTotal) * 100).toFixed(2)),
          },
        },
        null,
        2
      )
    );
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
    await pool.end();
  }
}

const isDirectRun =
  process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectRun) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
