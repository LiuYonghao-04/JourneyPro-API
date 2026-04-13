import express from "express";
import { pool } from "../db/connect.js";
import { requireAdminUser } from "../utils/accessGuard.js";

const router = express.Router();
const OVERVIEW_TTL_MS = 60 * 1000;
const LARGE_TABLE_NAMES = [
  "post_comments",
  "post_comments_archive",
];

let overviewCache = {
  expiresAt: 0,
  data: null,
};
let overviewInflight = null;

const safeNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
};

const safeRows = (result) => result?.[0] || [];

const oneValue = (result, key = "total") => {
  const row = safeRows(result)?.[0] || {};
  return safeNumber(row[key]);
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

async function fetchApproxTableRows() {
  const placeholders = LARGE_TABLE_NAMES.map(() => "?").join(", ");
  const [rows] = await pool.query(
    `
      SELECT TABLE_NAME, TABLE_ROWS
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME IN (${placeholders})
    `,
    LARGE_TABLE_NAMES
  );
  const map = new Map();
  rows.forEach((row) => {
    map.set(String(row.TABLE_NAME || ""), safeNumber(row.TABLE_ROWS));
  });
  return map;
}

async function fetchOverviewData() {
  const settled = await Promise.allSettled([
    pool.query(`SELECT COUNT(*) AS total FROM posts WHERE COALESCE(status, 'NORMAL') = 'NORMAL'`),
    pool.query(`SELECT COUNT(*) AS total FROM poi`),
    pool.query(`SELECT COUNT(*) AS total FROM users`),
    fetchApproxTableRows(),
    pool.query(
      `
        SELECT
          SUM(CASE WHEN created_at >= NOW() - INTERVAL 1 DAY THEN 1 ELSE 0 END) AS posts_24h,
          COUNT(DISTINCT CASE WHEN created_at >= NOW() - INTERVAL 30 DAY THEN user_id ELSE NULL END) AS active_creators_30d,
          COALESCE(AVG(like_count), 0) AS avg_likes,
          COALESCE(AVG(favorite_count), 0) AS avg_favorites,
          COALESCE(AVG(view_count), 0) AS avg_views,
          COALESCE(SUM(like_count), 0) AS like_total,
          COALESCE(SUM(favorite_count), 0) AS favorite_total,
          SUM(CASE WHEN poi_id IS NOT NULL THEN 1 ELSE 0 END) AS poi_linked_posts,
          COUNT(*) AS post_total
        FROM posts
        WHERE COALESCE(status, 'NORMAL') = 'NORMAL'
      `
    ),
    pool.query(`SELECT COUNT(*) AS total FROM user_follows WHERE COALESCE(status, 'NORMAL') = 'NORMAL'`),
    pool.query(
      `
        SELECT
          p.id,
          p.title,
          p.like_count,
          p.favorite_count,
          p.view_count,
          p.created_at,
          u.nickname
        FROM posts p FORCE INDEX (idx_posts_status_hot)
        LEFT JOIN users u ON u.id = p.user_id
        WHERE COALESCE(p.status, 'NORMAL') = 'NORMAL'
        ORDER BY p.like_count DESC, p.favorite_count DESC, p.view_count DESC, p.created_at DESC
        LIMIT 6
      `,
    ),
    pool.query(
      `
        SELECT
          u.id,
          u.nickname,
          0 AS post_count,
          COUNT(*) AS follower_count,
          0 AS like_count,
          COUNT(*) * 1.8 AS activity_score
        FROM user_follows f
        JOIN users u ON u.id = f.following_id
        WHERE COALESCE(f.status, 'NORMAL') = 'NORMAL'
        GROUP BY u.id, u.nickname
        ORDER BY follower_count DESC, u.id ASC
        LIMIT 8
      `
    ),
    pool.query(
      `
        SELECT
          SUM(CASE WHEN role = 'ADMIN' THEN 1 ELSE 0 END) AS admin_total,
          SUM(CASE WHEN role = 'SVIP' AND role_expires_at > NOW() THEN 1 ELSE 0 END) AS svip_active,
          SUM(CASE WHEN role = 'VIP' AND role_expires_at > NOW() THEN 1 ELSE 0 END) AS vip_active,
          SUM(CASE WHEN role IN ('VIP', 'SVIP') AND role_expires_at IS NOT NULL AND role_expires_at <= NOW() THEN 1 ELSE 0 END) AS membership_expired,
          SUM(
            CASE
              WHEN role = 'USER' OR role IS NULL OR role = '' THEN 1
              WHEN role IN ('VIP', 'SVIP') AND (role_expires_at IS NULL OR role_expires_at <= NOW()) THEN 1
              ELSE 0
            END
          ) AS standard_total,
          SUM(CASE WHEN role IN ('VIP', 'SVIP') AND role_expires_at > NOW() THEN 1 ELSE 0 END) AS paying_active
        FROM users
      `
    ),
    pool.query(
      `
        SELECT
          COUNT(*) AS orders_total,
          COALESCE(SUM(amount_cny), 0) AS revenue_total,
          SUM(CASE WHEN created_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END) AS orders_30d,
          COALESCE(SUM(CASE WHEN created_at >= NOW() - INTERVAL 30 DAY THEN amount_cny ELSE 0 END), 0) AS revenue_30d
        FROM membership_orders
        WHERE status = 'PAID'
      `
    ),
    pool.query(
      `
        SELECT
          mo.id,
          mo.user_id,
          mo.role_after,
          mo.billing_cycle,
          mo.amount_cny,
          mo.created_at,
          mo.expires_after,
          u.nickname
        FROM membership_orders mo
        LEFT JOIN users u ON u.id = mo.user_id
        WHERE mo.status = 'PAID'
        ORDER BY mo.id DESC
        LIMIT 8
      `
    ),
    pool.query(
      `
        SELECT
          id,
          nickname,
          role,
          role_expires_at
        FROM users
        WHERE role IN ('VIP', 'SVIP')
          AND role_expires_at IS NOT NULL
          AND role_expires_at > NOW()
        ORDER BY role_expires_at ASC, id ASC
        LIMIT 8
      `
    ),
    pool.query(
      `
        SELECT
          COUNT(*) AS total_campaigns,
          SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_campaigns,
          SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_campaigns,
          SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_campaigns,
          SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_campaigns,
          COALESCE(SUM(impression_count), 0) AS impression_total,
          COALESCE(SUM(unique_viewer_count), 0) AS viewer_total
        FROM ad_campaigns
        WHERE status <> 'DELETED'
      `
    ),
    pool.query(
      `
        SELECT
          c.id,
          c.title,
          c.placement,
          c.status,
          c.impression_count,
          c.unique_viewer_count,
          c.updated_at,
          u.nickname
        FROM ad_campaigns c
        LEFT JOIN users u ON u.id = c.user_id
        WHERE c.status <> 'DELETED'
        ORDER BY c.updated_at DESC, c.id DESC
        LIMIT 8
      `
    ),
    pool.query(
      `
        SELECT
          c.id,
          c.title,
          c.placement,
          c.status,
          c.updated_at,
          c.created_at,
          u.nickname
        FROM ad_campaigns c
        LEFT JOIN users u ON u.id = c.user_id
        WHERE c.status IN ('PENDING', 'REJECTED')
        ORDER BY FIELD(c.status, 'PENDING', 'REJECTED'), c.updated_at DESC, c.id DESC
        LIMIT 10
      `
    ),
    pool.query(
      `
        SELECT
          SUM(CASE WHEN is_featured = 1 AND COALESCE(status, 'NORMAL') = 'NORMAL' THEN 1 ELSE 0 END) AS featured_posts,
          SUM(CASE WHEN COALESCE(status, 'NORMAL') = 'HIDDEN' THEN 1 ELSE 0 END) AS hidden_posts,
          (SELECT COUNT(*) FROM post_reports WHERE status = 'OPEN') AS open_reports,
          (SELECT COUNT(*) FROM post_reports) AS total_reports
        FROM posts
      `
    ),
    pool.query(
      `
        SELECT
          pr.id,
          pr.post_id,
          pr.reason,
          pr.details,
          pr.status,
          pr.created_at,
          p.title,
          p.status AS post_status,
          p.is_featured,
          reporter.nickname AS reporter_nickname
        FROM post_reports pr
        JOIN posts p ON p.id = pr.post_id
        LEFT JOIN users reporter ON reporter.id = pr.reporter_user_id
        WHERE pr.status = 'OPEN'
        ORDER BY pr.created_at DESC, pr.id DESC
        LIMIT 10
      `
    ),
  ]);

  const approxMap = settled[3]?.status === "fulfilled" ? settled[3].value : new Map();
  const postSnapshot = settled[4]?.status === "fulfilled" ? safeRows(settled[4].value)?.[0] || {} : {};
  const exactFollowTotal = settled[5]?.status === "fulfilled" ? oneValue(settled[5].value) : 0;
  const totalPosts = safeNumber(postSnapshot.post_total);
  const poiLinkedPosts = safeNumber(postSnapshot.poi_linked_posts);
  const roleSnapshot = settled[8]?.status === "fulfilled" ? safeRows(settled[8].value)?.[0] || {} : {};
  const membershipSnapshot = settled[9]?.status === "fulfilled" ? safeRows(settled[9].value)?.[0] || {} : {};
  const adsSnapshot = settled[12]?.status === "fulfilled" ? safeRows(settled[12].value)?.[0] || {} : {};
  const governanceSnapshot = settled[15]?.status === "fulfilled" ? safeRows(settled[15].value)?.[0] || {} : {};

  return {
    totals: {
      posts: settled[0]?.status === "fulfilled" ? oneValue(settled[0].value) : 0,
      pois: settled[1]?.status === "fulfilled" ? oneValue(settled[1].value) : 0,
      users: settled[2]?.status === "fulfilled" ? oneValue(settled[2].value) : 0,
      comments:
        safeNumber(approxMap.get("post_comments")) + safeNumber(approxMap.get("post_comments_archive")),
      likes: safeNumber(postSnapshot.like_total),
      favorites: safeNumber(postSnapshot.favorite_total),
      follows: exactFollowTotal,
    },
    recent: {
      posts_24h: safeNumber(postSnapshot.posts_24h),
      active_creators_30d: safeNumber(postSnapshot.active_creators_30d),
      avg_likes_per_post: Math.round(safeNumber(postSnapshot.avg_likes) * 10) / 10,
      avg_favorites_per_post: Math.round(safeNumber(postSnapshot.avg_favorites) * 10) / 10,
      avg_views_per_post: Math.round(safeNumber(postSnapshot.avg_views) * 10) / 10,
      poi_link_rate: totalPosts > 0 ? Math.round((poiLinkedPosts * 100) / totalPosts) : 0,
    },
    role_breakdown: {
      admin: safeNumber(roleSnapshot.admin_total),
      svip: safeNumber(roleSnapshot.svip_active),
      vip: safeNumber(roleSnapshot.vip_active),
      standard: safeNumber(roleSnapshot.standard_total),
      expired_memberships: safeNumber(roleSnapshot.membership_expired),
      paying_active: safeNumber(roleSnapshot.paying_active),
    },
    membership_metrics: {
      orders_total: safeNumber(membershipSnapshot.orders_total),
      revenue_total: safeNumber(membershipSnapshot.revenue_total),
      orders_30d: safeNumber(membershipSnapshot.orders_30d),
      revenue_30d: safeNumber(membershipSnapshot.revenue_30d),
    },
    recent_membership_orders:
      settled[10]?.status === "fulfilled"
        ? safeRows(settled[10].value).map((row) => ({
            ...row,
            amount_cny: safeNumber(row.amount_cny),
            user_id: safeNumber(row.user_id),
          }))
        : [],
    expiring_memberships:
      settled[11]?.status === "fulfilled"
        ? safeRows(settled[11].value).map((row) => ({
            ...row,
            id: safeNumber(row.id),
          }))
        : [],
    ads_metrics: {
      total_campaigns: safeNumber(adsSnapshot.total_campaigns),
      active_campaigns: safeNumber(adsSnapshot.active_campaigns),
      paused_campaigns: safeNumber(adsSnapshot.paused_campaigns),
      pending_campaigns: safeNumber(adsSnapshot.pending_campaigns),
      rejected_campaigns: safeNumber(adsSnapshot.rejected_campaigns),
      impression_total: safeNumber(adsSnapshot.impression_total),
      viewer_total: safeNumber(adsSnapshot.viewer_total),
    },
    recent_ads:
      settled[13]?.status === "fulfilled"
        ? safeRows(settled[13].value).map((row) => ({
            ...row,
            id: safeNumber(row.id),
            impression_count: safeNumber(row.impression_count),
            unique_viewer_count: safeNumber(row.unique_viewer_count),
          }))
        : [],
    ad_review_queue:
      settled[14]?.status === "fulfilled"
        ? safeRows(settled[14].value).map((row) => ({
            ...row,
            id: safeNumber(row.id),
          }))
        : [],
    post_governance: {
      featured_posts: safeNumber(governanceSnapshot.featured_posts),
      hidden_posts: safeNumber(governanceSnapshot.hidden_posts),
      open_reports: safeNumber(governanceSnapshot.open_reports),
      total_reports: safeNumber(governanceSnapshot.total_reports),
    },
    post_report_queue:
      settled[16]?.status === "fulfilled"
        ? safeRows(settled[16].value).map((row) => ({
            ...row,
            id: safeNumber(row.id),
            post_id: safeNumber(row.post_id),
            is_featured: safeNumber(row.is_featured) > 0,
          }))
        : [],
    top_posts: settled[6]?.status === "fulfilled" ? safeRows(settled[6].value) : [],
    active_users:
      settled[7]?.status === "fulfilled"
        ? safeRows(settled[7].value).map((row) => ({
            ...row,
            activity_score: safeNumber(row.activity_score),
            comment_count: 0,
          }))
        : [],
    generated_at: new Date().toISOString(),
    approximate_large_tables: true,
  };
}

async function getCachedOverview(force = false) {
  if (force) {
    const data = await fetchOverviewData();
    overviewCache = {
      data,
      expiresAt: Date.now() + OVERVIEW_TTL_MS,
    };
    return data;
  }
  const now = Date.now();
  if (overviewCache.data && overviewCache.expiresAt > now) {
    return overviewCache.data;
  }
  if (!overviewInflight) {
    overviewInflight = fetchOverviewData()
      .then((data) => {
        overviewCache = {
          data,
          expiresAt: Date.now() + OVERVIEW_TTL_MS,
        };
        return data;
      })
      .finally(() => {
        overviewInflight = null;
      });
  }
  return overviewInflight;
}

router.post("/integrity-sweep", requireAdminUser, async (req, res) => {
  const adminId = Number(req.adminUser?.id) || 0;
  try {
    await tryAlter(`ALTER TABLE ad_campaigns ADD COLUMN reviewed_at DATETIME NULL`);
    await tryAlter(`ALTER TABLE ad_campaigns ADD COLUMN reviewed_by BIGINT UNSIGNED NULL`);
    await tryAlter(`ALTER TABLE ad_campaigns ADD COLUMN review_note VARCHAR(255) NULL`);
    const [demotedVip] = await pool.query(
      `
        UPDATE users
        SET role = 'USER', membership_updated_at = NOW()
        WHERE role IN ('VIP', 'SVIP')
          AND role_expires_at IS NOT NULL
          AND role_expires_at <= NOW()
      `
    );
    const [pausedAds] = await pool.query(
      `
        UPDATE ad_campaigns c
        JOIN users u ON u.id = c.user_id
        SET
          c.status = 'PAUSED',
          c.reviewed_at = NOW(),
          c.reviewed_by = ?,
          c.review_note = CASE
            WHEN COALESCE(c.review_note, '') = '' THEN 'Paused by integrity sweep: creator no longer has active SVIP/Admin ad privileges.'
            ELSE c.review_note
          END
        WHERE c.status = 'ACTIVE'
          AND NOT (
            u.role = 'ADMIN'
            OR (u.role = 'SVIP' AND (u.role_expires_at IS NULL OR u.role_expires_at > NOW()))
          )
      `,
      [adminId || null]
    );
    const [rejectedAds] = await pool.query(
      `
        UPDATE ad_campaigns c
        JOIN users u ON u.id = c.user_id
        SET
          c.status = 'REJECTED',
          c.reviewed_at = NOW(),
          c.reviewed_by = ?,
          c.review_note = CASE
            WHEN COALESCE(c.review_note, '') = '' THEN 'Rejected by integrity sweep: creator no longer has active SVIP/Admin ad privileges.'
            ELSE c.review_note
          END
        WHERE c.status = 'PENDING'
          AND NOT (
            u.role = 'ADMIN'
            OR (u.role = 'SVIP' AND (u.role_expires_at IS NULL OR u.role_expires_at > NOW()))
          )
      `,
      [adminId || null]
    );

    overviewCache = { expiresAt: 0, data: null };
    res.json({
      success: true,
      result: {
        memberships_demoted: safeNumber(demotedVip?.affectedRows),
        active_ads_paused: safeNumber(pausedAds?.affectedRows),
        pending_ads_rejected: safeNumber(rejectedAds?.affectedRows),
      },
    });
  } catch (err) {
    console.error("admin integrity sweep error", err);
    res.status(500).json({ success: false, message: "integrity sweep failed" });
  }
});

router.get("/overview", requireAdminUser, async (req, res) => {
  try {
    const data = await getCachedOverview(String(req.query.force || "") === "1");
    res.json({
      success: true,
      data: {
        viewer: req.adminUser,
        ...data,
      },
    });
  } catch (err) {
    console.error("admin overview error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
