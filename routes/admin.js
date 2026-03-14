import express from "express";
import { pool } from "../db/connect.js";
import { ensureAdminAccess } from "../utils/admin.js";

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

const parseUserId = (req) => {
  const raw = req.query.user_id ?? req.body?.user_id ?? req.get("x-user-id");
  const uid = Number.parseInt(String(raw || ""), 10);
  return Number.isFinite(uid) && uid > 0 ? uid : 0;
};

const safeNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
};

const safeRows = (result) => result?.[0] || [];

const oneValue = (result, key = "total") => {
  const row = safeRows(result)?.[0] || {};
  return safeNumber(row[key]);
};

async function requireAdmin(req, res, next) {
  try {
    const userId = parseUserId(req);
    if (!userId) {
      return res.status(401).json({ success: false, message: "admin user_id required" });
    }
    const adminUser = await ensureAdminAccess(pool, userId);
    if (!adminUser) {
      return res.status(403).json({ success: false, message: "admin access required" });
    }
    req.adminUser = adminUser;
    return next();
  } catch (err) {
    console.error("admin access check error", err);
    return res.status(500).json({ success: false, message: "server error" });
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
  ]);

  const approxMap = settled[3]?.status === "fulfilled" ? settled[3].value : new Map();
  const postSnapshot = settled[4]?.status === "fulfilled" ? safeRows(settled[4].value)?.[0] || {} : {};
  const exactFollowTotal = settled[5]?.status === "fulfilled" ? oneValue(settled[5].value) : 0;
  const totalPosts = safeNumber(postSnapshot.post_total);
  const poiLinkedPosts = safeNumber(postSnapshot.poi_linked_posts);

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

async function getCachedOverview() {
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

router.get("/overview", requireAdmin, async (req, res) => {
  try {
    const data = await getCachedOverview();
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
