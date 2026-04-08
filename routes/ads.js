import express from "express";
import { pool } from "../db/connect.js";
import { fetchUserAccessById, getRoleMeta } from "../utils/userAccess.js";

const router = express.Router();

const VALID_PLACEMENTS = new Set(["map", "posts"]);
const ACTIVE_STATUSES = new Set(["ACTIVE", "PAUSED"]);
let ensureAdsSchemaPromise = null;
const CAMPAIGN_SELECT = `
  SELECT
    c.*,
    p.title AS linked_post_title,
    p.cover_image AS linked_post_cover_image
  FROM ad_campaigns c
  LEFT JOIN posts p
    ON p.id = c.linked_post_id
   AND COALESCE(p.status, 'NORMAL') = 'NORMAL'
`;

const parseUserId = (value) => {
  const uid = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(uid) && uid > 0 ? uid : 0;
};

const parseAdId = (value) => {
  const id = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
};

const normalizePlacement = (value) => {
  const placement = String(value || "").trim().toLowerCase();
  return VALID_PLACEMENTS.has(placement) ? placement : "map";
};

const normalizeStatus = (value, fallback = "ACTIVE") => {
  const status = String(value || "").trim().toUpperCase();
  if (["ACTIVE", "PAUSED", "DELETED"].includes(status)) return status;
  return fallback;
};

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

const truncate = (value, limit) => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3)).trim()}...`;
};

const normalizeHttpUrl = (value, { allowEmpty = false } = {}) => {
  const text = truncate(value, 1024);
  if (!text) return allowEmpty ? "" : null;
  try {
    const url = new URL(text);
    if (!["http:", "https:"].includes(url.protocol)) return null;
    return url.toString();
  } catch {
    return null;
  }
};

const toUsageMonth = (value = new Date()) => {
  const year = value.getUTCFullYear();
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
};

const buildViewerKey = ({ userId, sessionKey }) => {
  const uid = parseUserId(userId);
  if (uid) return `user:${uid}`;
  const raw = String(sessionKey || "").trim().replace(/[^\w:-]/g, "").slice(0, 120);
  return raw ? `guest:${raw}` : "guest:anonymous";
};

async function requireAdManager(req, res, next) {
  try {
    const userId = parseUserId(req.query.user_id ?? req.body?.user_id ?? req.get("x-user-id"));
    if (!userId) {
      return res.status(401).json({ success: false, message: "user_id required" });
    }
    const user = await fetchUserAccessById(userId);
    if (!user) {
      return res.status(404).json({ success: false, message: "user not found" });
    }
    if (!user.can_manage_ads) {
      return res.status(403).json({ success: false, message: "SVIP or admin access required" });
    }
    req.adUser = user;
    return next();
  } catch (err) {
    console.error("ad access check error", err);
    return res.status(500).json({ success: false, message: "server error" });
  }
}

async function ensureAdsSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ad_campaigns (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id BIGINT UNSIGNED NOT NULL,
      role_snapshot VARCHAR(20) NOT NULL DEFAULT 'SVIP',
      title VARCHAR(160) NOT NULL,
      subtitle VARCHAR(200) NULL,
      body TEXT NULL,
      image_url VARCHAR(1024) NULL,
      linked_post_id BIGINT UNSIGNED NULL,
      placement VARCHAR(20) NOT NULL DEFAULT 'map',
      cta_text VARCHAR(80) NULL,
      cta_link VARCHAR(1024) NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
      usage_month CHAR(7) NOT NULL,
      impression_count INT NOT NULL DEFAULT 0,
      unique_viewer_count INT NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_ad_campaigns_user_month (user_id, usage_month, status, created_at),
      KEY idx_ad_campaigns_placement_status (placement, status, updated_at),
      KEY idx_ad_campaigns_linked_post (linked_post_id)
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ad_impressions (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      ad_id BIGINT UNSIGNED NOT NULL,
      user_id BIGINT UNSIGNED NULL,
      viewer_key VARCHAR(140) NOT NULL,
      placement VARCHAR(20) NOT NULL,
      viewed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_ad_impressions_ad (ad_id, viewed_at),
      KEY idx_ad_impressions_viewer (viewer_key, viewed_at)
    )
  `);

  await tryAlter(`ALTER TABLE ad_campaigns ADD COLUMN linked_post_id BIGINT UNSIGNED NULL`);
  await tryAlter(`ALTER TABLE ad_campaigns ADD KEY idx_ad_campaigns_linked_post (linked_post_id)`);
}

function ensureAdsSchemaReady() {
  if (!ensureAdsSchemaPromise) {
    ensureAdsSchemaPromise = ensureAdsSchema().catch((err) => {
      ensureAdsSchemaPromise = null;
      throw err;
    });
  }
  return ensureAdsSchemaPromise;
}

const mapAdRow = (row) => ({
  id: Number(row.id),
  user_id: Number(row.user_id),
  role_snapshot: String(row.role_snapshot || "").trim() || "SVIP",
  title: String(row.title || "").trim(),
  subtitle: String(row.subtitle || "").trim(),
  body: String(row.body || "").trim(),
  image_url: String(row.image_url || "").trim(),
  linked_post_id: row.linked_post_id ? Number(row.linked_post_id) : null,
  linked_post_title: String(row.linked_post_title || "").trim(),
  linked_post_cover_image: String(row.linked_post_cover_image || "").trim(),
  placement: normalizePlacement(row.placement),
  cta_text: String(row.cta_text || "").trim(),
  cta_link: String(row.cta_link || "").trim(),
  status: normalizeStatus(row.status),
  usage_month: String(row.usage_month || "").trim(),
  impression_count: Number(row.impression_count) || 0,
  unique_viewer_count: Number(row.unique_viewer_count) || 0,
  created_at: row.created_at,
  updated_at: row.updated_at,
});

async function buildMonthlyUsage(userId, usageMonth) {
  const [[row]] = await pool.query(
    `
      SELECT COUNT(*) AS total
      FROM ad_campaigns
      WHERE user_id = ?
        AND usage_month = ?
    `,
    [userId, usageMonth]
  );
  return Number(row?.total) || 0;
}

async function fetchCampaignById(adId, userId) {
  const [rows] = await pool.query(
    `${CAMPAIGN_SELECT} WHERE c.id = ? AND c.user_id = ? LIMIT 1`,
    [adId, userId]
  );
  return rows[0] || null;
}

async function fetchValidLinkedPost(postId) {
  const pid = parseAdId(postId);
  if (!pid) return null;
  const [rows] = await pool.query(
    `
      SELECT id, title, cover_image
      FROM posts
      WHERE id = ?
        AND COALESCE(status, 'NORMAL') = 'NORMAL'
      LIMIT 1
    `,
    [pid]
  );
  return rows[0] || null;
}

router.get("/mine", requireAdManager, async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const user = req.adUser;
    const usageMonth = String(req.query.usage_month || toUsageMonth()).trim();
    const [rows] = await pool.query(
      `
        ${CAMPAIGN_SELECT}
        WHERE c.user_id = ?
          AND c.status <> 'DELETED'
        ORDER BY c.created_at DESC, c.id DESC
        LIMIT 40
      `,
      [user.id]
    );
    const roleMeta = getRoleMeta(user.role);
    const usedThisMonth = await buildMonthlyUsage(user.id, usageMonth);
    res.json({
      success: true,
      usage_month: usageMonth,
      quota: {
        role: user.role,
        role_label: user.role_label,
        monthly_limit: roleMeta.adLimit,
        used: usedThisMonth,
        remaining: roleMeta.adLimit === null ? null : Math.max(0, roleMeta.adLimit - usedThisMonth),
        unlimited: roleMeta.adLimit === null,
      },
      items: rows.map(mapAdRow),
    });
  } catch (err) {
    console.error("ads mine error", err);
    res.status(500).json({ success: false, message: "Failed to load ad campaigns." });
  }
});

router.get("/post-search", requireAdManager, async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const q = truncate(req.query.q, 120);
    const limit = Math.max(4, Math.min(Number(req.query.limit) || 8, 12));
    const params = [];
    let sql = `
      SELECT id, title, cover_image, created_at
      FROM posts
      WHERE COALESCE(status, 'NORMAL') = 'NORMAL'
    `;
    if (q) {
      sql += ` AND (title LIKE ? OR content LIKE ?)`;
      params.push(`%${q}%`, `%${q}%`);
    }
    sql += ` ORDER BY created_at DESC, id DESC LIMIT ?`;
    params.push(limit);
    const [rows] = await pool.query(sql, params);
    res.json({
      success: true,
      items: rows.map((row) => ({
        id: Number(row.id),
        title: String(row.title || "").trim() || "Untitled story",
        cover_image: String(row.cover_image || "").trim(),
        created_at: row.created_at,
      })),
    });
  } catch (err) {
    console.error("ads post search error", err);
    res.status(500).json({ success: false, message: "Failed to search stories." });
  }
});

router.post("/", requireAdManager, async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const user = req.adUser;
    const roleMeta = getRoleMeta(user.role);
    const title = truncate(req.body?.title, 160);
    const subtitle = truncate(req.body?.subtitle, 200);
    const body = truncate(req.body?.body, 1800);
    const imageUrl = normalizeHttpUrl(req.body?.image_url);
    const linkedPostId = parseAdId(req.body?.linked_post_id);
    const placement = normalizePlacement(req.body?.placement);
    const ctaText = truncate(req.body?.cta_text || "Learn more", 80);
    const ctaLink = normalizeHttpUrl(req.body?.cta_link, { allowEmpty: true });
    const usageMonth = toUsageMonth();

    if (!title || !body || !imageUrl) {
      return res.status(400).json({ success: false, message: "title, body and a valid remote image_url are required" });
    }

    if (req.body?.cta_link && !ctaLink) {
      return res.status(400).json({ success: false, message: "cta_link must be a valid http/https URL" });
    }

    if (req.body?.linked_post_id && !linkedPostId) {
      return res.status(400).json({ success: false, message: "linked_post_id must be a valid post id" });
    }

    if (linkedPostId) {
      const linkedPost = await fetchValidLinkedPost(linkedPostId);
      if (!linkedPost) {
        return res.status(404).json({ success: false, message: "Linked post not found" });
      }
    }

    const usedThisMonth = await buildMonthlyUsage(user.id, usageMonth);
    if (roleMeta.adLimit !== null && usedThisMonth >= roleMeta.adLimit) {
      return res.status(403).json({
        success: false,
        message: `Monthly ad limit reached (${usedThisMonth}/${roleMeta.adLimit}).`,
      });
    }

    const [result] = await pool.query(
      `
        INSERT INTO ad_campaigns (
          user_id, role_snapshot, title, subtitle, body, image_url, linked_post_id,
          placement, cta_text, cta_link, status, usage_month
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
      `,
      [
        user.id,
        user.role,
        title,
        subtitle || null,
        body,
        imageUrl,
        linkedPostId || null,
        placement,
        ctaText || null,
        ctaLink || null,
        usageMonth,
      ]
    );

    const created = await fetchCampaignById(result.insertId, user.id);
    res.json({
      success: true,
      item: mapAdRow(created),
      quota: {
        role: user.role,
        role_label: user.role_label,
        monthly_limit: roleMeta.adLimit,
        used: usedThisMonth + 1,
        remaining: roleMeta.adLimit === null ? null : Math.max(0, roleMeta.adLimit - usedThisMonth - 1),
        unlimited: roleMeta.adLimit === null,
      },
    });
  } catch (err) {
    console.error("ads create error", err);
    res.status(500).json({ success: false, message: "Failed to create ad campaign." });
  }
});

router.patch("/:id", requireAdManager, async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const user = req.adUser;
    const adId = parseAdId(req.params.id);
    if (!adId) {
      return res.status(400).json({ success: false, message: "invalid ad id" });
    }
    const current = await fetchCampaignById(adId, user.id);
    if (!current) {
      return res.status(404).json({ success: false, message: "ad campaign not found" });
    }
    const nextStatus = normalizeStatus(req.body?.status, current.status);
    if (!ACTIVE_STATUSES.has(nextStatus)) {
      return res.status(400).json({ success: false, message: "Only ACTIVE or PAUSED status is allowed here." });
    }
    await pool.query(`UPDATE ad_campaigns SET status = ? WHERE id = ? AND user_id = ?`, [nextStatus, adId, user.id]);
    const updated = await fetchCampaignById(adId, user.id);
    res.json({ success: true, item: mapAdRow(updated) });
  } catch (err) {
    console.error("ads update error", err);
    res.status(500).json({ success: false, message: "Failed to update ad campaign." });
  }
});

router.delete("/:id", requireAdManager, async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const user = req.adUser;
    const adId = parseAdId(req.params.id);
    if (!adId) {
      return res.status(400).json({ success: false, message: "invalid ad id" });
    }
    await pool.query(`UPDATE ad_campaigns SET status = 'DELETED' WHERE id = ? AND user_id = ?`, [adId, user.id]);
    res.json({ success: true });
  } catch (err) {
    console.error("ads delete error", err);
    res.status(500).json({ success: false, message: "Failed to delete ad campaign." });
  }
});

router.get("/serve", async (req, res) => {
  try {
    await ensureAdsSchemaReady();
    const placement = normalizePlacement(req.query.placement);
    const viewerUserId = parseUserId(req.query.user_id);
    const viewerKey = buildViewerKey({ userId: viewerUserId, sessionKey: req.query.session_key });
    const [rows] = await pool.query(
      `
        ${CAMPAIGN_SELECT}
        WHERE c.placement = ?
          AND c.status = 'ACTIVE'
        ORDER BY c.updated_at DESC, c.id DESC
        LIMIT 12
      `,
      [placement]
    );

    if (!rows.length) {
      return res.json({ success: true, item: null });
    }

    const candidates = rows.map(mapAdRow);
    const hashSeed = [...viewerKey].reduce((sum, char) => sum + char.charCodeAt(0), 0);
    const selected = candidates[hashSeed % candidates.length] || candidates[0];

    await pool.query(
      `INSERT INTO ad_impressions (ad_id, user_id, viewer_key, placement) VALUES (?, ?, ?, ?)`,
      [selected.id, viewerUserId || null, viewerKey, placement]
    );

    await pool.query(
      `
        UPDATE ad_campaigns c
        SET
          impression_count = (SELECT COUNT(*) FROM ad_impressions WHERE ad_id = c.id),
          unique_viewer_count = (SELECT COUNT(DISTINCT viewer_key) FROM ad_impressions WHERE ad_id = c.id)
        WHERE c.id = ?
      `,
      [selected.id]
    );

    res.json({ success: true, item: selected });
  } catch (err) {
    console.error("ads serve error", err);
    res.status(500).json({ success: false, message: "Failed to serve popup ad." });
  }
});

export default router;
