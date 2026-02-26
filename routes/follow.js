import express from "express";
import { pool } from "../db/connect.js";
import { pushNotification } from "./notifications.js";

const router = express.Router();
const ENABLE_RUNTIME_SCHEMA_MIGRATION = process.env.ENABLE_RUNTIME_SCHEMA_MIGRATION === "1";
let schemaMigrationNoticePrinted = false;

router.use((req, res, next) => {
  res.setHeader("Cache-Control", "no-store");
  next();
});

async function createIndexSafe(sql, indexName) {
  if (!ENABLE_RUNTIME_SCHEMA_MIGRATION) return;
  try {
    await pool.query(sql);
  } catch (e) {
    if (e?.code !== "ER_DUP_KEYNAME") {
      console.error(`create ${indexName} error`, e);
    }
  }
}

async function ensureFollowTable() {
  if (!ENABLE_RUNTIME_SCHEMA_MIGRATION) {
    if (!schemaMigrationNoticePrinted) {
      schemaMigrationNoticePrinted = true;
      console.warn("[follow] runtime schema migration disabled (set ENABLE_RUNTIME_SCHEMA_MIGRATION=1 to enable)");
    }
    return;
  }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_follows (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      follower_id BIGINT NOT NULL,
      following_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status VARCHAR(20) DEFAULT 'NORMAL',
      CONSTRAINT fk_follows_follower FOREIGN KEY (follower_id) REFERENCES users(id),
      CONSTRAINT fk_follows_following FOREIGN KEY (following_id) REFERENCES users(id),
      CONSTRAINT uk_follows_pair UNIQUE (follower_id, following_id),
      CONSTRAINT chk_follows_not_self CHECK (follower_id <> following_id)
    );
  `);
  await createIndexSafe(
    `CREATE INDEX idx_user_follows_following_created ON user_follows(following_id, created_at, follower_id)`,
    "idx_user_follows_following_created"
  );
  await createIndexSafe(
    `CREATE INDEX idx_follows_follower_created ON user_follows(follower_id, created_at, following_id)`,
    "idx_follows_follower_created"
  );
}

let ensureFollowTablePromise = null;
function ensureFollowTableReady() {
  if (!ensureFollowTablePromise) {
    ensureFollowTablePromise = ensureFollowTable().catch((err) => {
      ensureFollowTablePromise = null;
      throw err;
    });
  }
  return ensureFollowTablePromise;
}

router.get("/status", async (req, res) => {
  try {
    await ensureFollowTableReady();
    const userId = parseInt(req.query.user_id || "0", 10);
    const targetId = parseInt(req.query.target_id || "0", 10);
    if (!userId || !targetId) return res.json({ success: true, following: false });
    const [[row]] = await pool.query(
      `SELECT id FROM user_follows WHERE follower_id = ? AND following_id = ? LIMIT 1`,
      [userId, targetId]
    );
    res.json({ success: true, following: !!row });
  } catch (err) {
    console.error("follow status error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/toggle", async (req, res) => {
  try {
    await ensureFollowTableReady();
    const followerId = req.body?.user_id;
    const targetId = req.body?.target_id;
    if (!followerId || !targetId) {
      return res.status(400).json({ success: false, message: "user_id and target_id required" });
    }
    if (Number(followerId) === Number(targetId)) {
      return res.status(400).json({ success: false, message: "cannot follow yourself" });
    }
    const [[existing]] = await pool.query(
      `SELECT id FROM user_follows WHERE follower_id = ? AND following_id = ? LIMIT 1`,
      [followerId, targetId]
    );
    if (existing) {
      await pool.query(`DELETE FROM user_follows WHERE id = ?`, [existing.id]);
      return res.json({ success: true, following: false });
    }
    await pool.query(`INSERT INTO user_follows (follower_id, following_id) VALUES (?, ?)`, [
      followerId,
      targetId,
    ]);
    // push follow notification
    try {
      const [[actor]] = await pool.query(`SELECT id, nickname, avatar_url FROM users WHERE id = ? LIMIT 1`, [
        followerId,
      ]);
      pushNotification(targetId, {
        type: "follow",
        actor_id: followerId,
        actor_nickname: actor?.nickname,
        actor_avatar: actor?.avatar_url,
      });
    } catch (e) {
      // ignore
    }
    res.json({ success: true, following: true });
  } catch (err) {
    console.error("follow toggle error", err);
    res
      .status(500)
      .json({ success: false, message: err?.message?.substring(0, 200) || "server error" });
  }
});

// GET /api/follow/followers?target_id=1
router.get("/followers", async (req, res) => {
  try {
    await ensureFollowTableReady();
    const targetId = parseInt(req.query.target_id || "0", 10);
    if (!targetId) return res.json({ success: true, data: [] });
    const countOnly = String(req.query.count_only || "0") === "1";
    const limit = Math.max(0, Math.min(parseInt(req.query.limit || "200", 10) || 200, 500));
    const [[countRow]] = await pool.query(
      `SELECT COUNT(*) AS c FROM user_follows WHERE following_id = ?`,
      [targetId]
    );
    const count = Number(countRow?.c || 0);
    if (countOnly) {
      return res.json({ success: true, data: [], count });
    }
    if (limit <= 0 || count <= 0) {
      return res.json({ success: true, data: [], count });
    }
    const [rows] = await pool.query(
      `SELECT uf.id, uf.follower_id AS user_id, uf.created_at,
              u.nickname, u.avatar_url
       FROM user_follows uf
       LEFT JOIN users u ON uf.follower_id = u.id
       WHERE uf.following_id = ?
       ORDER BY uf.created_at DESC
       LIMIT ?`,
      [targetId, limit]
    );
    res.json({ success: true, data: rows, count });
  } catch (err) {
    console.error("followers list error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
