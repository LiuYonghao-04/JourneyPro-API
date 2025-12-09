import express from "express";
import { pool } from "../db/connect.js";
import { pushNotification } from "./notifications.js";

const router = express.Router();

async function ensureFollowTable() {
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
  try {
    await pool.query(`CREATE INDEX idx_follows_follower ON user_follows(follower_id)`);
  } catch (e) {
    if (e?.code !== "ER_DUP_KEYNAME") {
      console.error("create idx_follows_follower error", e);
    }
  }
  try {
    await pool.query(`CREATE INDEX idx_follows_following ON user_follows(following_id)`);
  } catch (e) {
    if (e?.code !== "ER_DUP_KEYNAME") {
      console.error("create idx_follows_following error", e);
    }
  }
}

router.get("/status", async (req, res) => {
  try {
    await ensureFollowTable();
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
    await ensureFollowTable();
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
    await ensureFollowTable();
    const targetId = parseInt(req.query.target_id || "0", 10);
    if (!targetId) return res.json({ success: true, data: [] });
    const [rows] = await pool.query(
      `SELECT uf.id, uf.follower_id AS user_id, uf.created_at,
              u.nickname, u.avatar_url
       FROM user_follows uf
       LEFT JOIN users u ON uf.follower_id = u.id
       WHERE uf.following_id = ?
       ORDER BY uf.created_at DESC
       LIMIT 200`,
      [targetId]
    );
    res.json({ success: true, data: rows, count: rows.length });
  } catch (err) {
    console.error("followers list error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
