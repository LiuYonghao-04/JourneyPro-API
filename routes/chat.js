import express from "express";
import { pool } from "../db/connect.js";
import { pushNotification } from "./notifications.js";

const router = express.Router();

router.use((req, res, next) => {
  res.setHeader("Cache-Control", "no-store");
  next();
});

async function createIndexSafe(sql, indexName) {
  try {
    await pool.query(sql);
  } catch (e) {
    if (e?.code !== "ER_DUP_KEYNAME") {
      console.error(`create ${indexName} error`, e);
    }
  }
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS chat_messages (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      sender_id BIGINT NOT NULL,
      receiver_id BIGINT NOT NULL,
      content TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_sender (sender_id),
      INDEX idx_receiver (receiver_id),
      CONSTRAINT fk_chat_sender FOREIGN KEY (sender_id) REFERENCES users(id),
      CONSTRAINT fk_chat_receiver FOREIGN KEY (receiver_id) REFERENCES users(id)
    );
  `);
  await createIndexSafe(`CREATE INDEX idx_chat_sender_id ON chat_messages(sender_id, id)`, "idx_chat_sender_id");
  await createIndexSafe(`CREATE INDEX idx_chat_receiver_id ON chat_messages(receiver_id, id)`, "idx_chat_receiver_id");
  await createIndexSafe(
    `CREATE INDEX idx_chat_pair_forward ON chat_messages(sender_id, receiver_id, id)`,
    "idx_chat_pair_forward"
  );
  await createIndexSafe(
    `CREATE INDEX idx_chat_pair_reverse ON chat_messages(receiver_id, sender_id, id)`,
    "idx_chat_pair_reverse"
  );
}

let ensureTablesPromise = null;
function ensureTablesReady() {
  if (!ensureTablesPromise) {
    ensureTablesPromise = ensureTables().catch((err) => {
      ensureTablesPromise = null;
      throw err;
    });
  }
  return ensureTablesPromise;
}

// GET /api/chat/list?user_id=1
router.get("/list", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = parseInt(req.query.user_id || "0", 10);
    const limit = Math.min(parseInt(req.query.limit || "50", 10), 100);
    if (!userId) return res.json({ success: true, data: [] });
    const [rows] = await pool.query(
      `
      SELECT c.id, c.sender_id, c.receiver_id, c.content, c.created_at,
             IF(c.sender_id = ?, c.receiver_id, c.sender_id) AS peer_id,
             u.nickname, u.avatar_url
      FROM chat_messages c
      JOIN (
        SELECT GREATEST(sender_id, receiver_id) AS a, LEAST(sender_id, receiver_id) AS b, MAX(id) AS max_id
        FROM chat_messages
        WHERE sender_id = ? OR receiver_id = ?
        GROUP BY a, b
      ) t ON c.id = t.max_id
      LEFT JOIN users u ON u.id = IF(c.sender_id = ?, c.receiver_id, c.sender_id)
      ORDER BY c.id DESC
      LIMIT ?
      `,
      [userId, userId, userId, userId, limit]
    );
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("chat list error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/chat/history?user_id=1&peer_id=2
router.get("/history", async (req, res) => {
  try {
    await ensureTablesReady();
    const userId = parseInt(req.query.user_id || "0", 10);
    const peerId = parseInt(req.query.peer_id || "0", 10);
    const limit = Math.min(parseInt(req.query.limit || "100", 10), 200);
    if (!userId || !peerId) return res.json({ success: true, data: [] });
    const [rows] = await pool.query(
      `
      SELECT id, sender_id, receiver_id, content, created_at
      FROM chat_messages
      WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?)
      ORDER BY id ASC
      LIMIT ?
      `,
      [userId, peerId, peerId, userId, limit]
    );
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("chat history error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/chat/search?keyword=abc&user_id=1
router.get("/search", async (req, res) => {
  try {
    const keyword = (req.query.keyword || "").trim();
    const userId = parseInt(req.query.user_id || "0", 10);
    if (!keyword) return res.json({ success: true, data: [] });
    const limit = Math.min(parseInt(req.query.limit || "10", 10), 30);
    const [rows] = await pool.query(
      `
      SELECT id, nickname, username, avatar_url
      FROM users
      WHERE (nickname LIKE ? OR username LIKE ? OR id = ?)
        AND id <> ?
      ORDER BY id DESC
      LIMIT ?
      `,
      [`%${keyword}%`, `%${keyword}%`, keyword, userId || 0, limit]
    );
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("chat search error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// POST /api/chat/send { sender_id, receiver_id, content }
router.post("/send", async (req, res) => {
  try {
    await ensureTablesReady();
    const { sender_id, receiver_id, content } = req.body || {};
    if (!sender_id || !receiver_id || !content) {
      return res.status(400).json({ success: false, message: "sender_id, receiver_id and content are required" });
    }
    const [r] = await pool.query(
      `INSERT INTO chat_messages (sender_id, receiver_id, content) VALUES (?, ?, ?)`,
      [sender_id, receiver_id, content]
    );
    const messageId = r.insertId;
    const [[row]] = await pool.query(
      `SELECT id, sender_id, receiver_id, content, created_at FROM chat_messages WHERE id = ?`,
      [messageId]
    );
    try {
      const [[actor]] = await pool.query(`SELECT nickname, avatar_url FROM users WHERE id = ? LIMIT 1`, [sender_id]);
      pushNotification(receiver_id, {
        type: "chat",
        actor_id: sender_id,
        actor_nickname: actor?.nickname,
        actor_avatar: actor?.avatar_url,
        content,
      });
    } catch (e) {
      // ignore push failures
    }
    res.json({ success: true, data: row });
  } catch (err) {
    console.error("chat send error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
