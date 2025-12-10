import express from "express";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import { pool } from "../db/connect.js";

const router = express.Router();
const JWT_SECRET = process.env.JWT_SECRET || "journeypro-secret";
const SALT_ROUNDS = 10;
const captchaStore = new Map(); // key -> { code, expires }

const mapUser = (row) => ({
  id: row.id,
  username: row.username,
  nickname: row.nickname,
  avatar_url: row.avatar_url || null,
});

const randomKey = () => Math.random().toString(36).slice(2, 10) + Date.now();
const randomCode = () => Math.random().toString(36).slice(2, 6).toUpperCase();
const cleanCaptcha = () => {
  const now = Date.now();
  for (const [k, v] of captchaStore.entries()) {
    if (!v || v.expires < now) captchaStore.delete(k);
  }
};

router.get("/captcha", async (_req, res) => {
  cleanCaptcha();
  const key = randomKey();
  const code = randomCode();
  const expires = Date.now() + 5 * 60 * 1000;
  captchaStore.set(key, { code, expires });
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="120" height="40"><rect width="120" height="40" fill="#f5f5f5"/><text x="14" y="26" font-size="20" font-family="monospace" fill="#333">${code}</text></svg>`;
  const image = `data:image/svg+xml;base64,${Buffer.from(svg).toString("base64")}`;
  res.json({ success: true, key, image });
});

router.post("/register", async (req, res) => {
  try {
    const { username, password, nickname, avatarUrl, avatar_url, captcha_key, captcha_code } = req.body || {};
    const finalAvatar = avatarUrl || avatar_url || null;
    if (!username || !password || !nickname) {
      return res.status(400).json({ success: false, message: "username/password/nickname required" });
    }
    if ((password || "").length < 6) {
      return res.status(400).json({ success: false, message: "password must be at least 6 characters" });
    }
    cleanCaptcha();
    const saved = captchaStore.get(captcha_key);
    if (!saved || saved.code !== String(captcha_code || "").toUpperCase()) {
      return res.status(400).json({ success: false, message: "invalid captcha" });
    }
    captchaStore.delete(captcha_key);

    const [existRows] = await pool.query("SELECT id FROM users WHERE username = ? LIMIT 1", [username]);
    if (existRows.length > 0) {
      return res.status(409).json({ success: false, message: "username already exists" });
    }

    const hash = await bcrypt.hash(password, SALT_ROUNDS);
    const [result] = await pool.query(
      "INSERT INTO users (username, password_hash, nickname, avatar_url) VALUES (?, ?, ?, ?)",
      [username, hash, nickname, finalAvatar]
    );

    const newUser = { id: result.insertId, username, nickname, avatar_url: finalAvatar };
    const token = jwt.sign({ uid: newUser.id }, JWT_SECRET, { expiresIn: "7d" });
    res.json({ success: true, token, user: newUser });
  } catch (err) {
    console.error("register error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/login", async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ success: false, message: "username/password required" });
    }

    const [rows] = await pool.query("SELECT * FROM users WHERE username = ? LIMIT 1", [username]);
    if (rows.length === 0) {
      return res.status(401).json({ success: false, message: "invalid credentials" });
    }

    const userRow = rows[0];
    const match = await bcrypt.compare(password, userRow.password_hash);
    if (!match) {
      return res.status(401).json({ success: false, message: "invalid credentials" });
    }

    const user = mapUser(userRow);
    const token = jwt.sign({ uid: user.id }, JWT_SECRET, { expiresIn: "7d" });
    res.json({ success: true, token, user });
  } catch (err) {
    console.error("login error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// GET /api/auth/user?id=123
router.get("/user", async (req, res) => {
  try {
    const id = parseInt(req.query.id || "0", 10);
    if (!id) {
      return res.status(400).json({ success: false, message: "id required" });
    }
    const [rows] = await pool.query("SELECT * FROM users WHERE id = ? LIMIT 1", [id]);
    if (!rows.length) {
      return res.status(404).json({ success: false, message: "user not found" });
    }
    res.json({ success: true, user: mapUser(rows[0]) });
  } catch (err) {
    console.error("fetch user error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

// POST /api/auth/avatar  { user_id, avatar_url }
router.post("/avatar", async (req, res) => {
  try {
    const { user_id, avatar_url } = req.body || {};
    const uid = parseInt(user_id || "0", 10);
    if (!uid || !avatar_url) {
      return res.status(400).json({ success: false, message: "user_id and avatar_url required" });
    }
    await pool.query("UPDATE users SET avatar_url = ? WHERE id = ?", [avatar_url, uid]);
    const [rows] = await pool.query("SELECT * FROM users WHERE id = ? LIMIT 1", [uid]);
    if (!rows.length) return res.status(404).json({ success: false, message: "user not found" });
    res.json({ success: true, user: mapUser(rows[0]) });
  } catch (err) {
    console.error("update avatar error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
