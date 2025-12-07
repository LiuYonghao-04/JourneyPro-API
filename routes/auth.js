import express from "express";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import { pool } from "../db/connect.js";

const router = express.Router();
const JWT_SECRET = process.env.JWT_SECRET || "journeypro-secret";
const SALT_ROUNDS = 10;

const mapUser = (row) => ({
  id: row.id,
  username: row.username,
  nickname: row.nickname,
  avatar_url: row.avatar_url || null,
});

router.post("/register", async (req, res) => {
  try {
    const { username, password, nickname, avatarUrl, avatar_url } = req.body || {};
    const finalAvatar = avatarUrl || avatar_url || null;
    if (!username || !password || !nickname) {
      return res.status(400).json({ success: false, message: "username/password/nickname required" });
    }

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

export default router;
