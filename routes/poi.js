import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();

// GET /api/poi/search?keyword=xx&limit=10
router.get("/search", async (req, res) => {
  try {
    const keyword = (req.query.keyword || "").trim();
    const limit = Math.min(parseInt(req.query.limit || "10", 10), 50);
    if (!keyword) {
      return res.json({ success: true, data: [] });
    }
    const like = `%${keyword}%`;
    const [rows] = await pool.query(
      `SELECT id, name, category, lat, lng, popularity, price, tags, image_url
       FROM poi
       WHERE name LIKE ?
       ORDER BY popularity DESC, id DESC
       LIMIT ?`,
      [like, limit]
    );
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error("poi search error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
