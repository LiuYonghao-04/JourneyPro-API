import express from "express";
import { pool } from "../db/connect.js";

const router = express.Router();

router.post("/seed/posts", async (_req, res) => {
  try {
    const samples = [
      {
        title: "周末小众海边 24h",
        content: "住在海边 BnB，骑行+落日晚霞+烧烤。预算 500/人。",
        tags: ["旅行", "小众", "海边"],
        images: [
          "https://images.unsplash.com/photo-1507525428034-b723cf961d3e",
          "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?2",
        ],
      },
      {
        title: "成都火锅 3 家踩点",
        content: "九宫格+番茄鸳鸯，安利 3 家不踩雷的火锅店。",
        tags: ["美食", "火锅", "成都"],
        images: ["https://images.unsplash.com/photo-1540189549336-e6e99c3679fe"],
      },
      {
        title: "胶片风穿搭日记",
        content: "灰色大衣 + 马丁靴，低饱和氛围感。",
        tags: ["穿搭", "胶片", "日常"],
        images: ["https://images.unsplash.com/photo-1521572267360-ee0c2909d518"],
      },
    ];

    for (const item of samples) {
      const [r] = await pool.query(
        `INSERT INTO posts (user_id, title, content, cover_image, image_count, like_count, favorite_count, view_count)
         VALUES (1, ?, ?, ?, ?, ?, ?, ?)`,
        [
          item.title,
          item.content,
          item.images[0],
          item.images.length,
          Math.floor(Math.random() * 200 + 30),
          Math.floor(Math.random() * 80 + 10),
          Math.floor(Math.random() * 300 + 50),
        ]
      );
      const postId = r.insertId;
      const rows = item.images.map((url, idx) => [postId, url, idx]);
      await pool.query(`INSERT INTO post_images (post_id, image_url, sort_order) VALUES ?`, [rows]);
      // tags
      for (const tag of item.tags) {
        const [exist] = await pool.query(`SELECT id FROM tags WHERE name=? LIMIT 1`, [tag]);
        let tid;
        if (exist.length > 0) tid = exist[0].id;
        else {
          const [rt] = await pool.query(`INSERT INTO tags (name) VALUES (?)`, [tag]);
          tid = rt.insertId;
        }
        await pool.query(`INSERT IGNORE INTO post_tags (post_id, tag_id) VALUES (?,?)`, [postId, tid]);
      }
    }

    res.json({ success: true, message: "seeded" });
  } catch (err) {
    console.error("seed error", err);
    res.status(500).json({ success: false, message: "seed failed" });
  }
});

export default router;
