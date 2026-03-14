import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { pool } from "../db/connect.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUTPUT_DIR = path.join(__dirname, "..", "artifacts", "ai");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "journeypro_finetune_dataset.jsonl");
const ROW_LIMIT = Math.max(100, Math.min(Number(process.env.AI_FINETUNE_EXPORT_LIMIT || 1500), 20000));

const clip = (value, max = 420) => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 1)).trimEnd()}...`;
};

const promptFromRow = (row) => {
  const parts = [];
  if (row.poi_name) parts.push(`Plan a London stop around ${row.poi_name}`);
  if (row.tags) parts.push(`Focus on ${row.tags}`);
  if (row.poi_category) parts.push(`Prefer ${row.poi_category}`);
  return parts.length ? `${parts.join(". ")}.` : "Plan a practical London stop.";
};

const assistantFromRow = (row) => {
  const bullets = [];
  if (row.title) bullets.push(`Anchor: ${row.title}.`);
  if (row.content) bullets.push(`Community note: ${clip(row.content, 260)}`);
  if (row.poi_name) bullets.push(`Place: ${row.poi_name}${row.poi_category ? ` (${row.poi_category})` : ""}.`);
  if (row.tags) bullets.push(`Tags: ${row.tags}.`);
  return bullets.join(" ");
};

const toIdList = (rows) =>
  Array.from(new Set((rows || []).map((row) => Number(row.id)).filter((value) => Number.isFinite(value) && value > 0)));

const attachTags = async (rows) => {
  const ids = toIdList(rows);
  if (!ids.length) return rows || [];
  const placeholders = ids.map(() => "?").join(", ");
  const [tagRows] = await pool.query(
    `
      SELECT
        pt.post_id,
        GROUP_CONCAT(DISTINCT t.name ORDER BY t.name SEPARATOR ', ') AS tags
      FROM post_tags pt
      INNER JOIN tags t ON t.id = pt.tag_id
      WHERE pt.post_id IN (${placeholders})
      GROUP BY pt.post_id
    `,
    ids
  );
  const tagMap = new Map((tagRows || []).map((row) => [Number(row.post_id), row.tags || ""]));
  return (rows || []).map((row) => ({
    ...row,
    tags: tagMap.get(Number(row.id)) || "",
  }));
};

async function main() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const [baseRows] = await pool.query(
    `
      SELECT
        p.id,
        p.title,
        p.content,
        p.like_count,
        p.favorite_count,
        p.view_count,
        poi.name AS poi_name,
        poi.category AS poi_category
      FROM posts p
      LEFT JOIN poi ON poi.id = p.poi_id
      WHERE COALESCE(p.status, 'NORMAL') = 'NORMAL'
      ORDER BY
        (COALESCE(p.like_count, 0) * 3 + COALESCE(p.favorite_count, 0) * 4 + COALESCE(p.view_count, 0) * 0.02) DESC,
        p.id DESC
      LIMIT ?
    `,
    [ROW_LIMIT]
  );
  const rows = await attachTags(baseRows);

  const lines = (rows || []).map((row) =>
    JSON.stringify({
      messages: [
        {
          role: "system",
          content:
            "You are JourneyPro AI, a London-only travel planning assistant. Answer with practical, route-aware, community-grounded recommendations.",
        },
        {
          role: "user",
          content: promptFromRow(row),
        },
        {
          role: "assistant",
          content: assistantFromRow(row),
        },
      ],
      metadata: {
        post_id: row.id,
        poi_name: row.poi_name || "",
        poi_category: row.poi_category || "",
      },
    })
  );

  fs.writeFileSync(OUTPUT_FILE, `${lines.join("\n")}\n`, "utf8");
  console.log(JSON.stringify({ success: true, output: OUTPUT_FILE, rows: lines.length }, null, 2));
  await pool.end();
}

main().catch(async (err) => {
  console.error(err);
  try {
    await pool.end();
  } catch {
    // ignore
  }
  process.exit(1);
});
