import "dotenv/config";
import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CFG = {
  likesTable: String(process.env.SOCIAL_LIKES_TABLE || "post_likes").trim(),
  favoritesTable: String(process.env.SOCIAL_FAVORITES_TABLE || "post_favorites").trim(),
  postsTable: String(process.env.SOCIAL_POSTS_TABLE || "posts").trim(),
  postBatch: Math.max(200, Math.min(5000, Number(process.env.SOCIAL_ALIGN_POST_BATCH || 1200))),
};

const qTable = (value) => `\`${String(value || "").replace(/`/g, "``")}\``;

async function deleteExtraRows(conn, tableName, ids, countColumn) {
  if (!ids.length) return 0;
  const [res] = await conn.query(
    `
      DELETE target
      FROM ${qTable(tableName)} target
      JOIN (
        SELECT id
        FROM (
          SELECT
            t.id,
            ROW_NUMBER() OVER (
              PARTITION BY t.post_id
              ORDER BY t.created_at DESC, t.user_id DESC, t.id DESC
            ) AS rn,
            GREATEST(COALESCE(p.${countColumn}, 0), 0) AS keep_cnt
          FROM ${qTable(tableName)} t
          INNER JOIN ${qTable(CFG.postsTable)} p ON p.id = t.post_id
          WHERE t.post_id IN (?)
        ) ranked
        WHERE ranked.rn > ranked.keep_cnt
      ) doomed ON doomed.id = target.id
    `,
    [ids]
  );
  return Number(res?.affectedRows || 0);
}

async function fetchSummary(conn) {
  const [[row]] = await conn.query(
    `
      SELECT
        (SELECT COUNT(*) FROM ${qTable(CFG.likesTable)}) AS likes_rows,
        (SELECT COUNT(*) FROM ${qTable(CFG.favoritesTable)}) AS favorites_rows
    `
  );
  return {
    likes_rows: Number(row?.likes_rows || 0),
    favorites_rows: Number(row?.favorites_rows || 0),
  };
}

async function main() {
  const pool = mysql.createPool({ ...DB, connectionLimit: 8, waitForConnections: true });
  const conn = await pool.getConnection();
  try {
    let cursorId = 0;
    let processed = 0;
    let likesDeleted = 0;
    let favoritesDeleted = 0;

    const before = await fetchSummary(conn);

    while (true) {
      const [rows] = await conn.query(
        `
          SELECT id
          FROM ${qTable(CFG.postsTable)}
          WHERE id > ?
          ORDER BY id ASC
          LIMIT ?
        `,
        [cursorId, CFG.postBatch]
      );
      if (!rows.length) break;

      const ids = rows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
      cursorId = ids[ids.length - 1];

      // eslint-disable-next-line no-await-in-loop
      likesDeleted += await deleteExtraRows(conn, CFG.likesTable, ids, "like_count");
      // eslint-disable-next-line no-await-in-loop
      favoritesDeleted += await deleteExtraRows(conn, CFG.favoritesTable, ids, "favorite_count");

      processed += ids.length;
      if (processed % 12000 === 0) {
        console.log(
          `align progress posts=${processed} likes_deleted=${likesDeleted} favorites_deleted=${favoritesDeleted}`
        );
      }
    }

    const after = await fetchSummary(conn);
    console.log(
      JSON.stringify(
        {
          success: true,
          config: CFG,
          before,
          processed_posts: processed,
          likes_deleted: likesDeleted,
          favorites_deleted: favoritesDeleted,
          after,
        },
        null,
        2
      )
    );
  } finally {
    conn.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
