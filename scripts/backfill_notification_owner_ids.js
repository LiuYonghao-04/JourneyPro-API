import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const BATCH_SIZE = Math.max(1000, Math.min(200000, Number(process.env.OWNER_BACKFILL_BATCH || 60000)));
const MAX_SECONDS = Math.max(60, Number(process.env.OWNER_BACKFILL_MAX_SECONDS || 1800));
const ONLY_TABLE = String(process.env.OWNER_BACKFILL_TABLE || "").trim().toLowerCase();

const JOBS = [
  { table: "post_likes", alias: "pl" },
  { table: "post_favorites", alias: "pf" },
  { table: "post_comments", alias: "pc" },
];

function canRetry(err) {
  const msg = String(err?.message || err);
  return msg.includes("Lock wait timeout") || msg.includes("Deadlock found");
}

async function ensureColumnsAndIndexes(conn) {
  const sqls = [
    `ALTER TABLE post_likes ADD COLUMN post_owner_id BIGINT NULL`,
    `ALTER TABLE post_favorites ADD COLUMN post_owner_id BIGINT NULL`,
    `ALTER TABLE post_comments ADD COLUMN post_owner_id BIGINT NULL`,
    `ALTER TABLE post_likes ADD INDEX idx_post_likes_owner_created (post_owner_id, created_at, post_id, user_id)`,
    `ALTER TABLE post_favorites ADD INDEX idx_post_favorites_owner_created (post_owner_id, created_at, post_id, user_id)`,
    `ALTER TABLE post_comments ADD INDEX idx_post_comments_owner_created (post_owner_id, created_at, post_id, user_id)`,
  ];
  for (const sql of sqls) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await conn.query(sql);
    } catch (err) {
      const msg = String(err?.message || err);
      if (!msg.includes("Duplicate") && !msg.includes("check that column/key exists")) {
        throw err;
      }
    }
  }
}

async function hasNullRows(conn, table) {
  const [rows] = await conn.query(`SELECT id FROM ${table} WHERE post_owner_id IS NULL ORDER BY id ASC LIMIT 1`);
  return rows.length > 0;
}

async function runBackfill(conn, job, startedAt) {
  let updated = 0;
  let retries = 0;
  let rounds = 0;

  while (true) {
    const elapsed = (Date.now() - startedAt) / 1000;
    if (elapsed >= MAX_SECONDS) {
      return { updated, retries, rounds, timedOut: true, nullLeft: await hasNullRows(conn, job.table) };
    }

    // Fetch a small contiguous id range with null owner first.
    // This keeps each UPDATE bounded and resumable across runs.
    const [idRows] = await conn.query(
      `SELECT id FROM ${job.table} WHERE post_owner_id IS NULL ORDER BY id ASC LIMIT ?`,
      [BATCH_SIZE]
    );
    if (!idRows.length) {
      return { updated, retries, rounds, timedOut: false, nullLeft: false };
    }

    const minId = Number(idRows[0].id);
    const maxId = Number(idRows[idRows.length - 1].id);
    let done = false;
    let attempt = 0;
    while (!done && attempt < 5) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const [res] = await conn.query(
          `
            UPDATE ${job.table} ${job.alias}
            JOIN posts p ON p.id = ${job.alias}.post_id
            SET ${job.alias}.post_owner_id = p.user_id
            WHERE ${job.alias}.post_owner_id IS NULL
              AND ${job.alias}.id >= ?
              AND ${job.alias}.id <= ?
          `,
          [minId, maxId]
        );
        updated += Number(res?.affectedRows || 0);
        rounds += 1;
        done = true;
      } catch (err) {
        if (!canRetry(err)) throw err;
        retries += 1;
        attempt += 1;
        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setTimeout(resolve, 800 * attempt));
      }
    }

    if (rounds % 10 === 0) {
      console.log(
        `progress table=${job.table} rounds=${rounds} updated=${updated} retries=${retries} last_range=${minId}-${maxId}`
      );
    }
  }
}

async function main() {
  const conn = await mysql.createConnection({ ...DB });
  try {
    await ensureColumnsAndIndexes(conn);
    const startedAt = Date.now();
    const jobs = ONLY_TABLE ? JOBS.filter((j) => j.table === ONLY_TABLE) : JOBS;
    if (!jobs.length) {
      throw new Error(`Invalid OWNER_BACKFILL_TABLE: ${ONLY_TABLE}`);
    }

    const result = {};
    for (const job of jobs) {
      // eslint-disable-next-line no-await-in-loop
      result[job.table] = await runBackfill(conn, job, startedAt);
      if (((Date.now() - startedAt) / 1000) >= MAX_SECONDS) break;
    }

    console.log(
      JSON.stringify(
        {
          success: true,
          config: {
            batch_size: BATCH_SIZE,
            max_seconds: MAX_SECONDS,
            only_table: ONLY_TABLE || null,
          },
          result,
        },
        null,
        2
      )
    );
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
