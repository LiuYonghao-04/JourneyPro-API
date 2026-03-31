import "dotenv/config";
import mysql from "mysql2/promise";
import { DB, qTable, readManifestByTag } from "./social_rebalance_common.js";

const CFG = {
  batchSize: Math.max(500, Math.min(10000, Number(process.env.POST_COUNT_REBALANCE_BATCH || 4000))),
};

function readTagArg() {
  const arg = process.argv.slice(2).find((item) => item.startsWith("--tag="));
  if (!arg) {
    throw new Error("missing --tag=<manifest-tag>");
  }
  return arg.slice("--tag=".length);
}

async function main() {
  const tag = readTagArg();
  const manifest = await readManifestByTag(tag);
  const backupTable = manifest?.tables?.backupTable;
  if (!backupTable) {
    throw new Error(`backup table missing in manifest for ${tag}`);
  }
  const conn = await mysql.createConnection(DB);
  try {
    let processed = 0;
    let lastId = 0;
    while (true) {
      const [rows] = await conn.query(
        `
          SELECT post_id
          FROM ${qTable(backupTable)}
          WHERE post_id > ?
          ORDER BY post_id ASC
          LIMIT ?
        `,
        [lastId, CFG.batchSize]
      );
      if (!rows.length) break;
      const ids = rows.map((row) => Number(row.post_id));
      const placeholders = ids.map(() => "?").join(",");
      await conn.query(
        `
          UPDATE posts p
          JOIN ${qTable(backupTable)} b ON b.post_id = p.id
          SET
            p.like_count = b.old_like_count,
            p.favorite_count = b.old_favorite_count
          WHERE p.id IN (${placeholders})
        `,
        ids
      );
      processed += ids.length;
      lastId = ids[ids.length - 1];
      console.log(`restored ${processed}`);
    }
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
