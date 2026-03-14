import "dotenv/config";
import { backupRoot, connectDb, readManifestByTag, swapRestoreFromBackup } from "./social_rebalance_common.js";

const args = process.argv.slice(2);
const arg = (name, fallback = "") => {
  const hit = args.find((item) => item.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

async function main() {
  const tag = String(arg("tag", "")).trim();
  if (!tag) {
    throw new Error("missing --tag=YYYYMMDD_HHMMSS");
  }
  const manifest = await readManifestByTag(tag);
  const conn = await connectDb();
  try {
    console.log(`[social-restore] restoring from ${backupRoot(tag)}`);
    const failedTables = await swapRestoreFromBackup(conn, manifest.backup_tables || {}, tag);
    console.log(JSON.stringify({ restored_from: manifest.backup_tables || {}, displaced_tables: failedTables }, null, 2));
    console.log("[social-restore] restore complete");
  } finally {
    await conn.end();
  }
}

main().catch((err) => {
  console.error("[social-restore] failed", err);
  process.exit(1);
});
