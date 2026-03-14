import "dotenv/config";
import fs from "fs/promises";
import path from "path";
import { spawn } from "child_process";
import {
  backupRoot,
  collectDistributions,
  connectDb,
  createFastLiveBackup,
  createTag,
  restoreFromLiveBackup,
  writeManifest,
} from "./social_rebalance_common.js";

function runSeedProcess(extraEnv) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["scripts/seed_social_interactions.js"], {
      cwd: process.cwd(),
      stdio: "inherit",
      env: {
        ...process.env,
        ...extraEnv,
      },
      windowsHide: true,
    });
    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`seed_social_interactions exit=${code}`));
    });
  });
}

async function main() {
  const tag = createTag();
  const dir = backupRoot(`fast_${tag}`);
  const conn = await connectDb();
  const envPlan = {
    SOCIAL_RESET_TABLES: "0",
    SOCIAL_SYNC_COUNTERS: "1",
    SOCIAL_TARGET_USERS: process.env.SOCIAL_TARGET_USERS || "420",
    SOCIAL_POST_LIMIT: process.env.SOCIAL_POST_LIMIT || "700000",
    SOCIAL_POST_BATCH: process.env.SOCIAL_POST_BATCH || "3200",
    SOCIAL_EVENT_INSERT_BATCH: process.env.SOCIAL_EVENT_INSERT_BATCH || "8000",
    SOCIAL_FOLLOW_INSERT_BATCH: process.env.SOCIAL_FOLLOW_INSERT_BATCH || "2200",
    SOCIAL_LIKE_MIN: process.env.SOCIAL_LIKE_MIN || "0",
    SOCIAL_LIKE_MAX: process.env.SOCIAL_LIKE_MAX || "28",
    SOCIAL_FAV_MIN: process.env.SOCIAL_FAV_MIN || "0",
    SOCIAL_FAV_MAX: process.env.SOCIAL_FAV_MAX || "14",
    SOCIAL_FOLLOW_MIN: process.env.SOCIAL_FOLLOW_MIN || "8",
    SOCIAL_FOLLOW_MAX: process.env.SOCIAL_FOLLOW_MAX || "110",
    SOCIAL_EVENT_LOOKBACK_DAYS: process.env.SOCIAL_EVENT_LOOKBACK_DAYS || "75",
    SOCIAL_RECENT_BOOST_HOURS: process.env.SOCIAL_RECENT_BOOST_HOURS || "48",
  };

  try {
    const before = await collectDistributions(conn);
    const liveBackup = await createFastLiveBackup(conn, tag);
    const manifest = {
      tag: `fast_${tag}`,
      generated_at: new Date().toISOString(),
      phase: "working_copy_created",
      live_backup_tables: liveBackup,
      seed_env: envPlan,
      before,
    };
    await writeManifest(dir, manifest);
    await conn.end();

    await runSeedProcess(envPlan);

    const verifyConn = await connectDb();
    try {
      const after = await collectDistributions(verifyConn);
      await writeManifest(dir, {
        ...manifest,
        phase: "completed",
        completed_at: new Date().toISOString(),
        after,
      });
      console.log(JSON.stringify({ tag: `fast_${tag}`, before, after }, null, 2));
    } finally {
      await verifyConn.end();
    }
  } catch (err) {
    console.error("[social-rebalance-fast] failed", err);
    try {
      const restoreConn = await connectDb();
      try {
        const manifestRaw = await fs.readFile(path.join(dir, "manifest.json"), "utf8");
        const manifest = JSON.parse(manifestRaw);
        const failedTables = await restoreFromLiveBackup(restoreConn, manifest.live_backup_tables || {}, createTag());
        const restored = await collectDistributions(restoreConn);
        await writeManifest(dir, {
          ...manifest,
          phase: "restored_after_failure",
          restored_at: new Date().toISOString(),
          restored,
          failed_tables: failedTables,
          failure: String(err?.message || err),
        });
      } finally {
        await restoreConn.end();
      }
    } catch (restoreErr) {
      console.error("[social-rebalance-fast] restore failed", restoreErr);
    }
    process.exit(1);
  } finally {
    try {
      await conn.end();
    } catch {
      // ignore
    }
  }
}

main().catch((err) => {
  console.error("[social-rebalance-fast] fatal", err);
  process.exit(1);
});
