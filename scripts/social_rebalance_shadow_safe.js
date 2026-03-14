import "dotenv/config";
import fs from "fs/promises";
import path from "path";
import { spawn } from "child_process";
import {
  backupRoot,
  collectDistributions,
  connectDb,
  createShadowTables,
  createTag,
  restoreFromLiveBackup,
  swapInShadowTables,
  syncAllPostCounters,
  writeManifest,
} from "./social_rebalance_common.js";

const TABLES = ["post_likes", "post_favorites", "user_follows"];
const WAIT_MS = Math.max(60_000, Number(process.env.SOCIAL_SWAP_WAIT_MS || 3 * 60 * 60 * 1000));
const POLL_MS = Math.max(5_000, Number(process.env.SOCIAL_SWAP_POLL_MS || 30_000));

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

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchLiveBlockers(conn) {
  const [trxRows] = await conn.query(`
    SELECT
      trx_mysql_thread_id AS thread_id,
      trx_state,
      TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS age_s,
      trx_query
    FROM information_schema.innodb_trx
    WHERE trx_query IS NOT NULL
      AND (
        trx_query LIKE '%post_likes%'
        OR trx_query LIKE '%post_favorites%'
        OR trx_query LIKE '%user_follows%'
      )
  `);

  const [processRows] = await conn.query(`SHOW FULL PROCESSLIST`);
  const ddlRows = processRows
    .filter((row) => {
      const info = String(row.Info || "");
      const cmd = String(row.Command || "");
      if (!info) return false;
      if (!TABLES.some((name) => info.includes(name))) return false;
      if (cmd === "Sleep") return false;
      if (!/(RENAME TABLE|ALTER TABLE|TRUNCATE TABLE|INSERT INTO|DELETE FROM|UPDATE )/i.test(info)) return false;
      return true;
    })
    .map((row) => ({
      thread_id: row.Id,
      command: row.Command,
      time_s: row.Time,
      state: row.State,
      info: String(row.Info || "").slice(0, 240),
    }));

  return {
    trx: trxRows.map((row) => ({
      thread_id: row.thread_id,
      state: row.trx_state,
      age_s: row.age_s,
      query: String(row.trx_query || "").slice(0, 240),
    })),
    ddl: ddlRows,
  };
}

async function waitForLiveBlockersToClear(conn, timeoutMs) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const blockers = await fetchLiveBlockers(conn);
    if (!blockers.trx.length && !blockers.ddl.length) {
      return blockers;
    }
    console.log("[social-rebalance-shadow] waiting for live table blockers to clear");
    console.log(JSON.stringify(blockers, null, 2));
    await sleep(POLL_MS);
  }
  const blockers = await fetchLiveBlockers(conn);
  throw new Error(`live table blockers did not clear within ${timeoutMs}ms: ${JSON.stringify(blockers)}`);
}

async function writeFailureManifest(dir, manifest, extra) {
  await writeManifest(dir, {
    ...manifest,
    ...extra,
    failed_at: new Date().toISOString(),
  });
}

async function main() {
  const tag = createTag();
  const dir = backupRoot(`shadow_${tag}`);
  const conn = await connectDb();
  const envPlan = {
    SOCIAL_RESET_TABLES: "0",
    SOCIAL_SYNC_COUNTERS: "0",
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

  let manifest = null;
  let shadowTables = null;
  let liveBackupTables = null;
  let swapped = false;

  try {
    const before = await collectDistributions(conn);
    shadowTables = await createShadowTables(conn, tag);
    manifest = {
      tag: `shadow_${tag}`,
      generated_at: new Date().toISOString(),
      phase: "shadow_created",
      shadow_tables: shadowTables,
      seed_env: {
        ...envPlan,
        SOCIAL_LIKES_TABLE: shadowTables.post_likes,
        SOCIAL_FAVORITES_TABLE: shadowTables.post_favorites,
        SOCIAL_FOLLOWS_TABLE: shadowTables.user_follows,
      },
      before,
    };
    await writeManifest(dir, manifest);
    await conn.end();

    await runSeedProcess({
      ...envPlan,
      SOCIAL_LIKES_TABLE: shadowTables.post_likes,
      SOCIAL_FAVORITES_TABLE: shadowTables.post_favorites,
      SOCIAL_FOLLOWS_TABLE: shadowTables.user_follows,
    });

    const verifyConn = await connectDb();
    try {
      const shadow = await collectDistributions(verifyConn, shadowTables);
      manifest = {
        ...manifest,
        phase: "shadow_ready",
        shadow_completed_at: new Date().toISOString(),
        shadow,
      };
      await writeManifest(dir, manifest);

      await waitForLiveBlockersToClear(verifyConn, WAIT_MS);

      liveBackupTables = await swapInShadowTables(verifyConn, shadowTables, tag);
      swapped = true;
      await syncAllPostCounters(verifyConn);
      const after = await collectDistributions(verifyConn);

      manifest = {
        ...manifest,
        phase: "completed",
        completed_at: new Date().toISOString(),
        live_backup_tables: liveBackupTables,
        after,
      };
      await writeManifest(dir, manifest);
      console.log(JSON.stringify({ tag: `shadow_${tag}`, before, shadow, after }, null, 2));
    } finally {
      await verifyConn.end();
    }
  } catch (err) {
    console.error("[social-rebalance-shadow] failed", err);
    try {
      const restoreConn = await connectDb();
      try {
        if (swapped && liveBackupTables) {
          const failedTables = await restoreFromLiveBackup(restoreConn, liveBackupTables, createTag());
          await syncAllPostCounters(restoreConn);
          const restored = await collectDistributions(restoreConn);
          await writeFailureManifest(dir, manifest || {}, {
            phase: "restored_after_failure",
            restored,
            live_backup_tables: liveBackupTables,
            failed_tables: failedTables,
            failure: String(err?.message || err),
          });
        } else {
          await writeFailureManifest(dir, manifest || {}, {
            phase: "failed_shadow_preserved",
            shadow_tables: shadowTables,
            failure: String(err?.message || err),
          });
        }
      } finally {
        await restoreConn.end();
      }
    } catch (restoreErr) {
      console.error("[social-rebalance-shadow] restore failed", restoreErr);
      await writeFailureManifest(dir, manifest || {}, {
        phase: "restore_failed",
        shadow_tables: shadowTables,
        live_backup_tables: liveBackupTables,
        failure: String(err?.message || err),
        restore_failure: String(restoreErr?.message || restoreErr),
      });
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
  console.error("[social-rebalance-shadow] fatal", err);
  process.exit(1);
});
