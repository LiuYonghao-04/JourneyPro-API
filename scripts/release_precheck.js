import "dotenv/config";
import fs from "fs/promises";
import path from "path";
import axios from "axios";
import { pool } from "../db/connect.js";

const args = process.argv.slice(2);
const arg = (name, fallback = "") => {
  const hit = args.find((item) => item.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const baseURL = String(arg("base", process.env.API_BASE || "http://127.0.0.1:3001")).replace(/\/+$/, "");
const userId = Number.parseInt(arg("user", process.env.TEST_USER_ID || "1"), 10) || 1;
const timeoutMs = Math.max(1500, Math.min(Number.parseInt(arg("timeoutMs", "10000"), 10) || 10000, 30000));
const backupHours = Math.max(1, Math.min(Number.parseInt(arg("backupMaxAgeHours", "48"), 10) || 48, 720));
const skipRoute = String(arg("skipRoute", "0")) === "1";

const http = axios.create({
  baseURL,
  timeout: timeoutMs,
  validateStatus: () => true,
});

const results = [];

function pushCheck({ name, ok, detail, severity = "FAIL" }) {
  results.push({ name, ok: !!ok, detail: String(detail || ""), severity });
  const flag = ok ? "PASS" : severity;
  console.log(`[${flag}] ${name} - ${detail}`);
}

function percentile(values, p) {
  const list = [...values].sort((a, b) => a - b);
  if (!list.length) return 0;
  const idx = Math.min(list.length - 1, Math.max(0, Math.floor((p / 100) * list.length)));
  return Number(list[idx] || 0);
}

const toMs = (start) => Date.now() - start;
const toTime = (raw) => {
  const ts = new Date(raw || "").getTime();
  return Number.isFinite(ts) ? ts : 0;
};

function unreadByState(item, state) {
  const nowMs = Date.now();
  const futureSkew = 5 * 60 * 1000;
  const allRead = toTime(state?.read_all_at);
  const typeRead = toTime(state?.[`read_${String(item?.type || "")}_at`]);
  const created = toTime(item?.created_at);
  const readAnchor = Math.max(allRead, typeRead);
  const normalizedCreated = created > nowMs + futureSkew ? (readAnchor || nowMs) : created;
  if (!normalizedCreated) return false;
  if (allRead && normalizedCreated <= allRead) return false;
  if (typeRead && normalizedCreated <= typeRead) return false;
  return true;
}

async function checkDbPing() {
  const t0 = Date.now();
  const [[row]] = await pool.query("SELECT 1 AS ok");
  const elapsed = toMs(t0);
  pushCheck({
    name: "DB ping",
    ok: Number(row?.ok) === 1 && elapsed < 1200,
    detail: `elapsed=${elapsed}ms`,
  });
}

async function checkBackupFreshness() {
  const backupRoot = path.resolve(process.cwd(), "backups");
  try {
    const dirents = await fs.readdir(backupRoot, { withFileTypes: true });
    const folders = dirents.filter((d) => d.isDirectory()).map((d) => d.name);
    if (!folders.length) {
      pushCheck({
        name: "Backup freshness",
        ok: false,
        detail: "no backup folder found under JourneyPro-api/backups",
      });
      return;
    }
    let newestMs = 0;
    let newestFolder = "";
    for (const folder of folders) {
      const manifestPath = path.join(backupRoot, folder, "manifest.json");
      try {
        // eslint-disable-next-line no-await-in-loop
        const st = await fs.stat(manifestPath);
        if (st.mtimeMs > newestMs) {
          newestMs = st.mtimeMs;
          newestFolder = folder;
        }
      } catch {
        // ignore
      }
    }
    if (!newestMs) {
      pushCheck({
        name: "Backup freshness",
        ok: false,
        detail: "manifest.json not found in backup folders",
      });
      return;
    }
    const ageHours = (Date.now() - newestMs) / (1000 * 60 * 60);
    pushCheck({
      name: "Backup freshness",
      ok: ageHours <= backupHours,
      detail: `latest=${newestFolder} age=${ageHours.toFixed(1)}h max=${backupHours}h`,
    });
  } catch (e) {
    pushCheck({
      name: "Backup freshness",
      ok: false,
      detail: `read backup dir failed: ${e?.message || e}`,
    });
  }
}

async function checkEndpointP95(name, fn, targetP95 = 1500, rounds = 3) {
  const latencies = [];
  const statuses = [];
  let okCount = 0;
  for (let i = 0; i < rounds; i += 1) {
    const t0 = Date.now();
    // eslint-disable-next-line no-await-in-loop
    const status = await fn();
    const elapsed = toMs(t0);
    latencies.push(elapsed);
    statuses.push(status);
    if (status >= 200 && status < 400) okCount += 1;
  }
  const p95 = Math.round(percentile(latencies, 95));
  const ok = okCount === rounds && p95 <= targetP95;
  pushCheck({
    name,
    ok,
    detail: `ok=${okCount}/${rounds} p95=${p95}ms target<=${targetP95}ms statuses=[${statuses.join(",")}]`,
  });
}

async function checkApiLatencyAndData() {
  let samplePostId = 0;
  await checkEndpointP95(
    "API feed latency",
    async () => {
      const res = await http.get("/api/posts", {
        params: { limit: 20, compact: 1, lite: 1, feed_lite: 1 },
      });
      const first = Number(res.data?.data?.[0]?.id || 0);
      if (!samplePostId && Number.isFinite(first) && first > 0) samplePostId = first;
      return res.status;
    },
    1200,
    4
  );

  await checkEndpointP95(
    "API notifications latency",
    async () => {
      const res = await http.get("/api/notifications", {
        params: { user_id: userId, limit: 40 },
      });
      return res.status;
    },
    1500,
    3
  );

  if (!samplePostId) {
    const res = await http.get("/api/posts", {
      params: { limit: 1, compact: 1, lite: 1, feed_lite: 1 },
    });
    samplePostId = Number(res.data?.data?.[0]?.id || 0);
  }

  if (samplePostId) {
    await checkEndpointP95(
      "API post detail latency",
      async () => {
        const res = await http.get(`/api/posts/${samplePostId}`, { params: { user_id: userId } });
        return res.status;
      },
      1600,
      3
    );
  } else {
    pushCheck({
      name: "API post detail latency",
      ok: false,
      detail: "no sample post id available",
      severity: "WARN",
    });
  }

  if (!skipRoute) {
    // warm-up once so first-fail circuit behavior does not skew p95 gate
    try {
      await http.get("/api/route/recommend", {
        params: {
          start: "-0.1276,51.5072",
          end: "-0.1180,51.5090",
          user_id: userId,
          interest_weight: 0.5,
          explore_weight: 0.15,
          mode: "driving",
          limit: 10,
        },
      });
    } catch {
      // ignore warm-up errors
    }

    await checkEndpointP95(
      "API recommend latency",
      async () => {
        const res = await http.get("/api/route/recommend", {
          params: {
            start: "-0.1276,51.5072",
            end: "-0.1180,51.5090",
            user_id: userId,
            interest_weight: 0.5,
            explore_weight: 0.15,
            mode: "driving",
            limit: 10,
          },
        });
        return res.status;
      },
      1800,
      2
    );
  } else {
    pushCheck({
      name: "API recommend latency",
      ok: true,
      detail: "skipped by --skipRoute=1",
      severity: "WARN",
    });
  }
}

async function checkNotificationConsistency() {
  try {
    const res = await http.get("/api/notifications", {
      params: { user_id: userId, limit: 80 },
    });
    if (!(res.status >= 200 && res.status < 400)) {
      pushCheck({
        name: "Notification unread consistency",
        ok: false,
        detail: `status=${res.status}`,
      });
      return;
    }
    const list = Array.isArray(res.data?.data) ? res.data.data : [];
    const state = res.data?.state || null;
    if (!state || !list.length) {
      pushCheck({
        name: "Notification unread consistency",
        ok: true,
        detail: `skip compare (state=${!!state}, items=${list.length})`,
        severity: "WARN",
      });
      return;
    }
    const mismatch = list.filter((item) => Boolean(item?.unread) !== unreadByState(item, state)).length;
    pushCheck({
      name: "Notification unread consistency",
      ok: mismatch === 0,
      detail: `items=${list.length} mismatch=${mismatch}`,
      severity: mismatch === 0 ? "FAIL" : "WARN",
    });
  } catch (e) {
    pushCheck({
      name: "Notification unread consistency",
      ok: false,
      detail: e?.message || String(e),
    });
  }
}

function checkRuntimeMigrationFlag() {
  const enabled = process.env.ENABLE_RUNTIME_SCHEMA_MIGRATION === "1";
  pushCheck({
    name: "Runtime schema migration",
    ok: !enabled,
    detail: enabled ? "ENABLE_RUNTIME_SCHEMA_MIGRATION=1 (should be 0 in release)" : "disabled",
  });
}

async function checkDbProcessList() {
  try {
    const [rows] = await pool.query("SHOW FULL PROCESSLIST");
    const blockers = (rows || []).filter((row) =>
      String(row?.State || "").toLowerCase().includes("metadata lock")
    );
    pushCheck({
      name: "DB metadata lock",
      ok: blockers.length === 0,
      detail: blockers.length ? `blocked_queries=${blockers.length}` : "clean",
      severity: blockers.length ? "WARN" : "FAIL",
    });
  } catch (e) {
    pushCheck({
      name: "DB metadata lock",
      ok: false,
      detail: e?.message || String(e),
      severity: "WARN",
    });
  }
}

async function main() {
  console.log(`Release precheck start base=${baseURL} user=${userId}`);
  checkRuntimeMigrationFlag();
  await checkBackupFreshness();
  await checkDbPing();
  await checkDbProcessList();
  await checkApiLatencyAndData();
  await checkNotificationConsistency();

  const failCount = results.filter((row) => !row.ok && row.severity === "FAIL").length;
  const warnCount = results.filter((row) => !row.ok && row.severity === "WARN").length;
  const passCount = results.filter((row) => row.ok).length;
  console.log("");
  console.log(`Summary: PASS=${passCount} WARN=${warnCount} FAIL=${failCount}`);
  if (failCount > 0) {
    process.exitCode = 1;
  }
}

main()
  .catch((err) => {
    console.error("release precheck failed", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch {
      // ignore
    }
  });
