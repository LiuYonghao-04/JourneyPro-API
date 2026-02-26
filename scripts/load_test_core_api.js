import "dotenv/config";
import axios from "axios";

const args = process.argv.slice(2);
const arg = (name, fallback = "") => {
  const hit = args.find((item) => item.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const baseURL = String(arg("base", process.env.API_BASE || "http://127.0.0.1:3001")).replace(/\/+$/, "");
const userId = Number.parseInt(arg("user", process.env.TEST_USER_ID || "1"), 10) || 1;
const durationSec = Math.max(10, Math.min(Number.parseInt(arg("duration", "30"), 10) || 30, 600));
const concurrency = Math.max(1, Math.min(Number.parseInt(arg("concurrency", "8"), 10) || 8, 64));
const skipRoute = String(arg("skipRoute", "0")) === "1";
const timeoutMs = Math.max(1500, Math.min(Number.parseInt(arg("timeoutMs", "8000"), 10) || 8000, 30000));

const http = axios.create({
  baseURL,
  timeout: timeoutMs,
  validateStatus: () => true,
});

const stats = new Map();
let sampledPostIds = [];

const nowMs = () => Date.now();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function statOf(name) {
  if (!stats.has(name)) {
    stats.set(name, {
      name,
      count: 0,
      ok: 0,
      fail: 0,
      latency: [],
      statusCodes: {},
    });
  }
  return stats.get(name);
}

function record(name, statusCode, elapsedMs) {
  const row = statOf(name);
  row.count += 1;
  row.statusCodes[statusCode] = (row.statusCodes[statusCode] || 0) + 1;
  row.latency.push(elapsedMs);
  if (row.latency.length > 20000) row.latency.shift();
  if (statusCode >= 200 && statusCode < 400) row.ok += 1;
  else row.fail += 1;
}

function percentile(values, p) {
  const list = [...values].sort((a, b) => a - b);
  if (!list.length) return 0;
  const idx = Math.min(list.length - 1, Math.max(0, Math.floor((p / 100) * list.length)));
  return Number(list[idx] || 0);
}

async function warmupPosts() {
  const t0 = nowMs();
  const res = await http.get("/api/posts", {
    params: { limit: 30, compact: 1, lite: 1, feed_lite: 1 },
  });
  const elapsed = nowMs() - t0;
  record("feed", Number(res.status || 0), elapsed);
  if (res.status >= 200 && res.status < 400) {
    sampledPostIds = (res.data?.data || [])
      .map((row) => Number(row?.id))
      .filter((id) => Number.isFinite(id) && id > 0)
      .slice(0, 50);
  }
}

async function hitFeed() {
  const t0 = nowMs();
  const res = await http.get("/api/posts", {
    params: { limit: 20, compact: 1, lite: 1, feed_lite: 1 },
  });
  const elapsed = nowMs() - t0;
  record("feed", Number(res.status || 0), elapsed);
}

async function hitNotifications() {
  const t0 = nowMs();
  const res = await http.get("/api/notifications", {
    params: { user_id: userId, limit: 40 },
  });
  const elapsed = nowMs() - t0;
  record("notifications", Number(res.status || 0), elapsed);
}

async function hitPostDetail() {
  if (!sampledPostIds.length) {
    await hitFeed();
    return;
  }
  const id = sampledPostIds[Math.floor(Math.random() * sampledPostIds.length)];
  const t0 = nowMs();
  const res = await http.get(`/api/posts/${id}`, { params: { user_id: userId } });
  const elapsed = nowMs() - t0;
  record("post_detail", Number(res.status || 0), elapsed);
}

async function hitRecommend() {
  const t0 = nowMs();
  const res = await http.get("/api/route/recommend", {
    params: {
      start: "-0.1276,51.5072",
      end: "-0.1180,51.5090",
      user_id: userId,
      interest_weight: 0.5,
      explore_weight: 0.15,
      limit: 10,
      mode: "driving",
    },
  });
  const elapsed = nowMs() - t0;
  record("route_recommend", Number(res.status || 0), elapsed);
}

async function runWorker(deadlineTs) {
  while (nowMs() < deadlineTs) {
    const r = Math.random();
    try {
      if (r < 0.45) {
        // eslint-disable-next-line no-await-in-loop
        await hitFeed();
      } else if (r < 0.7) {
        // eslint-disable-next-line no-await-in-loop
        await hitNotifications();
      } else if (r < 0.9) {
        // eslint-disable-next-line no-await-in-loop
        await hitPostDetail();
      } else if (!skipRoute) {
        // eslint-disable-next-line no-await-in-loop
        await hitRecommend();
      } else {
        // eslint-disable-next-line no-await-in-loop
        await hitPostDetail();
      }
    } catch (e) {
      const name = r < 0.45 ? "feed" : r < 0.7 ? "notifications" : r < 0.9 ? "post_detail" : "route_recommend";
      record(name, 599, timeoutMs);
    }
    // eslint-disable-next-line no-await-in-loop
    await sleep(5);
  }
}

function printReport(startTs) {
  const elapsedSec = ((nowMs() - startTs) / 1000).toFixed(1);
  console.log("");
  console.log(`Core API load test done base=${baseURL} elapsed=${elapsedSec}s concurrency=${concurrency}`);
  console.log("-----------------------------------------------------------------------");
  console.log("endpoint           count   ok     fail   p50(ms)  p95(ms)  p99(ms)  ok%");
  console.log("-----------------------------------------------------------------------");
  for (const row of stats.values()) {
    const okRate = row.count ? ((row.ok / row.count) * 100).toFixed(1) : "0.0";
    const p50 = Math.round(percentile(row.latency, 50));
    const p95 = Math.round(percentile(row.latency, 95));
    const p99 = Math.round(percentile(row.latency, 99));
    const name = String(row.name).padEnd(17, " ");
    const line = `${name} ${String(row.count).padStart(6, " ")} ${String(row.ok).padStart(6, " ")} ${String(row.fail).padStart(6, " ")} ${String(p50).padStart(8, " ")} ${String(p95).padStart(8, " ")} ${String(p99).padStart(8, " ")} ${okRate.padStart(5, " ")}%`;
    console.log(line);
  }
}

async function main() {
  console.log(
    `Start core API load test base=${baseURL} user=${userId} duration=${durationSec}s concurrency=${concurrency} skipRoute=${skipRoute}`
  );
  await warmupPosts();
  const startTs = nowMs();
  const deadlineTs = startTs + durationSec * 1000;
  await Promise.all(Array.from({ length: concurrency }).map(() => runWorker(deadlineTs)));
  printReport(startTs);
}

main().catch((err) => {
  console.error("load test failed", err);
  process.exitCode = 1;
});
