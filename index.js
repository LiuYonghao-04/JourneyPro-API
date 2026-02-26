import "dotenv/config";
import express from "express";
import cors from "cors";
import compression from "compression";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";
import { pool } from "./db/connect.js";

import routeRouter from "./routes/route.js";
import replanRouter from "./routes/replan.js";
import authRouter from "./routes/auth.js";
import postsRouter from "./routes/posts.js";
import seedRouter from "./routes/seed.js";
import poiRouter from "./routes/poi.js";
import followRouter from "./routes/follow.js";
import notificationsRouter from "./routes/notifications.js";
import chatRouter from "./routes/chat.js";
import uploadRouter from "./routes/upload.js";
import recommendationRouter from "./routes/recommendation.js";
import recoEventsRouter from "./routes/recoEvents.js";

const app = express();
app.use(cors());
app.use(compression({ threshold: 1024 }));
app.use(express.json({ limit: "1mb" }));

const SLOW_API_MS = Number(process.env.SLOW_API_MS || 800);
const OPS_TOKEN = String(process.env.OPS_METRICS_TOKEN || "").trim();
const METRIC_SAMPLE_LIMIT = Math.max(50, Math.min(Number(process.env.METRIC_SAMPLE_LIMIT || 400), 4000));
const BOOT_TS = Date.now();
const endpointMetrics = new Map();
const totalStatusCounts = new Map();
let slowApiCount = 0;

const percentile = (values, p) => {
  const list = Array.isArray(values) ? [...values].sort((a, b) => a - b) : [];
  if (!list.length) return 0;
  const index = Math.max(0, Math.min(list.length - 1, Math.floor((p / 100) * list.length)));
  return Number(list[index] || 0);
};

const normalizeApiPath = (rawPath) =>
  String(rawPath || "")
    .replace(/\/\d+(?=\/|$)/g, "/:id")
    .replace(/\/[0-9a-f]{16,}(?=\/|$)/gi, "/:id")
    .replace(/postsid=\d+/g, "postsid=:id");

const ensureEndpointMetric = (key) => {
  if (!endpointMetrics.has(key)) {
    endpointMetrics.set(key, {
      key,
      count: 0,
      ok: 0,
      fail: 0,
      slow: 0,
      last_ms: 0,
      max_ms: 0,
      latency_samples: [],
      last_status: 0,
      last_at: null,
    });
  }
  return endpointMetrics.get(key);
};

const trackMetric = ({ method, pathKey, statusCode, elapsedMs }) => {
  const key = `${String(method || "GET").toUpperCase()} ${pathKey}`;
  const stat = ensureEndpointMetric(key);
  stat.count += 1;
  stat.last_ms = elapsedMs;
  stat.last_at = new Date().toISOString();
  stat.last_status = statusCode;
  stat.max_ms = Math.max(stat.max_ms, elapsedMs);
  if (statusCode >= 200 && statusCode < 400) stat.ok += 1;
  else stat.fail += 1;
  if (elapsedMs >= SLOW_API_MS) stat.slow += 1;
  stat.latency_samples.push(elapsedMs);
  if (stat.latency_samples.length > METRIC_SAMPLE_LIMIT) stat.latency_samples.shift();

  totalStatusCounts.set(statusCode, (totalStatusCounts.get(statusCode) || 0) + 1);
  if (elapsedMs >= SLOW_API_MS) slowApiCount += 1;
};

const hasOpsAccess = (req) => {
  if (!OPS_TOKEN) return true;
  const token = String(req.get("x-ops-token") || req.query.token || "").trim();
  return !!token && token === OPS_TOKEN;
};

const opsGuard = (req, res, next) => {
  if (!hasOpsAccess(req)) {
    return res.status(403).json({ success: false, message: "forbidden" });
  }
  return next();
};

async function dbPing(timeoutMs = 1200) {
  return Promise.race([
    pool.query("SELECT 1 AS ok").then(() => ({ ok: true })),
    new Promise((resolve) => setTimeout(() => resolve({ ok: false, reason: "timeout" }), timeoutMs)),
  ]);
}

app.use((req, res, next) => {
  const start = process.hrtime.bigint();
  const requestId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `req_${Date.now()}`;
  res.setHeader("x-request-id", requestId);
  res.on("finish", () => {
    if (!req.originalUrl?.startsWith("/api/")) return;
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    const pathWithoutQuery = String(req.originalUrl || "").split("?")[0];
    const pathKey = normalizeApiPath(pathWithoutQuery);
    trackMetric({
      method: req.method,
      pathKey,
      statusCode: res.statusCode,
      elapsedMs,
    });
    if (elapsedMs >= SLOW_API_MS) {
      console.warn(
        `[slow-api] ${req.method} ${req.originalUrl} ${res.statusCode} ${elapsedMs.toFixed(1)}ms request_id=${requestId}`
      );
    }
  });
  next();
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use("/uploads", express.static(path.join(__dirname, "uploads")));

app.get("/api/ops/health", opsGuard, async (_req, res) => {
  try {
    const ping = await dbPing(1200);
    const mem = process.memoryUsage();
    const status = ping.ok ? "ok" : "degraded";
    res.json({
      success: true,
      status,
      now: new Date().toISOString(),
      uptime_sec: Math.round(process.uptime()),
      process: {
        pid: process.pid,
        node: process.version,
      },
      memory: {
        rss_mb: Math.round((mem.rss / 1024 / 1024) * 10) / 10,
        heap_used_mb: Math.round((mem.heapUsed / 1024 / 1024) * 10) / 10,
        heap_total_mb: Math.round((mem.heapTotal / 1024 / 1024) * 10) / 10,
      },
      db: ping.ok ? { ok: true } : { ok: false, reason: ping.reason || "query_failed" },
      slow_api_threshold_ms: SLOW_API_MS,
    });
  } catch (err) {
    res.status(500).json({
      success: false,
      status: "error",
      message: err?.message || "health_check_failed",
    });
  }
});

app.get("/api/ops/metrics", opsGuard, (_req, res) => {
  const endpoints = [...endpointMetrics.values()]
    .map((row) => {
      const p50 = percentile(row.latency_samples, 50);
      const p95 = percentile(row.latency_samples, 95);
      const p99 = percentile(row.latency_samples, 99);
      return {
        endpoint: row.key,
        count: row.count,
        ok: row.ok,
        fail: row.fail,
        slow: row.slow,
        ok_rate: row.count ? Number(((row.ok / row.count) * 100).toFixed(2)) : 0,
        p50_ms: Number(p50.toFixed(1)),
        p95_ms: Number(p95.toFixed(1)),
        p99_ms: Number(p99.toFixed(1)),
        max_ms: Number(row.max_ms.toFixed(1)),
        last_ms: Number(row.last_ms.toFixed(1)),
        last_status: row.last_status,
        last_at: row.last_at,
      };
    })
    .sort((a, b) => b.p95_ms - a.p95_ms || b.count - a.count);

  const statusCounts = {};
  for (const [code, count] of totalStatusCounts.entries()) {
    statusCounts[String(code)] = count;
  }

  res.json({
    success: true,
    now: new Date().toISOString(),
    started_at: new Date(BOOT_TS).toISOString(),
    uptime_sec: Math.round((Date.now() - BOOT_TS) / 1000),
    slow_api_threshold_ms: SLOW_API_MS,
    slow_api_count: slowApiCount,
    endpoint_count: endpoints.length,
    status_counts: statusCounts,
    endpoints: endpoints.slice(0, 60),
  });
});

app.use("/api/route", routeRouter);
app.use("/api/route", replanRouter);
app.use("/api/auth", authRouter);
app.use("/api/posts", postsRouter);
app.use("/api/dev", seedRouter);
app.use("/api/poi", poiRouter);
app.use("/api/follow", followRouter);
app.use("/api/notifications", notificationsRouter);
app.use("/api/chat", chatRouter);
app.use("/api/upload", uploadRouter);
app.use("/api/recommendation", recommendationRouter);
app.use("/api/recommendation", recoEventsRouter);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`JourneyPro API running at http://localhost:${PORT}`);
});
