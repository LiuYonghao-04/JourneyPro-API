import express from "express";
import { pool } from "../db/connect.js";
import { ensureUserExists } from "../services/reco/profiles.js";

const router = express.Router();

let ensureAiPlansTablePromise = null;

const parseUserId = (value) => {
  const uid = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(uid) && uid > 0 ? uid : 0;
};

const clamp = (value, min, max) => Math.min(Math.max(Number(value) || 0, min), max);

const truncate = (value, limit) => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3)).trim()}...`;
};

const safeJsonParse = (value, fallback = null) => {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
};

const normalizeSnapshot = (value) => {
  const parsed = safeJsonParse(value, null);
  return parsed && typeof parsed === "object" ? parsed : null;
};

const extractMessages = (snapshot) => (Array.isArray(snapshot?.messages) ? snapshot.messages : []);

const extractLatestPrompt = (snapshot, fallback = "") => {
  const fromMessages = [...extractMessages(snapshot)]
    .reverse()
    .find((row) => String(row?.role || "") === "user" && String(row?.content || "").trim());
  return truncate(fromMessages?.content || fallback || "", 220);
};

const extractSummary = (snapshot) =>
  truncate(snapshot?.planner_intent?.summary, 220) ||
  truncate(snapshot?.planner_meta?.prompt_summary, 220) ||
  truncate(snapshot?.itinerary?.segments?.[0]?.summary, 220) ||
  truncate(
    [...extractMessages(snapshot)]
      .filter((row) => String(row?.role || "") === "assistant")
      .map((row) => String(row?.content || "").trim())
      .find(Boolean),
    220
  ) ||
  "";

const buildDefaultTitle = (snapshot, fallbackPrompt = "") => {
  const prompt = extractLatestPrompt(snapshot, fallbackPrompt);
  const summary = extractSummary(snapshot);
  const scopeCity = String(snapshot?.scope?.supported_city || "London").trim();
  if (prompt) return truncate(prompt, 84);
  if (summary) return truncate(summary, 84);
  return `${scopeCity} AI trip plan`;
};

const normalizeRouteContext = (snapshot) => {
  const context = snapshot?.route_context;
  if (!context || typeof context !== "object") return null;
  return {
    start: context.start || null,
    end: context.end || null,
    via: Array.isArray(context.via) ? context.via : [],
    interest_weight: Number(context.interest_weight) || null,
    explore_weight: Number(context.explore_weight) || null,
    detour_tolerance: Number(context.detour_tolerance) || null,
  };
};

const extractIntentSnapshot = (snapshot) => {
  const intent = snapshot?.planner_intent;
  if (!intent || typeof intent !== "object") return null;
  return {
    summary: truncate(intent.summary, 140),
    pace: String(intent.pace || "").trim() || "balanced",
    exploration: String(intent.exploration || "").trim() || "balanced",
    preferred_categories: Array.isArray(intent.preferred_categories) ? intent.preferred_categories.slice(0, 4) : [],
    avoid_categories: Array.isArray(intent.avoid_categories) ? intent.avoid_categories.slice(0, 3) : [],
    tags: Array.isArray(intent.tags) ? intent.tags.slice(0, 4) : [],
  };
};

const extractProfileSnapshot = (snapshot) => {
  const profile = snapshot?.profile_snapshot;
  const routeContext = normalizeRouteContext(snapshot);
  if (!profile && !routeContext) return null;
  return {
    archetype: truncate(profile?.archetype, 60) || "",
    confidence: Number(profile?.confidence) || 0,
    dominant_category: truncate(profile?.dominant_category, 60) || "",
    dominant_tag: truncate(profile?.dominant_tag, 60) || "",
    source: truncate(profile?.source, 40) || "",
    interest_weight:
      Number(profile?.interest_weight ?? routeContext?.interest_weight) || 0,
    explore_weight:
      Number(profile?.explore_weight ?? routeContext?.explore_weight) || 0,
    detour_tolerance:
      Number(profile?.detour_tolerance ?? routeContext?.detour_tolerance) || 0,
  };
};

const buildListRow = (row) => {
  const snapshot = normalizeSnapshot(row.payload_json);
  return {
    id: Number(row.id),
    user_id: Number(row.user_id),
    title: String(row.title || "").trim(),
    summary: String(row.summary || "").trim(),
    prompt_preview: String(row.prompt_preview || "").trim(),
    scope_city: String(row.scope_city || "London").trim(),
    engine_mode: String(row.engine_mode || "").trim(),
    request_id: String(row.request_id || "").trim(),
    stop_count: Number(row.stop_count) || 0,
    via_count: Number(row.via_count) || 0,
    is_starred: !!Number(row.is_starred),
    intent_snapshot: extractIntentSnapshot(snapshot),
    profile_snapshot: extractProfileSnapshot(snapshot),
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
};

const buildStoredPayload = ({ bodySnapshot, title, summary, promptPreview }) => {
  const base = bodySnapshot && typeof bodySnapshot === "object" ? bodySnapshot : {};
  return {
    ...base,
    saved_plan_meta: {
      title,
      summary,
      prompt_preview: promptPreview,
      saved_at: new Date().toISOString(),
    },
  };
};

async function ensureAiPlansTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ai_trip_plans (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id BIGINT UNSIGNED NOT NULL,
      title VARCHAR(160) NOT NULL,
      summary VARCHAR(255) NULL,
      prompt_preview VARCHAR(255) NULL,
      request_id VARCHAR(80) NULL,
      scope_city VARCHAR(80) NOT NULL DEFAULT 'London',
      engine_mode VARCHAR(40) NULL,
      stop_count INT NOT NULL DEFAULT 0,
      via_count INT NOT NULL DEFAULT 0,
      is_starred TINYINT(1) NOT NULL DEFAULT 0,
      status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
      payload_json MEDIUMTEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_ai_trip_plans_user_status_updated (user_id, status, updated_at),
      KEY idx_ai_trip_plans_user_starred (user_id, is_starred, updated_at)
    )
  `);
}

function ensureAiPlansTableReady() {
  if (!ensureAiPlansTablePromise) {
    ensureAiPlansTablePromise = ensureAiPlansTable().catch((err) => {
      ensureAiPlansTablePromise = null;
      throw err;
    });
  }
  return ensureAiPlansTablePromise;
}

async function requireUser(userId) {
  if (!userId) return false;
  return ensureUserExists(userId);
}

router.get("/plans", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id);
    const limit = clamp(req.query.limit, 1, 60);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    await ensureAiPlansTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [rows] = await pool.query(
      `
        SELECT id, user_id, title, summary, prompt_preview, request_id, scope_city, engine_mode,
               stop_count, via_count, is_starred, payload_json, created_at, updated_at
        FROM ai_trip_plans
        WHERE user_id = ? AND status = 'ACTIVE'
        ORDER BY is_starred DESC, updated_at DESC, id DESC
        LIMIT ?
      `,
      [userId, limit]
    );

    res.json({
      success: true,
      user_id: userId,
      count: rows.length,
      items: rows.map(buildListRow),
    });
  } catch (err) {
    console.error("ai plans list error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/plans/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id);
    const planId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !planId) {
      return res.status(400).json({ success: false, message: "user_id and plan id required" });
    }
    await ensureAiPlansTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [[row]] = await pool.query(
      `
        SELECT *
        FROM ai_trip_plans
        WHERE id = ? AND user_id = ? AND status = 'ACTIVE'
        LIMIT 1
      `,
      [planId, userId]
    );

    if (!row) {
      return res.status(404).json({ success: false, message: "plan not found" });
    }

    res.json({
      success: true,
      item: {
        ...buildListRow(row),
        payload: normalizeSnapshot(row.payload_json),
      },
    });
  } catch (err) {
    console.error("ai plan detail error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/plans", async (req, res) => {
  try {
    const userId = parseUserId(req.body?.user_id);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    await ensureAiPlansTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const snapshot = normalizeSnapshot(req.body?.payload);
    if (!snapshot) {
      return res.status(400).json({ success: false, message: "payload required" });
    }

    const title = truncate(req.body?.title, 160) || buildDefaultTitle(snapshot, req.body?.prompt);
    const summary = truncate(req.body?.summary, 255) || extractSummary(snapshot);
    const promptPreview = extractLatestPrompt(snapshot, req.body?.prompt);
    const routeContext = normalizeRouteContext(snapshot);
    const stopCount = clamp(Array.isArray(snapshot?.recommendations) ? snapshot.recommendations.length : 0, 0, 50);
    const viaCount = clamp(Array.isArray(routeContext?.via) ? routeContext.via.length : 0, 0, 32);
    const scopeCity = truncate(snapshot?.scope?.supported_city || "London", 80) || "London";
    const engineMode = truncate(snapshot?.llm?.mode || "", 40) || null;
    const requestId = truncate(snapshot?.request_id || req.body?.request_id || "", 80) || null;
    const storedPayload = buildStoredPayload({
      bodySnapshot: snapshot,
      title,
      summary,
      promptPreview,
    });

    const [result] = await pool.query(
      `
        INSERT INTO ai_trip_plans (
          user_id, title, summary, prompt_preview, request_id, scope_city,
          engine_mode, stop_count, via_count, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        userId,
        title,
        summary || null,
        promptPreview || null,
        requestId,
        scopeCity,
        engineMode,
        stopCount,
        viaCount,
        JSON.stringify(storedPayload),
      ]
    );

    const [[row]] = await pool.query(
      `
        SELECT id, user_id, title, summary, prompt_preview, request_id, scope_city, engine_mode,
               stop_count, via_count, is_starred, payload_json, created_at, updated_at
        FROM ai_trip_plans
        WHERE id = ?
        LIMIT 1
      `,
      [result.insertId]
    );

    res.json({
      success: true,
      item: buildListRow(row),
    });
  } catch (err) {
    console.error("ai plan save error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.patch("/plans/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.body?.user_id ?? req.query.user_id);
    const planId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !planId) {
      return res.status(400).json({ success: false, message: "user_id and plan id required" });
    }
    await ensureAiPlansTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const fields = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "title")) {
      fields.push("title = ?");
      params.push(truncate(req.body?.title, 160) || "Untitled trip plan");
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "is_starred")) {
      fields.push("is_starred = ?");
      params.push(req.body?.is_starred ? 1 : 0);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "payload")) {
      const snapshot = normalizeSnapshot(req.body?.payload);
      if (!snapshot) {
        return res.status(400).json({ success: false, message: "invalid payload" });
      }
      const summary = truncate(req.body?.summary, 255) || extractSummary(snapshot);
      const promptPreview = extractLatestPrompt(snapshot, req.body?.prompt);
      const routeContext = normalizeRouteContext(snapshot);
      const stopCount = clamp(Array.isArray(snapshot?.recommendations) ? snapshot.recommendations.length : 0, 0, 50);
      const viaCount = clamp(Array.isArray(routeContext?.via) ? routeContext.via.length : 0, 0, 32);
      const scopeCity = truncate(snapshot?.scope?.supported_city || "London", 80) || "London";
      const engineMode = truncate(snapshot?.llm?.mode || "", 40) || null;
      const storedPayload = buildStoredPayload({
        bodySnapshot: snapshot,
        title: truncate(req.body?.title, 160),
        summary,
        promptPreview,
      });
      fields.push(
        "summary = ?",
        "prompt_preview = ?",
        "scope_city = ?",
        "engine_mode = ?",
        "stop_count = ?",
        "via_count = ?",
        "payload_json = ?"
      );
      params.push(summary || null, promptPreview || null, scopeCity, engineMode, stopCount, viaCount, JSON.stringify(storedPayload));
    }

    if (!fields.length) {
      return res.status(400).json({ success: false, message: "no updatable fields" });
    }

    params.push(planId, userId);
    const [result] = await pool.query(
      `
        UPDATE ai_trip_plans
        SET ${fields.join(", ")}
        WHERE id = ? AND user_id = ? AND status = 'ACTIVE'
      `,
      params
    );

    if (!result.affectedRows) {
      return res.status(404).json({ success: false, message: "plan not found" });
    }

    const [[row]] = await pool.query(
      `
        SELECT id, user_id, title, summary, prompt_preview, request_id, scope_city, engine_mode,
               stop_count, via_count, is_starred, payload_json, created_at, updated_at
        FROM ai_trip_plans
        WHERE id = ?
        LIMIT 1
      `,
      [planId]
    );

    res.json({
      success: true,
      item: buildListRow(row),
    });
  } catch (err) {
    console.error("ai plan patch error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.delete("/plans/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id ?? req.body?.user_id);
    const planId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !planId) {
      return res.status(400).json({ success: false, message: "user_id and plan id required" });
    }
    await ensureAiPlansTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [result] = await pool.query(
      `
        UPDATE ai_trip_plans
        SET status = 'DELETED'
        WHERE id = ? AND user_id = ? AND status = 'ACTIVE'
      `,
      [planId, userId]
    );

    if (!result.affectedRows) {
      return res.status(404).json({ success: false, message: "plan not found" });
    }

    res.json({
      success: true,
      id: planId,
    });
  } catch (err) {
    console.error("ai plan delete error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
