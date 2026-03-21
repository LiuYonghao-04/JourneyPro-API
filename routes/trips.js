import express from "express";
import { pool } from "../db/connect.js";
import { ensureUserExists } from "../services/reco/profiles.js";

const router = express.Router();

let ensureTripsTablePromise = null;

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

const normalizeStatus = (value, fallback = "DRAFT") => {
  const raw = String(value || "").trim().toUpperCase();
  if (["DRAFT", "ACTIVE", "COMPLETED"].includes(raw)) return raw;
  return fallback;
};

const extractMessages = (snapshot) => (Array.isArray(snapshot?.messages) ? snapshot.messages : []);

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

const extractPrompt = (snapshot) =>
  truncate(
    [...extractMessages(snapshot)]
      .reverse()
      .find((row) => String(row?.role || "") === "user" && String(row?.content || "").trim())?.content,
    220
  ) || "";

const buildDefaultTitle = (snapshot) => {
  const prompt = extractPrompt(snapshot);
  const summary = extractSummary(snapshot);
  if (prompt) return truncate(prompt, 84);
  if (summary) return truncate(summary, 84);
  return "Untitled trip workspace";
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

const extractProfileSnapshot = (snapshot, routeContextRaw = null) => {
  const profile = snapshot?.profile_snapshot;
  const routeContext =
    (routeContextRaw && typeof routeContextRaw === "object" ? routeContextRaw : null) || normalizeRouteContext(snapshot);
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
  };
};

const buildListRow = (row) => {
  const snapshot = normalizeSnapshot(row.planner_snapshot_json);
  const routeContext = safeJsonParse(row.route_context_json, null);
  return {
    id: Number(row.id),
    user_id: Number(row.user_id),
    title: String(row.title || "").trim(),
    summary: String(row.summary || "").trim(),
    prompt_preview: String(row.prompt_preview || "").trim(),
    status: String(row.status || "DRAFT").trim(),
    progress_state: String(row.progress_state || "PLANNING").trim(),
    source_plan_id: row.source_plan_id ? Number(row.source_plan_id) : null,
    source_plan_title: String(row.source_plan_title || "").trim(),
    stop_count: Number(row.stop_count) || 0,
    via_count: Number(row.via_count) || 0,
    note_count: Number(row.note_count) || 0,
    is_starred: !!Number(row.is_starred),
    route_ready:
      !!routeContext?.start ||
      !!routeContext?.end ||
      (Array.isArray(routeContext?.via) && routeContext.via.length > 0) ||
      (Array.isArray(snapshot?.recommendations) && snapshot.recommendations.length > 0),
    intent_snapshot: extractIntentSnapshot(snapshot),
    profile_snapshot: extractProfileSnapshot(snapshot, routeContext),
    created_at: row.created_at,
    updated_at: row.updated_at,
    started_at: row.started_at,
    completed_at: row.completed_at,
  };
};

async function ensureTripsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS trip_workspaces (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id BIGINT UNSIGNED NOT NULL,
      source_plan_id BIGINT UNSIGNED NULL,
      title VARCHAR(160) NOT NULL,
      summary VARCHAR(255) NULL,
      prompt_preview VARCHAR(255) NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
      progress_state VARCHAR(20) NOT NULL DEFAULT 'PLANNING',
      note_count INT NOT NULL DEFAULT 0,
      stop_count INT NOT NULL DEFAULT 0,
      via_count INT NOT NULL DEFAULT 0,
      is_starred TINYINT(1) NOT NULL DEFAULT 0,
      notes_text MEDIUMTEXT NULL,
      planner_snapshot_json MEDIUMTEXT NOT NULL,
      route_context_json MEDIUMTEXT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      started_at DATETIME NULL,
      completed_at DATETIME NULL,
      PRIMARY KEY (id),
      KEY idx_trip_workspaces_user_status_updated (user_id, status, updated_at),
      KEY idx_trip_workspaces_user_starred (user_id, is_starred, updated_at)
    )
  `);
}

function ensureTripsTableReady() {
  if (!ensureTripsTablePromise) {
    ensureTripsTablePromise = ensureTripsTable().catch((err) => {
      ensureTripsTablePromise = null;
      throw err;
    });
  }
  return ensureTripsTablePromise;
}

async function requireUser(userId) {
  if (!userId) return false;
  return ensureUserExists(userId);
}

router.get("/trips", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id);
    const limit = clamp(req.query.limit, 1, 80);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    await ensureTripsTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [rows] = await pool.query(
      `
        SELECT tw.id, tw.user_id, tw.source_plan_id, tw.title, tw.summary, tw.prompt_preview, tw.status, tw.progress_state,
               tw.note_count, tw.stop_count, tw.via_count, tw.is_starred, tw.planner_snapshot_json, tw.route_context_json,
               tw.created_at, tw.updated_at, tw.started_at, tw.completed_at, COALESCE(ap.title, '') AS source_plan_title
        FROM trip_workspaces tw
        LEFT JOIN ai_trip_plans ap ON ap.id = tw.source_plan_id
        WHERE tw.user_id = ? AND tw.status <> 'DELETED'
        ORDER BY tw.is_starred DESC, tw.updated_at DESC, tw.id DESC
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
    console.error("trip workspace list error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.get("/trips/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id);
    const tripId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !tripId) {
      return res.status(400).json({ success: false, message: "user_id and trip id required" });
    }
    await ensureTripsTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [[row]] = await pool.query(
      `
        SELECT tw.*, COALESCE(ap.title, '') AS source_plan_title
        FROM trip_workspaces tw
        LEFT JOIN ai_trip_plans ap ON ap.id = tw.source_plan_id
        WHERE tw.id = ? AND tw.user_id = ? AND tw.status <> 'DELETED'
        LIMIT 1
      `,
      [tripId, userId]
    );

    if (!row) {
      return res.status(404).json({ success: false, message: "trip not found" });
    }

    res.json({
      success: true,
      item: {
        ...buildListRow(row),
        notes_text: String(row.notes_text || ""),
        planner_snapshot: normalizeSnapshot(row.planner_snapshot_json),
        route_context: safeJsonParse(row.route_context_json, null),
      },
    });
  } catch (err) {
    console.error("trip workspace detail error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.post("/trips", async (req, res) => {
  try {
    const userId = parseUserId(req.body?.user_id);
    if (!userId) {
      return res.status(400).json({ success: false, message: "user_id required" });
    }
    await ensureTripsTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const snapshot = normalizeSnapshot(req.body?.planner_snapshot || req.body?.payload);
    if (!snapshot) {
      return res.status(400).json({ success: false, message: "planner_snapshot required" });
    }

    const routeContext = normalizeRouteContext(snapshot) || safeJsonParse(req.body?.route_context, null);
    const title = truncate(req.body?.title, 160) || buildDefaultTitle(snapshot);
    const summary = truncate(req.body?.summary, 255) || extractSummary(snapshot);
    const promptPreview = extractPrompt(snapshot);
    const sourcePlanId = req.body?.source_plan_id ? Number(req.body.source_plan_id) : null;
    const status = normalizeStatus(req.body?.status, "DRAFT");
    const progressState = status === "COMPLETED" ? "DONE" : status === "ACTIVE" ? "IN_PROGRESS" : "PLANNING";
    const notesText = String(req.body?.notes_text || "").trim();
    const stopCount = clamp(Array.isArray(snapshot?.recommendations) ? snapshot.recommendations.length : 0, 0, 50);
    const viaCount = clamp(Array.isArray(routeContext?.via) ? routeContext.via.length : 0, 0, 32);
    const noteCount = notesText ? notesText.split(/\r?\n/).filter((line) => String(line || "").trim()).length : 0;

    const [result] = await pool.query(
      `
        INSERT INTO trip_workspaces (
          user_id, source_plan_id, title, summary, prompt_preview, status, progress_state,
          note_count, stop_count, via_count, notes_text, planner_snapshot_json, route_context_json,
          started_at, completed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        userId,
        sourcePlanId,
        title,
        summary || null,
        promptPreview || null,
        status,
        progressState,
        noteCount,
        stopCount,
        viaCount,
        notesText || null,
        JSON.stringify(snapshot),
        routeContext ? JSON.stringify(routeContext) : null,
        status === "ACTIVE" ? new Date() : null,
        status === "COMPLETED" ? new Date() : null,
      ]
    );

    const [[row]] = await pool.query(
      `
        SELECT tw.id, tw.user_id, tw.source_plan_id, tw.title, tw.summary, tw.prompt_preview, tw.status, tw.progress_state,
               tw.note_count, tw.stop_count, tw.via_count, tw.is_starred, tw.planner_snapshot_json, tw.route_context_json,
               tw.created_at, tw.updated_at, tw.started_at, tw.completed_at, COALESCE(ap.title, '') AS source_plan_title
        FROM trip_workspaces tw
        LEFT JOIN ai_trip_plans ap ON ap.id = tw.source_plan_id
        WHERE tw.id = ?
        LIMIT 1
      `,
      [result.insertId]
    );

    res.json({
      success: true,
      item: buildListRow(row),
    });
  } catch (err) {
    console.error("trip workspace create error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.patch("/trips/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.body?.user_id ?? req.query.user_id);
    const tripId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !tripId) {
      return res.status(400).json({ success: false, message: "user_id and trip id required" });
    }
    await ensureTripsTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const fields = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "title")) {
      fields.push("title = ?");
      params.push(truncate(req.body?.title, 160) || "Untitled trip workspace");
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "summary")) {
      fields.push("summary = ?");
      params.push(truncate(req.body?.summary, 255) || null);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "is_starred")) {
      fields.push("is_starred = ?");
      params.push(req.body?.is_starred ? 1 : 0);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "notes_text")) {
      const notesText = String(req.body?.notes_text || "").trim();
      const noteCount = notesText ? notesText.split(/\r?\n/).filter((line) => String(line || "").trim()).length : 0;
      fields.push("notes_text = ?", "note_count = ?");
      params.push(notesText || null, noteCount);
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "status")) {
      const status = normalizeStatus(req.body?.status, "DRAFT");
      const progressState = status === "COMPLETED" ? "DONE" : status === "ACTIVE" ? "IN_PROGRESS" : "PLANNING";
      fields.push("status = ?", "progress_state = ?");
      params.push(status, progressState);
      if (status === "ACTIVE") {
        fields.push("started_at = COALESCE(started_at, CURRENT_TIMESTAMP)", "completed_at = NULL");
      } else if (status === "COMPLETED") {
        fields.push("completed_at = CURRENT_TIMESTAMP");
      } else {
        fields.push("completed_at = NULL");
      }
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "planner_snapshot")) {
      const snapshot = normalizeSnapshot(req.body?.planner_snapshot);
      if (!snapshot) {
        return res.status(400).json({ success: false, message: "invalid planner_snapshot" });
      }
      const routeContext = normalizeRouteContext(snapshot) || safeJsonParse(req.body?.route_context, null);
      fields.push("planner_snapshot_json = ?", "route_context_json = ?", "prompt_preview = ?", "stop_count = ?", "via_count = ?");
      params.push(
        JSON.stringify(snapshot),
        routeContext ? JSON.stringify(routeContext) : null,
        extractPrompt(snapshot) || null,
        clamp(Array.isArray(snapshot?.recommendations) ? snapshot.recommendations.length : 0, 0, 50),
        clamp(Array.isArray(routeContext?.via) ? routeContext.via.length : 0, 0, 32)
      );
    }

    if (!fields.length) {
      return res.status(400).json({ success: false, message: "no updatable fields" });
    }

    params.push(tripId, userId);
    const [result] = await pool.query(
      `
        UPDATE trip_workspaces
        SET ${fields.join(", ")}
        WHERE id = ? AND user_id = ? AND status <> 'DELETED'
      `,
      params
    );

    if (!result.affectedRows) {
      return res.status(404).json({ success: false, message: "trip not found" });
    }

    const [[row]] = await pool.query(
      `
        SELECT tw.id, tw.user_id, tw.source_plan_id, tw.title, tw.summary, tw.prompt_preview, tw.status, tw.progress_state,
               tw.note_count, tw.stop_count, tw.via_count, tw.is_starred, tw.planner_snapshot_json, tw.route_context_json,
               tw.created_at, tw.updated_at, tw.started_at, tw.completed_at, COALESCE(ap.title, '') AS source_plan_title
        FROM trip_workspaces tw
        LEFT JOIN ai_trip_plans ap ON ap.id = tw.source_plan_id
        WHERE tw.id = ?
        LIMIT 1
      `,
      [tripId]
    );

    res.json({
      success: true,
      item: buildListRow(row),
    });
  } catch (err) {
    console.error("trip workspace patch error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

router.delete("/trips/:id", async (req, res) => {
  try {
    const userId = parseUserId(req.query.user_id ?? req.body?.user_id);
    const tripId = Number.parseInt(String(req.params.id || ""), 10);
    if (!userId || !tripId) {
      return res.status(400).json({ success: false, message: "user_id and trip id required" });
    }
    await ensureTripsTableReady();
    const exists = await requireUser(userId);
    if (!exists) {
      return res.status(404).json({ success: false, message: "user not found" });
    }

    const [result] = await pool.query(
      `
        UPDATE trip_workspaces
        SET status = 'DELETED'
        WHERE id = ? AND user_id = ? AND status <> 'DELETED'
      `,
      [tripId, userId]
    );

    if (!result.affectedRows) {
      return res.status(404).json({ success: false, message: "trip not found" });
    }

    res.json({
      success: true,
      id: tripId,
    });
  } catch (err) {
    console.error("trip workspace delete error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
