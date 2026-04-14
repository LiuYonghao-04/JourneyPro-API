import { pool } from "../db/connect.js";

const EVENT_STATUSES = new Set(["OPEN", "ACKNOWLEDGED", "RESOLVED", "IGNORED"]);
const EVENT_SOURCES = new Set(["WINDOW", "VUE", "ROUTER", "FETCH", "AXIOS", "MANUAL"]);
let ensureOpsCenterPromise = null;

const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const cleanText = (value, max = 255) =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max);

const cleanLongText = (value, max = 4000) =>
  String(value || "")
    .replace(/\u0000/g, "")
    .trim()
    .slice(0, max);

const normalizeStatus = (value) => {
  const next = String(value || "OPEN").trim().toUpperCase();
  return EVENT_STATUSES.has(next) ? next : "OPEN";
};

const normalizeSource = (value) => {
  const next = String(value || "WINDOW").trim().toUpperCase();
  return EVENT_SOURCES.has(next) ? next : "MANUAL";
};

const toIso = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
};

const toMysqlDateTime = (value) => {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes()
  )}:${pad(date.getSeconds())}`;
};

const parseContextJson = (value) => {
  if (!value) return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

export async function ensureOpsCenterSchema() {
  if (!ensureOpsCenterPromise) {
    ensureOpsCenterPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS app_error_events (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          user_id BIGINT NULL,
          session_id VARCHAR(80) NULL,
          source VARCHAR(20) NOT NULL DEFAULT 'WINDOW',
          error_type VARCHAR(40) NOT NULL DEFAULT 'CLIENT_RUNTIME',
          severity VARCHAR(20) NOT NULL DEFAULT 'ERROR',
          page_path VARCHAR(255) NULL,
          page_name VARCHAR(120) NULL,
          surface VARCHAR(120) NULL,
          endpoint VARCHAR(255) NULL,
          http_method VARCHAR(12) NULL,
          http_status INT NULL,
          request_id VARCHAR(80) NULL,
          message VARCHAR(500) NOT NULL,
          stack_text TEXT NULL,
          context_json LONGTEXT NULL,
          user_agent VARCHAR(255) NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
          admin_note VARCHAR(255) NULL,
          resolved_by BIGINT NULL,
          resolved_at DATETIME NULL,
          occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_app_error_events_status_occurred (status, occurred_at, id),
          KEY idx_app_error_events_user_occurred (user_id, occurred_at, id),
          KEY idx_app_error_events_source_occurred (source, occurred_at, id),
          KEY idx_app_error_events_http_status (http_status, occurred_at, id),
          KEY idx_app_error_events_surface (surface, occurred_at, id)
        )
      `);
    })().catch((err) => {
      ensureOpsCenterPromise = null;
      throw err;
    });
  }
  return ensureOpsCenterPromise;
}

export async function recordClientErrorEvent(payload = {}) {
  await ensureOpsCenterSchema();
  const userId = safeNumber(payload.user_id, 0) > 0 ? safeNumber(payload.user_id, 0) : null;
  const sessionId = cleanText(payload.session_id, 80) || null;
  const source = normalizeSource(payload.source);
  const errorType = cleanText(payload.error_type || "CLIENT_RUNTIME", 40) || "CLIENT_RUNTIME";
  const severity = cleanText(payload.severity || "ERROR", 20).toUpperCase() || "ERROR";
  const pagePath = cleanText(payload.page_path, 255) || null;
  const pageName = cleanText(payload.page_name, 120) || null;
  const surface = cleanText(payload.surface || payload.page_name || payload.page_path, 120) || null;
  const endpoint = cleanText(payload.endpoint, 255) || null;
  const httpMethod = cleanText(payload.http_method, 12).toUpperCase() || null;
  const httpStatus = safeNumber(payload.http_status, 0) > 0 ? safeNumber(payload.http_status, 0) : null;
  const requestId = cleanText(payload.request_id, 80) || null;
  const message = cleanText(payload.message, 500) || "Unknown client error";
  const stackText = cleanLongText(payload.stack_text || payload.stack, 4000) || null;
  const context =
    payload.context && typeof payload.context === "object"
      ? JSON.stringify(payload.context)
      : cleanLongText(payload.context_json, 4000) || null;
  const userAgent = cleanText(payload.user_agent, 255) || null;
  const occurredAt = toMysqlDateTime(payload.occurred_at) || toMysqlDateTime(new Date());

  const [result] = await pool.query(
    `
      INSERT INTO app_error_events (
        user_id, session_id, source, error_type, severity,
        page_path, page_name, surface, endpoint,
        http_method, http_status, request_id, message,
        stack_text, context_json, user_agent, status, occurred_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
    `,
    [
      userId,
      sessionId,
      source,
      errorType,
      severity,
      pagePath,
      pageName,
      surface,
      endpoint,
      httpMethod,
      httpStatus,
      requestId,
      message,
      stackText,
      context,
      userAgent,
      occurredAt,
    ]
  );
  return Number(result.insertId) || null;
}

export async function fetchOpsErrorFeed({
  status = "ALL",
  source = "ALL",
  search = "",
  limit = 40,
} = {}) {
  await ensureOpsCenterSchema();
  const where = [];
  const params = [];
  const normalizedStatus = String(status || "ALL").trim().toUpperCase();
  const normalizedSource = String(source || "ALL").trim().toUpperCase();
  if (EVENT_STATUSES.has(normalizedStatus)) {
    where.push("e.status = ?");
    params.push(normalizedStatus);
  }
  if (EVENT_SOURCES.has(normalizedSource)) {
    where.push("e.source = ?");
    params.push(normalizedSource);
  }
  const q = cleanText(search, 120);
  if (q) {
    where.push(
      `(e.message LIKE ? OR e.surface LIKE ? OR e.page_path LIKE ? OR e.endpoint LIKE ? OR u.nickname LIKE ? OR CAST(e.user_id AS CHAR) LIKE ?)`
    );
    const wildcard = `%${q}%`;
    params.push(wildcard, wildcard, wildcard, wildcard, wildcard, wildcard);
  }
  const safeLimit = Math.max(1, Math.min(Number(limit) || 40, 120));
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const [rows] = await pool.query(
    `
      SELECT
        e.id,
        e.user_id,
        e.session_id,
        e.source,
        e.error_type,
        e.severity,
        e.page_path,
        e.page_name,
        e.surface,
        e.endpoint,
        e.http_method,
        e.http_status,
        e.request_id,
        e.message,
        e.stack_text,
        e.context_json,
        e.user_agent,
        e.status,
        e.admin_note,
        e.resolved_by,
        e.resolved_at,
        e.occurred_at,
        e.created_at,
        e.updated_at,
        u.nickname AS user_nickname,
        resolver.nickname AS resolved_by_nickname
      FROM app_error_events e
      LEFT JOIN users u ON u.id = e.user_id
      LEFT JOIN users resolver ON resolver.id = e.resolved_by
      ${whereSql}
      ORDER BY e.occurred_at DESC, e.id DESC
      LIMIT ?
    `,
    [...params, safeLimit]
  );
  return rows.map((row) => ({
    id: Number(row.id),
    user_id: row.user_id ? Number(row.user_id) : null,
    user_nickname: row.user_nickname || null,
    session_id: row.session_id || null,
    source: normalizeSource(row.source),
    error_type: cleanText(row.error_type, 40) || "CLIENT_RUNTIME",
    severity: cleanText(row.severity, 20).toUpperCase() || "ERROR",
    page_path: row.page_path || null,
    page_name: row.page_name || null,
    surface: row.surface || row.page_name || row.page_path || "Unknown surface",
    endpoint: row.endpoint || null,
    http_method: row.http_method || null,
    http_status: row.http_status ? Number(row.http_status) : null,
    request_id: row.request_id || null,
    message: row.message || "Unknown error",
    stack_text: row.stack_text || null,
    context: parseContextJson(row.context_json),
    user_agent: row.user_agent || null,
    status: normalizeStatus(row.status),
    admin_note: row.admin_note || "",
    resolved_by: row.resolved_by ? Number(row.resolved_by) : null,
    resolved_by_nickname: row.resolved_by_nickname || null,
    resolved_at: toIso(row.resolved_at),
    occurred_at: toIso(row.occurred_at),
    created_at: toIso(row.created_at),
    updated_at: toIso(row.updated_at),
  }));
}

export async function fetchOpsErrorSummary() {
  await ensureOpsCenterSchema();
  const [[summaryRow]] = await pool.query(`
    SELECT
      COUNT(*) AS total_events,
      SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_events,
      SUM(CASE WHEN occurred_at >= NOW() - INTERVAL 1 DAY THEN 1 ELSE 0 END) AS last_24h,
      COUNT(DISTINCT CASE WHEN occurred_at >= NOW() - INTERVAL 7 DAY THEN user_id ELSE NULL END) AS impacted_users_7d,
      SUM(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS server_error_events,
      SUM(CASE WHEN source IN ('WINDOW', 'VUE', 'ROUTER') THEN 1 ELSE 0 END) AS runtime_error_events
    FROM app_error_events
  `);
  const [surfaceRows] = await pool.query(`
    SELECT surface, COUNT(*) AS total, MAX(occurred_at) AS last_seen_at
    FROM app_error_events
    WHERE occurred_at >= NOW() - INTERVAL 7 DAY
    GROUP BY surface
    ORDER BY total DESC, last_seen_at DESC
    LIMIT 6
  `);
  const [endpointRows] = await pool.query(`
    SELECT endpoint, COUNT(*) AS total, MAX(occurred_at) AS last_seen_at
    FROM app_error_events
    WHERE endpoint IS NOT NULL
      AND endpoint <> ''
      AND occurred_at >= NOW() - INTERVAL 7 DAY
    GROUP BY endpoint
    ORDER BY total DESC, last_seen_at DESC
    LIMIT 6
  `);
  return {
    total_events: safeNumber(summaryRow?.total_events),
    open_events: safeNumber(summaryRow?.open_events),
    last_24h: safeNumber(summaryRow?.last_24h),
    impacted_users_7d: safeNumber(summaryRow?.impacted_users_7d),
    server_error_events: safeNumber(summaryRow?.server_error_events),
    runtime_error_events: safeNumber(summaryRow?.runtime_error_events),
    top_surfaces: surfaceRows.map((row) => ({
      surface: row.surface || "Unknown surface",
      total: safeNumber(row.total),
      last_seen_at: toIso(row.last_seen_at),
    })),
    top_endpoints: endpointRows.map((row) => ({
      endpoint: row.endpoint || "Unknown endpoint",
      total: safeNumber(row.total),
      last_seen_at: toIso(row.last_seen_at),
    })),
  };
}

export async function updateOpsErrorEventStatus({
  eventId,
  status,
  adminNote = "",
  resolvedBy = null,
}) {
  await ensureOpsCenterSchema();
  const id = safeNumber(eventId, 0);
  if (id <= 0) throw new Error("invalid error event id");
  const nextStatus = normalizeStatus(status);
  if (!["ACKNOWLEDGED", "RESOLVED", "IGNORED", "OPEN"].includes(nextStatus)) {
    throw new Error("invalid status");
  }
  const note = cleanText(adminNote, 255) || null;
  const resolverId = safeNumber(resolvedBy, 0) > 0 ? safeNumber(resolvedBy, 0) : null;
  await pool.query(
    `
      UPDATE app_error_events
      SET
        status = ?,
        admin_note = ?,
        resolved_by = CASE WHEN ? IN ('RESOLVED', 'IGNORED') THEN ? ELSE NULL END,
        resolved_at = CASE WHEN ? IN ('RESOLVED', 'IGNORED') THEN NOW() ELSE NULL END
      WHERE id = ?
      LIMIT 1
    `,
    [nextStatus, note, nextStatus, resolverId, nextStatus, id]
  );
  return true;
}
