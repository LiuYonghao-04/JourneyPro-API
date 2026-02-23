import { pool } from "../../db/connect.js";
import {
  EVENT_TYPES,
  RECOMMENDATION_VERSION,
  normalizeMode,
  round,
} from "./constants.js";
import { ensureRecoTables } from "./schema.js";

const EVENT_SET = new Set(EVENT_TYPES);

export const isValidRecommendationEventType = (eventType) => EVENT_SET.has(String(eventType || ""));

const normalizeTs = (value) => {
  if (!value) return new Date();
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return new Date();
  return date;
};

const normalizeEventPayload = (event = {}) => {
  const eventType = String(event.event_type || "").trim();
  if (!isValidRecommendationEventType(eventType)) return null;

  const poiId = Number.parseInt(event.poi_id, 10);
  if (!poiId) return null;

  const userId = event.user_id ? Number.parseInt(event.user_id, 10) : null;
  const rankPosition = event.rank_position !== undefined && event.rank_position !== null
    ? Number.parseInt(event.rank_position, 10)
    : null;
  const eventValue = Number(event.event_value);

  return {
    user_id: Number.isFinite(userId) && userId > 0 ? userId : null,
    session_id: event.session_id ? String(event.session_id).slice(0, 128) : null,
    request_id: event.request_id ? String(event.request_id).slice(0, 128) : "",
    algorithm_version: event.algorithm_version
      ? String(event.algorithm_version).slice(0, 32)
      : RECOMMENDATION_VERSION,
    bucket: event.bucket ? String(event.bucket).slice(0, 32) : null,
    mode: normalizeMode(event.mode),
    route_hash: event.route_hash ? String(event.route_hash).slice(0, 128) : null,
    poi_id: poiId,
    rank_position: Number.isFinite(rankPosition) ? rankPosition : null,
    event_type: eventType,
    event_value: Number.isFinite(eventValue) ? round(eventValue, 6) : 1,
    ts: normalizeTs(event.ts),
  };
};

export const insertRecommendationEvents = async (events) => {
  const list = Array.isArray(events) ? events : [events];
  const normalized = list.map(normalizeEventPayload).filter(Boolean);
  if (!normalized.length) return { inserted: 0, accepted: 0, dropped: list.length };

  await ensureRecoTables();

  const values = normalized.map((row) => [
    row.user_id,
    row.session_id,
    row.request_id,
    row.algorithm_version,
    row.bucket,
    row.mode,
    row.route_hash,
    row.poi_id,
    row.rank_position,
    row.event_type,
    row.event_value,
    row.ts,
  ]);

  await pool.query(
    `
      INSERT INTO recommendation_events (
        user_id, session_id, request_id, algorithm_version, bucket,
        mode, route_hash, poi_id, rank_position, event_type, event_value, ts
      )
      VALUES ?
    `,
    [values]
  );

  return {
    inserted: normalized.length,
    accepted: normalized.length,
    dropped: list.length - normalized.length,
  };
};
