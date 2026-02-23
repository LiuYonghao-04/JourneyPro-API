import crypto from "crypto";

export const RECOMMENDATION_EXPERIMENT_KEY = "reco_v2";
export const RECOMMENDATION_VERSION = "v2";

export const DEFAULT_LIMIT = 10;
export const DEFAULT_CANDIDATE_LIMIT = 180;
export const DEFAULT_INTEREST_WEIGHT = 0.5;
export const DEFAULT_EXPLORE_WEIGHT = 0.15;

export const BASE_MODEL_FIXED_WEIGHTS = {
  quality: 0.18,
  novelty: 0.1,
  context: 0.12,
};

export const BANDIT_ALPHA = 0.35;
export const DIVERSITY_LAMBDA = 0.72;

export const SUPPORTED_MODES = ["driving", "walking", "cycling"];

export const MODE_DEFAULTS = {
  driving: {
    osrmProfile: "driving",
    sampleStepM: 300,
    corridorRadiusM: 500,
    maxDetourMinutes: 15,
    maxDetourRatio: 0.35,
    speedMps: 13.89,
  },
  walking: {
    osrmProfile: "walking",
    sampleStepM: 120,
    corridorRadiusM: 220,
    maxDetourMinutes: 10,
    maxDetourRatio: 0.4,
    speedMps: 1.4,
  },
  cycling: {
    osrmProfile: "cycling",
    sampleStepM: 180,
    corridorRadiusM: 320,
    maxDetourMinutes: 12,
    maxDetourRatio: 0.35,
    speedMps: 4.2,
  },
};

export const EVENT_TYPES = [
  "impression",
  "detail_view",
  "open_posts",
  "save",
  "add_via",
  "navigate",
  "dismiss",
  "remove_via",
  "like_post",
  "favorite_post",
];

export const EVENT_REWARD_WEIGHTS = {
  impression: 0,
  detail_view: 1,
  open_posts: 2,
  save: 3,
  add_via: 5,
  navigate: 4,
  dismiss: -1,
  remove_via: -3,
  like_post: 2,
  favorite_post: 3,
};

export const FEATURE_TYPES = {
  TAG: "tag",
  CATEGORY: "category",
  POI: "poi",
};

export const MAX_DETOUR_CAP_SECONDS = 45 * 60;

export const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

export const normalizeWeight = (value, fallback = 0.5) => {
  let num = Number(value);
  if (!Number.isFinite(num)) num = fallback;
  if (num > 1) num /= 100;
  return clamp(num, 0, 1);
};

export const normalizeInteger = (value, fallback, min, max) => {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return fallback;
  return clamp(num, min, max);
};

export const normalizeMode = (value) => {
  const mode = String(value || "driving").trim().toLowerCase();
  return SUPPORTED_MODES.includes(mode) ? mode : "driving";
};

export const parseBoolFlag = (value, fallback = false) => {
  if (value === undefined || value === null || value === "") return fallback;
  const raw = String(value).toLowerCase();
  if (raw === "1" || raw === "true" || raw === "yes") return true;
  if (raw === "0" || raw === "false" || raw === "no") return false;
  return fallback;
};

export const getModeConfig = (mode, modeDefaults = null) => {
  const safeMode = normalizeMode(mode);
  const base = { ...MODE_DEFAULTS[safeMode] };
  const overrides =
    modeDefaults && typeof modeDefaults === "object" ? modeDefaults[safeMode] || null : null;
  if (!overrides || typeof overrides !== "object") return { mode: safeMode, ...base };

  return {
    mode: safeMode,
    ...base,
    sampleStepM: clamp(Number(overrides.sampleStepM) || base.sampleStepM, 60, 1000),
    corridorRadiusM: clamp(Number(overrides.corridorRadiusM) || base.corridorRadiusM, 120, 2000),
    maxDetourMinutes: clamp(Number(overrides.maxDetourMinutes) || base.maxDetourMinutes, 5, 40),
    maxDetourRatio: clamp(Number(overrides.maxDetourRatio) || base.maxDetourRatio, 0.15, 0.65),
    speedMps: clamp(Number(overrides.speedMps) || base.speedMps, 0.8, 30),
  };
};

export const round = (value, digits = 4) => {
  const factor = 10 ** digits;
  return Math.round(Number(value || 0) * factor) / factor;
};

export const uniqueList = (items) => [...new Set((items || []).filter(Boolean))];

export const hashToRatio = (input) => {
  const digest = crypto.createHash("sha1").update(String(input || "")).digest("hex");
  const integer = Number.parseInt(digest.slice(0, 12), 16);
  if (!Number.isFinite(integer)) return 0;
  return integer / 0xffffffffffff;
};

export const buildSubjectKey = ({ userId, sessionId, ip, userAgent }) => {
  if (userId) return `u:${userId}`;
  if (sessionId) return `s:${sessionId}`;
  return `anon:${crypto
    .createHash("sha1")
    .update(`${ip || "0.0.0.0"}|${userAgent || ""}`)
    .digest("hex")
    .slice(0, 24)}`;
};

export const nowIso = () => new Date().toISOString();
