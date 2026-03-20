import axios from "axios";
import crypto from "crypto";
import express from "express";
import {
  DEFAULT_EXPLORE_WEIGHT,
  DEFAULT_INTEREST_WEIGHT,
  clamp,
  normalizeInteger,
  normalizeWeight,
} from "../services/reco/constants.js";
import { fetchUserRecommendationSettings } from "../services/reco/profiles.js";
import { runRecommendationV2 } from "../services/reco/ranker.js";
import { buildPlannerKnowledgePack } from "../services/ai/retrieval.js";
import {
  classifyPlannerScopeWithLlm,
  getPlannerLlmConfig,
  streamPlannerNarrativeFromLlm,
} from "../services/ai/llm.js";

const router = express.Router();

const DEFAULT_START = { lng: -0.1278, lat: 51.5074 };
const DEFAULT_END = { lng: -0.118, lat: 51.509 };

const SUPPORTED_CITY = "London";
const SUPPORTED_SCOPE_ALIASES = [
  "london",
  "greater london",
  "central london",
  "city of london",
  "westminster",
  "camden",
  "greenwich",
  "shoreditch",
  "soho",
  "south bank",
  "covent garden",
  "notting hill",
  "kensington",
  "chelsea",
  "canary wharf",
];
const SUPPORTED_SCOPE_LANDMARKS = [
  "big ben",
  "london eye",
  "london eyes",
  "tower bridge",
  "buckingham palace",
  "british museum",
  "st paul s cathedral",
  "st pauls cathedral",
  "westminster abbey",
  "trafalgar square",
  "hyde park",
  "camden market",
  "borough market",
  "tower of london",
  "covent garden",
  "piccadilly circus",
];
const LOCATION_FILLER_TOKENS = new Set([
  "a",
  "an",
  "the",
  "i",
  "me",
  "my",
  "we",
  "our",
  "give",
  "show",
  "help",
  "build",
  "find",
  "suggest",
  "suggestion",
  "suggestions",
  "recommend",
  "recommended",
  "just",
  "want",
  "need",
  "have",
  "for",
  "to",
  "in",
  "around",
  "near",
  "visit",
  "visiting",
  "from",
  "start",
  "starting",
  "end",
  "ending",
  "travel",
  "trip",
  "itinerary",
  "route",
  "plan",
  "planning",
  "trail",
  "one",
  "two",
  "three",
  "four",
  "five",
  "day",
  "days",
  "weekend",
  "quick",
  "today",
  "tomorrow",
]);
const LOCATION_SCOPE_CACHE = new Map();

const CATEGORY_HINTS = [
  {
    category: "food",
    keywords: [
      "food",
      "restaurant",
      "restaurants",
      "cafe",
      "coffee",
      "brunch",
      "dinner",
      "lunch",
      "dessert",
      "bakery",
      "tea",
    ],
  },
  { category: "museum", keywords: ["museum", "gallery", "art", "exhibition", "curator", "architecture"] },
  { category: "history", keywords: ["history", "historic", "heritage", "monument", "castle", "roman"] },
  { category: "nature", keywords: ["nature", "park", "garden", "river", "lake", "green", "outdoor"] },
  { category: "shopping", keywords: ["shopping", "mall", "market", "store", "vintage", "fashion", "local shop"] },
  { category: "nightlife", keywords: ["nightlife", "bar", "pub", "night", "cocktail", "music", "late"] },
  { category: "attraction", keywords: ["landmark", "sightseeing", "attraction", "must see", "tower", "bridge"] },
];

const STOP_WORDS = new Set([
  "a",
  "an",
  "the",
  "and",
  "or",
  "to",
  "for",
  "with",
  "in",
  "on",
  "at",
  "of",
  "my",
  "trip",
  "travel",
  "plan",
  "need",
  "want",
  "have",
  "day",
  "days",
  "hour",
  "hours",
  "please",
  "around",
  "london",
  "itinerary",
  "route",
  "recommendation",
]);

const LOW_DETOUR_KEYWORDS = [
  "low detour",
  "short detour",
  "avoid detour",
  "avoid long detour",
  "near",
  "nearby",
  "not far",
  "close",
  "quick trip",
  "save time",
  "efficient",
  "minimal transfer",
  "avoid long walk",
  "avoid traffic",
  "direct",
];

const AVOID_KEYWORDS = ["avoid", "no ", "skip", "without", "not into"];

const SAFE_KEYWORDS = [
  "safe",
  "stable",
  "reliable",
  "classic",
  "mainstream",
  "popular",
  "proven",
  "high rated",
];

const EXPLORE_KEYWORDS = [
  "explore",
  "adventure",
  "hidden gem",
  "hidden gems",
  "offbeat",
  "surprise me",
  "local vibe",
  "new places",
  "something different",
  "less touristy",
];

const FAST_PACE_KEYWORDS = [
  "1 day",
  "one day",
  "half day",
  "few hours",
  "quick",
  "today",
  "tight schedule",
  "compact",
  "rush",
];

const RELAXED_PACE_KEYWORDS = [
  "slow",
  "relaxed",
  "easy pace",
  "chill",
  "not rushed",
  "leisure",
  "walk around",
  "take time",
];

const FAMILY_KEYWORDS = ["family", "kids", "child", "children", "parent"];
const RAIN_KEYWORDS = ["rain", "rainy", "bad weather", "indoor"];
const PHOTO_KEYWORDS = ["photo", "photography", "instagram", "scenic"];
const NIGHT_KEYWORDS = ["night", "evening", "sunset", "late"];
const ITINERARY_PERIODS = [
  { key: "morning", label: "Morning", title: "Efficient Start" },
  { key: "afternoon", label: "Afternoon", title: "Core Discovery" },
  { key: "evening", label: "Evening", title: "Flexible Finish" },
];
const SUPPORTED_SCOPE_BOUNDS = {
  minLat: 51.28,
  maxLat: 51.7,
  minLng: -0.52,
  maxLng: 0.33,
};

const clamp01 = (value) => clamp(Number(value) || 0, 0, 1);

const normalizeText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[\r\n\t]+/g, " ")
    .replace(/[^a-z0-9\s-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const CATEGORY_TOKEN_SET = new Set(
  CATEGORY_HINTS.flatMap((hint) => [hint.category, ...hint.keywords])
    .flatMap((value) => normalizeText(value).split(" "))
    .filter(Boolean)
);

const normalizeLocationText = (value) =>
  normalizeText(value)
    .replace(/\b(a1|1-day|one-day)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const escapeRegex = (value) => String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const createPhraseRegex = (value) => new RegExp(`\\b${escapeRegex(value).replace(/\s+/g, "\\s+")}\\b`, "i");

const SUPPORTED_SCOPE_PATTERNS = SUPPORTED_SCOPE_ALIASES.map((alias) => ({
  alias,
  regex: createPhraseRegex(alias),
}));

const SUPPORTED_LANDMARK_PATTERNS = SUPPORTED_SCOPE_LANDMARKS.map((alias) => ({
  alias,
  regex: createPhraseRegex(alias),
}));

const isSupportedLondonScope = (value) => {
  const text = normalizeLocationText(value);
  if (!text) return false;
  return SUPPORTED_SCOPE_ALIASES.some((alias) => text === alias || text.includes(alias) || alias.includes(text));
};

const extractSupportedAliasMentions = (value) => {
  const text = normalizeLocationText(value);
  if (!text) return [];
  return SUPPORTED_SCOPE_PATTERNS.filter((item) => item.regex.test(text)).map((item) => item.alias);
};

const extractSupportedLandmarkMentions = (value) => {
  const text = normalizeLocationText(value);
  if (!text) return [];
  return SUPPORTED_LANDMARK_PATTERNS.filter((item) => item.regex.test(text)).map((item) => item.alias);
};

const sanitizeLocationCandidate = (value) => {
  const normalized = normalizeLocationText(value);
  if (!normalized) return null;

  const tokens = normalized
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean);

  while (tokens.length && LOCATION_FILLER_TOKENS.has(tokens[0])) tokens.shift();
  while (tokens.length && LOCATION_FILLER_TOKENS.has(tokens[tokens.length - 1])) tokens.pop();

  if (!tokens.length || tokens.length > 4) return null;
  if (tokens.every((token) => LOCATION_FILLER_TOKENS.has(token) || CATEGORY_TOKEN_SET.has(token))) return null;

  const candidate = tokens.join(" ").trim();
  if (!candidate) return null;
  if (CATEGORY_TOKEN_SET.has(candidate)) return null;
  return candidate;
};

const LOCATION_PATTERNS = [
  /\b([a-z][a-z\s-]{1,40}?)\s+(?:trip|itinerary|route|vacation)\b/g,
  /\b([a-z][a-z\s-]{1,40}?)\s+(?:(?:\d+|one|two|three|four|five)\s+)?(?:day|days|weekend)\s+(?:trip|trail|plan|itinerary|route)\b/g,
  /\b(?:trip|itinerary|route|vacation|travel|visit(?:ing)?|day|plan)\s+(?:to|in|around|for)\s+([a-z][a-z\s-]{1,40}?)(?=(?:\s+(?:with|for|and|but|prefer|please|from|to|start(?:ing)?|end(?:ing)?|i|we|they|avoid|like|love|want|need))|$)/g,
  /\b(?:in|around|near|to|visit(?:ing)?)\s+([a-z][a-z\s-]{1,40}?)(?=(?:\s+(?:trip|itinerary|route|with|for|and|but|prefer|please|from|to|start(?:ing)?|end(?:ing)?|i|we|they|avoid|like|love|want|need))|$)/g,
  /\b(?:from|starting\s+at|start(?:ing)?\s+from)\s+([a-z][a-z\s-]{1,40}?)(?=(?:\s+(?:to|towards|toward|and|but|with|for|i|we|they|then|after|please))|$)/g,
  /\b(?:to|towards|toward|ending\s+at|end(?:ing)?\s+at)\s+([a-z][a-z\s-]{1,40}?)(?=(?:\s+(?:with|for|and|but|i|we|they|then|after|please|avoid|like|love|want|need))|$)/g,
];

const extractLocationCandidates = (prompt) => {
  const text = normalizeLocationText(prompt);
  if (!text) return [];

  const found = new Set();
  LOCATION_PATTERNS.forEach((pattern) => {
    const regex = new RegExp(pattern.source, pattern.flags);
    let match = regex.exec(text);
    while (match) {
      const candidate = sanitizeLocationCandidate(match[1]);
      if (candidate) found.add(candidate);
      match = regex.exec(text);
    }
  });

  return [...found].sort((a, b) => b.length - a.length);
};

const isPointWithinSupportedBounds = (point) => {
  const lat = Number(point?.lat);
  const lng = Number(point?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  return (
    lat >= SUPPORTED_SCOPE_BOUNDS.minLat &&
    lat <= SUPPORTED_SCOPE_BOUNDS.maxLat &&
    lng >= SUPPORTED_SCOPE_BOUNDS.minLng &&
    lng <= SUPPORTED_SCOPE_BOUNDS.maxLng
  );
};

const buildRouteContextScopeSignals = ({ startPoint, endPoint, viaPoints = [] } = {}) => {
  const startInside = isPointWithinSupportedBounds(startPoint);
  const endInside = isPointWithinSupportedBounds(endPoint);
  const viaInsideCount = (viaPoints || []).filter((point) => isPointWithinSupportedBounds(point)).length;
  return {
    startInside,
    endInside,
    viaInsideCount,
    viaCount: Array.isArray(viaPoints) ? viaPoints.length : 0,
    londonSignalCount: Number(startInside) + Number(endInside) + viaInsideCount,
  };
};

const resolveLocationCandidateScope = async (candidate) => {
  if (isSupportedLondonScope(candidate)) {
    return {
      supported: true,
      requestedLocation: candidate,
      resolvedLocation: SUPPORTED_CITY,
      reason: "supported_alias_candidate",
    };
  }

  if (LOCATION_SCOPE_CACHE.has(candidate)) {
    return LOCATION_SCOPE_CACHE.get(candidate);
  }

  const response = await axios.get("https://nominatim.openstreetmap.org/search", {
    params: {
      format: "json",
      q: candidate,
      limit: 1,
      addressdetails: 1,
    },
    timeout: 1500,
    headers: {
      "User-Agent": "JourneyPro-AIPlanner/1.0",
    },
    validateStatus: () => true,
  });

  const row = Array.isArray(response.data) ? response.data[0] : null;
  if (!row) {
    return {
      supported: null,
      requestedLocation: candidate,
      resolvedLocation: candidate,
      reason: "no_geocode_match",
    };
  }

  const resolvedText = normalizeLocationText(
    [row.display_name, row?.address?.city, row?.address?.town, row?.address?.state, row?.address?.country]
      .filter(Boolean)
      .join(" ")
  );
  const resolvedLocation =
    row?.address?.city ||
    row?.address?.town ||
    row?.address?.state ||
    row?.display_name ||
    candidate;
  const scoped = {
    supported: isSupportedLondonScope(resolvedText),
    requestedLocation: candidate,
    resolvedLocation,
    reason: "geocode_lookup",
  };
  LOCATION_SCOPE_CACHE.set(candidate, scoped);
  if (LOCATION_SCOPE_CACHE.size > 120) {
    const firstKey = LOCATION_SCOPE_CACHE.keys().next().value;
    if (firstKey) LOCATION_SCOPE_CACHE.delete(firstKey);
  }
  return scoped;
};

const resolvePromptScope = async (prompt, { startPoint, endPoint, viaPoints } = {}) => {
  const supportedAliasMentions = extractSupportedAliasMentions(prompt);
  const supportedLandmarkMentions = extractSupportedLandmarkMentions(prompt);
  const explicitCandidates = extractLocationCandidates(prompt);
  const routeSignals = buildRouteContextScopeSignals({ startPoint, endPoint, viaPoints });

  for (const candidate of explicitCandidates) {
    try {
      const scoped = await resolveLocationCandidateScope(candidate);
      if (scoped.supported === false) return scoped;
      if (scoped.supported === true) {
        return {
          supported: true,
          requestedLocation: candidate,
          resolvedLocation: scoped.resolvedLocation || SUPPORTED_CITY,
          reason: scoped.reason,
        };
      }
    } catch {
      if (supportedAliasMentions.length) {
        return {
          supported: true,
          requestedLocation: supportedAliasMentions[0],
          resolvedLocation: SUPPORTED_CITY,
          reason: "prompt_supported_alias",
        };
      }
    }
  }

  if (supportedAliasMentions.length) {
    return {
      supported: true,
      requestedLocation: supportedAliasMentions[0],
      resolvedLocation: SUPPORTED_CITY,
      reason: "prompt_supported_alias",
    };
  }

  if (supportedLandmarkMentions.length && routeSignals.londonSignalCount >= 2) {
    return {
      supported: true,
      requestedLocation: supportedLandmarkMentions[0],
      resolvedLocation: SUPPORTED_CITY,
      reason: "prompt_landmark_and_route_context",
    };
  }

  try {
    const llmScope = await classifyPlannerScopeWithLlm({
      prompt,
      routeContext: {
        supported_city: SUPPORTED_CITY,
        start_point: startPoint || null,
        end_point: endPoint || null,
        via_count: Array.isArray(viaPoints) ? viaPoints.length : 0,
        route_signals: routeSignals,
      },
    });
    if (llmScope.ok && llmScope.confidence >= 0.7) {
      return {
        supported: llmScope.supported,
        requestedLocation: llmScope.requestedLocation,
        resolvedLocation: llmScope.supported ? SUPPORTED_CITY : llmScope.requestedLocation,
        reason: `llm_scope_classifier:${llmScope.reason || "n/a"}`,
      };
    }
  } catch {
    // Best-effort fallback only.
  }

  if (explicitCandidates.length && !supportedAliasMentions.length && !supportedLandmarkMentions.length) {
    return {
      supported: false,
      requestedLocation: explicitCandidates[0],
      resolvedLocation: explicitCandidates[0],
      reason: "explicit_candidate_default_block",
    };
  }

  if (routeSignals.londonSignalCount >= 2) {
    return {
      supported: true,
      requestedLocation: null,
      resolvedLocation: SUPPORTED_CITY,
      reason: "route_context_default_allow",
    };
  }

  return {
    supported: true,
    requestedLocation: explicitCandidates[0] || supportedAliasMentions[0] || null,
    resolvedLocation: null,
    reason: "ambiguous_default_allow",
  };
};
const buildScopeGuardNarrative = ({ requestedLocation }) => {
  const place = requestedLocation || "that city";
  return [
    "JourneyPro AI Planner currently supports " + SUPPORTED_CITY + " only.",
    "Your request points to " + place + ", but the live map, POI index, route scoring, and via-point writeback are all scoped to " + SUPPORTED_CITY + ".",
    "I will not fabricate a " + place + " itinerary with " + SUPPORTED_CITY + " data.",
    "Please switch the request to " + SUPPORTED_CITY + ", or set a " + SUPPORTED_CITY + " route on the map and try again.",
  ].join(" ");
};

const includesAnyKeyword = (text, keywords) => keywords.some((keyword) => text.includes(keyword));

const keywordHits = (text, keywords) => {
  let score = 0;
  for (const keyword of keywords) {
    if (text.includes(keyword)) score += 1;
  }
  return score;
};

const countRegexMatches = (text, regex) => {
  const target = String(text || "");
  if (!target) return 0;
  const flags = regex.flags.includes("g") ? regex.flags : `${regex.flags}g`;
  const instance = new RegExp(regex.source, flags);
  let count = 0;
  let match = instance.exec(target);
  while (match) {
    count += 1;
    match = instance.exec(target);
  }
  return count;
};

const keywordPattern = (keyword) => {
  const normalized = normalizeText(keyword);
  if (!normalized) return "";
  if (normalized.includes(" ")) return escapeRegex(normalized).replace(/\s+/g, "\\s+");
  if (normalized.endsWith("y") && normalized.length > 3) {
    return `${escapeRegex(normalized.slice(0, -1))}(?:y|ies)`;
  }
  if (/(x|ch|sh)$/i.test(normalized)) {
    return `${escapeRegex(normalized)}(?:es)?`;
  }
  if (normalized.endsWith("s")) {
    return escapeRegex(normalized);
  }
  return `${escapeRegex(normalized)}s?`;
};

const parseUserId = (value) => {
  const uid = Number.parseInt(value || "0", 10);
  return Number.isFinite(uid) && uid > 0 ? uid : null;
};

const parseLngLat = (value, fallback = null) => {
  if (!value) return fallback;
  if (typeof value === "string") {
    const [lng, lat] = value.split(",").map(Number);
    if (Number.isFinite(lng) && Number.isFinite(lat)) return { lng, lat };
    return fallback;
  }
  if (typeof value === "object") {
    const lng = Number(value.lng);
    const lat = Number(value.lat);
    if (Number.isFinite(lng) && Number.isFinite(lat)) return { lng, lat };
  }
  return fallback;
};

const parseViaPoints = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.map((item) => parseLngLat(item)).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(";")
      .map((item) => parseLngLat(item))
      .filter(Boolean);
  }
  return [];
};

const toKm = (meters) => {
  const value = Number(meters || 0);
  if (!Number.isFinite(value) || value <= 0) return "0.0 km";
  return `${(value / 1000).toFixed(1)} km`;
};

const toMin = (seconds) => {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return "0 min";
  return `${Math.max(1, Math.round(value / 60))} min`;
};

const inferCategoryHint = (prompt) => {
  const text = normalizeText(prompt);
  if (!text) return null;
  for (const item of CATEGORY_HINTS) {
    if (item.keywords.some((word) => text.includes(word))) return item.category;
  }
  return null;
};

const extractDurationHint = (text) => {
  const dayMatch = text.match(/(\d+)\s*(day|days|d)\b/);
  if (dayMatch) {
    const days = Number(dayMatch[1]);
    if (Number.isFinite(days) && days > 0 && days <= 14) return `${days} day${days > 1 ? "s" : ""}`;
  }
  const hourMatch = text.match(/(\d+)\s*(hour|hours|hr|hrs|h)\b/);
  if (hourMatch) {
    const hours = Number(hourMatch[1]);
    if (Number.isFinite(hours) && hours > 0 && hours <= 72) return `${hours} hour${hours > 1 ? "s" : ""}`;
  }
  return null;
};

const extractIntentTags = (text) => {
  const rawTokens = text
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length >= 3 && token.length <= 20 && !STOP_WORDS.has(token) && !/^\d+$/.test(token));

  const counts = new Map();
  rawTokens.forEach((token) => {
    counts.set(token, (counts.get(token) || 0) + 1);
  });

  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 6)
    .map(([token]) => token);
};

const scoreCategoryIntent = (text, hint) => {
  const keywords = Array.isArray(hint?.keywords) ? hint.keywords : [];
  let baseScore = 0;
  let positiveScore = 0;
  let negativeScore = 0;

  for (const keyword of keywords) {
    const pattern = keywordPattern(keyword);
    baseScore += countRegexMatches(text, new RegExp(`\\b${pattern}\\b`, "i"));
    positiveScore += countRegexMatches(
      text,
      new RegExp(`\\b(?:like|love|prefer|enjoy|focus\\s+on|interested\\s+in|more\\s+of)\\b(?:\\s+\\w+){0,5}\\s+${pattern}\\b`, "i")
    );
    negativeScore += countRegexMatches(
      text,
      new RegExp(`\\b(?:avoid|skip|without|less|rather\\s+than|instead\\s+of|not\\s+into)\\b(?:\\s+\\w+){0,5}\\s+${pattern}\\b`, "i")
    );
    negativeScore += countRegexMatches(
      text,
      new RegExp(`\\b(?:do\\s+not|don\\s+t|dont|no)\\b(?:\\s+\\w+){0,3}\\s+want(?:\\s+\\w+){0,10}\\s+${pattern}\\b`, "i")
    );
    negativeScore += countRegexMatches(
      text,
      new RegExp(`\\b(?:not|no)\\b(?:\\s+\\w+){0,2}\\s+(?:just|only)(?:\\s+\\w+){0,6}\\s+${pattern}\\b`, "i")
    );
    negativeScore += countRegexMatches(
      text,
      new RegExp(`\\bjust\\s+like(?:\\s+\\w+){0,4}\\s+${pattern}\\b`, "i")
    );
  }

  const totalScore = baseScore + positiveScore * 2 - negativeScore * 3;
  return {
    category: hint.category,
    baseScore,
    positiveScore,
    negativeScore,
    totalScore,
  };
};

const extractCategoryIntentProfile = (text) =>
  CATEGORY_HINTS.map((hint) => scoreCategoryIntent(text, hint))
    .filter((row) => row.baseScore > 0 || row.positiveScore > 0 || row.negativeScore > 0)
    .sort((a, b) => b.totalScore - a.totalScore || b.positiveScore - a.positiveScore || a.category.localeCompare(b.category));

const extractAvoidCategories = (text, categorySignals = null) => {
  const avoidCategories = new Set();
  (Array.isArray(categorySignals) ? categorySignals : []).forEach((row) => {
    if (row.negativeScore > 0) avoidCategories.add(row.category);
  });

  for (const hint of CATEGORY_HINTS) {
    for (const keyword of hint.keywords) {
      for (const avoidWord of AVOID_KEYWORDS) {
        if (text.includes(`${avoidWord}${keyword}`) || text.includes(`${avoidWord} ${keyword}`)) {
          avoidCategories.add(hint.category);
        }
      }
    }
  }
  return avoidCategories;
};

const selectPlannerCategoryHint = (intent) => {
  const signals = Array.isArray(intent?.categorySignals) ? intent.categorySignals : [];
  const avoidSet = new Set(Array.isArray(intent?.avoidCategories) ? intent.avoidCategories : []);
  const candidates = signals.filter((row) => row.totalScore > 0 && !avoidSet.has(row.category));
  if (!candidates.length) return null;

  const [first, second] = candidates;
  if (first.positiveScore > 0 && first.negativeScore === 0 && (!second || first.totalScore >= second.totalScore + 1)) {
    return first.category;
  }
  if (!second && first.totalScore >= 2) {
    return first.category;
  }
  return null;
};

const parsePromptIntent = (prompt) => {
  const text = normalizeText(prompt);
  const categorySignals = extractCategoryIntentProfile(text);
  const avoidCategories = extractAvoidCategories(text, categorySignals);
  const avoidSet = new Set(avoidCategories);
  const preferredCategories = categorySignals
    .filter((row) => row.totalScore > 0 && !avoidSet.has(row.category))
    .map((row) => row.category);
  const hasExploreSignal = includesAnyKeyword(text, EXPLORE_KEYWORDS);
  const hasSafeSignal = includesAnyKeyword(text, SAFE_KEYWORDS);
  const hasLowDetourSignal = includesAnyKeyword(text, LOW_DETOUR_KEYWORDS);
  const hasFastSignal = includesAnyKeyword(text, FAST_PACE_KEYWORDS);
  const hasRelaxedSignal = includesAnyKeyword(text, RELAXED_PACE_KEYWORDS);
  const hasFamilySignal = includesAnyKeyword(text, FAMILY_KEYWORDS);
  const hasRainSignal = includesAnyKeyword(text, RAIN_KEYWORDS);
  const hasPhotoSignal = includesAnyKeyword(text, PHOTO_KEYWORDS);
  const hasNightSignal = includesAnyKeyword(text, NIGHT_KEYWORDS);

  const intentTags = extractIntentTags(text);
  const durationHint = extractDurationHint(text);
  const pace = hasFastSignal && !hasRelaxedSignal ? "fast" : hasRelaxedSignal ? "relaxed" : "balanced";
  const exploration = hasExploreSignal && !hasSafeSignal ? "explore" : hasSafeSignal ? "safe" : "balanced";

  return {
    text,
    categorySignals,
    preferredCategories,
    avoidCategories: [...avoidCategories],
    hasExploreSignal,
    hasSafeSignal,
    hasLowDetourSignal,
    hasFamilySignal,
    hasRainSignal,
    hasPhotoSignal,
    hasNightSignal,
    pace,
    exploration,
    durationHint,
    intentTags,
  };
};

const tuneWeightsByIntent = ({ baseInterestWeight, baseExploreWeight, intent }) => {
  let interestWeight = clamp01(baseInterestWeight);
  let exploreWeight = clamp01(baseExploreWeight);

  if (intent.hasLowDetourSignal) {
    interestWeight -= 0.09;
    exploreWeight -= 0.08;
  }
  if (intent.pace === "fast") {
    interestWeight -= 0.06;
    exploreWeight -= 0.05;
  }
  if (intent.pace === "relaxed") {
    interestWeight += 0.05;
  }
  if (intent.exploration === "explore") {
    interestWeight += 0.03;
    exploreWeight += 0.14;
  }
  if (intent.exploration === "safe") {
    exploreWeight -= 0.1;
  }
  if (intent.hasFamilySignal) {
    exploreWeight -= 0.04;
  }

  return {
    interestWeight: clamp(interestWeight, 0.08, 0.92),
    exploreWeight: clamp(exploreWeight, 0.03, 0.72),
  };
};

const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const calcExploreSignal = (scores) => {
  const novelty = clamp01(scores?.novelty);
  const quality = clamp01(scores?.quality);
  const context = clamp01(scores?.context);
  const banditBonus = safeNumber(scores?.bandit_bonus);
  const banditNorm = clamp01((banditBonus + 1) / 2);
  return clamp01(novelty * 0.52 + quality * 0.18 + context * 0.15 + banditNorm * 0.15);
};

const buildAiAdjustment = (item, intent) => {
  const reasons = [];
  const category = String(item?.category || "").toLowerCase();
  const distanceM = safeNumber(item?.distance_m || item?.distance);
  const detourMin = safeNumber(item?.detour_duration_s) / 60;
  const distanceFit = clamp01(item?.scores?.distance ?? item?.distance_score);
  const qualityFit = clamp01(item?.scores?.quality);
  const noveltyFit = clamp01(item?.scores?.novelty);

  let delta = 0;
  if (intent.preferredCategories.includes(category)) {
    delta += 0.11;
    reasons.push(`matches preferred ${category}`);
  }
  if (intent.avoidCategories.includes(category)) {
    delta -= 0.26;
    reasons.push(`deprioritized by avoid-${category}`);
  }

  if (intent.hasLowDetourSignal) {
    if (detourMin <= 8) {
      delta += 0.08;
      reasons.push("low detour");
    } else if (detourMin >= 16) {
      delta -= 0.14;
      reasons.push("detour too long");
    }
  }

  if (intent.pace === "fast") {
    if (detourMin <= 10 && distanceM <= 1200) {
      delta += 0.09;
      reasons.push("fast-pace friendly");
    } else if (detourMin >= 18) {
      delta -= 0.13;
    }
  } else if (intent.pace === "relaxed") {
    if (qualityFit >= 0.62) {
      delta += 0.05;
      reasons.push("good stay quality");
    }
  }

  if (intent.exploration === "explore") {
    const boost = noveltyFit * 0.12;
    if (boost > 0.01) {
      delta += boost;
      reasons.push("exploration bonus");
    }
  } else if (intent.exploration === "safe") {
    delta += qualityFit * 0.08;
    delta -= noveltyFit * 0.06;
    reasons.push("safe preference");
  }

  if (intent.hasFamilySignal && category === "nightlife") {
    delta -= 0.16;
    reasons.push("not family-friendly priority");
  }
  if (intent.hasRainSignal && category === "nature") {
    delta -= 0.12;
    reasons.push("rain condition penalty");
  }
  if (intent.hasPhotoSignal && (category === "attraction" || category === "nature")) {
    delta += 0.06;
    reasons.push("photo spot");
  }
  if (intent.hasNightSignal && category === "nightlife") {
    delta += 0.07;
    reasons.push("night activity");
  }
  if (distanceFit >= 0.72 && distanceM <= 600) {
    delta += 0.04;
    reasons.push("on-route");
  }

  return {
    delta: clamp(delta, -0.38, 0.38),
    reasons: reasons.slice(0, 3),
  };
};

const normalizeCategoryKey = (value) => String(value || "").trim().toLowerCase() || "unknown";

const buildCategoryMixPolicy = ({ intent, outputLimit }) => {
  const safeLimit = Math.max(1, Number(outputLimit) || 0);
  const avoidSet = new Set(Array.isArray(intent?.avoidCategories) ? intent.avoidCategories : []);
  const preferred = (Array.isArray(intent?.preferredCategories) ? intent.preferredCategories : []).filter(
    (category) => !avoidSet.has(category)
  );
  const preferredSet = new Set(preferred);
  const strongestPreferredSignal =
    (Array.isArray(intent?.categorySignals) ? intent.categorySignals : []).find((row) => preferredSet.has(row.category)) || null;
  const focusedSingleCategory =
    preferred.length === 1 &&
    strongestPreferredSignal &&
    (strongestPreferredSignal.positiveScore > 0 || strongestPreferredSignal.totalScore >= 2);

  const softCap = clamp(
    focusedSingleCategory ? Math.ceil(safeLimit * 0.62) : Math.ceil(safeLimit * 0.4),
    2,
    Math.max(2, safeLimit - 1)
  );
  const hardCap = clamp(
    focusedSingleCategory ? Math.ceil(safeLimit * 0.82) : Math.ceil(safeLimit * 0.55),
    softCap,
    safeLimit
  );

  return {
    preferredSet,
    avoidSet,
    focusedSingleCategory,
    softCap,
    hardCap,
  };
};

const hasAlternativeCategoryCapacity = ({ remaining, counts, currentCategory, softCap }) =>
  remaining.some((item) => {
    const category = normalizeCategoryKey(item?.category);
    if (category === currentCategory) return false;
    return (counts.get(category) || 0) < softCap;
  });

const selectDiversifiedItems = ({ candidates, intent, outputLimit }) => {
  const remaining = Array.isArray(candidates) ? [...candidates] : [];
  const selected = [];
  const counts = new Map();
  const safeLimit = Math.max(1, Number(outputLimit) || 0);
  const policy = buildCategoryMixPolicy({ intent, outputLimit: safeLimit });

  while (selected.length < safeLimit && remaining.length) {
    let bestIndex = 0;
    let bestAdjustedScore = Number.NEGATIVE_INFINITY;
    let bestPenalty = 0;
    let bestCategoryCountBefore = 0;
    let bestReasons = [];

    for (let index = 0; index < remaining.length; index += 1) {
      const item = remaining[index];
      const category = normalizeCategoryKey(item?.category);
      const categoryCountBefore = counts.get(category) || 0;
      const baseScore = safeNumber(item?.ai_meta?.ai_final);
      const reasons = [];
      let adjustedScore = baseScore;
      let penalty = 0;

      if (categoryCountBefore === 0) {
        adjustedScore += 0.045;
        reasons.push("category mix bonus");
      } else if (categoryCountBefore === 1) {
        adjustedScore += 0.012;
      }

      if (categoryCountBefore >= policy.softCap) {
        const repeatPenalty = 0.11 * (categoryCountBefore - policy.softCap + 1);
        adjustedScore -= repeatPenalty;
        penalty += repeatPenalty;
        reasons.push("repeat category penalty");
      }

      if (categoryCountBefore >= policy.hardCap) {
        const altAvailable = hasAlternativeCategoryCapacity({
          remaining,
          counts,
          currentCategory: category,
          softCap: policy.softCap,
        });
        if (altAvailable) {
          adjustedScore -= 0.32;
          penalty += 0.32;
          reasons.push("hard cap penalty");
        }
      }

      if (policy.avoidSet.has(category)) {
        adjustedScore -= 0.08;
        penalty += 0.08;
      }

      if (policy.preferredSet.size && !policy.preferredSet.has(category)) {
        const preferredSelected = [...counts.entries()].reduce(
          (sum, [key, value]) => sum + (policy.preferredSet.has(key) ? value : 0),
          0
        );
        if (preferredSelected < Math.min(policy.softCap, safeLimit) && selected.length < Math.min(4, safeLimit - 1)) {
          adjustedScore -= 0.03;
          penalty += 0.03;
          reasons.push("preferred-category coverage");
        }
      }

      if (adjustedScore > bestAdjustedScore) {
        bestIndex = index;
        bestAdjustedScore = adjustedScore;
        bestPenalty = penalty;
        bestCategoryCountBefore = categoryCountBefore;
        bestReasons = reasons;
      }
    }

    const [chosen] = remaining.splice(bestIndex, 1);
    const chosenCategory = normalizeCategoryKey(chosen?.category);
    counts.set(chosenCategory, (counts.get(chosenCategory) || 0) + 1);
    selected.push({
      ...chosen,
      ai_meta: {
        ...(chosen?.ai_meta || {}),
        category_count_before: bestCategoryCountBefore,
        category_adjusted_score: Number(bestAdjustedScore.toFixed(6)),
        category_penalty: Number(bestPenalty.toFixed(6)),
      },
      explanations: [
        ...(Array.isArray(chosen?.explanations) ? chosen.explanations : []),
        ...bestReasons.map((reason) => ({ tag: reason, contribution: null })),
      ].slice(0, 6),
    });
  }

  return selected;
};

const rerankByIntent = ({ items, interestWeight, exploreWeight, intent, outputLimit }) => {
  const withScore = (Array.isArray(items) ? items : []).map((item) => {
    const distanceScore = clamp01(item?.scores?.distance ?? item?.distance_score);
    const interestScore = clamp01(item?.scores?.interest ?? item?.interest_score);
    const baseFinal = clamp01(item?.scores?.final ?? item?.final_score ?? item?.base_score);

    const distanceWeight = 1 - interestWeight;
    const sliderScore = distanceScore * distanceWeight + interestScore * interestWeight;
    const exploreSignal = calcExploreSignal(item?.scores);
    const blended = sliderScore * (1 - exploreWeight) + exploreSignal * exploreWeight;

    const aiAdjust = buildAiAdjustment(item, intent);
    const aiComposite = blended * 0.8 + baseFinal * 0.2 + aiAdjust.delta;

    const explanation = [
      ...(Array.isArray(item?.explanations) ? item.explanations : []),
      ...aiAdjust.reasons.map((reason) => ({ tag: reason, contribution: null })),
    ];

    return {
      ...item,
      ai_meta: {
        blended_score: Number(blended.toFixed(6)),
        ai_delta: Number(aiAdjust.delta.toFixed(6)),
        ai_final: Number(aiComposite.toFixed(6)),
        reasons: aiAdjust.reasons,
      },
      explanations: explanation.slice(0, 6),
    };
  });

  withScore.sort((a, b) => {
    const scoreDiff = safeNumber(b?.ai_meta?.ai_final) - safeNumber(a?.ai_meta?.ai_final);
    if (scoreDiff) return scoreDiff;
    const baseDiff = safeNumber(b?.scores?.final) - safeNumber(a?.scores?.final);
    if (baseDiff) return baseDiff;
    return safeNumber(a?.distance_m || a?.distance) - safeNumber(b?.distance_m || b?.distance);
  });

  return selectDiversifiedItems({
    candidates: withScore,
    intent,
    outputLimit,
  });
};

const buildIntentSummary = (intent) => {
  const parts = [];
  if (intent.durationHint) parts.push(`duration: ${intent.durationHint}`);
  if (intent.preferredCategories.length) parts.push(`focus: ${intent.preferredCategories.slice(0, 3).join(", ")}`);
  if (intent.avoidCategories.length) parts.push(`avoid: ${intent.avoidCategories.slice(0, 2).join(", ")}`);
  if (intent.hasLowDetourSignal) parts.push("low-detour");
  if (intent.pace !== "balanced") parts.push(`${intent.pace} pace`);
  if (intent.exploration !== "balanced") {
    parts.push(intent.exploration === "explore" ? "more exploration" : "safer picks");
  }
  return parts.length ? parts.join(" | ") : "general city discovery";
};

const computeSegmentCounts = (total) => {
  const count = Math.max(0, Number(total) || 0);
  if (!count) return [0, 0, 0];
  const base = Math.floor(count / 3);
  const remainder = count % 3;
  const result = [base, base, base];
  for (let i = 0; i < remainder; i += 1) result[i] += 1;
  return result;
};

const buildSegmentSummary = ({ periodKey, stops, intent }) => {
  if (!Array.isArray(stops) || stops.length === 0) {
    return "No stops for this period.";
  }

  const first = stops[0];
  const categorySet = [...new Set(stops.map((stop) => String(stop?.category || "").toLowerCase()).filter(Boolean))];
  const categoryText = categorySet.length ? categorySet.slice(0, 2).join(" + ") : "mixed";
  const stopText = `${stops.length} stop${stops.length > 1 ? "s" : ""}`;
  const detourText = toMin(stops.reduce((sum, stop) => sum + (Number(stop?.detour_duration_s) || 0), 0));

  if (periodKey === "morning") {
    return `Start with ${first?.name || "a top stop"} (${categoryText}), ${stopText}, est. detour ${detourText}.`;
  }
  if (periodKey === "afternoon") {
    return `Focus on ${categoryText} cluster, ${stopText}, keep momentum with est. detour ${detourText}.`;
  }
  if (intent?.exploration === "explore") {
    return `Finish with exploratory picks, ${stopText}, est. detour ${detourText}.`;
  }
  return `Wrap up with stable picks, ${stopText}, est. detour ${detourText}.`;
};

const buildSegmentedItinerary = ({ items, intent, route }) => {
  const list = Array.isArray(items) ? items : [];
  const counts = computeSegmentCounts(list.length);
  const routeDistance = Number(route?.distance) || 0;
  const routeDuration = Number(route?.duration) || 0;

  const segments = [];
  let cursor = 0;
  for (let i = 0; i < ITINERARY_PERIODS.length; i += 1) {
    const period = ITINERARY_PERIODS[i];
    const take = counts[i];
    const slice = list.slice(cursor, cursor + take);
    cursor += take;
    if (!slice.length) continue;

    const stops = slice.map((stop, idx) => ({
      order: cursor - take + idx + 1,
      id: stop?.id ?? null,
      name: stop?.name || "POI",
      category: stop?.category || "",
      lat: Number(stop?.lat),
      lng: Number(stop?.lng),
      distance_m: Number(stop?.distance_m || 0),
      detour_duration_s: Number(stop?.detour_duration_s || 0),
      reason: stop?.reason || "",
    }));

    segments.push({
      period: period.key,
      label: period.label,
      title: period.title,
      summary: buildSegmentSummary({ periodKey: period.key, stops, intent }),
      stops,
    });
  }

  return {
    route_distance_m: routeDistance,
    route_duration_s: routeDuration,
    total_stops: list.length,
    segments,
  };
};

const buildNarrative = ({
  prompt,
  items,
  route,
  interestWeight,
  exploreWeight,
  intent,
  itinerary,
  knowledgePack,
  llmMode,
}) => {
  const list = Array.isArray(items) ? items.slice(0, 6) : [];
  const interestPct = Math.round(clamp(Number(interestWeight) || DEFAULT_INTEREST_WEIGHT, 0, 1) * 100);
  const distancePct = 100 - interestPct;
  const explorePct = Math.round(clamp(Number(exploreWeight) || DEFAULT_EXPLORE_WEIGHT, 0, 1) * 100);
  const safePct = 100 - explorePct;
  const tripDistance = route?.distance ? toKm(route.distance) : null;
  const tripDuration = route?.duration ? toMin(route.duration) : null;
  const top = list[0];
  const sourceCards = Array.isArray(knowledgePack?.cards) ? knowledgePack.cards : [];
  const insights = Array.isArray(knowledgePack?.insights) ? knowledgePack.insights : [];

  const lines = [
    "AI planner completed a route-aware first draft.",
    `Understood intent: ${buildIntentSummary(intent)}.`,
    `Ranking profile: Interest ${interestPct}% / Distance ${distancePct}%, Explore ${explorePct}% / Safe ${safePct}% (order only, no category suppression).`,
  ];

  if (llmMode === "fallback") {
    lines.push("External LLM output was unavailable, so this answer is generated from JourneyPro route ranking and local community evidence.");
  }

  if (tripDistance || tripDuration) {
    lines.push(`Base route estimate: ${tripDistance || "N/A"} and ${tripDuration || "N/A"}.`);
  }

  if (top) {
    lines.push(`Top anchor: ${top.name} (${top.category || "poi"}) with detour ${toMin(top.detour_duration_s)} and route distance ${toKm(top.distance_m)}.`);
  }

  if (insights.length) {
    lines.push("");
    lines.push("Community evidence:");
    insights.slice(0, 4).forEach((line) => lines.push(`- ${line}`));
  }

  if (sourceCards.length) {
    lines.push("");
    lines.push("Evidence cards:");
    sourceCards.slice(0, 4).forEach((card) => lines.push(`- ${card.type.toUpperCase()} | ${card.title}: ${card.snippet}`));
  }

  lines.push("");
  lines.push("Segmented itinerary:");
  const segments = Array.isArray(itinerary?.segments) ? itinerary.segments : [];
  if (segments.length) {
    segments.forEach((segment) => {
      lines.push(`${segment.label} - ${segment.title}: ${segment.summary}`);
      (segment.stops || []).forEach((stop) => {
        lines.push(`  - ${stop.order}. ${stop.name} (${stop.category || "poi"}) | ${toKm(stop.distance_m)} | detour ${toMin(stop.detour_duration_s)}`);
      });
    });
  } else {
    list.forEach((item, idx) => {
      const detour = toMin(item.detour_duration_s);
      const reasons = (item.ai_meta?.reasons || []).join(", ");
      lines.push(`${idx + 1}. ${item.name} (${item.category || "poi"}) | ${toKm(item.distance_m)} | detour ${detour}${reasons ? ` | ${reasons}` : ""}`);
    });
  }

  lines.push("");
  lines.push("Move the preference sliders to instantly reorder this same candidate pool.");
  lines.push(`Request summary: "${String(prompt || "").trim()}"`);
  return lines.join("\n");
};
const mapRecommendedItem = (item) => {
  const scores = {
    final: Number(item?.scores?.final ?? item?.final_score ?? item?.base_score ?? 0),
    base: Number(item?.scores?.base ?? item?.base_score ?? 0),
    bandit_bonus: Number(item?.scores?.bandit_bonus ?? 0),
    diversity_penalty: Number(item?.scores?.diversity_penalty ?? 0),
    distance: Number(item?.scores?.distance ?? item?.distance_score ?? 0),
    interest: Number(item?.scores?.interest ?? item?.interest_score ?? 0),
    quality: Number(item?.scores?.quality ?? 0),
    novelty: Number(item?.scores?.novelty ?? 0),
    context: Number(item?.scores?.context ?? 0),
  };

  return {
    id: item?.id ?? null,
    name: item?.name || "POI",
    category: item?.category || "",
    lat: Number(item?.lat),
    lng: Number(item?.lng),
    image_url: item?.image_url || "",
    reason: item?.reason || "",
    match_tags: Array.isArray(item?.match_tags) ? item.match_tags : [],
    distance_m: Number(item?.distance || 0),
    distance_to_start_m: Number(item?.distance_to_start || 0),
    distance_to_end_m: Number(item?.distance_to_end || 0),
    distance_score: scores.distance,
    interest_score: scores.interest,
    final_score: scores.final,
    detour_duration_s: Number(item?.detour?.extra_duration_s || 0),
    detour_distance_m: Number(item?.detour?.extra_distance_m || 0),
    explanations: Array.isArray(item?.explanations) ? item.explanations : [],
    ai_meta: item?.ai_meta || null,
    scores,
  };
};

const writeSse = (res, event, payload) => {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
  res.flush?.();
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const tokenizeForStream = (text) => {
  const content = String(text || "");
  if (!content) return [];
  const tokens = content.match(/\S+\s*/g);
  return Array.isArray(tokens) ? tokens : [content];
};

const streamText = async (res, text, isClosed) => {
  const tokens = tokenizeForStream(text);
  for (let i = 0; i < tokens.length; i += 1) {
    if (isClosed()) return;
    const token = tokens[i];
    writeSse(res, "delta", { token, text: token, index: i, total: tokens.length });
    // eslint-disable-next-line no-await-in-loop
    await sleep(14);
  }
};

// POST /api/ai/planner/stream
router.post("/planner/stream", async (req, res) => {
  let clientClosed = false;
  res.on("close", () => {
    clientClosed = true;
  });
  req.on("aborted", () => {
    clientClosed = true;
  });

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const requestId = crypto.randomUUID();
  const prompt = String(req.body?.prompt || "").trim();
  const userId = parseUserId(req.body?.user_id);
  const limit = normalizeInteger(req.body?.limit, 8, 3, 12);
  const mode = "driving";

  if (!prompt) {
    writeSse(res, "error", {
      request_id: requestId,
      message: "Prompt is required.",
    });
    writeSse(res, "done", { request_id: requestId, recommendation_count: 0 });
    res.end();
    return;
  }

  try {
    const settings = userId
      ? await fetchUserRecommendationSettings(userId)
      : {
          interestWeight: DEFAULT_INTEREST_WEIGHT,
          exploreWeight: DEFAULT_EXPLORE_WEIGHT,
          modeDefaults: null,
        };

    const startPoint = parseLngLat(req.body?.start, DEFAULT_START);
    const endPoint = parseLngLat(req.body?.end, DEFAULT_END);
    const viaPoints = parseViaPoints(req.body?.via);

    const rawInterestWeight = normalizeWeight(req.body?.interest_weight, settings.interestWeight);
    const rawExploreWeight = normalizeWeight(req.body?.explore_weight, settings.exploreWeight);
    const parsedIntent = parsePromptIntent(prompt);
    const tunedWeights = tuneWeightsByIntent({
      baseInterestWeight: rawInterestWeight,
      baseExploreWeight: rawExploreWeight,
      intent: parsedIntent,
    });
    const interestWeight = tunedWeights.interestWeight;
    const exploreWeight = tunedWeights.exploreWeight;
    const categoryHint =
      selectPlannerCategoryHint(parsedIntent) ||
      (parsedIntent.preferredCategories.length === 1 && !parsedIntent.avoidCategories.length
        ? parsedIntent.preferredCategories[0]
        : null);
    const scope = await resolvePromptScope(prompt, {
      startPoint,
      endPoint,
      viaPoints,
    });

    const responseLimit = clamp(limit, 3, 12);

    if (!scope.supported) {
      writeSse(res, "meta", {
        request_id: requestId,
        mode,
        prompt_summary: buildIntentSummary(parsedIntent),
        interest_weight: interestWeight,
        distance_weight: 1 - interestWeight,
        explore_weight: exploreWeight,
        intent: {
          pace: parsedIntent.pace,
          exploration: parsedIntent.exploration,
          duration_hint: parsedIntent.durationHint,
          preferred_categories: parsedIntent.preferredCategories,
          avoid_categories: parsedIntent.avoidCategories,
          tags: parsedIntent.intentTags,
        },
        category_hint: categoryHint,
        scope: {
          supported: false,
          supported_city: SUPPORTED_CITY,
          requested_location: scope.requestedLocation,
          resolved_location: scope.resolvedLocation,
        },
        route: {
          start: startPoint,
          end: endPoint,
          via_count: viaPoints.length,
        },
      });
      writeSse(res, "status", {
        stage: "scope_guard",
        message: SUPPORTED_CITY + "-only planner scope detected.",
      });
      await streamText(
        res,
        buildScopeGuardNarrative({ requestedLocation: scope.resolvedLocation || scope.requestedLocation }),
        () => clientClosed
      );
      if (clientClosed) return;
      writeSse(res, "recommendations", {
        request_id: requestId,
        mode,
        route: null,
        profile: {
          interest_weight: interestWeight,
          distance_weight: 1 - interestWeight,
          explore_weight: exploreWeight,
        },
        intent: {
          summary: buildIntentSummary(parsedIntent),
          pace: parsedIntent.pace,
          exploration: parsedIntent.exploration,
          preferred_categories: parsedIntent.preferredCategories,
          avoid_categories: parsedIntent.avoidCategories,
          tags: parsedIntent.intentTags,
        },
        itinerary: null,
        items: [],
        scope: {
          supported: false,
          supported_city: SUPPORTED_CITY,
          requested_location: scope.requestedLocation,
          resolved_location: scope.resolvedLocation,
        },
      });
      writeSse(res, "done", { request_id: requestId, recommendation_count: 0 });
      res.end();
      return;
    }
    const recallLimit = clamp(responseLimit + 8, responseLimit, 20);

    writeSse(res, "meta", {
      request_id: requestId,
      mode,
      prompt_summary: buildIntentSummary(parsedIntent),
      interest_weight: interestWeight,
      distance_weight: 1 - interestWeight,
      explore_weight: exploreWeight,
      intent: {
        pace: parsedIntent.pace,
        exploration: parsedIntent.exploration,
        duration_hint: parsedIntent.durationHint,
        preferred_categories: parsedIntent.preferredCategories,
        avoid_categories: parsedIntent.avoidCategories,
        tags: parsedIntent.intentTags,
      },
      category_hint: categoryHint,
      route: {
        start: startPoint,
        end: endPoint,
        via_count: viaPoints.length,
      },
      scope: {
        supported: true,
        supported_city: SUPPORTED_CITY,
      },
    });
    writeSse(res, "status", { stage: "analyzing", message: "Analyzing trip demand and route context..." });

    const reco = await runRecommendationV2({
      startPoint,
      endPoint,
      viaPoints,
      userId,
      requestedMode: mode,
      interestWeight,
      exploreWeight,
      limit: recallLimit,
      candidateLimit: Math.max(120, recallLimit * 16),
      category: categoryHint,
      radius: null,
      modeDefaults: settings.modeDefaults,
      requestId,
      bucket: "treatment",
      debug: false,
    });

    if (reco.error) {
      writeSse(res, "error", {
        request_id: requestId,
        message: reco.error || "Failed to generate plan.",
        detail: reco.payload || null,
      });
      writeSse(res, "done", { request_id: requestId, recommendation_count: 0 });
      res.end();
      return;
    }

    const payload = reco.payload || {};
    const mappedItems = Array.isArray(payload.recommended_pois) ? payload.recommended_pois.map(mapRecommendedItem) : [];

    const rankedItems = rerankByIntent({
      items: mappedItems,
      interestWeight,
      exploreWeight,
      intent: parsedIntent,
      outputLimit: responseLimit,
    });

    const itinerary = buildSegmentedItinerary({
      items: rankedItems,
      intent: parsedIntent,
      route: payload.base_route || null,
    });

    const knowledgePack = await buildPlannerKnowledgePack({
      prompt,
      rankedItems,
    });

    const llmConfig = getPlannerLlmConfig();
    let llmResult = {
      ok: false,
      mode: llmConfig.configured ? "fallback" : "local-fallback",
      provider: llmConfig.provider,
      model: llmConfig.model || "",
      styleProfile: llmConfig.styleProfile,
      reason: llmConfig.configured ? "not_started" : "missing_llm_config",
      text: "",
    };

    writeSse(res, "status", {
      stage: llmConfig.configured ? "retrieval" : "fallback",
      message: llmConfig.configured
        ? "Retrieved route and community evidence. Generating AI answer..."
        : "Retrieved route and community evidence. Using local fallback narrative...",
    });

    if (llmConfig.configured) {
      try {
        llmResult = await streamPlannerNarrativeFromLlm({
          prompt,
          itinerary,
          promptContext: knowledgePack.prompt_context,
          interestWeight,
          exploreWeight,
          onToken: (token) => {
            if (!token || clientClosed) return;
            writeSse(res, "delta", { token, text: token });
          },
        });
      } catch (llmErr) {
        llmResult = {
          ok: false,
          mode: "fallback",
          provider: llmConfig.provider,
          model: llmConfig.model || "",
          styleProfile: llmConfig.styleProfile,
          reason: llmErr?.message || "llm_stream_failed",
          text: "",
        };
      }
    }

    if (clientClosed) return;

    if (!llmResult.ok || !String(llmResult.text || "").trim()) {
      const narrative = buildNarrative({
        prompt,
        items: rankedItems,
        route: payload.base_route || null,
        interestWeight,
        exploreWeight,
        intent: parsedIntent,
        itinerary,
        knowledgePack,
        llmMode: "fallback",
      });
      writeSse(res, "status", { stage: "streaming", message: "Streaming fallback itinerary draft..." });
      await streamText(res, narrative, () => clientClosed);
      if (clientClosed) return;
    }

    writeSse(res, "itinerary", {
      request_id: payload.request_id || requestId,
      itinerary,
    });

    writeSse(res, "recommendations", {
      request_id: payload.request_id || requestId,
      mode: payload.mode || mode,
      route: payload.base_route
        ? {
            distance_m: Number(payload.base_route.distance || 0),
            duration_s: Number(payload.base_route.duration || 0),
          }
        : null,
      profile: payload.profile?.tuning || {
        interest_weight: interestWeight,
        distance_weight: 1 - interestWeight,
        explore_weight: exploreWeight,
      },
      intent: {
        summary: buildIntentSummary(parsedIntent),
        pace: parsedIntent.pace,
        exploration: parsedIntent.exploration,
        preferred_categories: parsedIntent.preferredCategories,
        avoid_categories: parsedIntent.avoidCategories,
        tags: parsedIntent.intentTags,
      },
      itinerary,
      items: rankedItems,
      scope: {
        supported: true,
        supported_city: SUPPORTED_CITY,
      },
      retrieval: knowledgePack.stats,
      insights: knowledgePack.insights,
      sources: knowledgePack.cards,
      llm: {
        configured: llmConfig.configured,
        provider: llmResult.provider || llmConfig.provider,
        model: llmResult.model || llmConfig.model || "",
        mode: llmResult.ok ? llmResult.mode : "fallback",
        style_profile: llmResult.styleProfile || llmConfig.styleProfile,
        reason: llmResult.reason || null,
      },
    });
    writeSse(res, "done", {
      request_id: payload.request_id || requestId,
      recommendation_count: rankedItems.length,
    });
    res.end();
  } catch (err) {
    console.error("ai planner stream error", err);
    if (!clientClosed) {
      writeSse(res, "error", {
        request_id: requestId,
        message: "Server error while generating AI plan.",
      });
      writeSse(res, "done", { request_id: requestId, recommendation_count: 0 });
      res.end();
    }
  }
});

export default router;
