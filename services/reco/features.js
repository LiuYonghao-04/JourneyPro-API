import { pool } from "../../db/connect.js";
import { computeInterestFit } from "./profiles.js";
import { clamp, MAX_DETOUR_CAP_SECONDS, round } from "./constants.js";
import { haversineMeters } from "./math.js";

const parseTagList = (value) => {
  if (!value) return [];
  return String(value)
    .split(/[,;|/]+/)
    .map((tag) => tag.trim())
    .filter(Boolean);
};

const toMapByPoiId = (rows) => {
  const map = new Map();
  (rows || []).forEach((row) => {
    const id = Number(row.poi_id || row.id);
    if (!id) return;
    map.set(id, row);
  });
  return map;
};

const fetchPoiQualityStats = async (poiIds, mode) => {
  if (!poiIds.length) return new Map();
  const [rows] = await pool.query(
    `
      SELECT poi_id, mode, impressions, interactions, add_via_count, save_count, quality_score, novelty_score
      FROM poi_quality_stats
      WHERE mode = ?
        AND poi_id IN (?)
    `,
    [mode, poiIds]
  );
  return toMapByPoiId(rows);
};

const fetchUserExposureMap = async (userId, poiIds) => {
  const uid = Number.parseInt(userId || "0", 10);
  if (!uid || !poiIds.length) return new Map();

  const [rows] = await pool.query(
    `
      SELECT poi_id, COUNT(*) AS impressions
      FROM recommendation_events
      WHERE user_id = ?
        AND event_type = 'impression'
        AND poi_id IN (?)
        AND ts >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      GROUP BY poi_id
    `,
    [uid, poiIds]
  );

  const map = new Map();
  rows.forEach((row) => {
    const id = Number(row.poi_id);
    if (!id) return;
    map.set(id, Number(row.impressions) || 0);
  });
  return map;
};

const estimateDetour = ({ candidate, samples, modeConfig, routeDurationS }) => {
  if (!samples.length) {
    return { extraDistanceM: null, extraDurationS: null, detourFit: 0.5 };
  }

  const idx = Number.isFinite(candidate.nearestSampleIndex)
    ? clamp(candidate.nearestSampleIndex, 0, samples.length - 1)
    : 0;
  const current = samples[idx] || samples[0];
  const prev = samples[Math.max(0, idx - 1)] || current;
  const next = samples[Math.min(samples.length - 1, idx + 1)] || current;

  const d1 = haversineMeters(prev.lat, prev.lng, candidate.lat, candidate.lng);
  const d2 = haversineMeters(candidate.lat, candidate.lng, next.lat, next.lng);
  const base = haversineMeters(prev.lat, prev.lng, next.lat, next.lng);

  let extraDistanceM = Math.max(0, d1 + d2 - base);
  const fallbackDistance = Math.max(Number(candidate.bestDistance) || 0, 0) * 1.9;
  if (!Number.isFinite(extraDistanceM) || extraDistanceM < fallbackDistance * 0.4) {
    extraDistanceM = fallbackDistance;
  }

  const speed = clamp(Number(modeConfig.speedMps) || 1.4, 0.8, 30);
  const extraDurationS = extraDistanceM / speed;

  const absoluteCap = Math.max(
    5 * 60,
    Math.min(MAX_DETOUR_CAP_SECONDS, Number(modeConfig.maxDetourMinutes) * 60 || 10 * 60)
  );
  const ratioCap = Number.isFinite(routeDurationS)
    ? clamp(routeDurationS * (Number(modeConfig.maxDetourRatio) || 0.35), 120, MAX_DETOUR_CAP_SECONDS)
    : absoluteCap;
  const cap = Math.min(absoluteCap, ratioCap);

  const detourFit = clamp(1 - extraDurationS / Math.max(cap, 1), 0, 1);
  return {
    extraDistanceM,
    extraDurationS,
    detourFit,
    detourCapS: cap,
  };
};

const deriveQualityFit = (candidate, qualityRow) => {
  if (qualityRow) {
    const stored = Number(qualityRow.quality_score);
    if (Number.isFinite(stored)) {
      return clamp(stored, 0, 1);
    }

    const impressions = Number(qualityRow.impressions) || 0;
    const interactions = Number(qualityRow.interactions) || 0;
    const ctr = impressions > 0 ? interactions / impressions : 0;
    return clamp((ctr * impressions + 0.12 * 30) / (impressions + 30), 0, 1);
  }

  const popularity = Number(candidate.popularity) || 0;
  return clamp(popularity / 5, 0, 1);
};

const deriveNoveltyFit = ({ qualityRow, userImpressions }) => {
  const storedNovelty = Number(qualityRow?.novelty_score);
  const globalNovelty = Number.isFinite(storedNovelty)
    ? clamp(storedNovelty, 0, 1)
    : clamp(1 / (1 + Math.log(1 + (Number(qualityRow?.impressions) || 0))), 0, 1);
  const personalNovelty = Math.exp(-Math.max(userImpressions, 0) / 4);
  return clamp(globalNovelty * 0.55 + personalNovelty * 0.45, 0, 1);
};

const buildReason = ({ topTag, distanceFit, interestFit, qualityFit, noveltyFit, detourFit }) => {
  if (interestFit >= 0.6 && topTag) {
    return `Matches your interests: ${topTag}`;
  }
  if (distanceFit >= 0.7) {
    return "Right along your route";
  }
  if (qualityFit >= 0.7) {
    return "Highly rated by travelers";
  }
  if (noveltyFit >= 0.7) {
    return "Fresh place with lower exposure";
  }
  if (detourFit >= 0.6) {
    return "Low detour impact";
  }
  return "Good fit for your route";
};

export const enrichCandidatesWithFeatures = async ({
  candidates,
  samples,
  route,
  modeConfig,
  userProfile,
  userId,
  radiusM,
}) => {
  const safeCandidates = Array.isArray(candidates) ? candidates : [];
  const poiIds = safeCandidates.map((candidate) => Number(candidate.id)).filter(Boolean);

  const [qualityStatsMap, userExposureMap] = await Promise.all([
    fetchPoiQualityStats(poiIds, modeConfig.mode),
    fetchUserExposureMap(userId, poiIds),
  ]);

  const routeDurationS = Number(route?.duration) || null;
  const segmentCounter = [0, 0, 0];
  const preliminary = [];

  for (const candidate of safeCandidates) {
    const nearestIndex = Number.isFinite(candidate.nearestSampleIndex)
      ? clamp(candidate.nearestSampleIndex, 0, Math.max(samples.length - 1, 0))
      : 0;
    const progress = samples.length > 1 ? nearestIndex / (samples.length - 1) : 0;
    const segmentIndex = progress < 1 / 3 ? 0 : progress < 2 / 3 ? 1 : 2;
    segmentCounter[segmentIndex] += 1;

    const distanceToRouteM = Number(candidate.bestDistance);
    const distanceRouteScore = Number.isFinite(distanceToRouteM)
      ? Math.exp(-distanceToRouteM / Math.max(radiusM * 1.15, 100))
      : 0.35;

    const startDistanceKm = (Number(candidate.distance_to_start) || 0) / 1000;
    const endDistanceKm = (Number(candidate.distance_to_end) || 0) / 1000;
    const endpointAffinity = clamp(
      Math.max(1 / (1 + startDistanceKm), 1 / (1 + endDistanceKm)),
      0,
      1
    );
    const distanceFit = clamp(distanceRouteScore * 0.72 + endpointAffinity * 0.28, 0, 1);

    const interest = computeInterestFit(candidate, userProfile);

    const qualityRow = qualityStatsMap.get(Number(candidate.id));
    const qualityFit = deriveQualityFit(candidate, qualityRow);
    const userImpressions = Number(userExposureMap.get(Number(candidate.id)) || 0);
    const noveltyFit = deriveNoveltyFit({ qualityRow, userImpressions });

    const detour = estimateDetour({
      candidate,
      samples,
      modeConfig,
      routeDurationS,
    });

    const walkingBonus = modeConfig.mode === "walking" ? distanceFit * 0.2 : 0;
    const drivingBonus = modeConfig.mode === "driving" ? qualityFit * 0.1 : 0;
    const contextFit = clamp(detour.detourFit * 0.62 + distanceFit * 0.28 + walkingBonus + drivingBonus, 0, 1);

    const parsedTags = parseTagList(candidate.tags);

    preliminary.push({
      ...candidate,
      tags_list: parsedTags,
      route_segment: segmentIndex,
      route_progress: progress,
      distance_fit: round(distanceFit, 6),
      interest_fit: round(interest.score, 6),
      quality_fit: round(qualityFit, 6),
      novelty_fit: round(noveltyFit, 6),
      context_fit: round(contextFit, 6),
      detour_fit: round(detour.detourFit, 6),
      detour_extra_distance_m: Math.round(detour.extraDistanceM || 0),
      detour_extra_duration_s: Math.round(detour.extraDurationS || 0),
      detour_cap_s: Math.round(detour.detourCapS || 0),
      topTag: interest.topTag,
      match_tags: interest.matchTags,
      reason: buildReason({
        topTag: interest.topTag,
        distanceFit,
        interestFit: interest.score,
        qualityFit,
        noveltyFit,
        detourFit: detour.detourFit,
      }),
    });
  }

  const maxSegment = Math.max(...segmentCounter, 1);
  const dedupeSet = new Set();
  const filterDropCounts = {
    detour: 0,
    out_of_scope: 0,
    duplicate: 0,
  };

  const filtered = [];
  for (const row of preliminary) {
    const key = `${String(row.name || "").toLowerCase()}|${row.lat.toFixed(5)}|${row.lng.toFixed(5)}`;
    if (dedupeSet.has(key)) {
      filterDropCounts.duplicate += 1;
      continue;
    }

    const detourDuration = Number(row.detour_extra_duration_s) || 0;
    const detourCap = Number(row.detour_cap_s) || Number.MAX_SAFE_INTEGER;
    if (detourDuration > detourCap) {
      filterDropCounts.detour += 1;
      continue;
    }

    const distanceToRoute = Number(row.bestDistance);
    if (Number.isFinite(distanceToRoute) && distanceToRoute > radiusM * 2.5) {
      filterDropCounts.out_of_scope += 1;
      continue;
    }

    dedupeSet.add(key);
    const coverageFit = clamp(1 - (segmentCounter[row.route_segment] - 1) / maxSegment, 0, 1);
    filtered.push({
      ...row,
      coverage_fit: round(coverageFit, 6),
    });
  }

  return {
    candidates: filtered,
    filterDropCounts,
  };
};
