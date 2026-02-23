import axios from "axios";
import { pool } from "../../db/connect.js";
import { getNearbyPOIs } from "../../models/poi.js";
import { clamp } from "./constants.js";
import { haversineMeters, mapWithConcurrency } from "./math.js";

const OSRM_URL = process.env.OSRM_URL || "http://localhost:5000";

const pickRouteProfile = (modeConfig) => modeConfig?.osrmProfile || "driving";

const fetchRouteWithProfile = async (coordString, profile) => {
  const url = `${OSRM_URL}/route/v1/${profile}/${coordString}?overview=full&geometries=geojson&steps=false`;
  const response = await axios.get(url, { timeout: 12000 });
  return response?.data?.routes?.[0] || null;
};

export const buildRouteByMode = async ({ waypoints, modeConfig }) => {
  const coordString = waypoints.map((point) => `${point.lng},${point.lat}`).join(";");
  const requestedProfile = pickRouteProfile(modeConfig);
  let resolvedMode = modeConfig.mode;
  let warning = null;

  let route = null;
  try {
    route = await fetchRouteWithProfile(coordString, requestedProfile);
  } catch (err) {
    if (requestedProfile !== "driving") {
      warning = `OSRM profile ${requestedProfile} unavailable, fallback to driving.`;
    } else {
      throw err;
    }
  }

  if (!route && requestedProfile !== "driving") {
    route = await fetchRouteWithProfile(coordString, "driving");
    resolvedMode = "driving";
  }

  if (!route || !route.geometry || !Array.isArray(route.geometry.coordinates)) {
    return {
      route: null,
      resolvedMode,
      fallbackUsed: resolvedMode !== modeConfig.mode,
      warning: warning || "No route found",
    };
  }

  return {
    route,
    resolvedMode,
    fallbackUsed: resolvedMode !== modeConfig.mode,
    warning,
  };
};

export const sampleRoutePoints = (coords, stepM, maxSamples) => {
  if (!Array.isArray(coords) || coords.length < 2) return [];
  const step = Math.max(Number(stepM) || 0, 50);
  const max = Math.max(Number.parseInt(maxSamples || "0", 10) || 0, 8);

  const samples = [];
  const [lng0, lat0] = coords[0];
  samples.push({ lat: lat0, lng: lng0, routeIndex: 0 });

  let travelled = 0;
  let nextAt = step;

  for (let i = 1; i < coords.length; i += 1) {
    const [lng1, lat1] = coords[i - 1];
    const [lng2, lat2] = coords[i];
    const segment = haversineMeters(lat1, lng1, lat2, lng2);
    if (!segment || !Number.isFinite(segment)) continue;

    while (travelled + segment >= nextAt) {
      const t = (nextAt - travelled) / segment;
      const lat = lat1 + (lat2 - lat1) * t;
      const lng = lng1 + (lng2 - lng1) * t;
      samples.push({ lat, lng, routeIndex: i });
      if (samples.length >= max) return samples;
      nextAt += step;
    }

    travelled += segment;
  }

  const [lngLast, latLast] = coords[coords.length - 1];
  const last = samples[samples.length - 1];
  if (!last || haversineMeters(last.lat, last.lng, latLast, lngLast) > 60) {
    samples.push({ lat: latLast, lng: lngLast, routeIndex: coords.length - 1 });
  }

  return samples.slice(0, max);
};

const findNearestSample = (samples, lat, lng) => {
  if (!Array.isArray(samples) || !samples.length) {
    return { index: null, distanceM: Number.POSITIVE_INFINITY };
  }

  let bestIndex = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let i = 0; i < samples.length; i += 1) {
    const sample = samples[i];
    const distance = haversineMeters(lat, lng, sample.lat, sample.lng);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIndex = i;
    }
  }

  return { index: bestIndex, distanceM: bestDistance };
};

const mergeCandidate = (candidateMap, poi, context) => {
  const id = Number(poi?.id);
  if (!id) return;

  const lat = Number(poi.lat);
  const lng = Number(poi.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

  const current = candidateMap.get(id);
  if (!current) {
    candidateMap.set(id, {
      id,
      name: poi.name,
      category: poi.category,
      lat,
      lng,
      popularity: Number(poi.popularity) || 0,
      price: Number(poi.price) || null,
      tags: poi.tags || "",
      image_url: poi.image_url || null,
      address: poi.address || null,
      city: poi.city || null,
      country: poi.country || null,
      hits: 1,
      bestDistance: Number.isFinite(context.distanceM) ? context.distanceM : Number(poi.distance) || Infinity,
      nearestSampleIndex: Number.isFinite(context.sampleIndex) ? context.sampleIndex : null,
      sourceSet: new Set(context.source ? [context.source] : []),
    });
    return;
  }

  current.hits += 1;
  const distance = Number.isFinite(context.distanceM) ? context.distanceM : Number(poi.distance) || Infinity;
  if (distance < current.bestDistance) {
    current.bestDistance = distance;
    if (Number.isFinite(context.sampleIndex)) {
      current.nearestSampleIndex = context.sampleIndex;
    }
  }
  if (Number.isFinite(context.sampleIndex)) {
    if (!Number.isFinite(current.nearestSampleIndex)) current.nearestSampleIndex = context.sampleIndex;
  }
  if (context.source) current.sourceSet.add(context.source);
};

const maybeRecallNovelty = async ({ mode, samples, radiusM, limit, minQuality = 0.15 }) => {
  if (!samples.length || limit <= 0) return [];

  const lats = samples.map((sample) => sample.lat);
  const lngs = samples.map((sample) => sample.lng);
  const latPad = (radiusM * 2.4) / 111320;
  const lngPad = (radiusM * 2.4) / 111320;
  const minLat = Math.min(...lats) - latPad;
  const maxLat = Math.max(...lats) + latPad;
  const minLng = Math.min(...lngs) - lngPad;
  const maxLng = Math.max(...lngs) + lngPad;

  const [rows] = await pool.query(
    `
      SELECT
        p.id,
        p.name,
        p.category,
        p.lat,
        p.lng,
        p.popularity,
        p.price,
        p.tags,
        p.image_url,
        p.address,
        p.city,
        p.country,
        COALESCE(q.impressions, 0) AS impressions,
        COALESCE(q.quality_score, 0) AS quality_score
      FROM poi p
      LEFT JOIN poi_quality_stats q ON q.poi_id = p.id AND q.mode = ?
      WHERE p.lat BETWEEN ? AND ?
        AND p.lng BETWEEN ? AND ?
        AND (q.quality_score IS NULL OR q.quality_score >= ?)
      ORDER BY COALESCE(q.impressions, 0) ASC, COALESCE(q.quality_score, 0) DESC, p.popularity DESC
      LIMIT ?
    `,
    [mode, minLat, maxLat, minLng, maxLng, minQuality, limit]
  );

  const withinCorridor = [];
  for (const row of rows) {
    const nearest = findNearestSample(samples, Number(row.lat), Number(row.lng));
    if (nearest.distanceM > radiusM * 2.2) continue;
    withinCorridor.push({ ...row, distance: nearest.distanceM, sampleIndex: nearest.index });
  }

  return withinCorridor;
};

export const recallCandidates = async ({
  route,
  startPoint,
  endPoint,
  modeConfig,
  userProfile,
  category = null,
  candidateLimit = 180,
  perSampleLimit = 18,
  radiusOverride = null,
}) => {
  const coords = route?.geometry?.coordinates || [];
  const routeDistance = Number(route?.distance) || 0;

  const maxSamples = clamp(Math.ceil(routeDistance / Math.max(modeConfig.sampleStepM, 80)), 12, 72);
  const dynamicStep = routeDistance > 0 ? Math.ceil(routeDistance / maxSamples) : modeConfig.sampleStepM;
  const sampleStepM = Math.max(modeConfig.sampleStepM, dynamicStep);
  const radiusM = clamp(Number(radiusOverride) || modeConfig.corridorRadiusM, 120, 4000);
  const samples = sampleRoutePoints(coords, sampleStepM, maxSamples);

  const candidateMap = new Map();
  const recallCounts = {
    corridor: 0,
    endpoint: 0,
    preference: 0,
    novelty: 0,
    rawCorridor: 0,
    rawEndpoint: 0,
    rawPreference: 0,
    rawNovelty: 0,
  };

  await mapWithConcurrency(samples, 6, async (sample, sampleIndex) => {
    const nearby = await getNearbyPOIs(sample.lat, sample.lng, radiusM, perSampleLimit, category || null);
    recallCounts.rawCorridor += nearby.length;
    nearby.forEach((poi) => {
      const distanceM = Number(poi.distance);
      mergeCandidate(candidateMap, poi, {
        source: "corridor",
        sampleIndex,
        distanceM: Number.isFinite(distanceM) ? distanceM : null,
      });
    });
  });

  const endpointPoints = [startPoint, endPoint];
  for (let i = 0; i < endpointPoints.length; i += 1) {
    const point = endpointPoints[i];
    // eslint-disable-next-line no-await-in-loop
    const nearby = await getNearbyPOIs(point.lat, point.lng, Math.round(radiusM * 0.75), 24, category || null);
    recallCounts.rawEndpoint += nearby.length;
    nearby.forEach((poi) => {
      const nearest = findNearestSample(samples, Number(poi.lat), Number(poi.lng));
      mergeCandidate(candidateMap, poi, {
        source: "endpoint",
        sampleIndex: nearest.index,
        distanceM: nearest.distanceM,
      });
    });
  }

  const preferredCategories = (userProfile?.topCategories || []).filter(Boolean).slice(0, 3);
  const anchors = samples.length
    ? [samples[0], samples[Math.floor(samples.length / 2)], samples[samples.length - 1]].filter(Boolean)
    : [];

  for (const categoryName of preferredCategories) {
    for (const anchor of anchors) {
      // eslint-disable-next-line no-await-in-loop
      const nearby = await getNearbyPOIs(anchor.lat, anchor.lng, Math.round(radiusM * 1.15), 18, categoryName);
      recallCounts.rawPreference += nearby.length;
      nearby.forEach((poi) => {
        const nearest = findNearestSample(samples, Number(poi.lat), Number(poi.lng));
        mergeCandidate(candidateMap, poi, {
          source: "preference",
          sampleIndex: nearest.index,
          distanceM: nearest.distanceM,
        });
      });
    }
  }

  const noveltyNeeded = Math.max(0, Math.min(candidateLimit, 60) - candidateMap.size);
  if (noveltyNeeded > 0) {
    const noveltyRows = await maybeRecallNovelty({
      mode: modeConfig.mode,
      samples,
      radiusM,
      limit: noveltyNeeded * 2,
      minQuality: 0.12,
    });
    recallCounts.rawNovelty += noveltyRows.length;
    noveltyRows.forEach((poi) => {
      mergeCandidate(candidateMap, poi, {
        source: "novelty",
        sampleIndex: poi.sampleIndex,
        distanceM: Number(poi.distance),
      });
    });
  }

  const candidates = Array.from(candidateMap.values()).map((candidate) => {
    const startDistance = haversineMeters(candidate.lat, candidate.lng, startPoint.lat, startPoint.lng);
    const endDistance = haversineMeters(candidate.lat, candidate.lng, endPoint.lat, endPoint.lng);
    const sourceList = [...candidate.sourceSet.values()];

    sourceList.forEach((source) => {
      if (source === "corridor") recallCounts.corridor += 1;
      if (source === "endpoint") recallCounts.endpoint += 1;
      if (source === "preference") recallCounts.preference += 1;
      if (source === "novelty") recallCounts.novelty += 1;
    });

    return {
      ...candidate,
      bestDistance: Number.isFinite(candidate.bestDistance) ? candidate.bestDistance : null,
      distance_to_start: Math.round(startDistance),
      distance_to_end: Math.round(endDistance),
      sourceList,
    };
  });

  return {
    candidates,
    samples,
    sampleStepM,
    radiusM,
    recallCounts,
  };
};
