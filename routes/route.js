import express from "express";
import {
  DEFAULT_CANDIDATE_LIMIT,
  DEFAULT_EXPLORE_WEIGHT,
  DEFAULT_INTEREST_WEIGHT,
  DEFAULT_LIMIT,
  clamp,
  normalizeInteger,
  normalizeMode,
  normalizeWeight,
  parseBoolFlag,
} from "../services/reco/constants.js";
import { assignRecommendationBucket } from "../services/reco/ab.js";
import { fetchOsrmRoute } from "../services/osrm/client.js";
import { runRecommendationV2 } from "../services/reco/ranker.js";
import { ensureRecoTables } from "../services/reco/schema.js";
import { fetchUserRecommendationSettings } from "../services/reco/profiles.js";

const router = express.Router();

const haversineMeters = (lat1, lng1, lat2, lng2) => {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
};

const buildApproxRoute = (points, speedMps = 13.89) => {
  const safePoints = (points || [])
    .filter((p) => Number.isFinite(Number(p?.lat)) && Number.isFinite(Number(p?.lng)))
    .map((p) => ({ lat: Number(p.lat), lng: Number(p.lng) }));
  if (safePoints.length < 2) return null;
  let totalDistanceM = 0;
  for (let i = 1; i < safePoints.length; i += 1) {
    totalDistanceM += haversineMeters(
      safePoints[i - 1].lat,
      safePoints[i - 1].lng,
      safePoints[i].lat,
      safePoints[i].lng
    );
  }
  const safeSpeed = Math.max(Number(speedMps) || 13.89, 0.8);
  const durationS = totalDistanceM / safeSpeed;
  return {
    geometry: {
      type: "LineString",
      coordinates: safePoints.map((p) => [p.lng, p.lat]),
    },
    distance: Math.round(totalDistanceM),
    duration: Math.round(durationS),
    legs: [],
  };
};

const parseLngLat = (value) => {
  const [lng, lat] = String(value || "").split(",").map(Number);
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return null;
  return { lng, lat };
};

const parseViaPoints = (value) => {
  if (!value) return [];
  return String(value)
    .split(";")
    .map((pair) => parseLngLat(pair))
    .filter(Boolean);
};

const parseUserId = (value) => {
  const uid = Number.parseInt(value || "0", 10);
  return Number.isFinite(uid) && uid > 0 ? uid : null;
};

// GET /api/route/recommend?start=lng,lat&end=lng,lat&via=lng,lat;lng,lat
router.get("/recommend", async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) {
      return res.status(400).json({ error: "Missing start or end parameters" });
    }

    const startPoint = parseLngLat(start);
    const endPoint = parseLngLat(end);
    if (!startPoint || !endPoint) {
      return res.status(400).json({ error: "Invalid start/end format" });
    }

    await ensureRecoTables();

    const userId = parseUserId(req.query.user_id);
    const viaPoints = parseViaPoints(req.query.via);

    const mode = normalizeMode(req.query.mode);
    const debug = parseBoolFlag(req.query.debug, false);
    const limit = normalizeInteger(req.query.limit, DEFAULT_LIMIT, 1, 50);
    const candidateLimit = normalizeInteger(
      req.query.candidate_limit,
      DEFAULT_CANDIDATE_LIMIT,
      limit,
      360
    );
    const radius = req.query.radius ? clamp(Number(req.query.radius) || 0, 120, 5000) : null;
    const category = String(req.query.category || "").trim() || null;

    const settings = userId
      ? await fetchUserRecommendationSettings(userId)
      : {
          interestWeight: DEFAULT_INTEREST_WEIGHT,
          exploreWeight: DEFAULT_EXPLORE_WEIGHT,
          modeDefaults: null,
        };

    const interestWeight = req.query.interest_weight
      ? normalizeWeight(req.query.interest_weight, settings.interestWeight)
      : settings.interestWeight;
    const exploreWeight = req.query.explore_weight
      ? normalizeWeight(req.query.explore_weight, settings.exploreWeight)
      : settings.exploreWeight;

    const sessionId = String(req.query.session_id || req.headers["x-session-id"] || "").trim() || null;
    const userAgent = req.headers["user-agent"] || "";
    const ip = req.ip || req.headers["x-forwarded-for"] || "";
    const ab = await assignRecommendationBucket({
      userId,
      sessionId,
      ip,
      userAgent,
    });

    const forceV2 = parseBoolFlag(req.query.force_v2, false);

    const reco = await runRecommendationV2({
      startPoint,
      endPoint,
      viaPoints,
      userId,
      requestedMode: mode,
      interestWeight,
      exploreWeight,
      limit,
      candidateLimit,
      category,
      radius,
      modeDefaults: settings.modeDefaults,
      requestId: String(req.query.request_id || "").trim() || null,
      bucket: forceV2 ? "treatment" : ab.bucket,
      debug,
    });

    if (reco.error) {
      return res.status(reco.status || 500).json({ error: reco.error, ...(reco.payload || {}) });
    }

    const payload = reco.payload;
    if (!forceV2 && ab.bucket === "control") {
      payload.algorithm_version = "v1_control";
    }

    payload.request_id = payload.request_id || String(req.query.request_id || "") || null;
    payload.bucket = forceV2 ? "treatment" : ab.bucket;
    payload.profile = {
      ...(payload.profile || {}),
      tuning: {
        ...(payload.profile?.tuning || {}),
        interest_weight: interestWeight,
        distance_weight: 1 - interestWeight,
        explore_weight: exploreWeight,
      },
    };

    if (!debug && payload.diagnostics === undefined) {
      delete payload.diagnostics;
    }

    res.json(payload);
  } catch (err) {
    console.error("Error in /recommend:", err);
    res.status(500).json({ error: "Server error" });
  }
});

// GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat&mode=driving|walking|cycling
router.get("/with-poi", async (req, res) => {
  try {
    const { start, poi, end } = req.query;
    if (!start || !poi || !end) {
      return res.status(400).json({
        success: false,
        message: "Missing params: start / poi / end",
      });
    }

    const mode = normalizeMode(req.query.mode);
    const profile = mode;
    const startPoint = parseLngLat(start);
    const poiPoint = parseLngLat(poi);
    const endPoint = parseLngLat(end);
    if (!startPoint || !poiPoint || !endPoint) {
      return res.status(400).json({
        success: false,
        message: "Invalid start/poi/end format",
      });
    }

    const coordinates = `${start};${poi};${end}`;

    let route = null;
    let fallback = false;
    let warning = null;
    let osrmBackend = null;
    const osrm = await fetchOsrmRoute({
      profile,
      coordinates,
      overview: "full",
      geometries: "geojson",
      steps: true,
    });
    route = osrm.route || null;
    osrmBackend = osrm.backend || null;
    if (!route && osrm.errors?.length) {
      warning = `OSRM ${profile} failed: ${osrm.errors.join(" | ")}`;
    }

    if (!route && profile !== "driving") {
      const fallbackOsrm = await fetchOsrmRoute({
        profile: "driving",
        coordinates,
        overview: "full",
        geometries: "geojson",
        steps: true,
      });
      route = fallbackOsrm.route || null;
      osrmBackend = fallbackOsrm.backend || osrmBackend;
      fallback = true;
      warning = route
        ? `OSRM profile ${profile} unavailable, fallback to driving.`
        : `OSRM profile ${profile} unavailable, fallback failed.`;
    }

    if (!route) {
      route = buildApproxRoute([startPoint, poiPoint, endPoint], mode === "walking" ? 1.4 : mode === "cycling" ? 4.2 : 13.89);
      if (route) fallback = true;
    }

    if (!route) {
      return res.status(404).json({ success: false, message: "No route found" });
    }

    res.json({
      success: true,
      mode,
      mode_fallback: fallback,
      osrm_backend: osrmBackend,
      warning,
      optimized_route: {
        geometry: route.geometry,
        distance: route.distance,
        duration: route.duration,
        legs: route.legs,
      },
    });
  } catch (err) {
    console.error("Error in /with-poi:", err);
    res.status(500).json({
      success: false,
      message: "Server error",
      error: err.message,
    });
  }
});

export default router;
