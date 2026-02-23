import express from "express";
import axios from "axios";
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
import { runRecommendationV2 } from "../services/reco/ranker.js";
import { ensureRecoTables } from "../services/reco/schema.js";
import { fetchUserRecommendationSettings } from "../services/reco/profiles.js";

const router = express.Router();

const OSRM_URL = process.env.OSRM_URL || "http://localhost:5000";

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

    const coordinates = `${start};${poi};${end}`;
    const url = `${OSRM_URL}/route/v1/${profile}/${coordinates}?overview=full&geometries=geojson&steps=true`;

    let route = null;
    let fallback = false;
    try {
      const osrmRes = await axios.get(url);
      route = osrmRes.data?.routes?.[0] || null;
    } catch (err) {
      if (profile !== "driving") {
        const fallbackUrl = `${OSRM_URL}/route/v1/driving/${coordinates}?overview=full&geometries=geojson&steps=true`;
        const osrmRes = await axios.get(fallbackUrl);
        route = osrmRes.data?.routes?.[0] || null;
        fallback = true;
      } else {
        throw err;
      }
    }

    if (!route) {
      return res.status(404).json({ success: false, message: "No route found" });
    }

    res.json({
      success: true,
      mode,
      mode_fallback: fallback,
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
