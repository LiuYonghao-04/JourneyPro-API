import express from "express";
import axios from "axios";
import { getNearbyPOIs } from "../models/poi.js";

const router = express.Router();

const OSRM_URL = process.env.OSRM_URL || "http://localhost:5000";

const toRad = (d) => (Number(d) * Math.PI) / 180;
const haversineMeters = (lat1, lng1, lat2, lng2) => {
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

const sampleRoutePoints = (coords, stepM, maxSamples) => {
  if (!Array.isArray(coords) || coords.length < 2) return [];
  const step = Math.max(Number(stepM) || 0, 50);
  const max = Math.max(parseInt(maxSamples || "0", 10) || 0, 5);

  const samples = [];
  const [lng0, lat0] = coords[0];
  samples.push({ lat: lat0, lng: lng0 });

  let traveled = 0;
  let nextAt = step;

  for (let i = 1; i < coords.length; i += 1) {
    const [lng1, lat1] = coords[i - 1];
    const [lng2, lat2] = coords[i];
    const seg = haversineMeters(lat1, lng1, lat2, lng2);
    if (!seg || !Number.isFinite(seg)) continue;

    while (traveled + seg >= nextAt) {
      const t = (nextAt - traveled) / seg;
      const lat = lat1 + (lat2 - lat1) * t;
      const lng = lng1 + (lng2 - lng1) * t;
      samples.push({ lat, lng });
      if (samples.length >= max) return samples;
      nextAt += step;
    }
    traveled += seg;
  }

  const [lngLast, latLast] = coords[coords.length - 1];
  const last = samples[samples.length - 1];
  if (samples.length < max && haversineMeters(last.lat, last.lng, latLast, lngLast) > 60) {
    samples.push({ lat: latLast, lng: lngLast });
  }

  return samples;
};

// GET /api/route/recommend?start=lng,lat&end=lng,lat&radius=700&limit=10&sample_m=350&category=museum
router.get("/recommend", async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) {
      return res.status(400).json({ error: "Missing start or end parameters" });
    }

    const [startLng, startLat] = String(start).split(",").map(Number);
    const [endLng, endLat] = String(end).split(",").map(Number);
    if (![startLng, startLat, endLng, endLat].every((n) => Number.isFinite(n))) {
      return res.status(400).json({ error: "Invalid start/end format" });
    }

    const radius = Math.min(Math.max(parseInt(req.query.radius || "700", 10), 50), 20000);
    const limit = Math.min(Math.max(parseInt(req.query.limit || "10", 10), 1), 50);
    const perSampleLimit = Math.min(Math.max(parseInt(req.query.per_sample_limit || "18", 10), 1), 60);
    const requestedSampleM = Math.min(Math.max(parseInt(req.query.sample_m || "350", 10), 150), 1200);
    const category = (req.query.category || "").toString().trim();

    const osrmRes = await axios.get(
      `${OSRM_URL}/route/v1/driving/${startLng},${startLat};${endLng},${endLat}?overview=full&geometries=geojson`
    );
    const route = osrmRes.data?.routes?.[0];
    if (!route || !route.geometry || !Array.isArray(route.geometry.coordinates)) {
      return res.status(404).json({ error: "No route found" });
    }

    const maxSamples = 60;
    const dynamicStep = route.distance ? Math.ceil(route.distance / maxSamples) : requestedSampleM;
    const sampleM = Math.max(requestedSampleM, dynamicStep);
    const samples = sampleRoutePoints(route.geometry.coordinates, sampleM, maxSamples);

    const byId = new Map(); // id -> { ...poi, hits, bestDistance }
    for (const s of samples) {
      const nearby = await getNearbyPOIs(s.lat, s.lng, radius, perSampleLimit, category || null);
      for (const p of nearby) {
        const id = p.id;
        if (!id) continue;
        const dist = Number(p.distance) || 0;
        const prev = byId.get(id);
        if (!prev) {
          byId.set(id, { ...p, hits: 1, bestDistance: dist });
        } else {
          prev.hits += 1;
          prev.bestDistance = Math.min(prev.bestDistance, dist || prev.bestDistance);
        }
      }
    }

    const scored = Array.from(byId.values()).map((p) => {
      const popularity = Number(p.popularity) || 0;
      const km = (Number(p.bestDistance) || 0) / 1000;
      const popScore = Math.max(Math.min(popularity / 5, 1), 0);
      const distScore = 1 / (km + 1);
      const hitScore = Math.max(Math.min((Number(p.hits) || 0) / 3, 1), 0);
      const score = popScore * 0.45 + distScore * 0.35 + hitScore * 0.2;

      return {
        id: p.id,
        name: p.name,
        category: p.category,
        lat: p.lat,
        lng: p.lng,
        distance: Math.round(Number(p.bestDistance) || Number(p.distance) || 0),
        popularity: p.popularity,
        score,
        image_url: p.image_url,
      };
    });

    scored.sort((a, b) => b.score - a.score);
    const topPois = scored.slice(0, limit);

    res.json({
      base_route: route,
      recommended_pois: topPois,
      debug: {
        osrm: OSRM_URL,
        sample_m: sampleM,
        samples: samples.length,
        radius_m: radius,
        candidates: scored.length,
      },
    });
  } catch (err) {
    console.error("Error in /recommend:", err);
    res.status(500).json({ error: "Server error" });
  }
});

// GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat
router.get("/with-poi", async (req, res) => {
  try {
    const { start, poi, end } = req.query;
    if (!start || !poi || !end) {
      return res.status(400).json({
        success: false,
        message: "Missing params: start / poi / end",
      });
    }

    const coordinates = `${start};${poi};${end}`;
    const url = `${OSRM_URL}/route/v1/driving/${coordinates}?overview=full&geometries=geojson&steps=true`;

    const osrmRes = await axios.get(url);
    const data = osrmRes.data;

    if (!data.routes || data.routes.length === 0) {
      return res.status(404).json({ success: false, message: "No route found" });
    }

    const route = data.routes[0];
    res.json({
      success: true,
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

