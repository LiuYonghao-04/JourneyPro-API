import express from "express";
import { fetchOsrmRoute } from "../services/osrm/client.js";

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

const buildApproxRoute = (points) => {
  const coords = (points || [])
    .filter((p) => Number.isFinite(Number(p?.lat)) && Number.isFinite(Number(p?.lng)))
    .map((p) => [Number(p.lng), Number(p.lat)]);
  if (coords.length < 2) return null;
  let distance = 0;
  for (let i = 1; i < coords.length; i += 1) {
    distance += haversineMeters(coords[i - 1][1], coords[i - 1][0], coords[i][1], coords[i][0]);
  }
  const duration = distance / 13.89;
  return {
    geometry: { type: "LineString", coordinates: coords },
    distance: Math.round(distance),
    duration: Math.round(duration),
    legs: [],
  };
};

// GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat
router.get("/with-poi", async (req, res) => {
    try {
        const { start, poi, end } = req.query;
        if (!start || !poi || !end)
            return res.status(400).json({ error: "Missing start, poi or end parameters" });

        const [startLng, startLat] = start.split(",").map(Number);
        const [poiLng, poiLat] = poi.split(",").map(Number);
        const [endLng, endLat] = end.split(",").map(Number);

        if (![startLng, startLat, poiLng, poiLat, endLng, endLat].every(Number.isFinite)) {
            return res.status(400).json({ error: "Invalid lng/lat format" });
        }

        const coordinates = `${startLng},${startLat};${poiLng},${poiLat};${endLng},${endLat}`;
        const osrm = await fetchOsrmRoute({
            profile: "driving",
            coordinates,
            overview: "full",
            geometries: "geojson",
            steps: true,
        });
        const route = osrm.route || buildApproxRoute([
            { lat: startLat, lng: startLng },
            { lat: poiLat, lng: poiLng },
            { lat: endLat, lng: endLng },
        ]);
        if (!route) return res.status(404).json({ error: "No route found" });

        // 璺嚎璺濈涓庢椂闂?
        const { distance, duration } = route;

        res.json({
            success: true,
            mode_fallback: !osrm.route,
            osrm_backend: osrm.backend || null,
            warning: osrm.route ? null : "OSRM unavailable, using linear route approximation.",
            poi: { lat: poiLat, lng: poiLng },
            optimized_route: route,
            stats: {
                distance_km: (distance / 1000).toFixed(2),
                duration_min: (duration / 60).toFixed(1),
            },
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Route replanning failed" });
    }
});

export default router;
