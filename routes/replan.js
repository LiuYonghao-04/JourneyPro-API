import express from "express";
import axios from "axios";

const router = express.Router();
const OSRM_URL = "http://localhost:5000";

// GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat
router.get("/with-poi", async (req, res) => {
    try {
        const { start, poi, end } = req.query;
        if (!start || !poi || !end)
            return res.status(400).json({ error: "Missing start, poi or end parameters" });

        const [startLng, startLat] = start.split(",").map(Number);
        const [poiLng, poiLat] = poi.split(",").map(Number);
        const [endLng, endLat] = end.split(",").map(Number);

        // 调 OSRM，包含途径点
        const osrmRes = await axios.get(
            `${OSRM_URL}/route/v1/driving/${startLng},${startLat};${poiLng},${poiLat};${endLng},${endLat}?overview=full&steps=true&geometries=geojson`
        );

        const route = osrmRes.data.routes[0];
        if (!route) return res.status(404).json({ error: "No route found" });

        // 路线距离与时间
        const { distance, duration } = route;

        res.json({
            success: true,
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
