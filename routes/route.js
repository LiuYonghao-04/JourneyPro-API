import express from "express";
import axios from "axios";
import { getNearbyPOIs } from "../models/poi.js";

const router = express.Router();

// OSRM 本地服务端口
const OSRM_URL = "http://localhost:5000";
// const OSRM_URL = "https://router.project-osrm.org";

// /api/route/recommend?start=lng,lat&end=lng,lat
router.get("/recommend", async (req, res) => {
    try {
        const { start, end } = req.query;
        if (!start || !end)
            return res.status(400).json({ error: "Missing start or end parameters" });

        const [startLng, startLat] = start.split(",").map(Number);
        const [endLng, endLat] = end.split(",").map(Number);

        // 1️⃣ 获取基本路线
        const osrmRes = await axios.get(
            `${OSRM_URL}/route/v1/driving/${startLng},${startLat};${endLng},${endLat}?overview=full&geometries=geojson`
        );
        const route = osrmRes.data.routes[0];
        if (!route) return res.status(404).json({ error: "No route found" });

        // 2️⃣ 取路线中点附近的POI（后期可以改为缓冲区筛选）
        const mid = route.geometry.coordinates[Math.floor(route.geometry.coordinates.length / 2)];
        const [midLng, midLat] = mid;
        const pois = await getNearbyPOIs(midLat, midLng, 8000); // 半径8km

        // 3️⃣ 简单评分模型
        const scored = pois.map(p => ({
            id: p.id,
            name: p.name,
            category: p.category,
            lat: p.lat,
            lng: p.lng,
            distance: p.distance,
            popularity: p.popularity,
            score: (p.popularity / 5) * 0.6 + (1 / (p.distance / 1000 + 1)) * 0.4,
            image_url: p.image_url
        }));

        // 4️⃣ 排序取Top10
        scored.sort((a, b) => b.score - a.score);
        const topPois = scored.slice(0, 10);

        // 5️⃣ 返回数据
        res.json({
            base_route: route,
            recommended_pois: topPois
        });

    } catch (err) {
        console.error("Error in /recommend:", err.message);
        res.status(500).json({ error: "Server error" });
    }
});

// ✅ 带途径点的路线重规划接口：A → POI → B
router.get("/with-poi", async (req, res) => {
    try {
        const { start, poi, end } = req.query;
        if (!start || !poi || !end) {
            return res.status(400).json({
                success: false,
                message: "缺少参数：start / poi / end"
            });
        }

        // 1️⃣ 组装 OSRM 请求 URL
        // 例如: http://localhost:5000/route/v1/driving/104.06,30.67;104.04,30.64;104.08,30.70?overview=full&geometries=geojson&steps=true
        const coordinates = `${start};${poi};${end}`;
        const url = `${OSRM_URL}/route/v1/driving/${coordinates}?overview=full&geometries=geojson&steps=true`;

        // 2️⃣ 调用 OSRM 计算路线
        const osrmRes = await axios.get(url);
        const data = osrmRes.data;

        if (!data.routes || data.routes.length === 0) {
            return res.status(404).json({ success: false, message: "未找到可行路线" });
        }

        const route = data.routes[0];

        // 3️⃣ 返回结果（前端 routeStore.js 会自动接收这些字段）
        res.json({
            success: true,
            optimized_route: {
                geometry: route.geometry,
                distance: route.distance,
                duration: route.duration,
                legs: route.legs
            }
        });

    } catch (err) {
        console.error("❌ Error in /with-poi:", err.message);
        res.status(500).json({
            success: false,
            message: "服务器内部错误",
            error: err.message
        });
    }
});


export default router;
