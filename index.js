import express from "express";
import cors from "cors";
import routeRouter from "./routes/route.js";

const app = express();
app.use(cors());
app.use(express.json());

// 注册路由
app.use("/api/route", routeRouter);

const PORT = 3001;
app.listen(PORT, () => {
    console.log(`✅ JourneyPro API running at http://localhost:${PORT}`);
});
