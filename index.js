import express from "express";
import cors from "cors";
import routeRouter from "./routes/route.js";
import replanRouter from "./routes/replan.js";

const app = express();
app.use(cors());
app.use(express.json());

app.use("/api/route", routeRouter);
app.use("/api/route", replanRouter);

const PORT = 3001;
app.listen(PORT, () => {
    console.log(`✅ JourneyPro API running at http://localhost:${PORT}`);
});
