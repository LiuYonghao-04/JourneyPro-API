import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import routeRouter from "./routes/route.js";
import replanRouter from "./routes/replan.js";
import authRouter from "./routes/auth.js";
import postsRouter from "./routes/posts.js";
import seedRouter from "./routes/seed.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

app.use("/api/route", routeRouter);
app.use("/api/route", replanRouter);
app.use("/api/auth", authRouter);
app.use("/api/posts", postsRouter);
app.use("/api/dev", seedRouter);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`✅ JourneyPro API running at http://localhost:${PORT}`);
});
