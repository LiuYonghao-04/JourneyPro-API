import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

import routeRouter from "./routes/route.js";
import replanRouter from "./routes/replan.js";
import authRouter from "./routes/auth.js";
import postsRouter from "./routes/posts.js";
import seedRouter from "./routes/seed.js";
import poiRouter from "./routes/poi.js";
import followRouter from "./routes/follow.js";
import notificationsRouter from "./routes/notifications.js";
import chatRouter from "./routes/chat.js";
import uploadRouter from "./routes/upload.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use("/uploads", express.static(path.join(__dirname, "uploads")));

app.use("/api/route", routeRouter);
app.use("/api/route", replanRouter);
app.use("/api/auth", authRouter);
app.use("/api/posts", postsRouter);
app.use("/api/dev", seedRouter);
app.use("/api/poi", poiRouter);
app.use("/api/follow", followRouter);
app.use("/api/notifications", notificationsRouter);
app.use("/api/chat", chatRouter);
app.use("/api/upload", uploadRouter);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`JourneyPro API running at http://localhost:${PORT}`);
});

