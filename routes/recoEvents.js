import express from "express";
import { insertRecommendationEvents } from "../services/reco/events.js";
import { ensureRecoTables } from "../services/reco/schema.js";

const router = express.Router();

// POST /api/recommendation/events
// body: { request_id, poi_id, event_type, mode, rank_position, ts } or { events: [...] }
router.post("/events", async (req, res) => {
  try {
    await ensureRecoTables();

    const body = req.body || {};
    const userId = body.user_id ? Number.parseInt(body.user_id, 10) : null;
    const sessionId = body.session_id ? String(body.session_id) : null;

    const events = Array.isArray(body.events) ? body.events : [body];
    const normalized = events.map((event) => ({
      ...event,
      user_id: event.user_id ?? userId,
      session_id: event.session_id ?? sessionId,
    }));

    const result = await insertRecommendationEvents(normalized);
    if (!result.inserted) {
      return res.status(400).json({
        success: false,
        message: "No valid recommendation events",
        ...result,
      });
    }

    res.json({
      success: true,
      ...result,
    });
  } catch (err) {
    console.error("recommendation events error", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
