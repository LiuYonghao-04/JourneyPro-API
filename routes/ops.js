import express from "express";
import { recordClientErrorEvent } from "../utils/opsCenter.js";

const router = express.Router();

router.post("/client-errors", async (req, res) => {
  try {
    const eventId = await recordClientErrorEvent(req.body || {});
    res.status(201).json({ success: true, event_id: eventId });
  } catch (err) {
    console.error("record client error event failed", err);
    res.status(500).json({ success: false, message: "server error" });
  }
});

export default router;
