import express from "express";
import axios from "axios";
import dns from "dns/promises";
import fs from "fs";
import net from "net";
import path from "path";
import multer from "multer";
import { fileURLToPath } from "url";

const router = express.Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const uploadsDir = path.join(__dirname, "..", "uploads");

fs.mkdirSync(uploadsDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const ext = (path.extname(file.originalname || "") || "").toLowerCase();
    const safeExt = [".jpg", ".jpeg", ".png", ".webp"].includes(ext) ? ext : ".jpg";
    const name = `jp_${Date.now()}_${Math.random().toString(16).slice(2)}${safeExt}`;
    cb(null, name);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = /^image\//.test(file.mimetype || "");
    cb(ok ? null : new Error("Only image files are allowed"), ok);
  },
});

router.post("/image", upload.single("file"), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: "file required" });
    }
    const base = `${req.protocol}://${req.get("host")}`;
    const url = `${base}/uploads/${req.file.filename}`;
    return res.json({ success: true, url });
  } catch (err) {
    console.error("upload image error", err);
    return res.status(500).json({ success: false, message: "upload failed" });
  }
});

const isPrivateIp = (ip) => {
  const normalized = String(ip || "").trim().toLowerCase();
  if (!normalized) return true;

  if (normalized === "localhost") return true;
  if (normalized === "::1") return true;
  if (normalized.startsWith("fe80:")) return true;
  if (normalized.startsWith("fc") || normalized.startsWith("fd")) return true;
  if (normalized.startsWith("::ffff:")) {
    const v4 = normalized.replace("::ffff:", "");
    return isPrivateIp(v4);
  }

  if (net.isIP(normalized) !== 4) return false;

  const parts = normalized.split(".").map((n) => Number(n));
  if (parts.length !== 4 || parts.some((n) => Number.isNaN(n))) return true;
  const [a, b] = parts;
  if (a === 10) return true;
  if (a === 127) return true;
  if (a === 0) return true;
  if (a === 169 && b === 254) return true;
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 192 && b === 168) return true;
  return false;
};

const assertSafeRemoteUrl = async (raw) => {
  let url;
  try {
    url = new URL(String(raw || ""));
  } catch {
    throw new Error("Invalid url");
  }
  if (!["http:", "https:"].includes(url.protocol)) throw new Error("Invalid protocol");
  if (url.username || url.password) throw new Error("Credentials not allowed");

  const port = url.port ? Number(url.port) : url.protocol === "https:" ? 443 : 80;
  if (![80, 443].includes(port)) throw new Error("Port not allowed");

  const hostname = url.hostname;
  const addrs = await dns.lookup(hostname, { all: true });
  if (!addrs.length) throw new Error("DNS lookup failed");
  if (addrs.some((a) => isPrivateIp(a.address))) throw new Error("Host not allowed");
  return url.toString();
};

// GET /api/upload/proxy?url=https%3A%2F%2F...
router.get("/proxy", async (req, res) => {
  try {
    const raw = req.query?.url;
    if (!raw) return res.status(400).send("url required");

    const safeUrl = await assertSafeRemoteUrl(raw);
    const resp = await axios.get(safeUrl, {
      responseType: "arraybuffer",
      timeout: 8000,
      maxContentLength: 8 * 1024 * 1024,
      maxBodyLength: 8 * 1024 * 1024,
      maxRedirects: 5,
      validateStatus: (s) => s >= 200 && s < 300,
      headers: { Accept: "image/*,*/*;q=0.8" },
    });

    const contentType = String(resp.headers?.["content-type"] || "").toLowerCase();
    if (!contentType.startsWith("image/")) {
      return res.status(415).send("not an image");
    }

    res.setHeader("Content-Type", contentType.split(";")[0] || "image/jpeg");
    res.setHeader("Cache-Control", "public, max-age=86400");
    return res.send(Buffer.from(resp.data));
  } catch (err) {
    console.error("proxy image error", err?.message || err);
    return res.status(502).send("proxy failed");
  }
});

export default router;
