// scripts/import_tfl_stoppoints.js
// Import TfL StopPoints (stations etc.) into MySQL poi table.
// - Only uses app_key (from TfL Portal Primary/Secondary key)
// - Avoids bus stops to keep dataset reasonable
// - Upserts into poi using unique(source, source_id)

import mysql from "mysql2/promise";

// ====== DB CONFIG (env) ======
const DB = {
    host: process.env.DB_HOST || "localhost",
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASS || "123456",
    database: process.env.DB_NAME || "journeypro",
};

// ====== TfL CONFIG (env) ======
const APP_KEY = process.env.TFL_APP_KEY || "";
// TfL sometimes works on both; digital is the documented swagger host
const BASE_URL = process.env.TFL_BASE_URL || "https://api.digital.tfl.gov.uk";

// ====== SEEDING CONFIG ======
const BBOX = {
    west: -0.55,
    east: 0.35,
    south: 51.28,
    north: 51.70,
};

const GRID_STEP = Number(process.env.GRID_STEP || 0.03); // ~3km
const RADIUS = Number(process.env.RADIUS || 2500);       // meters
const SLEEP_MS = Number(process.env.SLEEP_MS || 250);
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);

// Keep “station-like” stop types (exclude bus stops)
const STOP_TYPES = [
    "NaptanMetroStation",
    "NaptanRailStation",
    "NaptanTramMetroStation",
    "NaptanFerryPort",
    "NaptanAirport",
].join(",");

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

function buildUrl(lat, lon) {
    const base = `${BASE_URL}/StopPoint`;
    const params = new URLSearchParams({
        lat: String(lat),
        lon: String(lon),
        radius: String(RADIUS),
        stopTypes: STOP_TYPES,
    });
    if (APP_KEY) params.set("app_key", APP_KEY);
    return `${base}?${params.toString()}`;
}

function normalizeStopPoint(sp) {
    const sourceId = sp.naptanId || sp.id;
    if (!sourceId) return null;

    const name = sp.commonName || sp.name;
    if (!name) return null;

    const lat = sp.lat;
    const lng = sp.lon;
    if (typeof lat !== "number" || typeof lng !== "number") return null;

    const modes = Array.isArray(sp.modes) ? sp.modes.join("|") : "";
    const stopType = sp.stopType || "";
    const tags = `modes=${modes},stopType=${stopType}`;

    return {
        name: String(name).slice(0, 100),
        category: "transport",
        lat,
        lng,
        tags,
        sourceId: String(sourceId).slice(0, 80),
    };
}

async function flushBatch(conn, rows) {
    if (!rows.length) return;

    // Insert into your poi schema:
    // (name, category, lat, lng, popularity, price, tags, image_url, source, source_id, address, city, country, updated_at, geom)
    const valuesSql = rows
        .map(
            () =>
                "(?,?,?,?,0,0,?,NULL,'TFL',?,NULL,'London','GB',CURRENT_TIMESTAMP,ST_SRID(POINT(?, ?), 4326))"
        )
        .join(",");

    const sql = `
        INSERT INTO poi
        (name, category, lat, lng, popularity, price, tags, image_url, source, source_id, address, city, country, updated_at, geom)
        VALUES ${valuesSql}
            AS new
        ON DUPLICATE KEY UPDATE
                             name=new.name,
                             category=new.category,
                             lat=new.lat,
                             lng=new.lng,
                             tags=new.tags,
                             geom=new.geom,
                             updated_at=CURRENT_TIMESTAMP
    `;

    const params = [];
    for (const r of rows) {
        params.push(
            r.name,
            r.category,
            r.lat,
            r.lng,
            r.tags,
            r.sourceId,
            r.lng,
            r.lat
        );
    }

    await conn.execute(sql, params);
}

async function main() {
    if (!APP_KEY) {
        console.warn("WARNING: TFL_APP_KEY is empty. You may get 403. Set it before running.");
    }

    const conn = await mysql.createConnection(DB);

    let reqCount = 0;
    let inserted = 0;

    const dedup = new Set();
    let batch = [];

    for (let lat = BBOX.south; lat <= BBOX.north; lat += GRID_STEP) {
        for (let lon = BBOX.west; lon <= BBOX.east; lon += GRID_STEP) {
            const url = buildUrl(lat, lon);
            reqCount++;

            let res;
            try {
                res = await fetch(url);
            } catch (e) {
                console.log(`[fetch error] ${url}`, e?.message || e);
                await sleep(SLEEP_MS);
                continue;
            }

            if (!res.ok) {
                console.log(`[${res.status}] ${url}`);
                await sleep(SLEEP_MS);
                continue;
            }

            const data = await res.json();
            const stopPoints = Array.isArray(data.stopPoints) ? data.stopPoints : [];

            for (const sp of stopPoints) {
                const row = normalizeStopPoint(sp);
                if (!row) continue;
                if (dedup.has(row.sourceId)) continue;
                dedup.add(row.sourceId);

                batch.push(row);

                if (batch.length >= BATCH_SIZE) {
                    await flushBatch(conn, batch);
                    inserted += batch.length;
                    batch = [];
                    console.log(`inserted~=${inserted}, req=${reqCount}`);
                }
            }

            await sleep(SLEEP_MS);
        }
    }

    if (batch.length) {
        await flushBatch(conn, batch);
        inserted += batch.length;
    }

    await conn.end();
    console.log(`DONE. inserted~=${inserted}, requests=${reqCount}`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
