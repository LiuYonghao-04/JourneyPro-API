// scripts/import_naptan_stops_csv.js
// Import NaPTAN stop points (Greater London) into MySQL poi table.
// Source: DfT NaPTAN CSV download (free, no API calls).
// Keeps station-like StopType values, excludes on-street bus stops (BCT).

import * as fs from "fs";
import mysql from "mysql2/promise";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { parse } = require("csv-parse");

const DB = {
    host: process.env.DB_HOST || "localhost",
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASS || "123456",
    database: process.env.DB_NAME || "journeypro",
};

const CSV_PATH = process.env.NAPTAN_CSV_PATH; // full path to Stops CSV
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);

// NaPTAN StopType enum (common ones):
// Rail: RSE (station entrance), RLY (access area), RPL (platform)
// Metro/Underground/Tram: TMU (entrance), MET (access area), PLT (platform)
// Air: AIR (entrance), GAT (access)
// Ferry: FTD (terminal entrance), FER (access), FBT (berth)
// Bus/Coach station (not on-street): BCE/BST/BCS/BCQ
// We EXCLUDE BCT (on-street bus stop) to avoid tens of thousands.
const KEEP_STOP_TYPES = new Set([
    "RSE", "RLY",
    "TMU", "MET",
    "AIR", "GAT",
    "FTD", "FER", "FBT",
    "TXR", "STR",
    "BCE", "BST", "BCS", "BCQ",
]);

function toNum(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

async function flush(conn, rows) {
    if (!rows.length) return;

    const valuesSql = rows
        .map(
            () =>
                "(?,?,?,?,0,0,?,NULL,'NAPTAN',?,NULL,'London','GB',CURRENT_TIMESTAMP,ST_SRID(POINT(?, ?), 4326))"
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
        // NOTE: POINT(lng, lat)
        params.push(r.name, r.category, r.lat, r.lng, r.tags, r.sourceId, r.lng, r.lat);
    }
    await conn.execute(sql, params);
}

async function main() {
    if (!CSV_PATH) {
        console.error("Missing env: NAPTAN_CSV_PATH (path to Stops CSV)");
        process.exit(1);
    }
    if (!fs.existsSync(CSV_PATH)) {
        console.error("CSV not found:", CSV_PATH);
        process.exit(1);
    }

    const conn = await mysql.createConnection(DB);

    let batch = [];
    let seen = 0;
    let kept = 0;

    const parser = parse({
        columns: true,
        bom: true,
        relax_column_count: true,
        trim: true,
        skip_empty_lines: true,
    });

    const stream = fs.createReadStream(CSV_PATH).pipe(parser);

    for await (const row of stream) {
        seen++;

        // Standard NaPTAN columns (from data.gov.uk preview / schema guide):
        // ATCOCode, CommonName, Longitude, Latitude, StopType, Status, NaptanCode, Indicator, Street, ...
        const atco = row.ATCOCode || row.AtcoCode || row.AtcoCode;
        const name = row.CommonName;
        const stopType = row.StopType;
        const status = (row.Status || "").toLowerCase();

        if (!atco || !name || !stopType) continue;
        if (status && status !== "active") continue;
        if (!KEEP_STOP_TYPES.has(stopType)) continue;

        const lat = toNum(row.Latitude);
        const lng = toNum(row.Longitude);
        if (lat === null || lng === null) continue;

        const naptanCode = row.NaptanCode || "";
        const indicator = row.Indicator || "";
        const tags = `StopType=${stopType},ATCOCode=${atco}` +
            (naptanCode ? `,NaptanCode=${naptanCode}` : "") +
            (indicator ? `,Indicator=${indicator}` : "");

        batch.push({
            name: String(name).slice(0, 100),
            category: "transport",
            lat,
            lng,
            tags,
            sourceId: String(atco).slice(0, 80),
        });
        kept++;

        if (batch.length >= BATCH_SIZE) {
            await flush(conn, batch);
            batch = [];
            if (kept % (BATCH_SIZE * 5) === 0) {
                console.log(`progress: kept=${kept}, seen=${seen}`);
            }
        }
    }

    if (batch.length) await flush(conn, batch);
    await conn.end();

    console.log(`DONE. kept=${kept}, seen=${seen}`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
