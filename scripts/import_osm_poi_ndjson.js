// scripts/import_osm_poi_ndjson.js
// Import filtered OSM POIs (NDJSON) into MySQL with upsert.
// Keeps only food/attraction/park (configurable) to avoid junk like barrier=cycle_barrier.

import * as fs from "fs";
import * as readline from "readline";
import * as turf from "@turf/turf";
import mysql from "mysql2/promise";

const NDJSON_PATH =
    process.env.NDJSON_PATH ||
    "C:\\Users\\lenovo\\Desktop\\毕业设计\\项目代码\\data\\poi\\london-poi.ndjson";

const DB = {
    host: process.env.DB_HOST || "localhost",
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASS || "123456",
    database: process.env.DB_NAME || "journeypro",
};

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);

/** Choose the best display name from OSM tags */
function pickName(p) {
    return p.name || p["name:en"] || p["name:zh"] || null;
}

/** OSM object id for stable upsert: "node/123", "way/456", etc. */
function pickSourceId(feature) {
    return feature.id || feature?.properties?.["@id"] || null;
}


/** Map OSM tags -> your simplified categories */
function mapCategory(p) {
    const a = p.amenity;
    const t = p.tourism;
    const l = p.leisure;

    // Food / drink
    if (a && ["restaurant", "cafe", "fast_food", "pub", "bar"].includes(a)) return "food";

    // Attractions
    if (t && ["attraction", "museum", "gallery"].includes(t)) return "attraction";

    // Parks / green space
    if (l && ["park", "garden"].includes(l)) return "park";

    return null; // null => skip
}

function buildTags(p) {
    // keep some useful tags; keep it short(ish)
    const keys = [
        "amenity",
        "tourism",
        "leisure",
        "cuisine",
        "opening_hours",
        "website",
        "phone",
        "wheelchair",
        "fhrs:id",
    ];
    const parts = [];
    for (const k of keys) {
        if (p[k]) parts.push(`${k}=${String(p[k]).slice(0, 120)}`);
    }
    const s = parts.join(",");
    return s.length > 1000 ? s.slice(0, 1000) : s; // your schema is varchar(255) now; see note below
}

function buildAddress(p) {
    const hn = p["addr:housenumber"];
    const st = p["addr:street"];
    const pc = p["addr:postcode"];
    const city = p["addr:city"];
    const parts = [hn, st, city, pc].filter(Boolean);
    const s = parts.join(", ");
    return s.length > 255 ? s.slice(0, 255) : s;
}

/** Convert geometry to a point: Point stays, Polygon/LineString -> centroid */
function toPointCoords(feature) {
    const g = feature.geometry;
    if (!g) return null;

    if (g.type === "Point") return g.coordinates;

    try {
        const c = turf.centroid(feature);
        return c?.geometry?.coordinates || null;
    } catch {
        return null;
    }
}

async function flushBatch(conn, rows) {
    if (rows.length === 0) return;

    // Note: geom = ST_SRID(POINT(lng, lat), 4326)
    const valuesSql = rows
        .map(
            () =>
                "(?,?,?,?,0,0,?,NULL,'OSM',?,?,'London','GB',CURRENT_TIMESTAMP,ST_SRID(POINT(?, ?), 4326))"
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
      address=new.address,
      city=new.city,
      country=new.country,
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
            r.address,
            r.lng,
            r.lat
        );
    }

    await conn.execute(sql, params);
}

async function main() {
    const conn = await mysql.createConnection(DB);

    const rl = readline.createInterface({
        input: fs.createReadStream(NDJSON_PATH, { encoding: "utf8" }),
        crlfDelay: Infinity,
    });

    let batch = [];
    let seen = 0;
    let kept = 0;

    for await (const line of rl) {
        if (!line.trim()) continue;
        let feature;
        try {
            feature = JSON.parse(line);
        } catch {
            continue;
        }

        const p = feature.properties || {};
        const sourceId = pickSourceId(feature);
        if (!sourceId) continue;

        const category = mapCategory(p);
        if (!category) continue; // skip junk like barrier=cycle_barrier

        const name = pickName(p);
        if (!name) continue; // optional: you can allow unnamed, but UX is worse

        const coords = toPointCoords(feature);
        if (!coords) continue;

        const [lng, lat] = coords;
        if (typeof lat !== "number" || typeof lng !== "number") continue;

        batch.push({
            name: String(name).slice(0, 100),
            category,
            lat,
            lng,
            tags: buildTags(p),
            sourceId: String(sourceId).slice(0, 80),
            address: buildAddress(p),
        });

        kept++;
        seen++;

        if (batch.length >= BATCH_SIZE) {
            await flushBatch(conn, batch);
            batch = [];
            if (kept % (BATCH_SIZE * 10) === 0) {
                console.log(`kept=${kept} (from ${seen} lines)`);
            }
        }
    }

    if (batch.length) {
        await flushBatch(conn, batch);
    }

    await conn.end();
    console.log(`DONE. kept=${kept} (from ${seen} lines)`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
