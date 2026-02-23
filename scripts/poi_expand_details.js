import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";
import * as turf from "@turf/turf";
import mysql from "mysql2/promise";

const NDJSON_PATH = process.env.NDJSON_PATH || path.resolve(process.cwd(), "../data/poi/london-poi.ndjson");
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 700);
const IMPORT_LIMIT = Number(process.env.IMPORT_LIMIT || 0);

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const FOOD_AMENITIES = new Set([
  "restaurant",
  "cafe",
  "fast_food",
  "pub",
  "bar",
  "biergarten",
  "food_court",
  "ice_cream",
]);
const SHOP_TYPES = new Set([
  "supermarket",
  "mall",
  "convenience",
  "clothes",
  "bakery",
  "department_store",
  "books",
  "gift",
  "beauty",
  "shoes",
  "sports",
  "electronics",
  "jewelry",
]);
const ATTRACTION_TOURISM = new Set([
  "attraction",
  "viewpoint",
  "theme_park",
  "zoo",
  "aquarium",
]);
const MUSEUM_TOURISM = new Set(["museum", "gallery", "artwork"]);
const STAY_TOURISM = new Set(["hotel", "hostel", "guest_house", "motel", "apartment"]);
const CULTURE_AMENITIES = new Set([
  "theatre",
  "cinema",
  "arts_centre",
  "library",
  "music_venue",
  "community_centre",
]);
const PARK_LEISURE = new Set(["park", "garden", "nature_reserve", "playground", "dog_park"]);
const SPORTS_LEISURE = new Set([
  "sports_centre",
  "stadium",
  "swimming_pool",
  "fitness_centre",
  "pitch",
]);
const HERITAGE_TYPES = new Set([
  "castle",
  "monument",
  "memorial",
  "ruins",
  "building",
  "archaeological_site",
]);
const TRANSPORT_AMENITIES = new Set([
  "bus_station",
  "ferry_terminal",
  "taxi",
  "parking",
  "bicycle_parking",
  "charging_station",
]);

const STAY_MINUTES = {
  food: 75,
  attraction: 95,
  museum: 120,
  park: 90,
  shopping: 80,
  culture: 110,
  heritage: 85,
  sports: 100,
  market: 70,
  stay: 180,
  transport: 25,
};

const BEST_VISIT_TIME = {
  food: "11:30-14:00, 18:00-20:30",
  attraction: "10:00-17:00",
  museum: "10:00-16:30",
  park: "08:00-18:00",
  shopping: "11:00-18:00",
  culture: "13:00-21:00",
  heritage: "09:30-16:30",
  sports: "16:00-20:00",
  market: "09:00-14:00",
  stay: "Anytime",
  transport: "Peak and off-peak",
};

const DEFAULT_OPENING = {
  food: "10:00-22:00",
  attraction: "09:00-18:00",
  museum: "10:00-17:00",
  park: "Open 24 hours",
  shopping: "10:00-20:00",
  culture: "10:00-21:00",
  heritage: "09:30-17:00",
  sports: "07:00-22:00",
  market: "08:00-17:00",
  stay: "Open 24 hours",
  transport: "Open 24 hours",
};

const CATEGORY_PRICE = {
  food: [1.2, 3.6],
  attraction: [1.4, 3.4],
  museum: [1.2, 3.0],
  park: [0.6, 1.6],
  shopping: [1.3, 3.8],
  culture: [1.2, 3.2],
  heritage: [0.8, 2.6],
  sports: [1.0, 2.8],
  market: [0.8, 2.4],
  stay: [2.0, 4.5],
  transport: [0.5, 1.8],
};

const normalize = (value) => String(value || "").trim().toLowerCase();

const hashInt = (value) => {
  const str = String(value || "");
  let hash = 2166136261;
  for (let i = 0; i < str.length; i += 1) {
    hash ^= str.charCodeAt(i);
    hash +=
      (hash << 1) +
      (hash << 4) +
      (hash << 7) +
      (hash << 8) +
      (hash << 24);
  }
  return Math.abs(hash >>> 0);
};

const random01 = (key, salt = "") => {
  const h = hashInt(`${key}|${salt}`);
  return (h % 10000) / 10000;
};

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const pickName = (properties) => {
  return properties.name || properties["name:en"] || properties["name:zh"] || null;
};

const pickSourceId = (feature) => {
  return feature.id || feature?.properties?.["@id"] || null;
};

const mapCategory = (properties) => {
  const amenity = normalize(properties.amenity);
  const tourism = normalize(properties.tourism);
  const leisure = normalize(properties.leisure);
  const shop = normalize(properties.shop);
  const historic = normalize(properties.historic);

  if (FOOD_AMENITIES.has(amenity)) return "food";
  if (shop === "mall" || amenity === "marketplace") return "market";
  if (SHOP_TYPES.has(shop)) return "shopping";
  if (MUSEUM_TOURISM.has(tourism)) return "museum";
  if (ATTRACTION_TOURISM.has(tourism)) return "attraction";
  if (PARK_LEISURE.has(leisure)) return "park";
  if (SPORTS_LEISURE.has(leisure)) return "sports";
  if (CULTURE_AMENITIES.has(amenity)) return "culture";
  if (STAY_TOURISM.has(tourism)) return "stay";
  if (HERITAGE_TYPES.has(historic)) return "heritage";
  if (TRANSPORT_AMENITIES.has(amenity)) return "transport";

  return null;
};

const toPointCoords = (feature) => {
  const geom = feature.geometry;
  if (!geom) return null;
  if (geom.type === "Point") return geom.coordinates;

  try {
    const centroid = turf.centroid(feature);
    return centroid?.geometry?.coordinates || null;
  } catch {
    return null;
  }
};

const safeTrim = (value, max = 255) => {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.length > max ? text.slice(0, max) : text;
};

const buildAddress = (properties) => {
  const parts = [
    properties["addr:housenumber"],
    properties["addr:street"],
    properties["addr:city"],
    properties["addr:postcode"],
  ].filter(Boolean);
  return safeTrim(parts.join(", "), 255);
};

const buildTags = (properties, category) => {
  const tags = [
    category,
    properties.amenity,
    properties.tourism,
    properties.leisure,
    properties.shop,
    properties.cuisine,
    properties.historic,
    properties.natural,
    properties.wheelchair,
  ]
    .map((item) => safeTrim(item, 48))
    .filter(Boolean);

  const unique = [...new Set(tags.map((item) => item.toLowerCase()))];
  return unique.join(",").slice(0, 800);
};

const buildDescription = (name, category, city) => {
  const place = city || "London";
  if (category === "food") return `${name} is a popular food stop in ${place}, suitable for quick breaks and local meals.`;
  if (category === "museum") return `${name} is a museum-style destination in ${place} with culture-focused exhibits.`;
  if (category === "park") return `${name} is an open-air park area in ${place}, ideal for short rest and scenery.`;
  if (category === "shopping") return `${name} is a shopping-oriented point in ${place} with multiple nearby retail options.`;
  if (category === "culture") return `${name} is a culture and entertainment venue in ${place}.`;
  if (category === "heritage") return `${name} is a heritage landmark in ${place} with historical value.`;
  if (category === "sports") return `${name} is a sports and activity location in ${place}.`;
  if (category === "market") return `${name} is a market-style stop in ${place} for local browsing.`;
  if (category === "stay") return `${name} is a stay/lodging related place in ${place}.`;
  if (category === "transport") return `${name} is a transport utility point in ${place}.`;
  return `${name} is a point of interest in ${place}.`;
};

const buildImageUrl = (sourceId, category) => {
  const seed = encodeURIComponent(`jp-${category}-${String(sourceId).slice(-18)}`);
  return `https://picsum.photos/seed/${seed}/960/640`;
};

const buildPrice = (sourceId, category, properties) => {
  const raw = normalize(properties.fee);
  if (raw === "yes") return 2.8;
  if (raw === "no") return 0.8;
  const [min, max] = CATEGORY_PRICE[category] || [1, 3];
  const ratio = random01(sourceId, "price");
  return Number((min + (max - min) * ratio).toFixed(2));
};

const buildPopularity = (sourceId, category) => {
  const base = {
    food: 4.1,
    attraction: 4.0,
    museum: 4.2,
    park: 3.9,
    shopping: 4.0,
    culture: 4.0,
    heritage: 4.1,
    sports: 3.9,
    market: 3.8,
    stay: 4.0,
    transport: 3.5,
  }[category] || 3.8;
  const jitter = (random01(sourceId, "pop") - 0.5) * 0.8;
  return Number(clamp(base + jitter, 2.8, 4.9).toFixed(2));
};

const buildDetailFields = ({ sourceId, category, properties, name, city }) => {
  const ratingCount = Math.round(40 + random01(sourceId, "rating_count") * 3600);
  const reviewCount = Math.round(ratingCount * (0.35 + random01(sourceId, "review_ratio") * 0.45));
  const stayMinutes = STAY_MINUTES[category] || 75;
  const bestVisitTime = BEST_VISIT_TIME[category] || "10:00-18:00";
  const openingHours =
    safeTrim(properties.opening_hours, 160) ||
    safeTrim(properties["opening_hours:covid19"], 160) ||
    DEFAULT_OPENING[category] ||
    "Open 24 hours";

  const phone = safeTrim(properties.phone || properties["contact:phone"], 64);
  const website = safeTrim(properties.website || properties["contact:website"], 255);

  const indoor = ["museum", "culture", "shopping", "stay"].includes(category) ? 1 : 0;
  const familyFriendly = ["park", "attraction", "museum", "culture", "market", "sports"].includes(category)
    ? 1
    : 0;
  const petFriendly = ["park", "heritage", "market"].includes(category) ? 1 : 0;

  const crowdLevel = Math.round(clamp(1 + random01(sourceId, "crowd") * 4, 1, 5));
  const description = safeTrim(buildDescription(name, category, city), 1200);

  return {
    description,
    openingHours,
    phone,
    website,
    ratingCount,
    reviewCount,
    stayMinutes,
    bestVisitTime,
    familyFriendly,
    petFriendly,
    indoor,
    crowdLevel,
  };
};

const ensurePoiDetailColumns = async (conn) => {
  const safeAlter = async (sql) => {
    try {
      await conn.query(sql);
    } catch (err) {
      const msg = String(err?.message || err || "");
      if (!msg.includes("Duplicate column") && !msg.includes("already exists")) {
        throw err;
      }
    }
  };

  await safeAlter(`ALTER TABLE poi ADD COLUMN description TEXT NULL`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN opening_hours VARCHAR(160) NULL`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN phone VARCHAR(64) NULL`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN website VARCHAR(255) NULL`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN rating_count INT NOT NULL DEFAULT 0`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN review_count INT NOT NULL DEFAULT 0`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN stay_minutes SMALLINT NOT NULL DEFAULT 60`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN best_visit_time VARCHAR(64) NULL`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN family_friendly TINYINT(1) NOT NULL DEFAULT 0`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN pet_friendly TINYINT(1) NOT NULL DEFAULT 0`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN indoor TINYINT(1) NOT NULL DEFAULT 0`);
  await safeAlter(`ALTER TABLE poi ADD COLUMN crowd_level TINYINT NOT NULL DEFAULT 2`);
};

const flushBatch = async (conn, batch) => {
  if (!batch.length) return;

  const valueSql = batch
    .map(
      () =>
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,ST_SRID(POINT(?, ?), 4326))"
    )
    .join(",");

  const sql = `
    INSERT INTO poi (
      name, category, lat, lng, popularity, price, tags, image_url,
      source, source_id, address, city, country,
      description, opening_hours, phone, website,
      rating_count, review_count, stay_minutes, best_visit_time,
      family_friendly, pet_friendly, indoor, crowd_level,
      updated_at, geom
    )
    VALUES ${valueSql}
    AS new
    ON DUPLICATE KEY UPDATE
      name = new.name,
      category = new.category,
      lat = new.lat,
      lng = new.lng,
      popularity = new.popularity,
      price = new.price,
      tags = new.tags,
      image_url = COALESCE(poi.image_url, new.image_url),
      address = COALESCE(new.address, poi.address),
      city = COALESCE(new.city, poi.city),
      country = COALESCE(new.country, poi.country),
      description = COALESCE(new.description, poi.description),
      opening_hours = COALESCE(new.opening_hours, poi.opening_hours),
      phone = COALESCE(new.phone, poi.phone),
      website = COALESCE(new.website, poi.website),
      rating_count = GREATEST(new.rating_count, poi.rating_count),
      review_count = GREATEST(new.review_count, poi.review_count),
      stay_minutes = COALESCE(new.stay_minutes, poi.stay_minutes),
      best_visit_time = COALESCE(new.best_visit_time, poi.best_visit_time),
      family_friendly = GREATEST(new.family_friendly, poi.family_friendly),
      pet_friendly = GREATEST(new.pet_friendly, poi.pet_friendly),
      indoor = GREATEST(new.indoor, poi.indoor),
      crowd_level = COALESCE(new.crowd_level, poi.crowd_level),
      geom = new.geom,
      updated_at = CURRENT_TIMESTAMP
  `;

  const params = [];
  batch.forEach((row) => {
    params.push(
      row.name,
      row.category,
      row.lat,
      row.lng,
      row.popularity,
      row.price,
      row.tags,
      row.image_url,
      "OSM",
      row.sourceId,
      row.address,
      row.city,
      row.country,
      row.description,
      row.opening_hours,
      row.phone,
      row.website,
      row.rating_count,
      row.review_count,
      row.stay_minutes,
      row.best_visit_time,
      row.family_friendly,
      row.pet_friendly,
      row.indoor,
      row.crowd_level,
      row.lng,
      row.lat
    );
  });

  await conn.execute(sql, params);
};

const fillFallbackDetails = async (conn) => {
  await conn.query(`
    UPDATE poi
    SET
      description = COALESCE(
        description,
        CONCAT(name, ' is a ', COALESCE(NULLIF(category, ''), 'point of interest'), ' in ', COALESCE(city, 'London'), '.')
      ),
      opening_hours = COALESCE(
        opening_hours,
        CASE
          WHEN category IN ('park', 'transport', 'stay') THEN 'Open 24 hours'
          WHEN category IN ('food') THEN '10:00-22:00'
          WHEN category IN ('museum', 'heritage', 'attraction') THEN '10:00-17:00'
          WHEN category IN ('shopping', 'market') THEN '10:00-20:00'
          ELSE '09:00-18:00'
        END
      ),
      stay_minutes = CASE
        WHEN stay_minutes IS NULL OR stay_minutes <= 0 THEN
          CASE
            WHEN category = 'transport' THEN 25
            WHEN category = 'food' THEN 75
            WHEN category = 'park' THEN 90
            WHEN category = 'museum' THEN 120
            WHEN category = 'attraction' THEN 95
            WHEN category = 'shopping' THEN 80
            ELSE 70
          END
        ELSE stay_minutes
      END,
      best_visit_time = COALESCE(
        best_visit_time,
        CASE
          WHEN category = 'food' THEN '11:30-14:00, 18:00-20:30'
          WHEN category = 'park' THEN '08:00-18:00'
          WHEN category = 'museum' THEN '10:00-16:30'
          WHEN category = 'attraction' THEN '10:00-17:00'
          WHEN category = 'transport' THEN 'Peak and off-peak'
          ELSE '10:00-18:00'
        END
      ),
      rating_count = CASE
        WHEN rating_count IS NULL OR rating_count <= 0 THEN 30 + MOD(id * 37, 2200)
        ELSE rating_count
      END,
      review_count = CASE
        WHEN review_count IS NULL OR review_count <= 0 THEN 10 + MOD(id * 19, 900)
        ELSE review_count
      END,
      crowd_level = CASE
        WHEN crowd_level IS NULL OR crowd_level < 1 OR crowd_level > 5 THEN 1 + MOD(id, 5)
        ELSE crowd_level
      END,
      family_friendly = CASE
        WHEN family_friendly IS NULL THEN 0
        ELSE family_friendly
      END,
      pet_friendly = CASE
        WHEN pet_friendly IS NULL THEN 0
        ELSE pet_friendly
      END,
      indoor = CASE
        WHEN indoor IS NULL THEN 0
        ELSE indoor
      END
  `);
};

const main = async () => {
  if (!fs.existsSync(NDJSON_PATH)) {
    throw new Error(`NDJSON file not found: ${NDJSON_PATH}`);
  }

  const conn = await mysql.createConnection(DB);
  await ensurePoiDetailColumns(conn);

  const rl = readline.createInterface({
    input: fs.createReadStream(NDJSON_PATH, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let seen = 0;
  let kept = 0;
  let batch = [];

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    let feature;
    try {
      feature = JSON.parse(line);
    } catch {
      continue;
    }

    seen += 1;

    const properties = feature.properties || {};
    const sourceId = pickSourceId(feature);
    if (!sourceId) continue;

    const category = mapCategory(properties);
    if (!category) continue;

    const name = safeTrim(pickName(properties), 100);
    if (!name) continue;

    const coords = toPointCoords(feature);
    if (!coords) continue;

    const [lng, lat] = coords;
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    if (lat < 51.1 || lat > 51.9 || lng < -0.7 || lng > 0.4) continue;

    const city = safeTrim(properties["addr:city"] || "London", 80);
    const country = safeTrim(properties["addr:country"] || "GB", 40);
    const address = buildAddress(properties);

    const details = buildDetailFields({
      sourceId,
      category,
      properties,
      name,
      city,
    });

    batch.push({
      sourceId: safeTrim(sourceId, 80),
      name,
      category,
      lat,
      lng,
      popularity: buildPopularity(sourceId, category),
      price: buildPrice(sourceId, category, properties),
      tags: buildTags(properties, category),
      image_url: buildImageUrl(sourceId, category),
      address,
      city,
      country,
      description: details.description,
      opening_hours: details.openingHours,
      phone: details.phone,
      website: details.website,
      rating_count: details.ratingCount,
      review_count: details.reviewCount,
      stay_minutes: details.stayMinutes,
      best_visit_time: details.bestVisitTime,
      family_friendly: details.familyFriendly,
      pet_friendly: details.petFriendly,
      indoor: details.indoor,
      crowd_level: details.crowdLevel,
    });

    kept += 1;

    if (batch.length >= BATCH_SIZE) {
      // eslint-disable-next-line no-await-in-loop
      await flushBatch(conn, batch);
      batch = [];
      if (kept % (BATCH_SIZE * 5) === 0) {
        console.log(`processed seen=${seen} kept=${kept}`);
      }
    }

    if (IMPORT_LIMIT > 0 && kept >= IMPORT_LIMIT) break;
  }

  if (batch.length) {
    await flushBatch(conn, batch);
  }

  await fillFallbackDetails(conn);

  const [[total]] = await conn.query("SELECT COUNT(*) AS c FROM poi");
  const [topCats] = await conn.query(
    "SELECT category, COUNT(*) AS c FROM poi GROUP BY category ORDER BY c DESC LIMIT 12"
  );

  await conn.end();

  console.log(`DONE seen=${seen} imported_or_updated=${kept} total_poi=${total.c}`);
  console.log(topCats);
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
