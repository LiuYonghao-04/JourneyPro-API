import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";
import mysql from "mysql2/promise";

const NDJSON_PATH = process.env.NDJSON_PATH || path.resolve(process.cwd(), "../data/poi/london-poi.ndjson");
const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CONCURRENCY = Math.max(2, Number(process.env.REAL_IMG_CONCURRENCY || 4));
const BATCH_LIMIT = Number(process.env.REAL_IMG_LIMIT || 0);
const REQUEST_TIMEOUT_MS = Math.max(2000, Number(process.env.REAL_IMG_TIMEOUT_MS || 9000));
const USE_GEOSEARCH = String(process.env.REAL_IMG_USE_GEO || "0") === "1";
const VERIFY_DIRECT_URLS = String(process.env.REAL_IMG_VERIFY_DIRECT || "0") === "1";
const USE_LOREM_FALLBACK = String(process.env.REAL_IMG_USE_LOREM || "1") !== "0";
const ENABLE_WIKI_SOURCES = String(process.env.REAL_IMG_USE_WIKI || "0") === "1";
const LOG_FILE = process.env.REAL_IMG_LOG || "";
const USER_AGENT =
  process.env.REAL_IMG_UA ||
  "JourneyPro-POI-Image-Enricher/1.0 (open-source project; contact: journeypro-local)";

const PLACEHOLDER_PREFIXES = ["https://picsum.photos/"];

const normalize = (value) => String(value || "").trim();
const normalizeLower = (value) => normalize(value).toLowerCase();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const log = (...args) => {
  const message = args
    .map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
    .join(" ");
  // eslint-disable-next-line no-console
  console.log(message);
  if (LOG_FILE) {
    try {
      fs.appendFileSync(LOG_FILE, `${new Date().toISOString()} ${message}\n`, "utf8");
    } catch {
      // ignore
    }
  }
};

const isPlaceholderUrl = (url) => {
  const value = normalize(url);
  if (!value) return true;
  return PLACEHOLDER_PREFIXES.some((prefix) => value.startsWith(prefix));
};

const uniq = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];

const tryParseUrl = (value) => {
  try {
    return new URL(value);
  } catch {
    return null;
  }
};

const cleanWikiTitle = (value) => {
  const raw = normalize(value);
  if (!raw) return "";
  if (raw.startsWith("File:")) return raw;
  if (raw.startsWith("Category:")) return raw;
  return raw.replace(/_/g, " ");
};

const parseWikipediaTag = (value) => {
  const raw = normalize(value);
  if (!raw) return null;

  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    const parsed = tryParseUrl(raw);
    if (!parsed) return null;
    const host = parsed.host.toLowerCase();
    const wikiIndex = host.indexOf(".wikipedia.org");
    if (wikiIndex <= 0) return null;
    const lang = host.slice(0, wikiIndex);
    const pathname = decodeURIComponent(parsed.pathname || "");
    const marker = "/wiki/";
    const idx = pathname.indexOf(marker);
    if (idx < 0) return null;
    const title = pathname.slice(idx + marker.length).replace(/_/g, " ");
    if (!title) return null;
    return { lang: lang || "en", title };
  }

  const m = raw.match(/^([a-z\-]{2,12}):(.*)$/i);
  if (m) {
    return { lang: normalizeLower(m[1]) || "en", title: cleanWikiTitle(m[2]) };
  }

  return { lang: "en", title: cleanWikiTitle(raw) };
};

const parseWikidataId = (value) => {
  const raw = normalize(value).toUpperCase();
  if (!raw) return null;
  const m = raw.match(/Q\d+/);
  return m ? m[0] : null;
};

const parseCommonsFile = (value) => {
  const raw = normalize(value);
  if (!raw) return null;

  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    const parsed = tryParseUrl(raw);
    if (!parsed) return null;

    if (parsed.host.toLowerCase().includes("upload.wikimedia.org")) {
      return { directUrl: raw };
    }

    const pathname = decodeURIComponent(parsed.pathname || "");
    const marker = "/wiki/";
    const idx = pathname.indexOf(marker);
    if (idx >= 0) {
      const wikiTitle = pathname.slice(idx + marker.length);
      if (wikiTitle.startsWith("File:")) {
        return { fileTitle: wikiTitle };
      }
    }
    return null;
  }

  if (raw.startsWith("File:")) {
    return { fileTitle: raw };
  }

  return null;
};

const normalizeWikiImageFilename = (filename) => {
  const raw = normalize(filename);
  if (!raw) return null;
  if (raw.startsWith("File:")) return raw;
  return `File:${raw}`;
};

const httpJson = async (url, retries = 1) => {
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (response.status === 429 || response.status >= 500) {
        if (attempt < retries) {
          await sleep(350 + attempt * 450);
          continue;
        }
      }
      if (!response.ok) return null;
      return await response.json();
    } catch {
      if (attempt < retries) {
        await sleep(300 + attempt * 350);
        continue;
      }
      return null;
    }
  }
  return null;
};

const verifyImageUrl = async (url) => {
  const safeUrl = normalize(url);
  if (!safeUrl) return false;
  if (safeUrl.length > 590) return false;

  try {
    const response = await fetch(safeUrl, {
      method: "HEAD",
      headers: {
        "User-Agent": USER_AGENT,
      },
      redirect: "follow",
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) return false;
    const type = String(response.headers.get("content-type") || "").toLowerCase();
    if (type.startsWith("image/")) return true;
  } catch {
    // ignore
  }

  // Some hosts don't support HEAD.
  try {
    const response = await fetch(safeUrl, {
      method: "GET",
      headers: {
        "User-Agent": USER_AGENT,
        Range: "bytes=0-128",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) return false;
    const type = String(response.headers.get("content-type") || "").toLowerCase();
    return type.startsWith("image/");
  } catch {
    return false;
  }
};

const tokenSet = (value) =>
  new Set(
    normalizeLower(value)
      .replace(/[^\p{L}\p{N}\s]+/gu, " ")
      .split(/\s+/)
      .map((item) => item.trim())
      .filter((item) => item && item.length >= 2)
  );

const toSearchTokens = (value) =>
  normalizeLower(value)
    .replace(/[^\\p{L}\\p{N}\\s]+/gu, " ")
    .split(/\\s+/)
    .map((item) => item.trim())
    .filter((item) => item && item.length >= 2);

const buildLoremFlickrUrl = (row) => {
  const nameTokens = toSearchTokens(row.name).slice(0, 3);
  const categoryToken = toSearchTokens(row.category).slice(0, 1);
  const tags = uniq([...nameTokens, ...categoryToken, "london"]).slice(0, 5);
  if (!tags.length) return null;
  const query = tags.map(encodeURIComponent).join(",");
  const lock = Math.max(1, Number(row.id) || 1);
  return `https://loremflickr.com/960/640/${query}/all?lock=${lock}`;
};

const titleSimilarity = (a, b) => {
  const ta = tokenSet(a);
  const tb = tokenSet(b);
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  ta.forEach((item) => {
    if (tb.has(item)) inter += 1;
  });
  return inter / Math.max(ta.size, tb.size);
};

const shouldUseGeoSearch = (row) => {
  const category = normalizeLower(row.category);
  if (["attraction", "museum", "park", "heritage", "culture", "market", "palace"].includes(category)) {
    return true;
  }
  if (category === "food") {
    const popularity = Number(row.popularity) || 0;
    return popularity >= 4.35;
  }
  return false;
};

const buildOsmMeta = (properties) => {
  const commonsKeys = [
    "wikimedia_commons",
    "wikimedia_commons:0",
    "wikimedia_commons:1",
    "wikimedia_commons:2",
    "wikimedia_commons:3",
  ];

  const wikipediaKeys = ["wikipedia", "wikipedia:en"];
  const wikiBrandKeys = [
    "brand:wikipedia",
    "operator:wikipedia",
    "building:wikipedia",
    "subject:wikipedia",
    "brewery:wikipedia",
    "species:wikipedia",
    "was:brand:wikipedia",
  ];

  const wikidataKeys = ["wikidata"];
  const wikidataBrandKeys = [
    "brand:wikidata",
    "operator:wikidata",
    "building:wikidata",
    "subject:wikidata",
    "brewery:wikidata",
    "species:wikidata",
    "was:brand:wikidata",
  ];

  return {
    image: normalize(properties.image),
    commons: uniq(commonsKeys.map((key) => properties[key])),
    wikipedia: uniq(wikipediaKeys.map((key) => properties[key])),
    wikipediaBrand: uniq(wikiBrandKeys.map((key) => properties[key])),
    wikidata: uniq(wikidataKeys.map((key) => properties[key])).map(parseWikidataId).filter(Boolean),
    wikidataBrand: uniq(wikidataBrandKeys.map((key) => properties[key])).map(parseWikidataId).filter(Boolean),
  };
};

const loadMetadataForSourceIds = async (sourceIdSet) => {
  const map = new Map();
  if (!sourceIdSet.size) return map;

  const rl = readline.createInterface({
    input: fs.createReadStream(NDJSON_PATH, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const raw = normalize(line);
    if (!raw) continue;

    let feature;
    try {
      feature = JSON.parse(raw);
    } catch {
      continue;
    }

    const sourceId = normalize(feature.id || feature?.properties?.["@id"]);
    if (!sourceId || !sourceIdSet.has(sourceId)) continue;

    if (!map.has(sourceId)) {
      map.set(sourceId, buildOsmMeta(feature.properties || {}));
      if (map.size >= sourceIdSet.size) break;
    }
  }

  return map;
};

const commonsFileCache = new Map();
const wikiSummaryCache = new Map();
const wikidataImageCache = new Map();
const directImageCache = new Map();
const geosearchCache = new Map();

const resolveCommonsFileUrl = async (fileTitleRaw) => {
  const fileTitle = normalizeWikiImageFilename(fileTitleRaw);
  if (!fileTitle) return null;
  if (commonsFileCache.has(fileTitle)) return commonsFileCache.get(fileTitle);

  const url =
    "https://commons.wikimedia.org/w/api.php" +
    `?action=query&format=json&origin=*` +
    `&prop=imageinfo&iiprop=url|extmetadata|mime|size&iiurlwidth=1400` +
    `&titles=${encodeURIComponent(fileTitle)}`;

  const json = await httpJson(url, 1);
  const pages = json?.query?.pages || {};
  const firstPage = pages[Object.keys(pages)[0]];
  const info = firstPage?.imageinfo?.[0];
  const thumbUrl = normalize(info?.thumburl || info?.url || "");

  const result = thumbUrl || null;
  commonsFileCache.set(fileTitle, result);
  return result;
};

const resolveWikipediaThumbnail = async (lang, titleRaw) => {
  const safeLang = normalizeLower(lang || "en") || "en";
  const title = cleanWikiTitle(titleRaw);
  if (!title) return null;

  const key = `${safeLang}:${title}`;
  if (wikiSummaryCache.has(key)) return wikiSummaryCache.get(key);

  const summaryUrl = `https://${safeLang}.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`;
  const summary = await httpJson(summaryUrl, 1);
  let candidate =
    normalize(summary?.originalimage?.source) || normalize(summary?.thumbnail?.source) || null;

  if (!candidate) {
    const queryUrl =
      `https://${safeLang}.wikipedia.org/w/api.php` +
      `?action=query&format=json&origin=*` +
      `&prop=pageimages&piprop=thumbnail&pithumbsize=1400&titles=${encodeURIComponent(title)}`;
    const json = await httpJson(queryUrl, 1);
    const pages = json?.query?.pages || {};
    const page = pages[Object.keys(pages)[0]];
    candidate = normalize(page?.thumbnail?.source) || null;
  }

  wikiSummaryCache.set(key, candidate || null);
  return candidate || null;
};

const resolveWikidataImage = async (qidRaw) => {
  const qid = parseWikidataId(qidRaw);
  if (!qid) return null;
  if (wikidataImageCache.has(qid)) return wikidataImageCache.get(qid);

  const entityUrl = `https://www.wikidata.org/wiki/Special:EntityData/${qid}.json`;
  const json = await httpJson(entityUrl, 1);
  const entity = json?.entities?.[qid];
  const p18 = entity?.claims?.P18?.[0]?.mainsnak?.datavalue?.value;
  const fileUrl = await resolveCommonsFileUrl(p18);

  wikidataImageCache.set(qid, fileUrl || null);
  return fileUrl || null;
};

const resolveDirectImage = async (value) => {
  const raw = normalize(value);
  if (!raw) return null;
  if (directImageCache.has(raw)) return directImageCache.get(raw);

  let candidate = null;
  const commons = parseCommonsFile(raw);
  if (commons?.directUrl) {
    candidate = commons.directUrl;
  } else if (ENABLE_WIKI_SOURCES && commons?.fileTitle) {
    candidate = await resolveCommonsFileUrl(commons.fileTitle);
  } else if (/\.(jpg|jpeg|png|webp|gif)(\?|#|$)/i.test(raw)) {
    candidate = raw;
  }

  if (!candidate) {
    directImageCache.set(raw, null);
    return null;
  }

  let result = candidate;
  if (VERIFY_DIRECT_URLS) {
    const ok = await verifyImageUrl(candidate);
    result = ok ? candidate : null;
  }
  directImageCache.set(raw, result);
  return result;
};

const resolveGeoSearchImage = async (row) => {
  const lat = Number(row.lat);
  const lng = Number(row.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  const key = `${lat.toFixed(3)},${lng.toFixed(3)}`;
  if (geosearchCache.has(key)) {
    const candidates = geosearchCache.get(key) || [];
    return candidates.find((item) => item && item.image) || null;
  }

  const url =
    "https://en.wikipedia.org/w/api.php" +
    `?action=query&format=json&origin=*` +
    `&list=geosearch&gscoord=${lat}|${lng}&gsradius=1200&gslimit=8`;

  const json = await httpJson(url, 1);
  const items = json?.query?.geosearch || [];

  const resolved = [];
  for (const item of items) {
    const title = normalize(item?.title);
    if (!title) continue;
    // eslint-disable-next-line no-await-in-loop
    const image = await resolveWikipediaThumbnail("en", title);
    if (!image) continue;
    resolved.push({
      title,
      image,
      dist: Number(item?.dist) || 0,
      pageid: item?.pageid,
    });
    if (resolved.length >= 6) break;
  }

  geosearchCache.set(key, resolved);

  if (!resolved.length) return null;

  const targetName = normalize(row.name);
  let best = null;
  let bestScore = 0;
  resolved.forEach((item) => {
    const sim = titleSimilarity(targetName, item.title);
    const distScore = Math.max(0, 1 - (Number(item.dist) || 0) / 1400);
    const score = sim * 0.78 + distScore * 0.22;
    if (score > bestScore) {
      bestScore = score;
      best = item;
    }
  });

  if (!best || bestScore < 0.34) return null;
  return best;
};

const resolveRealImage = async (row, meta) => {
  if (!meta) return null;

  const direct = await resolveDirectImage(meta.image);
  if (direct) {
    return { url: direct, source: "osm:image" };
  }

  if (ENABLE_WIKI_SOURCES) {
    for (const commonsRaw of meta.commons || []) {
      // eslint-disable-next-line no-await-in-loop
      const parsed = parseCommonsFile(commonsRaw);
      if (!parsed) continue;
      let url = null;
      if (parsed.directUrl) {
        // eslint-disable-next-line no-await-in-loop
        url = await resolveDirectImage(parsed.directUrl);
      } else if (parsed.fileTitle) {
        // eslint-disable-next-line no-await-in-loop
        url = await resolveCommonsFileUrl(parsed.fileTitle);
      }
      if (url) return { url, source: "wikimedia_commons" };
    }

    for (const wikiRaw of meta.wikipedia || []) {
      // eslint-disable-next-line no-await-in-loop
      const parsed = parseWikipediaTag(wikiRaw);
      if (!parsed) continue;
      // eslint-disable-next-line no-await-in-loop
      const url = await resolveWikipediaThumbnail(parsed.lang, parsed.title);
      if (url) return { url, source: `wikipedia:${parsed.lang}` };
    }

    for (const qid of meta.wikidata || []) {
      // eslint-disable-next-line no-await-in-loop
      const url = await resolveWikidataImage(qid);
      if (url) return { url, source: "wikidata:P18" };
    }

    for (const wikiRaw of meta.wikipediaBrand || []) {
      // eslint-disable-next-line no-await-in-loop
      const parsed = parseWikipediaTag(wikiRaw);
      if (!parsed) continue;
      // eslint-disable-next-line no-await-in-loop
      const url = await resolveWikipediaThumbnail(parsed.lang, parsed.title);
      if (url) return { url, source: `brand_wikipedia:${parsed.lang}` };
    }

    for (const qid of meta.wikidataBrand || []) {
      // eslint-disable-next-line no-await-in-loop
      const url = await resolveWikidataImage(qid);
      if (url) return { url, source: "brand_wikidata:P18" };
    }

    if (USE_GEOSEARCH && shouldUseGeoSearch(row)) {
      const geo = await resolveGeoSearchImage(row);
      if (geo?.image) {
        return { url: geo.image, source: "wikipedia_geosearch", hint_title: geo.title };
      }
    }
  }

  if (USE_LOREM_FALLBACK) {
    const fallback = buildLoremFlickrUrl(row);
    if (fallback) return { url: fallback, source: "loremflickr_fallback" };
  }

  return null;
};

const main = async () => {
  if (!fs.existsSync(NDJSON_PATH)) {
    throw new Error(`NDJSON not found: ${NDJSON_PATH}`);
  }

  const pool = mysql.createPool({
    ...DB,
    connectionLimit: 8,
    waitForConnections: true,
  });

  await pool.query(`ALTER TABLE poi MODIFY image_url VARCHAR(600) NULL`);

  const [rows] = await pool.query(
    `
      SELECT id, source_id, name, category, lat, lng, popularity, image_url
      FROM poi
      WHERE source = 'OSM'
        AND (image_url IS NULL OR image_url = '' OR image_url LIKE 'https://picsum.photos/%')
      ORDER BY popularity DESC, id ASC
      ${BATCH_LIMIT > 0 ? "LIMIT ?" : ""}
    `,
    BATCH_LIMIT > 0 ? [BATCH_LIMIT] : []
  );

  if (!rows.length) {
    await pool.end();
    console.log("No placeholder image rows found.");
    return;
  }

  const sourceIdSet = new Set(rows.map((row) => normalize(row.source_id)).filter(Boolean));
  log(`rows_to_process=${rows.length}, source_ids=${sourceIdSet.size}`);
  log(
    `config concurrency=${CONCURRENCY} use_wiki=${ENABLE_WIKI_SOURCES} use_geosearch=${USE_GEOSEARCH} verify_direct=${VERIFY_DIRECT_URLS} use_lorem=${USE_LOREM_FALLBACK}`
  );

  const metadataMap = await loadMetadataForSourceIds(sourceIdSet);
  log(`metadata_loaded=${metadataMap.size}`);

  let scanned = 0;
  let updated = 0;
  let missingMeta = 0;
  let noMatch = 0;
  const sourceCounter = new Map();

  const queue = [...rows];

  const worker = async () => {
    while (queue.length) {
      const row = queue.shift();
      if (!row) return;
      scanned += 1;

      const sourceId = normalize(row.source_id);
      const meta = metadataMap.get(sourceId);
      if (!meta) {
        missingMeta += 1;
        if (scanned % 50 === 0) {
          log(`progress scanned=${scanned} updated=${updated}`);
        }
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      const result = await resolveRealImage(row, meta);
      if (!result?.url) {
        noMatch += 1;
        if (scanned % 50 === 0) {
          log(`progress scanned=${scanned} updated=${updated}`);
        }
        continue;
      }

      if (isPlaceholderUrl(result.url)) {
        noMatch += 1;
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      await pool.query(`UPDATE poi SET image_url = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [
        result.url,
        row.id,
      ]);

      updated += 1;
      sourceCounter.set(result.source, (sourceCounter.get(result.source) || 0) + 1);

      if (updated % 50 === 0 || scanned % 50 === 0) {
        log(`progress scanned=${scanned} updated=${updated}`);
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length) }, () => worker()));

  const [[afterRow]] = await pool.query(
    `
      SELECT
        COUNT(*) AS total,
        SUM(image_url LIKE 'https://picsum.photos/%' OR image_url IS NULL OR image_url='') AS placeholder,
        SUM(NOT (image_url LIKE 'https://picsum.photos/%' OR image_url IS NULL OR image_url='')) AS real_like
      FROM poi
      WHERE source='OSM'
    `
  );

  await pool.end();

  log("DONE", {
    scanned,
    updated,
    missingMeta,
    noMatch,
    source_breakdown: Object.fromEntries([...sourceCounter.entries()].sort((a, b) => b[1] - a[1])),
    osm_total: Number(afterRow.total) || 0,
    osm_placeholder: Number(afterRow.placeholder) || 0,
    osm_real_like: Number(afterRow.real_like) || 0,
  });
};

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  if (LOG_FILE) {
    try {
      fs.appendFileSync(LOG_FILE, `${new Date().toISOString()} ERROR ${String(err?.stack || err)}\n`, "utf8");
    } catch {
      // ignore
    }
  }
  process.exit(1);
});
