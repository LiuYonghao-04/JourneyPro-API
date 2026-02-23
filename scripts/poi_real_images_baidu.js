import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const CONCURRENCY = Math.max(2, Number(process.env.REAL_IMG_BAIDU_CONCURRENCY || 6));
const LIMIT = Math.max(0, Number(process.env.REAL_IMG_BAIDU_LIMIT || 0));
const REQUEST_TIMEOUT_MS = Math.max(3000, Number(process.env.REAL_IMG_BAIDU_TIMEOUT_MS || 14000));
const VERIFY_URL = String(process.env.REAL_IMG_BAIDU_VERIFY || "1") !== "0";
const MAX_NAME_QUERIES = Math.max(0, Number(process.env.REAL_IMG_BAIDU_MAX_NAME_QUERIES || 22000));
const BAIDU_RESULT_COUNT = Math.max(10, Math.min(60, Number(process.env.REAL_IMG_BAIDU_RN || 30)));
const LOG_EVERY = Math.max(20, Number(process.env.REAL_IMG_BAIDU_LOG_EVERY || 120));
const RETRIES = Math.max(0, Number(process.env.REAL_IMG_BAIDU_RETRIES || 2));
const CATEGORY_POOL_TARGET = Math.max(8, Number(process.env.REAL_IMG_BAIDU_CATEGORY_POOL || 24));
const MIN_IMAGE_BYTES = Math.max(0, Number(process.env.REAL_IMG_BAIDU_MIN_BYTES || 16000));

const BAIDU_HEADERS = {
  "User-Agent":
    process.env.REAL_IMG_BAIDU_UA ||
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  Referer: "https://image.baidu.com/",
  Accept: "application/json,text/plain,*/*",
};

const PLACEHOLDER_PATTERNS = [
  "https://picsum.photos/",
  "https://loremflickr.com/",
  "/images/placeholder",
];

const CATEGORY_HINTS = {
  food: ["restaurant", "餐厅", "美食"],
  museum: ["museum", "博物馆", "展览"],
  park: ["park", "公园", "风景"],
  attraction: ["landmark", "景点", "打卡"],
  heritage: ["heritage", "历史建筑", "古迹"],
  culture: ["culture", "文化", "艺术"],
  market: ["market", "集市", "商圈"],
  palace: ["palace", "宫殿", "皇家"],
  transport: ["station", "交通", "地标"],
  shopping: ["shopping", "商场", "购物"],
};

const CATEGORY_QUERIES = {
  food: ["伦敦 餐厅 美食 实拍", "London restaurant street photo"],
  museum: ["伦敦 博物馆 实拍", "London museum exterior"],
  park: ["伦敦 公园 风景", "London park landscape"],
  attraction: ["伦敦 地标 景点", "London landmark photo"],
  heritage: ["伦敦 历史建筑", "London heritage building"],
  culture: ["伦敦 文化 艺术 空间", "London cultural center"],
  market: ["伦敦 市场 集市", "London market street"],
  palace: ["伦敦 宫殿 皇家建筑", "London palace"],
  transport: ["伦敦 车站 交通 地标", "London station architecture"],
  shopping: ["伦敦 商场 街区", "London shopping district"],
};

const GLOBAL_FALLBACK_QUERIES = ["伦敦 城市 地标 街景", "London city street landmark photo"];

const blockedHosts = ["douyinpic.com", "xiaohongshu.com", "xhslink.com"];

const normalize = (value) => String(value || "").trim();
const normalizeLower = (value) => normalize(value).toLowerCase();

const decodeHtmlEntities = (value) =>
  normalize(value)
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");

const stripNoiseFromName = (value) => {
  const raw = normalize(value);
  if (!raw) return "";
  return raw
    .replace(/\([^)]*\)/g, " ")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/[·•]/g, " ")
    .replace(/[^\p{L}\p{N}\s'&-]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
};

const stableHash = (input) => {
  const raw = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < raw.length; i += 1) {
    hash ^= raw.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const isPlaceholderUrl = (value) => {
  const raw = normalize(value);
  if (!raw) return true;
  return PLACEHOLDER_PATTERNS.some((p) => raw.startsWith(p));
};

const toTokenSet = (value) =>
  new Set(
    normalizeLower(value)
      .replace(/[^\p{L}\p{N}\s]+/gu, " ")
      .split(/\s+/)
      .map((t) => t.trim())
      .filter((t) => t.length >= 2)
  );

const jaccard = (aRaw, bRaw) => {
  const a = toTokenSet(aRaw);
  const b = toTokenSet(bRaw);
  if (!a.size || !b.size) return 0;
  let inter = 0;
  a.forEach((token) => {
    if (b.has(token)) inter += 1;
  });
  return inter / Math.max(a.size, b.size);
};

const decodeBaiduObjUrl = (input) => {
  let text = normalize(input);
  if (!text) return "";
  if (/^https?:\/\//i.test(text)) return text;

  const replacement = {
    _z2C$q: ":",
    "_z&e3B": ".",
    AzdH3F: "/",
  };
  Object.entries(replacement).forEach(([from, to]) => {
    text = text.split(from).join(to);
  });

  const map = {
    w: "a",
    k: "b",
    v: "c",
    "1": "d",
    j: "e",
    u: "f",
    "2": "g",
    i: "h",
    t: "i",
    "3": "j",
    h: "k",
    s: "l",
    "4": "m",
    g: "n",
    "5": "o",
    r: "p",
    q: "q",
    "6": "r",
    f: "s",
    p: "t",
    "7": "u",
    e: "v",
    o: "w",
    "8": "1",
    d: "2",
    n: "3",
    "9": "4",
    c: "5",
    m: "6",
    "0": "7",
    b: "8",
    l: "9",
    a: "0",
  };

  return text
    .split("")
    .map((ch) => map[ch] || ch)
    .join("");
};

const unique = (items) => [...new Set(items.filter(Boolean))];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const toSafeImageUrl = (value) => {
  const decoded = decodeBaiduObjUrl(value).replace(/\\\//g, "/");
  if (!decoded) return "";
  try {
    const u = new URL(decoded);
    if (!["http:", "https:"].includes(u.protocol)) return "";
    const out = u.toString();
    if (out.length > 590) return "";
    return out;
  } catch {
    return "";
  }
};

const urlVerifyCache = new Map();
const baiduQueryCache = new Map();
const categoryPoolCache = new Map();
const globalPoolCache = new Map();

const verifyImageUrl = async (url) => {
  const safeUrl = normalize(url);
  if (!safeUrl) return false;
  if (!VERIFY_URL) return true;
  if (urlVerifyCache.has(safeUrl)) return urlVerifyCache.get(safeUrl);

  const promise = (async () => {
    try {
      const head = await fetch(safeUrl, {
        method: "HEAD",
        headers: { "User-Agent": BAIDU_HEADERS["User-Agent"] },
        redirect: "follow",
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (head.ok) {
        const type = normalizeLower(head.headers.get("content-type"));
        const length = Number(head.headers.get("content-length") || 0);
        if (type.startsWith("image/") && (length <= 0 || length >= MIN_IMAGE_BYTES)) {
          return true;
        }
      }
    } catch {
      // fallback GET below
    }

    try {
      const get = await fetch(safeUrl, {
        method: "GET",
        headers: {
          "User-Agent": BAIDU_HEADERS["User-Agent"],
          Range: "bytes=0-1024",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (!get.ok) return false;
      const type = normalizeLower(get.headers.get("content-type"));
      return type.startsWith("image/");
    } catch {
      return false;
    }
  })();

  urlVerifyCache.set(safeUrl, promise);
  return promise;
};

const httpGetText = async (url) => {
  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: BAIDU_HEADERS,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (!response.ok) {
        if ((response.status === 429 || response.status >= 500) && attempt < RETRIES) {
          await sleep(350 + attempt * 550);
          continue;
        }
        return "";
      }
      return await response.text();
    } catch {
      if (attempt < RETRIES) {
        await sleep(300 + attempt * 550);
        continue;
      }
      return "";
    }
  }
  return "";
};

const parseBaiduCandidates = (payload) => {
  let json = null;
  try {
    json = JSON.parse(payload);
  } catch {
    json = null;
  }
  if (!json) {
    const rawUrls = [];
    const urlRegex = /"(?:objURL|middleURL|hoverURL|thumbURL)"\s*:\s*"([^"]+)"/gi;
    let m;
    while ((m = urlRegex.exec(payload))) {
      rawUrls.push(m[1]);
      if (rawUrls.length >= BAIDU_RESULT_COUNT * 8) break;
    }
    return unique(rawUrls)
      .map(toSafeImageUrl)
      .filter(Boolean)
      .map((url, idx) => ({ url, title: "", rank: idx }));
  }

  const out = [];
  const data = Array.isArray(json.data) ? json.data : [];
  data.forEach((item, index) => {
    if (!item || typeof item !== "object") return;

    const title = decodeHtmlEntities(item.fromPageTitleEnc || item.fromPageTitle || "");
    const rawUrls = [];
    rawUrls.push(item.objURL, item.middleURL, item.hoverURL, item.thumbURL);
    if (Array.isArray(item.replaceUrl)) {
      item.replaceUrl.forEach((entry) => {
        if (!entry || typeof entry !== "object") return;
        rawUrls.push(entry.objURL, entry.ObjUrl, entry.objurl, entry.OriginUrl, entry.originUrl);
      });
    }

    unique(rawUrls)
      .map(toSafeImageUrl)
      .filter(Boolean)
      .forEach((url) => {
        out.push({
          url,
          title,
          rank: index,
        });
      });
  });

  const dedup = new Map();
  out.forEach((item) => {
    const prev = dedup.get(item.url);
    if (!prev || item.rank < prev.rank) dedup.set(item.url, item);
  });
  return [...dedup.values()];
};

const fetchBaiduCandidates = async (query) => {
  const q = normalize(query);
  if (!q) return [];
  if (baiduQueryCache.has(q)) return baiduQueryCache.get(q);

  const task = (async () => {
    const url =
      "https://image.baidu.com/search/acjson?" +
      `tn=resultjson_com&ipn=rj&ct=201326592&fp=result&cl=2&lm=-1&ie=utf-8&oe=utf-8&st=-1` +
      `&face=0&istype=2&nc=1&pn=0&rn=${BAIDU_RESULT_COUNT}` +
      `&queryWord=${encodeURIComponent(q)}&word=${encodeURIComponent(q)}`;

    const body = await httpGetText(url);
    if (!body) return [];
    const candidates = parseBaiduCandidates(body);
    if (!candidates.length) return [];
    return candidates;
  })();

  baiduQueryCache.set(q, task);
  return task;
};

const buildNameQuery = (row) => {
  const cleanedName = stripNoiseFromName(row.name);
  if (!cleanedName || cleanedName.length < 3) return "";

  const city = normalize(row.city) || "London";
  const cat = normalizeLower(row.category);
  const hint = (CATEGORY_HINTS[cat] || [cat]).filter(Boolean).slice(0, 2).join(" ");

  return `${cleanedName} ${city} ${hint}`.replace(/\s+/g, " ").trim();
};

const scoreCandidate = (row, candidate) => {
  const rankScore = Math.max(0, 1 - candidate.rank / (BAIDU_RESULT_COUNT + 2));
  const titleSim = jaccard(row.name, candidate.title);
  const categoryHints = CATEGORY_HINTS[normalizeLower(row.category)] || [];
  const titleLower = normalizeLower(candidate.title);
  const hintHit = categoryHints.some((hint) => titleLower.includes(normalizeLower(hint)));

  let hostAdjust = 0;
  try {
    const host = new URL(candidate.url).host.toLowerCase();
    if (host.includes("baidu.com") || host.includes("bdstatic.com")) hostAdjust += 0.08;
    if (blockedHosts.some((blocked) => host.includes(blocked))) hostAdjust -= 0.24;
  } catch {
    hostAdjust -= 0.15;
  }

  const simWeight = titleSim > 0 ? 0.34 : 0;
  const score = rankScore * 0.58 + simWeight + (hintHit ? 0.06 : 0) + hostAdjust;
  return score;
};

const pickBestValidCandidate = async (row, candidates, maxChecks = 9) => {
  if (!Array.isArray(candidates) || !candidates.length) return null;

  const sorted = [...candidates].sort((a, b) => scoreCandidate(row, b) - scoreCandidate(row, a));
  for (let i = 0; i < sorted.length && i < maxChecks; i += 1) {
    const candidate = sorted[i];
    // eslint-disable-next-line no-await-in-loop
    const ok = await verifyImageUrl(candidate.url);
    if (!ok) continue;
    return candidate.url;
  }
  return null;
};

const getCategoryPool = async (category) => {
  const cat = normalizeLower(category) || "attraction";
  if (categoryPoolCache.has(cat)) return categoryPoolCache.get(cat);

  const task = (async () => {
    const queries = CATEGORY_QUERIES[cat] || [`伦敦 ${cat} 景点 实拍`, `London ${cat} photo`];
    const urls = [];

    for (const query of queries) {
      // eslint-disable-next-line no-await-in-loop
      const candidates = await fetchBaiduCandidates(query);
      if (!candidates.length) continue;
      const sorted = [...candidates];
      for (const candidate of sorted) {
        // eslint-disable-next-line no-await-in-loop
        const ok = await verifyImageUrl(candidate.url);
        if (!ok) continue;
        urls.push(candidate.url);
        if (urls.length >= CATEGORY_POOL_TARGET) break;
      }
      if (urls.length >= CATEGORY_POOL_TARGET) break;
    }

    return unique(urls);
  })();

  categoryPoolCache.set(cat, task);
  return task;
};

const getGlobalPool = async () => {
  const key = "__global__";
  if (globalPoolCache.has(key)) return globalPoolCache.get(key);

  const task = (async () => {
    const urls = [];
    for (const query of GLOBAL_FALLBACK_QUERIES) {
      // eslint-disable-next-line no-await-in-loop
      const candidates = await fetchBaiduCandidates(query);
      if (!candidates.length) continue;
      for (const candidate of candidates) {
        // eslint-disable-next-line no-await-in-loop
        const ok = await verifyImageUrl(candidate.url);
        if (!ok) continue;
        urls.push(candidate.url);
        if (urls.length >= CATEGORY_POOL_TARGET * 2) break;
      }
      if (urls.length >= CATEGORY_POOL_TARGET * 2) break;
    }
    return unique(urls);
  })();

  globalPoolCache.set(key, task);
  return task;
};

const pickFromCategoryPool = (row, pool) => {
  if (!Array.isArray(pool) || !pool.length) return "";
  const key = `${row.id}|${row.name}|${row.category}`;
  const index = stableHash(key) % pool.length;
  return pool[index] || "";
};

const formatTop = (map, limit = 8) =>
  Object.fromEntries([...map.entries()].sort((a, b) => b[1] - a[1]).slice(0, limit));

const main = async () => {
  const pool = mysql.createPool({
    ...DB,
    connectionLimit: 10,
    waitForConnections: true,
  });

  await pool.query(`ALTER TABLE poi MODIFY image_url VARCHAR(600) NULL`);

  const [rows] = await pool.query(
    `
      SELECT id, name, category, city, country, popularity, image_url
      FROM poi
      WHERE source='OSM'
        AND (
          image_url IS NULL OR image_url='' OR
          image_url LIKE 'https://picsum.photos/%' OR
          image_url LIKE 'https://loremflickr.com/%' OR
          NOT (image_url REGEXP '^https?://')
        )
      ORDER BY popularity DESC, id ASC
      ${LIMIT > 0 ? "LIMIT ?" : ""}
    `,
    LIMIT > 0 ? [LIMIT] : []
  );

  if (!rows.length) {
    await pool.end();
    console.log("No rows need replacement.");
    return;
  }

  console.log(
    `start rows=${rows.length} concurrency=${CONCURRENCY} verify=${VERIFY_URL} max_name_queries=${MAX_NAME_QUERIES}`
  );

  const queue = [...rows];
  let scanned = 0;
  let updated = 0;
  let noMatch = 0;
  let nameHit = 0;
  let categoryHit = 0;
  let nameQuerySkipped = 0;
  const hostCounter = new Map();
  const sourceCounter = new Map();
  const nameQueryUsed = new Set();

  const worker = async () => {
    while (queue.length) {
      const row = queue.shift();
      if (!row) return;
      scanned += 1;

      let chosenUrl = "";
      let source = "";
      const nameQuery = buildNameQuery(row);
      const canUseNameQuery = Boolean(
        nameQuery &&
          (nameQueryUsed.has(nameQuery) || MAX_NAME_QUERIES <= 0 || nameQueryUsed.size < MAX_NAME_QUERIES)
      );

      if (canUseNameQuery) {
        nameQueryUsed.add(nameQuery);
        // eslint-disable-next-line no-await-in-loop
        const candidates = await fetchBaiduCandidates(nameQuery);
        // eslint-disable-next-line no-await-in-loop
        chosenUrl = await pickBestValidCandidate(row, candidates, 9);
        if (chosenUrl) {
          source = "baidu:name";
          nameHit += 1;
        }
      } else if (nameQuery) {
        nameQuerySkipped += 1;
      }

      if (!chosenUrl) {
        // eslint-disable-next-line no-await-in-loop
        const categoryPool = await getCategoryPool(row.category);
        chosenUrl = pickFromCategoryPool(row, categoryPool);
        if (chosenUrl) {
          source = "baidu:category";
          categoryHit += 1;
        }
      }

      if (!chosenUrl) {
        // eslint-disable-next-line no-await-in-loop
        const globalPool = await getGlobalPool();
        chosenUrl = pickFromCategoryPool(row, globalPool);
        if (chosenUrl) {
          source = "baidu:global";
          categoryHit += 1;
        }
      }

      if (!chosenUrl) {
        noMatch += 1;
        if (scanned % LOG_EVERY === 0) {
          console.log(`progress scanned=${scanned} updated=${updated} no_match=${noMatch}`);
        }
        continue;
      }

      if (!isPlaceholderUrl(row.image_url) && normalize(row.image_url) === chosenUrl) {
        if (scanned % LOG_EVERY === 0) {
          console.log(`progress scanned=${scanned} updated=${updated} no_match=${noMatch}`);
        }
        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      await pool.query(`UPDATE poi SET image_url=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, [chosenUrl, row.id]);
      updated += 1;
      sourceCounter.set(source, (sourceCounter.get(source) || 0) + 1);

      try {
        const host = new URL(chosenUrl).host.toLowerCase();
        hostCounter.set(host, (hostCounter.get(host) || 0) + 1);
      } catch {
        // ignore
      }

      if (scanned % LOG_EVERY === 0 || updated % LOG_EVERY === 0) {
        console.log(`progress scanned=${scanned} updated=${updated} no_match=${noMatch}`);
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length) }, () => worker()));

  const [[summary]] = await pool.query(
    `
      SELECT
        COUNT(*) AS total,
        SUM(image_url IS NULL OR image_url='') AS empty_count,
        SUM(image_url LIKE 'https://loremflickr.com/%') AS lorem_count,
        SUM(image_url LIKE 'https://picsum.photos/%') AS picsum_count
      FROM poi
      WHERE source='OSM'
    `
  );

  await pool.end();

  console.log("done", {
    scanned,
    updated,
    noMatch,
    nameHit,
    categoryHit,
    nameQuerySkipped,
    uniqueNameQueries: nameQueryUsed.size,
    sourceBreakdown: formatTop(sourceCounter, 8),
    topHosts: formatTop(hostCounter, 12),
    totals: {
      total: Number(summary.total) || 0,
      empty: Number(summary.empty_count) || 0,
      lorem: Number(summary.lorem_count) || 0,
      picsum: Number(summary.picsum_count) || 0,
    },
  });
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
