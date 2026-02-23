const BAIDU_HEADERS = {
  "User-Agent":
    process.env.POI_PHOTO_UA ||
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  Referer: "https://image.baidu.com/",
  Accept: "application/json,text/plain,*/*",
};

const CATEGORY_HINTS = {
  food: ["restaurant", "dining", "food", "cafe", "餐厅", "美食"],
  museum: ["museum", "gallery", "exhibit", "博物馆"],
  park: ["park", "garden", "green", "公园"],
  attraction: ["landmark", "attraction", "view", "景点"],
  heritage: ["heritage", "historic", "history", "古迹"],
  culture: ["culture", "art", "theatre", "文化"],
  market: ["market", "street", "bazaar", "集市"],
  palace: ["palace", "royal", "宫殿"],
  transport: ["station", "transport", "terminal", "车站"],
  shopping: ["shopping", "mall", "retail", "商场"],
};

const CATEGORY_QUERIES = {
  food: ["London restaurant street photo", "伦敦 餐厅 美食 实拍"],
  museum: ["London museum exterior photo", "伦敦 博物馆 实拍"],
  park: ["London park landscape photo", "伦敦 公园 风景"],
  attraction: ["London landmark city photo", "伦敦 地标 景点"],
  heritage: ["London heritage historic building", "伦敦 历史建筑"],
  culture: ["London cultural center art venue", "伦敦 文化 艺术 空间"],
  market: ["London market street photo", "伦敦 集市 市场"],
  palace: ["London palace architecture", "伦敦 宫殿 皇家建筑"],
  transport: ["London station architecture photo", "伦敦 车站 交通 地标"],
  shopping: ["London shopping district photo", "伦敦 商场 街区"],
};

const queryCache = new Map();
const verifyCache = new Map();
const categoryPoolCache = new Map();

const normalize = (value) => String(value || "").trim();
const normalizeLower = (value) => normalize(value).toLowerCase();

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const isHttpUrl = (value) => /^https?:\/\//i.test(normalize(value));

const isHttpImageUrl = (value) => isHttpUrl(value);

const decodeBaiduObjUrl = (input) => {
  let text = normalize(input);
  if (!text) return "";
  if (isHttpUrl(text)) return text;

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

const toSafeHttpUrl = (value) => {
  const decoded = decodeBaiduObjUrl(value).replace(/\\\//g, "/");
  if (!decoded) return "";
  try {
    const parsed = new URL(decoded);
    if (!["http:", "https:"].includes(parsed.protocol)) return "";
    const out = parsed.toString();
    if (out.length > 590) return "";
    return out;
  } catch {
    return "";
  }
};

const tokenSet = (value) =>
  new Set(
    normalizeLower(value)
      .replace(/[^\p{L}\p{N}\s]+/gu, " ")
      .split(/\s+/)
      .map((item) => item.trim())
      .filter((item) => item.length >= 2)
  );

const similarity = (a, b) => {
  const sa = tokenSet(a);
  const sb = tokenSet(b);
  if (!sa.size || !sb.size) return 0;
  let overlap = 0;
  sa.forEach((token) => {
    if (sb.has(token)) overlap += 1;
  });
  return overlap / Math.max(sa.size, sb.size);
};

const fetchTextWithRetry = async (url, timeoutMs, retries = 1) => {
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: BAIDU_HEADERS,
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!response.ok) {
        if ((response.status === 429 || response.status >= 500) && attempt < retries) {
          await sleep(250 + attempt * 400);
          continue;
        }
        return "";
      }
      return await response.text();
    } catch {
      if (attempt < retries) {
        await sleep(250 + attempt * 400);
        continue;
      }
      return "";
    }
  }
  return "";
};

const parseCandidatesFromPayload = (payload) => {
  let json = null;
  try {
    json = JSON.parse(payload);
  } catch {
    json = null;
  }

  const entries = [];
  if (json && Array.isArray(json.data)) {
    json.data.forEach((item, index) => {
      if (!item || typeof item !== "object") return;
      const title = normalize(item.fromPageTitleEnc || item.fromPageTitle);
      const urls = [
        item.objURL,
        item.middleURL,
        item.hoverURL,
        item.thumbURL,
        ...(Array.isArray(item.replaceUrl)
          ? item.replaceUrl.flatMap((entry) => [
              entry?.objURL,
              entry?.ObjUrl,
              entry?.objurl,
              entry?.OriginUrl,
              entry?.originUrl,
            ])
          : []),
      ];
      unique(urls)
        .map(toSafeHttpUrl)
        .filter(Boolean)
        .forEach((url) => entries.push({ url, title, rank: index }));
    });
  }

  if (!entries.length) {
    const regex = /"(?:objURL|middleURL|hoverURL|thumbURL)"\s*:\s*"([^"]+)"/gi;
    let match;
    let rank = 0;
    while ((match = regex.exec(payload))) {
      const url = toSafeHttpUrl(match[1]);
      if (!url) continue;
      entries.push({ url, title: "", rank });
      rank += 1;
      if (entries.length >= 300) break;
    }
  }

  const dedup = new Map();
  entries.forEach((item) => {
    const current = dedup.get(item.url);
    if (!current || item.rank < current.rank) dedup.set(item.url, item);
  });
  return [...dedup.values()];
};

const fetchBaiduCandidates = async (query, options = {}) => {
  const q = normalize(query);
  if (!q) return [];
  if (queryCache.has(q)) return queryCache.get(q);

  const task = (async () => {
    const resultCount = Math.max(10, Math.min(50, Number(options.resultCount || 30)));
    const timeoutMs = Math.max(3000, Number(options.timeoutMs || 12000));
    const retries = Math.max(0, Number(options.retries || 1));

    const url =
      "https://image.baidu.com/search/acjson?" +
      `tn=resultjson_com&ipn=rj&ct=201326592&fp=result&cl=2&lm=-1&ie=utf-8&oe=utf-8&st=-1` +
      `&face=0&istype=2&nc=1&pn=0&rn=${resultCount}` +
      `&queryWord=${encodeURIComponent(q)}&word=${encodeURIComponent(q)}`;
    const text = await fetchTextWithRetry(url, timeoutMs, retries);
    if (!text) return [];
    return parseCandidatesFromPayload(text);
  })();

  queryCache.set(q, task);
  return task;
};

const verifyImageUrl = async (url, options = {}) => {
  const safeUrl = normalize(url);
  if (!safeUrl) return false;
  if (verifyCache.has(safeUrl)) return verifyCache.get(safeUrl);

  const task = (async () => {
    const timeoutMs = Math.max(3000, Number(options.timeoutMs || 12000));
    const minBytes = Math.max(0, Number(options.minBytes || 10000));
    try {
      const head = await fetch(safeUrl, {
        method: "HEAD",
        headers: { "User-Agent": BAIDU_HEADERS["User-Agent"] },
        redirect: "follow",
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (head.ok) {
        const type = normalizeLower(head.headers.get("content-type"));
        const len = Number(head.headers.get("content-length") || 0);
        if (type.startsWith("image/") && (len <= 0 || len >= minBytes)) return true;
      }
    } catch {
      // fall back to ranged GET
    }
    try {
      const get = await fetch(safeUrl, {
        method: "GET",
        headers: {
          "User-Agent": BAIDU_HEADERS["User-Agent"],
          Range: "bytes=0-1024",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!get.ok) return false;
      const type = normalizeLower(get.headers.get("content-type"));
      return type.startsWith("image/");
    } catch {
      return false;
    }
  })();

  verifyCache.set(safeUrl, task);
  return task;
};

const scoreCandidate = (poi, candidate, resultCount = 30) => {
  const rankScore = Math.max(0, 1 - candidate.rank / (resultCount + 2));
  const titleScore = similarity(poi?.name, candidate.title);
  const catKey = normalizeLower(poi?.category);
  const hints = CATEGORY_HINTS[catKey] || [];
  const titleLower = normalizeLower(candidate.title);
  const hintHit = hints.some((hint) => titleLower.includes(normalizeLower(hint)));

  let hostWeight = 0;
  try {
    const host = new URL(candidate.url).host.toLowerCase();
    if (host.includes("baidu.com") || host.includes("bdstatic.com")) hostWeight += 0.08;
    if (host.includes("douyinpic.com")) hostWeight -= 0.2;
  } catch {
    hostWeight -= 0.1;
  }

  return rankScore * 0.62 + titleScore * 0.28 + (hintHit ? 0.06 : 0) + hostWeight;
};

const pickFromCandidates = async (poi, candidates, options = {}) => {
  const targetCount = Math.max(1, Number(options.targetCount || 6));
  const resultCount = Math.max(10, Math.min(50, Number(options.resultCount || 30)));
  const verify = options.verify !== false;
  const maxChecks = Math.max(6, Number(options.maxChecks || 24));
  const chosen = [];
  const sorted = [...(candidates || [])].sort(
    (a, b) => scoreCandidate(poi, b, resultCount) - scoreCandidate(poi, a, resultCount)
  );
  for (let i = 0; i < sorted.length && i < maxChecks; i += 1) {
    const candidate = sorted[i];
    if (!candidate?.url || chosen.includes(candidate.url)) continue;
    if (verify) {
      // eslint-disable-next-line no-await-in-loop
      const ok = await verifyImageUrl(candidate.url, options);
      if (!ok) continue;
    }
    chosen.push(candidate.url);
    if (chosen.length >= targetCount) break;
  }
  return chosen;
};

const buildCategoryPool = async (category, options = {}) => {
  const cat = normalizeLower(category) || "attraction";
  if (categoryPoolCache.has(cat)) return categoryPoolCache.get(cat);

  const task = (async () => {
    const target = Math.max(12, Number(options.categoryPoolSize || 40));
    const queries = CATEGORY_QUERIES[cat] || [`London ${cat} photo`, `伦敦 ${cat} 景点 实拍`];
    const urls = [];
    for (const query of queries) {
      // eslint-disable-next-line no-await-in-loop
      const candidates = await fetchBaiduCandidates(query, options);
      // eslint-disable-next-line no-await-in-loop
      const picked = await pickFromCandidates({ category: cat, name: query }, candidates, {
        ...options,
        targetCount: target,
        maxChecks: 60,
      });
      picked.forEach((url) => {
        if (!urls.includes(url)) urls.push(url);
      });
      if (urls.length >= target) break;
    }
    return urls;
  })();

  categoryPoolCache.set(cat, task);
  return task;
};

const buildNameQueries = (poi) => {
  const queries = [];
  const name = normalize(poi?.name);
  if (!name) return queries;
  const city = normalize(poi?.city) || "London";
  const category = normalize(poi?.category);
  queries.push(`${name} ${city} ${category}`.trim());
  queries.push(`${name} ${city}`.trim());
  if (category) queries.push(`${name} ${category} photo`);
  return unique(queries).slice(0, 3);
};

const fillFromPoolDeterministically = (poi, pool, output, targetCount) => {
  if (!Array.isArray(pool) || !pool.length) return;
  const key = `${poi?.id || ""}|${poi?.name || ""}|${poi?.category || ""}`;
  const seed = stableHash(key);
  for (let i = 0; i < pool.length && output.length < targetCount; i += 1) {
    const idx = (seed + i * 17) % pool.length;
    const url = pool[idx];
    if (!url || output.includes(url)) continue;
    output.push(url);
  }
};

export const getPoiPhotoUrls = async (poi, options = {}) => {
  const targetCount = Math.max(1, Number(options.targetCount || 6));
  const verify = options.verify !== false;
  const output = [];

  if (isHttpImageUrl(poi?.image_url)) output.push(normalize(poi.image_url));

  const queries = buildNameQueries(poi);
  for (const query of queries) {
    if (output.length >= targetCount) break;
    // eslint-disable-next-line no-await-in-loop
    const candidates = await fetchBaiduCandidates(query, options);
    // eslint-disable-next-line no-await-in-loop
    const picked = await pickFromCandidates(poi, candidates, {
      ...options,
      targetCount: targetCount - output.length,
      verify,
    });
    picked.forEach((url) => {
      if (!output.includes(url) && output.length < targetCount) output.push(url);
    });
  }

  if (output.length < targetCount) {
    const pool = await buildCategoryPool(poi?.category, options);
    fillFromPoolDeterministically(poi, pool, output, targetCount);
  }

  return output.slice(0, targetCount);
};

export const clearPoiPhotoCaches = () => {
  queryCache.clear();
  verifyCache.clear();
  categoryPoolCache.clear();
};
