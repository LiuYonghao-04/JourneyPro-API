import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const DISTRIBUTION_MODE = String(process.env.SEED_POST_DISTRIBUTION || "weighted").trim().toLowerCase();
const POST_TARGET = Math.max(120, Number(process.env.SEED_POSTS_TARGET || 100000));
const USER_TARGET = Math.max(8, Number(process.env.SEED_USERS_TARGET || 28));
const POI_CANDIDATES = Math.max(POST_TARGET, Number(process.env.SEED_POI_CANDIDATES || 50000));
const COMMENTS_MIN = Math.max(0, Number(process.env.SEED_COMMENTS_MIN || 0));
const COMMENTS_MAX = Math.max(COMMENTS_MIN, Number(process.env.SEED_COMMENTS_MAX || 1));
const REPLY_PROB = Math.max(0, Math.min(1, Number(process.env.SEED_REPLY_PROB || 0.18)));
const DOUBLE_REPLY_PROB = Math.max(0, Math.min(1, Number(process.env.SEED_DOUBLE_REPLY_PROB || 0.08)));
const HOT_RATIO = Math.max(0.05, Math.min(0.6, Number(process.env.SEED_HOT_RATIO || 0.15)));
const WARM_RATIO = Math.max(0.1, Math.min(0.75, Number(process.env.SEED_WARM_RATIO || 0.35)));
const COLD_MIN_POSTS = Math.max(1, Number(process.env.SEED_COLD_MIN || 10));
const COLD_MAX_POSTS = Math.max(COLD_MIN_POSTS, Number(process.env.SEED_COLD_MAX || 12));
const WARM_MIN_POSTS = Math.max(1, Number(process.env.SEED_WARM_MIN || 14));
const WARM_MAX_POSTS = Math.max(WARM_MIN_POSTS, Number(process.env.SEED_WARM_MAX || 18));
const HOT_MIN_POSTS = Math.max(1, Number(process.env.SEED_HOT_MIN || 20));
const HOT_MAX_POSTS = Math.max(HOT_MIN_POSTS, Number(process.env.SEED_HOT_MAX || 24));
const NOW = Date.now();

const normalize = (value) => String(value || "").trim();
const unique = (items) => [...new Set((items || []).map((item) => normalize(item)).filter(Boolean))];
const isHttp = (value) => /^https?:\/\//i.test(normalize(value));

const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const randPick = (arr) => arr[randInt(0, arr.length - 1)];
const randSample = (arr, count) => {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = randInt(0, i);
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, Math.min(count, copy.length));
};

const toDate = (msOffset) => new Date(NOW - msOffset);

const PROFILE_NAMES = [
  "Liam Parker",
  "Emma Stone",
  "Noah Brooks",
  "Olivia Reed",
  "Mason Clark",
  "Sophia Hayes",
  "Ethan Cole",
  "Isabella Grant",
  "Lucas Ward",
  "Mia Bennett",
  "James Cooper",
  "Charlotte Hill",
  "Benjamin Scott",
  "Amelia Turner",
  "Henry Morris",
  "Harper Lane",
  "Alexander Price",
  "Evelyn Walsh",
  "Daniel Ross",
  "Abigail Brooks",
  "Sebastian Gray",
  "Ella Carter",
  "Jack Foster",
  "Avery Miller",
  "Owen Adams",
  "Grace Perry",
  "William Young",
  "Scarlett King",
  "Levi Collins",
  "Chloe Bailey",
  "Julian Rivera",
  "Zoe Howard",
  "Nathan Hughes",
  "Lily Bennett",
  "Samuel Watson",
  "Aria Jenkins",
  "Leo Simmons",
  "Hannah Palmer",
];

const tagPool = [
  "citywalk",
  "architecture",
  "localtips",
  "hidden-gems",
  "museum",
  "street-food",
  "coffee",
  "sunset",
  "weekend-trip",
  "photography",
  "night-view",
  "riverfront",
  "history",
  "budget-friendly",
  "public-transport",
  "family-friendly",
  "date-idea",
  "rainy-day",
  "route-planning",
  "must-see",
];

const categoryTagMap = {
  food: ["street-food", "coffee", "localtips"],
  museum: ["museum", "history", "rainy-day"],
  park: ["citywalk", "family-friendly", "sunset"],
  attraction: ["must-see", "photography", "citywalk"],
  heritage: ["history", "architecture", "must-see"],
  culture: ["architecture", "citywalk", "photography"],
  market: ["street-food", "localtips", "budget-friendly"],
  palace: ["history", "architecture", "must-see"],
  transport: ["public-transport", "route-planning", "citywalk"],
  shopping: ["budget-friendly", "citywalk", "date-idea"],
};

const categoryPhotoTokens = {
  food: ["london", "restaurant", "food", "cafe"],
  museum: ["london", "museum", "gallery", "architecture"],
  park: ["london", "park", "nature", "garden"],
  attraction: ["london", "landmark", "attraction", "travel"],
  heritage: ["london", "historic", "heritage", "architecture"],
  culture: ["london", "art", "culture", "theatre"],
  market: ["london", "market", "street", "shopping"],
  palace: ["london", "palace", "royal", "architecture"],
  transport: ["london", "station", "transport", "city"],
  shopping: ["london", "shopping", "mall", "street"],
};

const titleTemplates = [
  "A Practical Walk Through {name}",
  "How I Planned A Smooth Stop At {name}",
  "Route Notes: Visiting {name} Without Rushing",
  "What To Prioritize At {name} In Under Two Hours",
  "Is {name} Worth A Detour? My Honest Review",
  "Photo-Friendly Route Around {name}",
  "A Calm Weekday Visit To {name}",
  "Efficient Route Plan Centered On {name}",
  "First Visit To {name}: What Worked Best",
  "Best Time Windows For {name}",
  "How I Combined {name} With Nearby Stops",
  "Short-Trip Guide To {name}",
  "Real Notes After Visiting {name}",
];

const introTemplates = [
  "I planned this stop around {name} and arrived at about {time}.",
  "This was my second visit to {name}, and the route felt much more efficient this time.",
  "I added {name} as a via point and the total detour stayed reasonable.",
  "I visited {name} on a weekday and the flow was much smoother than I expected.",
  "I tested this route as a half-day plan with {name} as the anchor stop.",
  "For this trip, {name} was the key point and everything else was arranged around it.",
  "I wanted a low-stress route, so I used {name} as the central waypoint.",
];

const bodyTemplates = [
  "The best photo angles were on the quieter side streets, where foot traffic was lighter and framing was easier.",
  "If your schedule is tight, one focused loop works better than trying to cover every corner.",
  "Public transport access was straightforward, and the final walking segment was short.",
  "Nearby coffee options were solid, which made this a convenient stop between longer legs.",
  "Checking opening hours in advance helped avoid unnecessary waiting.",
  "Light changed quickly near sunset, so arriving 30-40 minutes early made a big difference.",
  "This place works well for solo routes and small groups because navigation is simple.",
  "I avoided backtracking by ordering the stops north-to-south, which saved time.",
  "The surrounding blocks had enough indoor options to keep the route flexible in bad weather.",
  "Queue times were manageable before peak hours, especially near the main entrance.",
  "I would keep this as a medium-length stop rather than an all-day segment.",
  "The area felt safe and walkable, even when I shifted the plan slightly on the fly.",
];

const outroTemplates = [
  "I would add this point again in a future route.",
  "Worth it if you enjoy architecture, city textures, and walkable segments.",
  "Hope this helps if you're deciding whether to include this stop.",
  "Happy to share a faster or budget-focused variation if needed.",
  "If you are choosing between nearby options, this one is a reliable pick.",
  "I would keep this stop, but I would still cap the time window to stay efficient.",
];

const commentTemplates = [
  "Great write-up. This is exactly the kind of route detail I needed.",
  "How long did you stay at this stop in total?",
  "I tried a similar route last month and your timing advice is very accurate.",
  "Thanks for sharing the crowd pattern, that part is super helpful.",
  "Do you think this route still works on rainy days?",
  "The photo-angle tip is useful. Saved this post.",
  "I like how practical this is, especially the route flow.",
  "Would you prioritize this stop over the nearby alternatives?",
  "This helped me remove one unnecessary detour. Thanks.",
  "Can confirm this area is much better close to sunset.",
  "Very clear notes. The pacing recommendations are easy to follow.",
  "I appreciate the realistic time estimate. Most posts skip that.",
  "This is one of the few posts that balances photos and logistics well.",
];

const replyTemplates = [
  "I stayed around 60-90 minutes depending on queue time.",
  "Yes, it still works in light rain if you keep an indoor backup nearby.",
  "I would keep this stop and pair it with a nearby coffee break.",
  "Agreed, timing is the key factor for this location.",
  "Thanks! I used route preview first, then adjusted on site.",
  "For me the result was better than expected, especially for photos.",
  "If you arrive early, the flow is much easier to manage.",
  "I would place this before the evening peak and leave before rush hour.",
];

const ensureTables = async (conn) => {
  await conn.query(`ALTER TABLE posts MODIFY cover_image VARCHAR(600) NULL`);
  await conn.query(`ALTER TABLE post_images MODIFY image_url VARCHAR(600) NOT NULL`);
  await conn.query(`
    CREATE TABLE IF NOT EXISTS poi_photos (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      poi_id BIGINT NOT NULL,
      image_url VARCHAR(600) NOT NULL,
      source VARCHAR(40) NOT NULL DEFAULT 'AUTO',
      sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_poi_photo (poi_id, image_url(255)),
      INDEX idx_poi_sort (poi_id, sort_order, id)
    );
  `);
};

const ensureUsers = async (conn) => {
  const [existing] = await conn.query(`SELECT id, username, nickname FROM users ORDER BY id ASC`);
  if (existing.length >= USER_TARGET) return existing.map((u) => u.id);

  const existingUsernames = new Set(existing.map((u) => u.username));
  let created = 0;
  for (let i = 0; i < PROFILE_NAMES.length && existing.length + created < USER_TARGET; i += 1) {
    const nickname = PROFILE_NAMES[i];
    const username = `traveler_seed_${String(i + 1).padStart(3, "0")}`;
    if (existingUsernames.has(username)) continue;
    const avatar = `https://api.dicebear.com/9.x/personas/svg?seed=${encodeURIComponent(nickname)}`;
    // password hash is not used by this data seeding flow
    await conn.query(
      `INSERT INTO users (username, password_hash, nickname, avatar_url) VALUES (?, ?, ?, ?)`,
      [username, "seed_password_hash_v1", nickname, avatar]
    );
    created += 1;
  }

  const [finalUsers] = await conn.query(`SELECT id FROM users ORDER BY id ASC`);
  return finalUsers.map((u) => u.id);
};

const fetchCandidatePois = async (conn) => {
  const [rows] = await conn.query(
    `
      SELECT
        p.id,
        p.name,
        p.category,
        p.city,
        p.address,
        p.lat,
        p.lng,
        p.image_url,
        p.popularity,
        COALESCE(pp.photo_count, 0) AS photo_count
      FROM poi p
      LEFT JOIN (
        SELECT poi_id, COUNT(*) AS photo_count
        FROM poi_photos
        GROUP BY poi_id
      ) pp ON pp.poi_id = p.id
      WHERE p.source IN ('OSM', 'NAPTAN', 'USER')
      ORDER BY p.popularity DESC, p.id ASC
      LIMIT ?
    `,
    [POI_CANDIDATES]
  );
  return rows;
};

const fetchPoiPhotoMap = async (conn, poiIds) => {
  if (!poiIds.length) return new Map();
  const [rows] = await conn.query(
    `
      SELECT poi_id, image_url
      FROM (
        SELECT
          poi_id,
          image_url,
          ROW_NUMBER() OVER (PARTITION BY poi_id ORDER BY sort_order ASC, id ASC) AS rn
        FROM poi_photos
        WHERE poi_id IN (?)
      ) t
      WHERE rn <= 6
      ORDER BY poi_id ASC, rn ASC
    `,
    [poiIds]
  );
  const map = new Map();
  rows.forEach((row) => {
    const key = Number(row.poi_id);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row.image_url);
  });
  return map;
};

const buildPostText = (poi) => {
  const openTime = randPick(["09:30", "10:00", "11:00", "16:45", "17:10"]);
  const intro = randPick(introTemplates).replaceAll("{name}", poi.name).replaceAll("{time}", openTime);
  const body = randSample(bodyTemplates, randInt(2, 3)).join(" ");
  const outro = randPick(outroTemplates);
  return `${intro} ${body} ${outro}`;
};

const buildPostTitle = (poi) => randPick(titleTemplates).replaceAll("{name}", poi.name);

const buildTags = (poi) => {
  const fromCategory = categoryTagMap[normalize(poi.category).toLowerCase()] || ["citywalk", "localtips"];
  const randomSet = randSample(tagPool, randInt(2, 3));
  return unique([...fromCategory, ...randomSet]).slice(0, randInt(3, 5));
};

const buildCommentText = () => randPick(commentTemplates);
const buildReplyText = () => randPick(replyTemplates);

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const normalizedPopularity = (poi) => {
  const value = Number(poi?.popularity);
  if (!Number.isFinite(value)) return 0;
  return value;
};

const getCategoryKey = (poi) => {
  const key = normalize(poi?.category).toLowerCase();
  return key || "attraction";
};

const buildSyntheticImageUrls = (poi) => {
  const categoryKey = getCategoryKey(poi);
  const tags = categoryPhotoTokens[categoryKey] || ["london", "travel", "city"];
  const pathTags = tags.map((item) => item.replace(/[^a-z0-9-]/gi, "")).filter(Boolean).join(",");
  const seedBase = Number(poi?.id) || stableHash(`${poi?.name || ""}|${categoryKey}|${poi?.city || ""}`);
  return [
    `https://loremflickr.com/1280/864/${pathTags}?lock=${seedBase * 13 + 1}`,
    `https://loremflickr.com/1280/864/${pathTags}?lock=${seedBase * 13 + 2}`,
    `https://picsum.photos/seed/journeypro-${seedBase}-a/1280/864`,
    `https://picsum.photos/seed/journeypro-${seedBase}-b/1280/864`,
  ];
};

const buildFixedPlans = (pois) => {
  const counts = new Map();
  for (let i = 0; i < POST_TARGET; i += 1) {
    const poi = pois[i % pois.length];
    const pid = Number(poi.id);
    counts.set(pid, (counts.get(pid) || 0) + 1);
  }
  return pois
    .map((poi) => ({
      poi,
      bucket: "fixed",
      count: counts.get(Number(poi.id)) || 0,
    }))
    .filter((item) => item.count > 0);
};

const buildWeightedPlans = (pois) => {
  const sorted = [...pois].sort((a, b) => normalizedPopularity(b) - normalizedPopularity(a));
  const total = sorted.length;
  const hotEnd = Math.max(1, Math.floor(total * HOT_RATIO));
  const warmEnd = Math.min(total, hotEnd + Math.floor(total * WARM_RATIO));
  const plans = [];

  sorted.forEach((poi, idx) => {
    let bucket = "cold";
    let minCount = COLD_MIN_POSTS;
    let maxCount = COLD_MAX_POSTS;
    if (idx < hotEnd) {
      bucket = "hot";
      minCount = HOT_MIN_POSTS;
      maxCount = HOT_MAX_POSTS;
    } else if (idx < warmEnd) {
      bucket = "warm";
      minCount = WARM_MIN_POSTS;
      maxCount = WARM_MAX_POSTS;
    }
    plans.push({ poi, bucket, count: randInt(minCount, maxCount) });
  });
  return plans;
};

const buildPostPlans = (pois) => {
  if (DISTRIBUTION_MODE === "fixed") return buildFixedPlans(pois);
  return buildWeightedPlans(pois);
};

const buildPoiImageMap = (pois, poiPhotoMap) => {
  const claimed = new Map();
  const map = new Map();

  const tryClaim = (poiId, rawUrl) => {
    const url = normalize(rawUrl);
    if (!isHttp(url)) return "";
    const owner = claimed.get(url);
    if (owner && owner !== poiId) return "";
    claimed.set(url, poiId);
    return url;
  };

  for (const poi of pois) {
    const poiId = Number(poi.id);
    const images = [];
    const generated = buildSyntheticImageUrls(poi);
    const cover = generated[0];
    images.push(cover);

    const candidates = unique([...(poiPhotoMap.get(poiId) || []), poi.image_url]);
    for (const candidate of candidates) {
      const claimedUrl = tryClaim(poiId, candidate);
      if (!claimedUrl || images.includes(claimedUrl)) continue;
      images.push(claimedUrl);
      if (images.length >= 4) break;
    }

    for (const generatedUrl of generated.slice(1)) {
      if (images.includes(generatedUrl)) continue;
      images.push(generatedUrl);
      if (images.length >= 4) break;
    }

    map.set(poiId, images.slice(0, 4));
  }
  return map;
};

const main = async () => {
  const pool = mysql.createPool({ ...DB, connectionLimit: 10, waitForConnections: true });
  const conn = await pool.getConnection();
  let fkDisabled = false;

  try {
    await ensureTables(conn);
    const userIds = await ensureUsers(conn);
    const pois = await fetchCandidatePois(conn);
    if (!pois.length) throw new Error("No POIs available for seeding posts.");

    const poiPhotoMap = await fetchPoiPhotoMap(
      conn,
      [...new Set(pois.map((p) => Number(p.id)).filter((id) => Number.isFinite(id) && id > 0))]
    );

    await conn.query(`SET FOREIGN_KEY_CHECKS = 0`);
    fkDisabled = true;
    await conn.beginTransaction();

    await conn.query(`DELETE FROM comment_likes`);
    await conn.query(`DELETE FROM post_comments`);
    await conn.query(`DELETE FROM post_views`);
    await conn.query(`DELETE FROM post_favorites`);
    await conn.query(`DELETE FROM post_likes`);
    await conn.query(`DELETE FROM post_tags`);
    await conn.query(`DELETE FROM post_images`);
    await conn.query(`DELETE FROM posts`);
    await conn.query(`DELETE FROM tags`);

    const allTags = unique([
      ...tagPool,
      ...Object.values(categoryTagMap).flat(),
      "london",
      "route",
      "weekend",
      "walkable",
    ]);
    const tagIdByName = new Map();
    for (const tag of allTags) {
      const [res] = await conn.query(`INSERT INTO tags (name, type) VALUES (?, 'CATEGORY')`, [tag]);
      tagIdByName.set(tag, res.insertId);
    }

    let postInserted = 0;
    let imageInserted = 0;
    let commentInserted = 0;
    const postPlans = buildPostPlans(pois);
    const poiImageMap = buildPoiImageMap(pois, poiPhotoMap);
    const planStats = postPlans.reduce(
      (acc, item) => {
        acc.total += item.count;
        acc[item.bucket] = (acc[item.bucket] || 0) + item.count;
        return acc;
      },
      { total: 0, hot: 0, warm: 0, cold: 0, fixed: 0 }
    );

    for (const plan of postPlans) {
      const poi = plan.poi;
      const baseImages = unique(poiImageMap.get(Number(poi.id)) || buildSyntheticImageUrls(poi)).slice(0, 4);
      if (!baseImages.length) continue;

      for (let i = 0; i < plan.count; i += 1) {
        const userId = randPick(userIds);
        const createdAt = toDate(randInt(0, 90 * 24 * 3600 * 1000));
        const postImages = baseImages;
        const title = buildPostTitle(poi).slice(0, 100);
        const content = buildPostText(poi);
        const tags = buildTags(poi);
        const bucketBoost = plan.bucket === "hot" ? 1.5 : plan.bucket === "warm" ? 1.15 : 0.9;
        const likeCount = Math.max(1, Math.round(randInt(4, 180) * bucketBoost));
        const favoriteCount = Math.max(1, Math.round(randInt(2, 90) * bucketBoost));
        const viewCount = Math.max(20, Math.round(randInt(60, 2600) * bucketBoost));

        const [postRes] = await conn.query(
          `
            INSERT INTO posts (
              user_id, poi_id, title, content, rating, cover_image, image_count,
              like_count, favorite_count, view_count, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NORMAL', ?, ?)
          `,
          [
            userId,
            poi.id,
            title,
            content,
            randInt(4, 5),
            postImages[0],
            postImages.length,
            likeCount,
            favoriteCount,
            viewCount,
            createdAt,
            createdAt,
          ]
        );
        const postId = postRes.insertId;
        postInserted += 1;

        const postImageRows = postImages.map((url, idx) => [postId, url, idx]);
        await conn.query(`INSERT INTO post_images (post_id, image_url, sort_order) VALUES ?`, [postImageRows]);
        imageInserted += postImageRows.length;

        const postTagRows = tags
          .map((tag) => tagIdByName.get(tag))
          .filter(Boolean)
          .map((tagId) => [postId, tagId]);
        if (postTagRows.length) {
          await conn.query(`INSERT IGNORE INTO post_tags (post_id, tag_id) VALUES ?`, [postTagRows]);
        }

        const rootCount = randInt(COMMENTS_MIN, COMMENTS_MAX);
        const rootCommentIds = [];
        for (let c = 0; c < rootCount; c += 1) {
          const commentUserId = randPick(userIds);
          const commentTime = new Date(createdAt.getTime() + randInt(30, 72 * 3600) * 1000);
          const [commentRes] = await conn.query(
            `
              INSERT INTO post_comments (
                post_id, user_id, parent_comment_id, type, content,
                like_count, reply_count, status, created_at, updated_at
              ) VALUES (?, ?, NULL, 'COMMENT', ?, ?, 0, 'NORMAL', ?, ?)
            `,
            [postId, commentUserId, buildCommentText(), randInt(0, 28), commentTime, commentTime]
          );
          const commentId = commentRes.insertId;
          rootCommentIds.push(commentId);
          commentInserted += 1;
        }

        for (const parentId of rootCommentIds) {
          const shouldReply = Math.random() < REPLY_PROB;
          if (!shouldReply) continue;
          const replyCount = Math.random() < DOUBLE_REPLY_PROB ? 2 : 1;
          for (let r = 0; r < replyCount; r += 1) {
            const replyUserId = randPick(userIds);
            const replyTime = new Date(createdAt.getTime() + randInt(2 * 3600, 96 * 3600) * 1000);
            const [replyRes] = await conn.query(
              `
                INSERT INTO post_comments (
                  post_id, user_id, parent_comment_id, type, content,
                  like_count, reply_count, status, created_at, updated_at
                ) VALUES (?, ?, ?, 'REPLY', ?, ?, 0, 'NORMAL', ?, ?)
              `,
              [postId, replyUserId, parentId, buildReplyText(), randInt(0, 12), replyTime, replyTime]
            );
            if (replyRes.insertId) {
              commentInserted += 1;
              await conn.query(`UPDATE post_comments SET reply_count = reply_count + 1 WHERE id = ?`, [parentId]);
            }
          }
        }
      }
    }

    await conn.commit();
    await conn.query(`SET FOREIGN_KEY_CHECKS = 1`);
    fkDisabled = false;

    console.log(
      JSON.stringify(
        {
          success: true,
          posts_seeded: postInserted,
          post_images_seeded: imageInserted,
          comments_seeded: commentInserted,
          users_available: userIds.length,
          pois_used: postPlans.length,
          distribution_mode: DISTRIBUTION_MODE,
          distribution_posts: {
            total: planStats.total,
            hot: planStats.hot,
            warm: planStats.warm,
            cold: planStats.cold,
            fixed: planStats.fixed,
          },
          tag_count: allTags.length,
        },
        null,
        2
      )
    );
  } catch (err) {
    try {
      await conn.rollback();
    } catch {
      // ignore
    }
    if (fkDisabled) {
      try {
        await conn.query(`SET FOREIGN_KEY_CHECKS = 1`);
      } catch {
        // ignore
      }
    }
    throw err;
  } finally {
    conn.release();
    await pool.end();
  }
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
