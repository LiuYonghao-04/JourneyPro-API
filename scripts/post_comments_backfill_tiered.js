import mysql from "mysql2/promise";

const DB = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "123456",
  database: process.env.DB_NAME || "journeypro",
};

const HOT_RATIO = Math.max(0.05, Math.min(0.5, Number(process.env.COMMENT_HOT_RATIO || 0.2)));
const COLD_RATIO = Math.max(0.1, Math.min(0.6, Number(process.env.COMMENT_COLD_RATIO || 0.3)));
const HOT_MIN = Math.max(1, Number(process.env.COMMENT_HOT_MIN || 30));
const HOT_MAX = Math.max(HOT_MIN, Number(process.env.COMMENT_HOT_MAX || 50));
const MID_MIN = Math.max(1, Number(process.env.COMMENT_MID_MIN || 15));
const MID_MAX = Math.max(MID_MIN, Number(process.env.COMMENT_MID_MAX || 30));
const COLD_MIN = Math.max(1, Number(process.env.COMMENT_COLD_MIN || 5));
const COLD_MAX = Math.max(COLD_MIN, Number(process.env.COMMENT_COLD_MAX || 15));
const BATCH_SIZE = Math.max(200, Math.min(2000, Number(process.env.COMMENT_BATCH_SIZE || 1000)));
const DRY_RUN = String(process.env.COMMENT_DRY_RUN || "0") === "1";

const normalize = (value) => String(value || "").trim();
const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const randPick = (arr) => arr[randInt(0, arr.length - 1)];

const stableHash = (input) => {
  const text = normalize(input);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const engagementScore = (row) => {
  const view = Number(row.view_count) || 0;
  const like = Number(row.like_count) || 0;
  const fav = Number(row.favorite_count) || 0;
  return view * 0.5 + like * 2.2 + fav * 2.8;
};

const categoryLexicon = {
  food: ["menu", "portion", "queue", "table", "service", "dish"],
  museum: ["gallery", "ticket", "exhibit", "collection", "hall", "audio guide"],
  park: ["path", "green space", "viewpoint", "shade", "walking loop", "bench"],
  attraction: ["landmark", "timing", "spot", "line", "view", "detour"],
  heritage: ["historic area", "architecture", "story", "building", "route", "timing"],
  culture: ["venue", "show", "space", "program", "crowd", "entry"],
  market: ["stall", "pricing", "crowd", "route", "opening hours", "best lane"],
  palace: ["palace area", "entry gate", "timed slot", "architecture", "queue", "courtyard"],
  transport: ["station", "platform", "connection", "transfer", "exit", "timing"],
  shopping: ["store mix", "peak hours", "queue", "floor layout", "traffic", "pricing"],
};

const qualityCommentTemplates = [
  "Useful post. The timing notes are realistic and helped me avoid peak traffic.",
  "I tested a similar plan last week. Your pacing around this stop is accurate.",
  "Thanks for sharing this. The route flow looks efficient and easy to repeat.",
  "This is practical advice. The way you split time across stops makes sense.",
  "Great context. I usually overstay here, but your schedule feels balanced.",
  "Very clear details. This is more helpful than generic travel tips.",
  "Saved this. The recommendation is specific enough to apply directly.",
  "I appreciate that you included operational details instead of only photos.",
  "This is exactly the kind of post that reduces trial-and-error on route planning.",
  "Good call on the visit window. That matches what I experienced as well.",
];

const categoryCommentTemplates = {
  food: [
    "For this area, did you notice any difference between lunch and dinner queue times?",
    "Your food stop sequencing is solid. I would keep this before the evening rush.",
    "Agree with this pick. The dish quality is stable and service speed is decent.",
    "Helpful note on portions and wait time. That detail saves a lot of guesswork.",
    "I tried this route and the table turnover was faster than expected before 1 PM.",
  ],
  museum: [
    "Excellent museum planning notes. Did you book the ticket slot in advance?",
    "The exhibit order you suggested is efficient and avoids backtracking.",
    "I like this approach. It keeps the key galleries without overloading the schedule.",
    "Your queue estimate is accurate. Early morning really does make a difference.",
    "Thanks for the museum timing breakdown. That part is usually hard to predict.",
  ],
  park: [
    "This park sequence looks good. The walking loop and rest points are well balanced.",
    "Great call on timing. The paths are much better outside the peak family window.",
    "I followed this path suggestion and it kept the route smooth and scenic.",
    "Useful details on entry points. That avoids unnecessary detours around the perimeter.",
    "Your estimate for a relaxed stop length is very realistic.",
  ],
  transport: [
    "Nice transfer guidance. Which station exit did you find the fastest for this route?",
    "This station timing tip is useful, especially for avoiding platform congestion.",
    "I can confirm this connection works well if you keep the same sequence.",
    "Good transport note. It helps reduce uncertainty between segments.",
    "The transfer buffer you suggested is practical and not over-conservative.",
  ],
};

const buildComment = (post) => {
  const category = normalize(post.category).toLowerCase();
  const specific = categoryCommentTemplates[category] || [];
  const lex = categoryLexicon[category] || ["stop", "timing", "flow"];
  const lead = Math.random() < 0.55 ? randPick(qualityCommentTemplates) : randPick(specific.length ? specific : qualityCommentTemplates);
  const tail =
    Math.random() < 0.5
      ? `The ${randPick(lex)} detail is especially useful.`
      : `I would keep this as a ${randPick(["reliable", "high-value", "repeatable"])} stop.`;
  return `${lead} ${tail}`;
};

const pickTier = (index, total, hotCount, coldCount) => {
  if (index < hotCount) return "hot";
  if (index >= total - coldCount) return "cold";
  return "mid";
};

const targetCountByTier = (postId, tier) => {
  const seed = stableHash(`comment-target:${postId}:${tier}`);
  const pick = (min, max) => min + (seed % (max - min + 1));
  if (tier === "hot") return pick(HOT_MIN, HOT_MAX);
  if (tier === "cold") return pick(COLD_MIN, COLD_MAX);
  return pick(MID_MIN, MID_MAX);
};

const main = async () => {
  const pool = mysql.createPool({ ...DB, connectionLimit: 12, waitForConnections: true });
  const conn = await pool.getConnection();

  try {
    const [users] = await conn.query(`SELECT id FROM users ORDER BY id ASC`);
    const userIds = users.map((u) => Number(u.id)).filter((id) => Number.isFinite(id) && id > 0);
    if (!userIds.length) throw new Error("No users found.");

    const [rows] = await conn.query(`
      SELECT
        p.id,
        p.user_id,
        p.poi_id,
        p.title,
        p.created_at,
        p.view_count,
        p.like_count,
        p.favorite_count,
        LOWER(COALESCE(po.category, 'attraction')) AS category,
        COALESCE(pc.comment_cnt, 0) AS comment_cnt
      FROM posts p
      LEFT JOIN poi po ON po.id = p.poi_id
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS comment_cnt
        FROM post_comments
        GROUP BY post_id
      ) pc ON pc.post_id = p.id
      ORDER BY p.id ASC
    `);
    if (!rows.length) throw new Error("No posts found.");

    const posts = rows.map((row) => ({
      ...row,
      score: engagementScore(row),
    }));
    posts.sort((a, b) => b.score - a.score || Number(a.id) - Number(b.id));

    const totalPosts = posts.length;
    const hotCount = clamp(Math.floor(totalPosts * HOT_RATIO), 1, totalPosts);
    const coldCount = clamp(Math.floor(totalPosts * COLD_RATIO), 1, totalPosts - hotCount);

    let plannedAdds = 0;
    let hotPosts = 0;
    let midPosts = 0;
    let coldPosts = 0;

    const enriched = posts.map((post, idx) => {
      const tier = pickTier(idx, totalPosts, hotCount, coldCount);
      if (tier === "hot") hotPosts += 1;
      else if (tier === "cold") coldPosts += 1;
      else midPosts += 1;

      const target = targetCountByTier(post.id, tier);
      const current = Number(post.comment_cnt) || 0;
      const toAdd = Math.max(0, target - current);
      plannedAdds += toAdd;
      return { ...post, tier, target, current, toAdd };
    });

    const summary = {
      total_posts: totalPosts,
      tiers: { hot: hotPosts, mid: midPosts, cold: coldPosts },
      comment_target_ranges: {
        hot: [HOT_MIN, HOT_MAX],
        mid: [MID_MIN, MID_MAX],
        cold: [COLD_MIN, COLD_MAX],
      },
      planned_comment_additions: plannedAdds,
      dry_run: DRY_RUN,
    };

    if (DRY_RUN || plannedAdds <= 0) {
      console.log(JSON.stringify({ success: true, ...summary }, null, 2));
      return;
    }

    let inserted = 0;
    const insertBuffer = [];

    const flush = async () => {
      if (!insertBuffer.length) return;
      await conn.query(
        `
          INSERT INTO post_comments (
            post_id, user_id, parent_comment_id, type, content,
            like_count, reply_count, status, created_at, updated_at
          ) VALUES ?
        `,
        [insertBuffer]
      );
      inserted += insertBuffer.length;
      insertBuffer.length = 0;
    };

    for (let i = 0; i < enriched.length; i += 1) {
      const post = enriched[i];
      if (post.toAdd <= 0) {
        if ((i + 1) % 5000 === 0) {
          console.log(`progress posts=${i + 1}/${totalPosts} inserted=${inserted}`);
        }
        continue;
      }

      const postCreatedAt = new Date(post.created_at).getTime();
      for (let n = 0; n < post.toAdd; n += 1) {
        const commentUserId = randPick(userIds);
        const offsetSec = randInt(1800, 120 * 24 * 3600);
        const commentTime = new Date(postCreatedAt + offsetSec * 1000);
        insertBuffer.push([
          post.id,
          commentUserId,
          null,
          "COMMENT",
          buildComment(post),
          randInt(0, 36),
          0,
          "NORMAL",
          commentTime,
          commentTime,
        ]);
        if (insertBuffer.length >= BATCH_SIZE) {
          // eslint-disable-next-line no-await-in-loop
          await flush();
        }
      }
      if ((i + 1) % 5000 === 0) {
        console.log(`progress posts=${i + 1}/${totalPosts} inserted=${inserted}`);
      }
    }
    await flush();

    const [[after]] = await conn.query(`
      SELECT AVG(c.cnt) AS avg_comments, MIN(c.cnt) AS min_comments, MAX(c.cnt) AS max_comments
      FROM posts p
      LEFT JOIN (
        SELECT post_id, COUNT(*) AS cnt
        FROM post_comments
        GROUP BY post_id
      ) c ON c.post_id = p.id
    `);

    console.log(
      JSON.stringify(
        {
          success: true,
          ...summary,
          inserted_root_comments: inserted,
          after_distribution: {
            avg_comments: Number(after?.avg_comments) || 0,
            min_comments: Number(after?.min_comments) || 0,
            max_comments: Number(after?.max_comments) || 0,
          },
        },
        null,
        2
      )
    );
  } finally {
    conn.release();
    await pool.end();
  }
};

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
