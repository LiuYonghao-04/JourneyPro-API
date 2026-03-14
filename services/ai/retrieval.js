import { pool } from "../../db/connect.js";

const clamp = (value, min, max) => Math.min(Math.max(Number(value) || 0, min), max);

const toLower = (value) => String(value || "").toLowerCase();

const normalize = (value) =>
  toLower(value)
    .replace(/[\r\n\t]+/g, " ")
    .replace(/[^a-z0-9\s-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const dedupeStrings = (list) => [...new Set((Array.isArray(list) ? list : []).filter(Boolean))];

const tokenizePrompt = (prompt) => {
  const words = normalize(prompt)
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length >= 3);
  return dedupeStrings(words).slice(0, 12);
};

const quoteLike = (token) => `%${String(token || "").replace(/[%_]/g, "\\$&")}%`;

const log1p = (value) => Math.log(1 + Math.max(0, Number(value) || 0));

const daysAgo = (value) => {
  const ts = value ? new Date(value).getTime() : NaN;
  if (!Number.isFinite(ts)) return 9999;
  return Math.max(0, (Date.now() - ts) / 86400000);
};

const recencyScore = (value) => {
  const days = daysAgo(value);
  if (!Number.isFinite(days)) return 0;
  return 1 / (1 + days / 21);
};

const clipText = (value, max = 220) => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 1)).trimEnd()}...`;
};

const textHitScore = (text, tokens) => {
  const haystack = normalize(text);
  if (!haystack || !Array.isArray(tokens) || !tokens.length) return 0;
  let hits = 0;
  tokens.forEach((token) => {
    if (haystack.includes(token)) hits += 1;
  });
  return hits;
};

const buildInClause = (values) => values.map(() => "?").join(", ");

const selectTopByGroup = (items, groupKey, perGroupLimit, totalLimit) => {
  const grouped = new Map();
  const output = [];
  for (const item of items) {
    const key = groupKey(item);
    const used = grouped.get(key) || 0;
    if (used >= perGroupLimit) continue;
    grouped.set(key, used + 1);
    output.push(item);
    if (output.length >= totalLimit) break;
  }
  return output;
};

const summarizeTagSpread = (posts) => {
  const counts = new Map();
  (Array.isArray(posts) ? posts : []).forEach((post) => {
    (post.tags || []).forEach((tag) => {
      const key = toLower(tag);
      if (!key) return;
      counts.set(key, (counts.get(key) || 0) + 1);
    });
  });
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 5)
    .map(([tag]) => tag);
};

const buildInsightLines = ({ posts, comments, routePois }) => {
  const lines = [];
  const topTags = summarizeTagSpread(posts);
  if (topTags.length) {
    lines.push(`Community tags repeated most often: ${topTags.join(", ")}.`);
  }
  const frequentPoiNames = dedupeStrings((routePois || []).slice(0, 4).map((poi) => poi?.name)).slice(0, 3);
  if (frequentPoiNames.length) {
    lines.push(`Top route anchors in this plan: ${frequentPoiNames.join(", ")}.`);
  }
  const commentThemes = dedupeStrings(
    (comments || [])
      .map((item) => clipText(item?.snippet || item?.content || "", 80))
      .filter(Boolean)
  ).slice(0, 2);
  commentThemes.forEach((theme) => {
    lines.push(`Community note: ${theme}`);
  });
  return lines.slice(0, 5);
};

const mapPostCard = (post, score, rank) => ({
  source_id: `post:${post.id}`,
  rank,
  type: "post",
  post_id: post.id,
  poi_id: post.poi_id,
  title: post.title || "Community post",
  snippet: clipText(post.content, 220),
  image_url: post.cover_image || post.poi_image_url || "",
  author: post.nickname || `User ${post.user_id}`,
  created_at: post.created_at || null,
  poi_name: post.poi_name || "",
  poi_category: post.poi_category || "",
  metrics: {
    likes: Number(post.like_count || 0),
    favorites: Number(post.favorite_count || 0),
    views: Number(post.view_count || 0),
  },
  tags: Array.isArray(post.tags) ? post.tags.slice(0, 4) : [],
  score: Number(score.toFixed(6)),
});

const mapCommentCard = (comment, score, rank) => ({
  source_id: `comment:${comment.id}`,
  rank,
  type: "comment",
  comment_id: comment.id,
  post_id: comment.post_id,
  poi_id: comment.poi_id,
  title: comment.post_title || "Community comment",
  snippet: clipText(comment.content, 200),
  image_url: comment.cover_image || "",
  author: comment.nickname || `User ${comment.user_id}`,
  created_at: comment.created_at || null,
  poi_name: comment.poi_name || "",
  metrics: {
    likes: Number(comment.like_count || 0),
    replies: Number(comment.reply_count || 0),
  },
  score: Number(score.toFixed(6)),
});

const mapPoiCard = (poi, rank) => ({
  source_id: `poi:${poi.id}`,
  rank,
  type: "poi",
  poi_id: poi.id,
  title: poi.name || "POI",
  snippet: clipText(
    poi.description ||
      `${poi.name || "This POI"} is a ${poi.category || "stop"} with review count ${Number(poi.review_count || 0)}.`,
    220
  ),
  image_url: poi.image_url || "",
  poi_name: poi.name || "",
  poi_category: poi.category || "",
  metrics: {
    popularity: Number(poi.popularity || 0),
    reviews: Number(poi.review_count || 0),
    stay_minutes: Number(poi.stay_minutes || 0),
  },
});

const buildPromptPoiWhere = (tokens) => {
  if (!Array.isArray(tokens) || !tokens.length) return { sql: "", params: [] };
  const clauses = [];
  const params = [];
  tokens.slice(0, 4).forEach((token) => {
    clauses.push("(LOWER(name) LIKE ? OR LOWER(category) LIKE ? OR LOWER(COALESCE(tags, '')) LIKE ? OR LOWER(COALESCE(description, '')) LIKE ?)");
    const like = quoteLike(token);
    params.push(like, like, like, like);
  });
  return {
    sql: clauses.length ? ` AND (${clauses.join(" OR ")})` : "",
    params,
  };
};

const fetchPromptPois = async ({ promptTokens, limit }) => {
  if (!promptTokens.length) return [];
  const where = buildPromptPoiWhere(promptTokens);
  const [rows] = await pool.query(
    `
      SELECT
        id,
        name,
        category,
        city,
        address,
        image_url,
        description,
        popularity,
        review_count,
        stay_minutes
      FROM poi
      WHERE 1 = 1
      ${where.sql}
      ORDER BY popularity DESC, review_count DESC, id DESC
      LIMIT ?
    `,
    [...where.params, clamp(limit, 2, 12)]
  );
  return rows || [];
};

const fetchPostsForPois = async ({ poiIds, rowLimit }) => {
  if (!Array.isArray(poiIds) || !poiIds.length) return [];
  const placeholders = buildInClause(poiIds);
  const [rows] = await pool.query(
    `
      SELECT
        p.id,
        p.user_id,
        p.poi_id,
        p.title,
        p.content,
        p.cover_image,
        p.like_count,
        p.favorite_count,
        p.view_count,
        p.created_at,
        u.nickname,
        u.avatar_url,
        poi.name AS poi_name,
        poi.category AS poi_category,
        poi.image_url AS poi_image_url
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      LEFT JOIN poi ON poi.id = p.poi_id
      WHERE COALESCE(p.status, 'NORMAL') = 'NORMAL'
        AND p.poi_id IN (${placeholders})
      ORDER BY p.created_at DESC
      LIMIT ?
    `,
    [...poiIds, clamp(rowLimit, 12, 180)]
  );
  return rows || [];
};

const attachPostTags = async (posts) => {
  const ids = dedupeStrings((posts || []).map((item) => item?.id).filter(Boolean));
  if (!ids.length) return posts;
  const placeholders = buildInClause(ids);
  const [rows] = await pool.query(
    `
      SELECT pt.post_id, t.name
      FROM post_tags pt
      INNER JOIN tags t ON t.id = pt.tag_id
      WHERE pt.post_id IN (${placeholders})
      ORDER BY pt.post_id DESC, t.name ASC
    `,
    ids
  );
  const tagMap = new Map();
  (rows || []).forEach((row) => {
    const list = tagMap.get(row.post_id) || [];
    list.push(row.name);
    tagMap.set(row.post_id, list);
  });
  return (posts || []).map((post) => ({
    ...post,
    tags: dedupeStrings(tagMap.get(post.id) || []).slice(0, 6),
  }));
};

const fetchCommentsForPosts = async ({ postIds, rowLimit }) => {
  if (!Array.isArray(postIds) || !postIds.length) return [];
  const placeholders = buildInClause(postIds);
  const hotSql = `
    SELECT
      c.id,
      c.post_id,
      c.user_id,
      c.content,
      c.like_count,
      c.reply_count,
      c.created_at,
      u.nickname,
      p.poi_id,
      p.title AS post_title,
      p.cover_image,
      poi.name AS poi_name
    FROM post_comments c
    INNER JOIN posts p ON p.id = c.post_id
    LEFT JOIN users u ON u.id = c.user_id
    LEFT JOIN poi ON poi.id = p.poi_id
    WHERE COALESCE(c.status, 'NORMAL') = 'NORMAL'
      AND c.parent_comment_id IS NULL
      AND c.post_id IN (${placeholders})
    ORDER BY c.created_at DESC
    LIMIT ?
  `;
  const archiveSql = `
    SELECT
      c.id,
      c.post_id,
      c.user_id,
      c.content,
      c.like_count,
      c.reply_count,
      c.created_at,
      u.nickname,
      p.poi_id,
      p.title AS post_title,
      p.cover_image,
      poi.name AS poi_name
    FROM post_comments_archive c
    INNER JOIN posts p ON p.id = c.post_id
    LEFT JOIN users u ON u.id = c.user_id
    LEFT JOIN poi ON poi.id = p.poi_id
    WHERE COALESCE(c.status, 'NORMAL') = 'NORMAL'
      AND c.parent_comment_id IS NULL
      AND c.post_id IN (${placeholders})
    ORDER BY c.created_at DESC
    LIMIT ?
  `;
  const capped = clamp(rowLimit, 8, 220);
  const [hotRows, archiveRows] = await Promise.all([
    pool.query(hotSql, [...postIds, capped]).then(([rows]) => rows || []),
    pool.query(archiveSql, [...postIds, Math.max(8, Math.floor(capped / 2))]).then(([rows]) => rows || []),
  ]);
  return [...hotRows, ...archiveRows];
};

const scorePost = ({ post, promptTokens, poiRankMap }) => {
  const routeRank = Number(poiRankMap.get(post.poi_id) || 99);
  const routeBoost = routeRank <= 12 ? Math.max(0, (14 - routeRank) * 0.18) : 0;
  const textBoost =
    textHitScore(post.title, promptTokens) * 1.8 +
    textHitScore(post.content, promptTokens) * 0.8 +
    textHitScore((post.tags || []).join(" "), promptTokens) * 1.4 +
    textHitScore(post.poi_name, promptTokens) * 1.3 +
    textHitScore(post.poi_category, promptTokens) * 0.7;
  const engagement =
    log1p(post.like_count) * 1.3 +
    log1p(post.favorite_count) * 1.4 +
    log1p(Number(post.view_count || 0) / 12);
  const freshness = recencyScore(post.created_at) * 1.1;
  return routeBoost + textBoost + engagement + freshness;
};

const scoreComment = ({ comment, promptTokens, postScoreMap, poiRankMap }) => {
  const routeRank = Number(poiRankMap.get(comment.poi_id) || 99);
  const routeBoost = routeRank <= 12 ? Math.max(0, (13 - routeRank) * 0.14) : 0;
  const textBoost =
    textHitScore(comment.content, promptTokens) * 1.1 +
    textHitScore(comment.post_title, promptTokens) * 0.8 +
    textHitScore(comment.poi_name, promptTokens) * 0.6;
  const engagement = log1p(comment.like_count) * 1.2 + log1p(comment.reply_count) * 1.25;
  const parentBoost = Number(postScoreMap.get(comment.post_id) || 0) * 0.22;
  const freshness = recencyScore(comment.created_at) * 0.9;
  return routeBoost + textBoost + engagement + parentBoost + freshness;
};

const buildPromptContext = ({ prompt, routePois, posts, comments, insights }) => {
  const lines = [];
  lines.push(`User request: ${String(prompt || "").trim() || "N/A"}`);
  if (Array.isArray(routePois) && routePois.length) {
    lines.push("Route candidates:");
    routePois.slice(0, 8).forEach((poi, index) => {
      lines.push(
        `- [R${index + 1}] ${poi.name} | ${poi.category || "poi"} | route ${Math.round(Number(poi.distance_m || 0))}m | detour ${Math.round(
          Number(poi.detour_duration_s || 0) / 60
        )}m | reason: ${poi.reason || "matched by route"}`
      );
    });
  }
  if (Array.isArray(posts) && posts.length) {
    lines.push("Community posts:");
    posts.slice(0, 8).forEach((post, index) => {
      lines.push(
        `- [P${index + 1}] ${post.title} | ${post.poi_name || "poi"} | tags: ${(post.tags || []).join(", ") || "none"} | ${clipText(
          post.content,
          180
        )}`
      );
    });
  }
  if (Array.isArray(comments) && comments.length) {
    lines.push("Comment evidence:");
    comments.slice(0, 6).forEach((comment, index) => {
      lines.push(`- [C${index + 1}] ${comment.poi_name || comment.post_title || "comment"} | ${clipText(comment.content, 160)}`);
    });
  }
  if (Array.isArray(insights) && insights.length) {
    lines.push("Synthesized insights:");
    insights.forEach((line) => lines.push(`- ${line}`));
  }
  return lines.join("\n");
};

export const buildPlannerKnowledgePack = async ({ prompt, rankedItems, promptPoiLimit = 6 }) => {
  const routePois = Array.isArray(rankedItems) ? rankedItems.slice(0, 10) : [];
  const routePoiIds = dedupeStrings(routePois.map((item) => item?.id).filter((id) => Number.isFinite(Number(id))));
  const promptTokens = tokenizePrompt(prompt);
  const promptPois = await fetchPromptPois({ promptTokens, limit: promptPoiLimit });
  const promptPoiIds = dedupeStrings(promptPois.map((item) => item?.id).filter((id) => Number.isFinite(Number(id))));
  const candidatePoiIds = dedupeStrings([...routePoiIds, ...promptPoiIds]).slice(0, 16);

  const poiRankMap = new Map();
  routePois.forEach((poi, index) => {
    if (poi?.id === null || poi?.id === undefined) return;
    poiRankMap.set(Number(poi.id), index + 1);
  });
  promptPois.forEach((poi) => {
    if (poi?.id === null || poi?.id === undefined) return;
    if (!poiRankMap.has(Number(poi.id))) poiRankMap.set(Number(poi.id), 24);
  });

  let posts = await fetchPostsForPois({ poiIds: candidatePoiIds, rowLimit: 120 });
  posts = await attachPostTags(posts);
  const scoredPosts = posts
    .map((post) => {
      const score = scorePost({ post, promptTokens, poiRankMap });
      return {
        ...post,
        _score: score,
      };
    })
    .sort((a, b) => b._score - a._score || Number(b.like_count || 0) - Number(a.like_count || 0));
  const selectedPosts = selectTopByGroup(scoredPosts, (item) => item.poi_id || item.id, 3, 12);
  const postScoreMap = new Map(selectedPosts.map((post) => [post.id, post._score]));

  const commentsRaw = await fetchCommentsForPosts({
    postIds: selectedPosts.map((item) => item.id),
    rowLimit: 160,
  });
  const scoredComments = commentsRaw
    .map((comment) => ({
      ...comment,
      _score: scoreComment({ comment, promptTokens, postScoreMap, poiRankMap }),
    }))
    .sort((a, b) => b._score - a._score || Number(b.like_count || 0) - Number(a.like_count || 0));
  const selectedComments = selectTopByGroup(scoredComments, (item) => item.post_id || item.id, 2, 10);

  const poiCards = dedupeStrings(candidatePoiIds)
    .map((id) => promptPois.find((poi) => Number(poi.id) === Number(id)) || routePois.find((poi) => Number(poi.id) === Number(id)))
    .filter(Boolean)
    .slice(0, 6)
    .map((poi, index) => mapPoiCard(poi, index + 1));

  const postCards = selectedPosts.map((post, index) => mapPostCard(post, post._score, index + 1));
  const commentCards = selectedComments.map((comment, index) => mapCommentCard(comment, comment._score, index + 1));
  const insights = buildInsightLines({
    posts: selectedPosts,
    comments: selectedComments,
    routePois,
  });

  const cards = [...poiCards, ...postCards, ...commentCards]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, 16)
    .map((item, index) => ({
      ...item,
      rank: index + 1,
    }));

  return {
    stats: {
      prompt_tokens: promptTokens,
      candidate_poi_count: candidatePoiIds.length,
      route_poi_count: routePoiIds.length,
      prompt_poi_count: promptPoiIds.length,
      selected_post_count: selectedPosts.length,
      selected_comment_count: selectedComments.length,
      card_count: cards.length,
    },
    prompt_context: buildPromptContext({
      prompt,
      routePois,
      posts: selectedPosts,
      comments: selectedComments,
      insights,
    }),
    insights,
    cards,
  };
};
