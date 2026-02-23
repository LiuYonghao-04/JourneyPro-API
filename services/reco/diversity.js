import { DIVERSITY_LAMBDA, round } from "./constants.js";
import { haversineMeters } from "./math.js";

const normalizeCategory = (value) => String(value || "unknown").trim().toLowerCase() || "unknown";

const tagOverlap = (a, b) => {
  const tagsA = new Set((a.tags_list || []).map((tag) => String(tag).toLowerCase()));
  const tagsB = new Set((b.tags_list || []).map((tag) => String(tag).toLowerCase()));
  if (!tagsA.size || !tagsB.size) return 0;
  let hits = 0;
  tagsA.forEach((tag) => {
    if (tagsB.has(tag)) hits += 1;
  });
  const denom = Math.max(tagsA.size, tagsB.size);
  return denom ? hits / denom : 0;
};

const similarity = (a, b) => {
  const categorySim = normalizeCategory(a.category) === normalizeCategory(b.category) ? 1 : 0;
  const geoDistance = haversineMeters(a.lat, a.lng, b.lat, b.lng);
  const geoSim = Math.exp(-Math.max(geoDistance, 0) / 420);
  const tagsSim = tagOverlap(a, b);
  return categorySim * 0.52 + geoSim * 0.3 + tagsSim * 0.18;
};

const shouldApplyCategoryCap = (candidate, categoryCounts, selectedLength) => {
  if (selectedLength >= 10) return false;
  const category = normalizeCategory(candidate.category);
  const used = categoryCounts.get(category) || 0;
  return used >= 3;
};

const needsSegmentSpread = (selected, pool, target) => {
  if (selected.length >= target) return [];
  const available = new Set((pool || []).map((item) => Number(item.route_segment) || 0));
  const covered = new Set((selected || []).map((item) => Number(item.route_segment) || 0));
  return [...available].filter((segment) => !covered.has(segment));
};

export const applyDiversityRerank = ({
  candidates,
  limit = 10,
  lambda = DIVERSITY_LAMBDA,
  topPool = 60,
}) => {
  const safeCandidates = Array.isArray(candidates) ? candidates : [];
  if (!safeCandidates.length) {
    return {
      selected: [],
      diagnostics: {
        lambda,
        top_pool: topPool,
        category_counts: {},
        segment_counts: {},
      },
    };
  }

  const pool = [...safeCandidates]
    .sort((a, b) => (Number(b.final_pre_diversity) || 0) - (Number(a.final_pre_diversity) || 0))
    .slice(0, topPool);

  const selected = [];
  const categoryCounts = new Map();
  const segmentCounts = new Map();

  while (selected.length < limit && pool.length > 0) {
    let bestIndex = 0;
    let bestScore = Number.NEGATIVE_INFINITY;
    let bestPenalty = 0;

    for (let i = 0; i < pool.length; i += 1) {
      const candidate = pool[i];
      const rawScore = Number(candidate.final_pre_diversity) || 0;
      const maxSimilarity =
        selected.length > 0
          ? Math.max(...selected.map((item) => similarity(candidate, item)))
          : 0;

      let mmrScore = lambda * rawScore - (1 - lambda) * maxSimilarity;

      if (shouldApplyCategoryCap(candidate, categoryCounts, selected.length)) {
        mmrScore -= 5;
      }

      const missingSegments = needsSegmentSpread(selected, pool, limit);
      const segment = Number(candidate.route_segment) || 0;
      if (missingSegments.length > 0 && !missingSegments.includes(segment)) {
        const currentSegmentCount = segmentCounts.get(segment) || 0;
        if (currentSegmentCount >= 1) {
          mmrScore -= 0.06;
        }
      }

      if (mmrScore > bestScore) {
        bestScore = mmrScore;
        bestIndex = i;
        bestPenalty = (1 - lambda) * maxSimilarity;
      }
    }

    const [picked] = pool.splice(bestIndex, 1);
    if (!picked) break;

    const category = normalizeCategory(picked.category);
    const segment = Number(picked.route_segment) || 0;
    categoryCounts.set(category, (categoryCounts.get(category) || 0) + 1);
    segmentCounts.set(segment, (segmentCounts.get(segment) || 0) + 1);

    selected.push({
      ...picked,
      diversity_penalty: round(bestPenalty, 6),
      final_score: round((Number(picked.final_pre_diversity) || 0) - bestPenalty, 6),
    });
  }

  return {
    selected,
    diagnostics: {
      lambda,
      top_pool: topPool,
      category_counts: Object.fromEntries(categoryCounts.entries()),
      segment_counts: Object.fromEntries(segmentCounts.entries()),
    },
  };
};
