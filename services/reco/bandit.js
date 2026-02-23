import { pool } from "../../db/connect.js";
import {
  BANDIT_ALPHA,
  EVENT_REWARD_WEIGHTS,
  clamp,
  round,
} from "./constants.js";
import {
  identity,
  invertMatrix,
  matAddScaled,
  matVecMul,
  minMaxNormalize,
  outer,
  vecAddScaled,
  vecDot,
} from "./math.js";

const ARM_DIM = 10;
const ARM_MIN_IMPRESSIONS = 8;

const normalizeCategoryGroup = (category) => {
  const normalized = String(category || "unknown").trim().toLowerCase();
  return normalized || "unknown";
};

export const buildArmKey = (mode, category) => `${mode}:${normalizeCategoryGroup(category)}`;

export const buildContextVector = (candidate, rankNorm, hour) => {
  const hourPhase = ((Number(hour) % 24) / 24) * 2 * Math.PI;
  return [
    Number(candidate.distance_fit) || 0,
    Number(candidate.detour_fit) || 0,
    Number(candidate.interest_fit) || 0,
    Number(candidate.quality_fit) || 0,
    Number(candidate.novelty_fit) || 0,
    Number(candidate.context_fit) || 0,
    Number(candidate.coverage_fit) || 0,
    clamp(Number(rankNorm) || 0, 0, 1),
    Math.sin(hourPhase),
    Math.cos(hourPhase),
  ];
};

const fetchArmHistory = async (mode, categoryGroups) => {
  if (!categoryGroups.length) return new Map();

  const rewardSql = Object.entries(EVENT_REWARD_WEIGHTS)
    .map(([eventType, weight]) => `WHEN '${eventType}' THEN ${Number(weight)}`)
    .join(" ");

  const [rows] = await pool.query(
    `
      SELECT
        COALESCE(NULLIF(TRIM(LOWER(p.category)), ''), 'unknown') AS category_group,
        SUM(CASE re.event_type ${rewardSql} ELSE 0 END * COALESCE(re.event_value, 1)) AS reward_sum,
        SUM(CASE WHEN re.event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
        COUNT(*) AS total_events
      FROM recommendation_events re
      LEFT JOIN poi p ON p.id = re.poi_id
      WHERE re.mode = ?
        AND re.ts >= DATE_SUB(NOW(), INTERVAL 90 DAY)
        AND COALESCE(NULLIF(TRIM(LOWER(p.category)), ''), 'unknown') IN (?)
      GROUP BY category_group
    `,
    [mode, categoryGroups]
  );

  const map = new Map();
  rows.forEach((row) => {
    map.set(String(row.category_group || "unknown"), {
      rewardSum: Number(row.reward_sum) || 0,
      impressions: Number(row.impressions) || 0,
      totalEvents: Number(row.total_events) || 0,
    });
  });
  return map;
};

const buildArmState = ({ vectors, stats }) => {
  const A = identity(ARM_DIM);
  const b = Array.from({ length: ARM_DIM }, () => 0);

  if (!vectors.length) {
    const invA = invertMatrix(A);
    return {
      A,
      invA,
      b,
      theta: Array.from({ length: ARM_DIM }, () => 0),
      impressions: 0,
      rewardSum: 0,
      sufficient: false,
    };
  }

  const centroid = Array.from({ length: ARM_DIM }, () => 0);
  vectors.forEach((vector) => {
    for (let i = 0; i < ARM_DIM; i += 1) {
      centroid[i] += Number(vector[i]) || 0;
    }
  });
  for (let i = 0; i < ARM_DIM; i += 1) {
    centroid[i] /= vectors.length;
  }

  const impressions = Number(stats?.impressions) || 0;
  const rewardSum = Number(stats?.rewardSum) || 0;
  const pseudoCount = clamp(impressions, 0, 5000);

  matAddScaled(A, outer(centroid), pseudoCount);
  vecAddScaled(b, centroid, rewardSum);

  const invA = invertMatrix(A);
  const theta = matVecMul(invA, b);

  return {
    A,
    invA,
    b,
    theta,
    impressions,
    rewardSum,
    sufficient: impressions >= ARM_MIN_IMPRESSIONS,
  };
};

const predictUcb = (state, context, alpha) => {
  const mean = vecDot(state.theta, context);
  const varianceProxy = vecDot(context, matVecMul(state.invA, context));
  const bonus = Number.isFinite(varianceProxy) ? Math.sqrt(Math.max(varianceProxy, 0)) : 0;
  return mean + alpha * bonus;
};

export const applyBanditBonus = async ({
  candidates,
  mode,
  exploreWeight,
  alpha = BANDIT_ALPHA,
  now = new Date(),
}) => {
  const safeCandidates = Array.isArray(candidates) ? candidates : [];
  if (!safeCandidates.length) {
    return {
      candidates: [],
      diagnostics: { enabled: false, reason: "no_candidates", arms: {} },
    };
  }

  const hour = now.getHours();
  const ranked = safeCandidates.map((candidate, index) => {
    const rankNorm = safeCandidates.length > 1 ? index / (safeCandidates.length - 1) : 0;
    const armCategory = normalizeCategoryGroup(candidate.category);
    const armKey = buildArmKey(mode, armCategory);
    const context = buildContextVector(candidate, rankNorm, hour);
    return {
      ...candidate,
      arm_key: armKey,
      arm_category_group: armCategory,
      rank_norm: rankNorm,
      bandit_context: context,
    };
  });

  const categoryGroups = [...new Set(ranked.map((item) => item.arm_category_group))];
  const historyMap = await fetchArmHistory(mode, categoryGroups);

  const groupedVectors = new Map();
  ranked.forEach((item) => {
    const list = groupedVectors.get(item.arm_key) || [];
    list.push(item.bandit_context);
    groupedVectors.set(item.arm_key, list);
  });

  const armStates = new Map();
  for (const [armKey, vectors] of groupedVectors.entries()) {
    const categoryGroup = armKey.split(":")[1] || "unknown";
    const state = buildArmState({
      vectors,
      stats: historyMap.get(categoryGroup),
    });
    armStates.set(armKey, state);
  }

  const anySufficient = [...armStates.values()].some((state) => state.sufficient);
  if (!anySufficient) {
    return {
      candidates: ranked.map((candidate) => ({
        ...candidate,
        bandit_raw: 0,
        bandit_norm: 0,
        bandit_bonus: 0,
        final_pre_diversity: candidate.base_score,
      })),
      diagnostics: {
        enabled: false,
        reason: "insufficient_arm_history",
        arms: Object.fromEntries(
          [...armStates.entries()].map(([armKey, state]) => [armKey, {
            impressions: state.impressions,
            reward_sum: round(state.rewardSum, 4),
            sufficient: state.sufficient,
          }])
        ),
      },
    };
  }

  const rawScores = ranked.map((candidate) => {
    const state = armStates.get(candidate.arm_key);
    if (!state?.sufficient) return 0;
    return predictUcb(state, candidate.bandit_context, alpha);
  });
  const normalizedScores = minMaxNormalize(rawScores, 0.5);

  const blended = ranked.map((candidate, index) => {
    const banditNorm = normalizedScores[index];
    const base = Number(candidate.base_score) || 0;
    const finalPreDiversity = (1 - exploreWeight) * base + exploreWeight * banditNorm;
    return {
      ...candidate,
      bandit_raw: round(rawScores[index], 6),
      bandit_norm: round(banditNorm, 6),
      bandit_bonus: round(banditNorm - base, 6),
      final_pre_diversity: round(finalPreDiversity, 6),
    };
  });

  return {
    candidates: blended,
    diagnostics: {
      enabled: true,
      alpha,
      explore_weight: exploreWeight,
      arms: Object.fromEntries(
        [...armStates.entries()].map(([armKey, state]) => [armKey, {
          impressions: state.impressions,
          reward_sum: round(state.rewardSum, 4),
          sufficient: state.sufficient,
        }])
      ),
    },
  };
};
