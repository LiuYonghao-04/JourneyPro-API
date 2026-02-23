import crypto from "crypto";
import {
  BASE_MODEL_FIXED_WEIGHTS,
  DEFAULT_CANDIDATE_LIMIT,
  DEFAULT_LIMIT,
  DEFAULT_EXPLORE_WEIGHT,
  DEFAULT_INTEREST_WEIGHT,
  RECOMMENDATION_VERSION,
  clamp,
  getModeConfig,
  normalizeWeight,
  round,
} from "./constants.js";
import { applyBanditBonus } from "./bandit.js";
import { applyDiversityRerank } from "./diversity.js";
import { enrichCandidatesWithFeatures } from "./features.js";
import { fetchUserPreferenceProfile } from "./profiles.js";
import { buildRouteByMode, recallCandidates } from "./recall.js";

const computeWeightedScore = ({
  distanceFit,
  interestFit,
  qualityFit,
  noveltyFit,
  contextFit,
  distanceWeight,
  interestWeight,
}) => {
  const wq = BASE_MODEL_FIXED_WEIGHTS.quality;
  const wn = BASE_MODEL_FIXED_WEIGHTS.novelty;
  const wc = BASE_MODEL_FIXED_WEIGHTS.context;
  const total = distanceWeight + interestWeight + wq + wn + wc;
  const raw =
    distanceFit * distanceWeight +
    interestFit * interestWeight +
    qualityFit * wq +
    noveltyFit * wn +
    contextFit * wc;
  return total > 0 ? clamp(raw / total, 0, 1) : 0;
};

const buildExplanations = ({
  distanceFit,
  interestFit,
  qualityFit,
  noveltyFit,
  contextFit,
  distanceWeight,
  interestWeight,
  exploreWeight,
  banditNorm,
}) => {
  const factors = [
    { tag: "distance", value: distanceFit * distanceWeight },
    { tag: "interest", value: interestFit * interestWeight },
    { tag: "quality", value: qualityFit * BASE_MODEL_FIXED_WEIGHTS.quality },
    { tag: "novelty", value: noveltyFit * BASE_MODEL_FIXED_WEIGHTS.novelty },
    { tag: "context", value: contextFit * BASE_MODEL_FIXED_WEIGHTS.context },
    { tag: "exploration", value: (Number(banditNorm) || 0) * exploreWeight },
  ];

  const positive = factors.filter((factor) => factor.value > 0);
  const total = positive.reduce((sum, item) => sum + item.value, 0);
  if (!total) return [];

  return positive
    .map((factor) => ({
      tag: factor.tag,
      contribution: round((factor.value / total) * 100, 2),
    }))
    .sort((a, b) => b.contribution - a.contribution)
    .slice(0, 4);
};

const finalizeRows = ({
  selected,
  limit,
  interestWeight,
  exploreWeight,
}) => {
  const distanceWeight = 1 - interestWeight;

  const ordered = [...selected]
    .map((row) => {
      const tunedBase = Number(row.tuned_base_score) || 0;
      const banditNorm = Number(row.bandit_norm) || 0;
      const final = (1 - exploreWeight) * tunedBase + exploreWeight * banditNorm - (Number(row.diversity_penalty) || 0);
      return {
        ...row,
        final_score: round(final, 6),
      };
    })
    .sort((a, b) => {
      const finalDiff = (Number(b.final_score) || 0) - (Number(a.final_score) || 0);
      if (finalDiff) return finalDiff;
      const tunedDiff = (Number(b.tuned_base_score) || 0) - (Number(a.tuned_base_score) || 0);
      if (tunedDiff) return tunedDiff;
      return (Number(a.distance) || 0) - (Number(b.distance) || 0);
    })
    .slice(0, limit)
    .map((row, index) => {
      const explanations = buildExplanations({
        distanceFit: row.distance_fit,
        interestFit: row.interest_fit,
        qualityFit: row.quality_fit,
        noveltyFit: row.novelty_fit,
        contextFit: row.context_fit,
        distanceWeight,
        interestWeight,
        exploreWeight,
        banditNorm: row.bandit_norm,
      });

      return {
        id: row.id,
        name: row.name,
        category: row.category,
        lat: row.lat,
        lng: row.lng,
        distance: Math.round(Number(row.bestDistance) || 0),
        distance_to_start: Math.round(Number(row.distance_to_start) || 0),
        distance_to_end: Math.round(Number(row.distance_to_end) || 0),
        popularity: row.popularity,
        image_url: row.image_url,
        tags: row.tags,
        source_list: row.sourceList || [],

        // Backward-compatible keys
        base_score: round(Number(row.base_score) || 0, 6),
        distance_score: round(Number(row.distance_fit) || 0, 6),
        interest_score: round(Number(row.interest_fit) || 0, 6),
        reason: row.reason,
        match_tags: row.match_tags || [],

        // New v2 fields
        rank_position: index + 1,
        scores: {
          final: round(Number(row.final_score) || 0, 6),
          base: round(Number(row.tuned_base_score) || 0, 6),
          bandit_bonus: round(Number(row.bandit_bonus) || 0, 6),
          diversity_penalty: round(Number(row.diversity_penalty) || 0, 6),
          distance: round(Number(row.distance_fit) || 0, 6),
          interest: round(Number(row.interest_fit) || 0, 6),
          quality: round(Number(row.quality_fit) || 0, 6),
          novelty: round(Number(row.novelty_fit) || 0, 6),
          context: round(Number(row.context_fit) || 0, 6),
        },
        explanations,
        detour: {
          extra_distance_m: Math.round(Number(row.detour_extra_distance_m) || 0),
          extra_duration_s: Math.round(Number(row.detour_extra_duration_s) || 0),
        },
      };
    });

  return ordered;
};

export const runRecommendationV2 = async ({
  startPoint,
  endPoint,
  viaPoints,
  userId,
  requestedMode,
  interestWeight,
  exploreWeight,
  limit,
  candidateLimit,
  category,
  radius,
  modeDefaults,
  requestId = null,
  bucket = "treatment",
  debug = false,
}) => {
  const timings = {};
  const t0 = Date.now();

  const stableRequestId = requestId || crypto.randomUUID();
  const safeLimit = clamp(Number(limit) || DEFAULT_LIMIT, 1, 50);
  const safeCandidateLimit = clamp(Number(candidateLimit) || DEFAULT_CANDIDATE_LIMIT, safeLimit, 360);
  const safeInterestWeight = normalizeWeight(interestWeight, DEFAULT_INTEREST_WEIGHT);
  const safeExploreWeight = normalizeWeight(exploreWeight, DEFAULT_EXPLORE_WEIGHT);
  const safeDistanceWeight = 1 - safeInterestWeight;

  const modeConfig = getModeConfig(requestedMode, modeDefaults);

  const waypoints = [startPoint, ...(viaPoints || []), endPoint];

  const tRouteStart = Date.now();
  const routeResult = await buildRouteByMode({ waypoints, modeConfig });
  timings.route_ms = Date.now() - tRouteStart;

  if (!routeResult.route) {
    return {
      error: "No route found",
      status: 404,
      payload: {
        request_id: stableRequestId,
        mode: modeConfig.mode,
        warning: routeResult.warning || null,
      },
    };
  }

  const effectiveModeConfig = getModeConfig(routeResult.resolvedMode, modeDefaults);

  const tProfileStart = Date.now();
  const userProfile = await fetchUserPreferenceProfile(userId);
  timings.profile_ms = Date.now() - tProfileStart;

  const tRecallStart = Date.now();
  const recall = await recallCandidates({
    route: routeResult.route,
    startPoint,
    endPoint,
    modeConfig: effectiveModeConfig,
    userProfile,
    category,
    candidateLimit: safeCandidateLimit,
    perSampleLimit: clamp(Math.ceil(safeCandidateLimit / 10), 12, 28),
    radiusOverride: radius,
  });
  timings.recall_ms = Date.now() - tRecallStart;

  const tFeaturesStart = Date.now();
  const featureResult = await enrichCandidatesWithFeatures({
    candidates: recall.candidates,
    samples: recall.samples,
    route: routeResult.route,
    modeConfig: effectiveModeConfig,
    userProfile,
    userId,
    radiusM: recall.radiusM,
  });
  timings.features_ms = Date.now() - tFeaturesStart;

  const scored = featureResult.candidates.map((candidate) => {
    const baseScore = computeWeightedScore({
      distanceFit: candidate.distance_fit,
      interestFit: candidate.interest_fit,
      qualityFit: candidate.quality_fit,
      noveltyFit: candidate.novelty_fit,
      contextFit: candidate.context_fit,
      distanceWeight: 0.5,
      interestWeight: 0.5,
    });

    const tunedBaseScore = computeWeightedScore({
      distanceFit: candidate.distance_fit,
      interestFit: candidate.interest_fit,
      qualityFit: candidate.quality_fit,
      noveltyFit: candidate.novelty_fit,
      contextFit: candidate.context_fit,
      distanceWeight: safeDistanceWeight,
      interestWeight: safeInterestWeight,
    });

    return {
      ...candidate,
      base_score: round(baseScore, 6),
      tuned_base_score: round(tunedBaseScore, 6),
      distance_weight: safeDistanceWeight,
      interest_weight: safeInterestWeight,
    };
  });

  const stableTop = [...scored]
    .sort((a, b) => {
      const diff = (Number(b.base_score) || 0) - (Number(a.base_score) || 0);
      if (diff) return diff;
      return (Number(a.bestDistance) || 0) - (Number(b.bestDistance) || 0);
    })
    .slice(0, Math.min(Math.max(safeLimit * 8, 60), safeCandidateLimit));

  const treatmentEnabled = bucket === "treatment";

  const tBanditStart = Date.now();
  const banditResult = treatmentEnabled
    ? await applyBanditBonus({
        candidates: stableTop,
        mode: effectiveModeConfig.mode,
        exploreWeight: safeExploreWeight,
      })
    : {
        candidates: stableTop.map((row) => ({
          ...row,
          bandit_raw: 0,
          bandit_norm: 0,
          bandit_bonus: 0,
          final_pre_diversity: row.base_score,
        })),
        diagnostics: {
          enabled: false,
          reason: "control_bucket",
          arms: {},
        },
      };
  timings.bandit_ms = Date.now() - tBanditStart;

  const tDiversityStart = Date.now();
  const diversityInput = [...banditResult.candidates].map((row) => ({
    ...row,
    final_pre_diversity: Number(row.final_pre_diversity) || Number(row.base_score) || 0,
  }));
  const diversity = applyDiversityRerank({
    candidates: diversityInput,
    limit: safeLimit,
    topPool: Math.min(60, diversityInput.length),
  });
  timings.diversity_ms = Date.now() - tDiversityStart;

  const tFinalizeStart = Date.now();
  const selectedRows = finalizeRows({
    selected: diversity.selected,
    limit: safeLimit,
    interestWeight: safeInterestWeight,
    exploreWeight: safeExploreWeight,
  });
  timings.finalize_ms = Date.now() - tFinalizeStart;

  timings.total_ms = Date.now() - t0;

  const diagnostics = {
    recall_counts: {
      ...recall.recallCounts,
      total_candidates: recall.candidates.length,
      after_feature_filters: featureResult.candidates.length,
      stable_pool_size: stableTop.length,
    },
    filter_drop_counts: featureResult.filterDropCounts,
    latency_ms: timings,
    bandit: banditResult.diagnostics,
    diversity: diversity.diagnostics,
  };

  return {
    status: 200,
    payload: {
      request_id: stableRequestId,
      algorithm_version: RECOMMENDATION_VERSION,
      bucket,
      mode: effectiveModeConfig.mode,
      mode_fallback: routeResult.fallbackUsed,
      warning: routeResult.warning || null,
      base_route: routeResult.route,
      recommended_pois: selectedRows,
      profile: {
        user_id: userId || null,
        tags: userProfile.topTags || [],
        categories: userProfile.topCategories || [],
        personalized: !!userProfile.hasProfile,
        source: userProfile.source || "unknown",
        tuning: {
          interest_weight: safeInterestWeight,
          distance_weight: safeDistanceWeight,
          explore_weight: safeExploreWeight,
        },
      },
      diagnostics: debug ? diagnostics : undefined,
      debug: {
        sample_m: recall.sampleStepM,
        samples: recall.samples.length,
        radius_m: recall.radiusM,
        candidates: recall.candidates.length,
      },
    },
  };
};
