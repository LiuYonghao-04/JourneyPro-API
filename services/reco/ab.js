import { pool } from "../../db/connect.js";
import {
  RECOMMENDATION_EXPERIMENT_KEY,
  buildSubjectKey,
  clamp,
  hashToRatio,
} from "./constants.js";
import { ensureRecoTables } from "./schema.js";

const normalizeTreatmentRatio = (ratio) => {
  const numeric = Number(ratio);
  if (!Number.isFinite(numeric)) return 0.5;
  return clamp(numeric > 1 ? numeric / 100 : numeric, 0, 1);
};

export const assignRecommendationBucket = async ({
  userId = null,
  sessionId = null,
  ip = null,
  userAgent = null,
  experimentKey = RECOMMENDATION_EXPERIMENT_KEY,
  treatmentRatio = null,
}) => {
  await ensureRecoTables();

  const subjectKey = buildSubjectKey({ userId, sessionId, ip, userAgent });
  const ratio = normalizeTreatmentRatio(
    treatmentRatio !== null && treatmentRatio !== undefined
      ? treatmentRatio
      : process.env.RECO_V2_ROLLOUT_RATIO || 0.5
  );

  const [[existing]] = await pool.query(
    `
      SELECT bucket, assigned_at
      FROM ab_assignments
      WHERE subject_key = ? AND experiment_key = ?
      LIMIT 1
    `,
    [subjectKey, experimentKey]
  );

  if (existing?.bucket) {
    return {
      subjectKey,
      experimentKey,
      bucket: existing.bucket,
      assignedAt: existing.assigned_at || null,
      treatmentRatio: ratio,
      fromCache: true,
    };
  }

  const ratioValue = hashToRatio(`${experimentKey}|${subjectKey}`);
  const bucket = ratioValue < ratio ? "treatment" : "control";

  await pool.query(
    `
      INSERT INTO ab_assignments (subject_key, experiment_key, bucket)
      VALUES (?, ?, ?)
      ON DUPLICATE KEY UPDATE bucket = VALUES(bucket)
    `,
    [subjectKey, experimentKey, bucket]
  );

  return {
    subjectKey,
    experimentKey,
    bucket,
    assignedAt: null,
    treatmentRatio: ratio,
    fromCache: false,
  };
};
