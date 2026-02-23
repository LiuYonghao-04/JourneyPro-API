import { clamp } from "./constants.js";

export const toRad = (deg) => (Number(deg) * Math.PI) / 180;

export const haversineMeters = (lat1, lng1, lat2, lng2) => {
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

export const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

export const normalizeToUnit = (value, min, max) => {
  const num = safeNumber(value, min);
  if (max <= min) return 0;
  return clamp((num - min) / (max - min), 0, 1);
};

export const sigmoid = (x) => {
  const n = safeNumber(x, 0);
  if (n > 20) return 1;
  if (n < -20) return 0;
  return 1 / (1 + Math.exp(-n));
};

export const cosineSimilarity = (a, b) => {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length || a.length === 0) return 0;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i += 1) {
    const av = safeNumber(a[i], 0);
    const bv = safeNumber(b[i], 0);
    dot += av * bv;
    na += av * av;
    nb += bv * bv;
  }
  if (na <= 0 || nb <= 0) return 0;
  return dot / Math.sqrt(na * nb);
};

export const vecDot = (a, b) => {
  let sum = 0;
  for (let i = 0; i < a.length; i += 1) {
    sum += safeNumber(a[i], 0) * safeNumber(b[i], 0);
  }
  return sum;
};

export const matVecMul = (matrix, vector) =>
  matrix.map((row) => row.reduce((sum, value, idx) => sum + value * safeNumber(vector[idx], 0), 0));

export const outer = (vector) => {
  const n = vector.length;
  const result = Array.from({ length: n }, () => Array.from({ length: n }, () => 0));
  for (let i = 0; i < n; i += 1) {
    for (let j = 0; j < n; j += 1) {
      result[i][j] = safeNumber(vector[i], 0) * safeNumber(vector[j], 0);
    }
  }
  return result;
};

export const identity = (n) =>
  Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)));

export const matAddScaled = (target, addend, scale = 1) => {
  for (let i = 0; i < target.length; i += 1) {
    for (let j = 0; j < target[i].length; j += 1) {
      target[i][j] += safeNumber(addend[i]?.[j], 0) * scale;
    }
  }
  return target;
};

export const vecAddScaled = (target, addend, scale = 1) => {
  for (let i = 0; i < target.length; i += 1) {
    target[i] += safeNumber(addend[i], 0) * scale;
  }
  return target;
};

// Small-matrix Gauss-Jordan inverse for LinUCB (d=10).
export const invertMatrix = (matrix) => {
  const n = matrix.length;
  const aug = matrix.map((row, i) => [
    ...row.map((v) => safeNumber(v, 0)),
    ...Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)),
  ]);

  for (let col = 0; col < n; col += 1) {
    let pivot = col;
    for (let row = col + 1; row < n; row += 1) {
      if (Math.abs(aug[row][col]) > Math.abs(aug[pivot][col])) {
        pivot = row;
      }
    }

    if (Math.abs(aug[pivot][col]) < 1e-9) {
      aug[pivot][col] = 1e-9;
    }

    if (pivot !== col) {
      const tmp = aug[col];
      aug[col] = aug[pivot];
      aug[pivot] = tmp;
    }

    const pivotVal = aug[col][col] || 1e-9;
    for (let j = 0; j < 2 * n; j += 1) {
      aug[col][j] /= pivotVal;
    }

    for (let row = 0; row < n; row += 1) {
      if (row === col) continue;
      const factor = aug[row][col];
      if (!factor) continue;
      for (let j = 0; j < 2 * n; j += 1) {
        aug[row][j] -= factor * aug[col][j];
      }
    }
  }

  return aug.map((row) => row.slice(n));
};

export const minMaxNormalize = (values, fallback = 0.5) => {
  if (!Array.isArray(values) || values.length === 0) return [];
  const nums = values.map((v) => safeNumber(v, 0));
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  if (Math.abs(max - min) < 1e-9) return nums.map(() => fallback);
  return nums.map((v) => (v - min) / (max - min));
};

export const chunk = (items, size) => {
  const output = [];
  for (let i = 0; i < items.length; i += size) {
    output.push(items.slice(i, i + size));
  }
  return output;
};

export const mapWithConcurrency = async (items, concurrency, mapper) => {
  if (!Array.isArray(items) || items.length === 0) return [];
  const limit = Math.max(1, Number(concurrency) || 1);
  const queue = items.map((item, index) => ({ item, index }));
  const results = [];

  const worker = async () => {
    while (queue.length) {
      const next = queue.shift();
      if (!next) return;
      // eslint-disable-next-line no-await-in-loop
      const value = await mapper(next.item, next.index);
      results.push(value);
    }
  };

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return results;
};
