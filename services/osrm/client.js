import axios from "axios";

const DEFAULT_LOCAL_OSRM = "http://localhost:5000";
const DEFAULT_PUBLIC_OSRM = "https://router.project-osrm.org";

const OSRM_LOCAL_TIMEOUT_MS = Math.max(
  200,
  Math.min(Number(process.env.OSRM_LOCAL_TIMEOUT_MS || 600), 10000)
);
const OSRM_REMOTE_TIMEOUT_MS = Math.max(
  300,
  Math.min(Number(process.env.OSRM_REMOTE_TIMEOUT_MS || 12000), 20000)
);
const OSRM_DOWN_COOLDOWN_MS = Math.max(
  1000,
  Math.min(Number(process.env.OSRM_DOWN_COOLDOWN_MS || 15000), 10 * 60 * 1000)
);

const normalizeBaseUrl = (value) => String(value || "").trim().replace(/\/+$/, "");

const parseBackends = () => {
  const listRaw = String(process.env.OSRM_URLS || "").trim();
  const singleRaw = String(process.env.OSRM_URL || "").trim();
  const enablePublicFallback = process.env.OSRM_ENABLE_PUBLIC_FALLBACK !== "0";

  let list = [];
  if (listRaw) {
    list = listRaw
      .split(",")
      .map((item) => normalizeBaseUrl(item))
      .filter(Boolean);
  } else if (singleRaw) {
    list = [normalizeBaseUrl(singleRaw)];
  } else {
    list = [DEFAULT_LOCAL_OSRM, DEFAULT_PUBLIC_OSRM];
  }

  if (enablePublicFallback && !list.includes(DEFAULT_PUBLIC_OSRM)) {
    list.push(DEFAULT_PUBLIC_OSRM);
  }

  const seen = new Set();
  return list.filter((url) => {
    if (!/^https?:\/\//i.test(url)) return false;
    if (seen.has(url)) return false;
    seen.add(url);
    return true;
  });
};

const OSRM_BACKENDS = parseBackends();
const backendCircuitMap = new Map(); // backend -> next available timestamp

const getBackendHost = (backend) => {
  try {
    return new URL(backend).hostname.toLowerCase();
  } catch {
    return "";
  }
};

const isLocalBackend = (backend) => {
  const host = getBackendHost(backend);
  return (
    host === "localhost" ||
    host === "127.0.0.1" ||
    host === "::1" ||
    host === "0.0.0.0" ||
    host.startsWith("192.168.") ||
    host.startsWith("10.") ||
    host.startsWith("172.16.") ||
    host.startsWith("172.17.") ||
    host.startsWith("172.18.") ||
    host.startsWith("172.19.") ||
    host.startsWith("172.20.") ||
    host.startsWith("172.21.") ||
    host.startsWith("172.22.") ||
    host.startsWith("172.23.") ||
    host.startsWith("172.24.") ||
    host.startsWith("172.25.") ||
    host.startsWith("172.26.") ||
    host.startsWith("172.27.") ||
    host.startsWith("172.28.") ||
    host.startsWith("172.29.") ||
    host.startsWith("172.30.") ||
    host.startsWith("172.31.")
  );
};

const getBackendTimeout = (backend) => (isLocalBackend(backend) ? OSRM_LOCAL_TIMEOUT_MS : OSRM_REMOTE_TIMEOUT_MS);

const isBackendOpen = (backend) => {
  const nextAt = Number(backendCircuitMap.get(backend) || 0);
  return Date.now() >= nextAt;
};

const markBackendFailure = (backend) => {
  backendCircuitMap.set(backend, Date.now() + OSRM_DOWN_COOLDOWN_MS);
};

const markBackendSuccess = (backend) => {
  backendCircuitMap.set(backend, 0);
};

const buildRouteUrl = ({
  backend,
  profile,
  coordinates,
  overview = "full",
  geometries = "geojson",
  steps = false,
  alternatives = false,
  annotations = false,
}) => {
  const query = new URLSearchParams({
    overview: String(overview),
    geometries: String(geometries),
    steps: steps ? "true" : "false",
    alternatives: alternatives ? "true" : "false",
    annotations: annotations ? "true" : "false",
  }).toString();
  return `${backend}/route/v1/${profile}/${coordinates}?${query}`;
};

export const getOsrmBackends = () => [...OSRM_BACKENDS];

export const fetchOsrmRoute = async ({
  profile = "driving",
  coordinates,
  overview = "full",
  geometries = "geojson",
  steps = false,
  alternatives = false,
  annotations = false,
}) => {
  const safeCoordinates = String(coordinates || "").trim();
  const safeProfile = String(profile || "driving").trim();
  if (!safeCoordinates) {
    return { ok: false, route: null, backend: null, errors: ["empty_coordinates"] };
  }
  if (!OSRM_BACKENDS.length) {
    return { ok: false, route: null, backend: null, errors: ["no_osrm_backends_configured"] };
  }

  const errors = [];
  const attempted = [];

  for (const backend of OSRM_BACKENDS) {
    if (!isBackendOpen(backend)) {
      errors.push(`${backend}:circuit_open`);
      continue;
    }

    const timeout = getBackendTimeout(backend);
    const url = buildRouteUrl({
      backend,
      profile: safeProfile,
      coordinates: safeCoordinates,
      overview,
      geometries,
      steps,
      alternatives,
      annotations,
    });

    attempted.push({ backend, timeout });
    try {
      const response = await axios.get(url, { timeout, validateStatus: () => true });
      const statusCode = Number(response.status || 0);
      const route = response?.data?.routes?.[0] || null;
      if (statusCode >= 200 && statusCode < 300 && route) {
        markBackendSuccess(backend);
        return {
          ok: true,
          route,
          backend,
          attempted,
          errors,
        };
      }
      markBackendFailure(backend);
      errors.push(`${backend}:status_${statusCode}`);
    } catch (err) {
      markBackendFailure(backend);
      errors.push(`${backend}:${err?.code || err?.message || "request_failed"}`);
    }
  }

  return {
    ok: false,
    route: null,
    backend: null,
    attempted,
    errors,
  };
};
