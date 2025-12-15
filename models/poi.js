import { pool } from "../db/connect.js";

const METERS_PER_DEGREE_LAT = 111320;

const buildBboxWkt = (lat, lng, radiusM) => {
  const r = Math.max(Number(radiusM) || 0, 0);
  const latRad = (Number(lat) * Math.PI) / 180;
  const dLat = r / METERS_PER_DEGREE_LAT;
  const metersPerDegreeLng = METERS_PER_DEGREE_LAT * Math.cos(latRad);
  const dLng = r / (metersPerDegreeLng || 1e-9);

  const minLat = Number(lat) - dLat;
  const maxLat = Number(lat) + dLat;
  const minLng = Number(lng) - dLng;
  const maxLng = Number(lng) + dLng;

  // MySQL uses axis order for SRID=4326 as (lat, lng) for WKT parsing in ST_GeomFromText.
  // Keep bbox WKT consistent with stored `geom` axis order so MBRContains can use the spatial index.
  return `POLYGON((${minLat} ${minLng},${maxLat} ${minLng},${maxLat} ${maxLng},${minLat} ${maxLng},${minLat} ${minLng}))`;
};

export async function getNearbyPOIs(lat, lng, radius = 3000, limit = 50, category = null) {
  if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lng))) return [];

  const finalRadius = Math.min(Math.max(parseInt(radius || "0", 10) || 0, 1), 50000);
  const finalLimit = Math.min(Math.max(parseInt(limit || "0", 10) || 0, 1), 200);
  const wkt = buildBboxWkt(lat, lng, finalRadius);

  const centerLng = Number(lng);
  const centerLat = Number(lat);

  let sql = `
    SELECT
      id, name, category, lat, lng, popularity, price, tags, image_url, source, source_id, address, city, country,
      ST_Distance_Sphere(geom, ST_SRID(POINT(?, ?), 4326)) AS distance
    FROM poi
    WHERE
      MBRContains(ST_GeomFromText(?, 4326), geom)
      AND ST_Distance_Sphere(geom, ST_SRID(POINT(?, ?), 4326)) <= ?
  `;

  const params = [centerLng, centerLat, wkt, centerLng, centerLat, finalRadius];

  if (category && String(category).trim()) {
    sql += ` AND category = ? `;
    params.push(String(category).trim());
  }

  sql += `
    ORDER BY distance ASC
    LIMIT ?
  `;
  params.push(finalLimit);

  const [rows] = await pool.query(sql, params);
  return rows;
}
