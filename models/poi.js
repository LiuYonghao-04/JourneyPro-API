import { pool } from "../db/connect.js";

// 查询距离指定点一定范围内的POI
export async function getNearbyPOIs(lat, lng, radius = 3000) {
    const sql = `
    SELECT *,
      (6371000 * ACOS(
        COS(RADIANS(?)) * COS(RADIANS(lat)) *
        COS(RADIANS(lng) - RADIANS(?)) + SIN(RADIANS(?)) * SIN(RADIANS(lat))
      )) AS distance
    FROM poi
    HAVING distance < ?
    ORDER BY distance ASC
    LIMIT 50;
  `;
    const [rows] = await pool.query(sql, [lat, lng, lat, radius]);
    return rows;
}
