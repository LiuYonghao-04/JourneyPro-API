const ADMIN_USERNAMES = new Set(["test"]);

const normalizeUsername = (value) => String(value || "").trim().toLowerCase();

export function isAdminUsername(username) {
  return ADMIN_USERNAMES.has(normalizeUsername(username));
}

export function appendAdminFlag(user) {
  if (!user || typeof user !== "object") return user;
  return {
    ...user,
    is_admin: isAdminUsername(user.username),
  };
}

export async function fetchAdminUserById(pool, userId) {
  const uid = Number.parseInt(String(userId || ""), 10);
  if (!Number.isFinite(uid) || uid <= 0) return null;
  const [rows] = await pool.query(
    `SELECT id, username, nickname, avatar_url FROM users WHERE id = ? LIMIT 1`,
    [uid]
  );
  if (!rows.length) return null;
  return appendAdminFlag(rows[0]);
}

export async function ensureAdminAccess(pool, userId) {
  const user = await fetchAdminUserById(pool, userId);
  if (!user?.is_admin) return null;
  return user;
}
