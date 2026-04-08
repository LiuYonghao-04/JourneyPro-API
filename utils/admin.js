import { appendUserAccess, fetchUserAccessById } from "./userAccess.js";

export function appendAdminFlag(user) {
  return appendUserAccess(user);
}

export async function fetchAdminUserById(poolOrConn, userId) {
  return fetchUserAccessById(userId, poolOrConn);
}

export async function ensureAdminAccess(poolOrConn, userId) {
  const user = await fetchUserAccessById(userId, poolOrConn);
  if (!user?.is_admin) return null;
  return user;
}
