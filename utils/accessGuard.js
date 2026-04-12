import { pool } from "../db/connect.js";
import { fetchUserAccessById } from "./userAccess.js";

export const parsePositiveInt = (value) => {
  const next = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(next) && next > 0 ? next : 0;
};

export const extractUserIdFromRequest = (req) =>
  parsePositiveInt(req?.query?.user_id ?? req?.body?.user_id ?? req?.get?.("x-user-id"));

const createAccessGuard = ({
  prop = "accessUser",
  requiredMessage = "user_id required",
  forbiddenMessage = "forbidden",
  allow = () => true,
} = {}) => {
  return async (req, res, next) => {
    try {
      const userId = extractUserIdFromRequest(req);
      if (!userId) {
        return res.status(401).json({ success: false, message: requiredMessage });
      }
      const user = await fetchUserAccessById(userId, pool);
      if (!user) {
        return res.status(404).json({ success: false, message: "user not found" });
      }
      if (!allow(user)) {
        return res.status(403).json({ success: false, message: forbiddenMessage });
      }
      req[prop] = user;
      return next();
    } catch (err) {
      console.error("access guard error", err);
      return res.status(500).json({ success: false, message: "server error" });
    }
  };
};

export const requireAdminUser = createAccessGuard({
  prop: "adminUser",
  requiredMessage: "admin user_id required",
  forbiddenMessage: "admin access required",
  allow: (user) => !!user?.is_admin,
});

export const requireAdManagerUser = createAccessGuard({
  prop: "adUser",
  requiredMessage: "user_id required",
  forbiddenMessage: "SVIP or admin access required",
  allow: (user) => !!user?.can_manage_ads,
});
