const jwt = require("jsonwebtoken");
const config = require("../config/config");
const User = require("../model/userSchema");
const { getEffectivePageAccess } = require("../config/pageAccess");

const USER_TOKEN_AUDIENCE = "airlineplan-tenant";
const TENANT_ADMIN_ROLES = new Set(["tenant_admin", "admin"]);

const getTokenFromRequest = (req) => {
  const token =
    req.body?.token || req.query?.token || req.headers["x-access-token"];

  return token;
};

const attachUserFromToken = async (req, token) => {
  const decoded = jwt.verify(token, config.secret);
  if (decoded?.aud !== USER_TOKEN_AUDIENCE || decoded?.tokenType !== "tenant_user") {
    const error = new Error("Invalid Token");
    error.statusCode = 401;
    throw error;
  }

  const user = await User.findById(decoded.id).select("_id email role isActive firstName lastName pageAccess pageAccessConfigured").lean();
  if (!user || user.isActive === false) {
    const error = new Error("User is inactive or no longer exists");
    error.statusCode = 401;
    throw error;
  }

  req.user = {
    id: user._id.toString(),
    email: user.email,
    role: user.role,
    firstName: user.firstName,
    lastName: user.lastName,
    pageAccess: getEffectivePageAccess(user),
    pageAccessConfigured: user.pageAccessConfigured === true,
  };
  return req.user;
};

const verifyToken = async (req, res, next) => {
  const token = getTokenFromRequest(req);

  if (!token) {
    return res.status(403).send("A token is required for authentication");
  }
  try {
    await attachUserFromToken(req, token);
  } catch (err) {
    return res.status(err.statusCode || 401).send(err.message || "Invalid Token");
  }
  return next();
};

const optionalToken = async (req, res, next) => {
  const token = getTokenFromRequest(req);
  if (!token) return next();

  try {
    await attachUserFromToken(req, token);
    return next();
  } catch (err) {
    return res.status(err.statusCode || 401).send(err.message || "Invalid Token");
  }
};

const requireTenantAdmin = (req, res, next) => {
  if (!TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return res.status(403).json({ error: "Tenant admin access is required" });
  }
  return next();
};

module.exports = verifyToken;
module.exports.USER_TOKEN_AUDIENCE = USER_TOKEN_AUDIENCE;
module.exports.TENANT_ADMIN_ROLES = TENANT_ADMIN_ROLES;
module.exports.getTokenFromRequest = getTokenFromRequest;
module.exports.optionalToken = optionalToken;
module.exports.requireTenantAdmin = requireTenantAdmin;
