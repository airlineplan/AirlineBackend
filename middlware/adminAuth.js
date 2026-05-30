const { verifyAdminToken } = require("../utils/adminAuth");

const rootDomain = () => String(process.env.ROOT_DOMAIN || "airlineplan.com").toLowerCase();

const isLocalHost = (hostname = "") => ["localhost", "127.0.0.1", "::1"].includes(hostname);

const requireRootAdminHost = (req, res, next) => {
  const hostname = String(req.hostname || "").toLowerCase();
  if (isLocalHost(hostname) || hostname === rootDomain()) {
    return next();
  }

  return res.status(404).json({ error: "Super admin is only available on the root domain" });
};

const verifyAdmin = (req, res, next) => {
  const header = req.headers.authorization || "";
  const bearerToken = header.startsWith("Bearer ") ? header.slice(7) : "";
  const token = bearerToken || req.headers["x-admin-token"] || req.body?.token || req.query?.token;

  if (!token) {
    return res.status(403).json({ error: "Admin token is required" });
  }

  try {
    req.admin = verifyAdminToken(token);
    return next();
  } catch (error) {
    return res.status(error.statusCode || 401).json({ error: "Invalid admin token" });
  }
};

module.exports = verifyAdmin;
module.exports.requireRootAdminHost = requireRootAdminHost;
