const { verifyAdminToken } = require("../utils/adminAuth");

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
