const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const ADMIN_TOKEN_AUDIENCE = "airlineplan-admin";

const getAdminJwtSecret = () => process.env.ADMIN_JWT_SECRET || "";

const ensureAdminAuthConfigured = () => {
  const missing = [];
  if (!process.env.ADMIN_EMAIL) missing.push("ADMIN_EMAIL");
  if (!process.env.ADMIN_PASSWORD_HASH) missing.push("ADMIN_PASSWORD_HASH");
  if (!getAdminJwtSecret()) missing.push("ADMIN_JWT_SECRET");
  return missing;
};

const verifyAdminCredentials = async ({ email, password }) => {
  const missing = ensureAdminAuthConfigured();
  if (missing.length > 0) {
    const error = new Error(`Admin auth is not configured. Missing: ${missing.join(", ")}`);
    error.statusCode = 500;
    throw error;
  }

  const expectedEmail = String(process.env.ADMIN_EMAIL).trim().toLowerCase();
  const suppliedEmail = String(email || "").trim().toLowerCase();
  if (!suppliedEmail || suppliedEmail !== expectedEmail) {
    return false;
  }

  return bcrypt.compare(String(password || ""), process.env.ADMIN_PASSWORD_HASH);
};

const signAdminToken = (email) =>
  jwt.sign(
    {
      email: String(email).trim().toLowerCase(),
      role: "super_admin",
      aud: ADMIN_TOKEN_AUDIENCE,
    },
    getAdminJwtSecret(),
    { expiresIn: process.env.ADMIN_JWT_EXPIRES_IN || "8h" }
  );

const verifyAdminToken = (token) => {
  const decoded = jwt.verify(token, getAdminJwtSecret());
  if (decoded?.role !== "super_admin" || decoded?.aud !== ADMIN_TOKEN_AUDIENCE) {
    const error = new Error("Invalid admin token");
    error.statusCode = 401;
    throw error;
  }
  return decoded;
};

module.exports = {
  ADMIN_TOKEN_AUDIENCE,
  ensureAdminAuthConfigured,
  signAdminToken,
  verifyAdminCredentials,
  verifyAdminToken,
};
