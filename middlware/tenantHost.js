const normalizeHostname = (value = "") =>
  String(value).split(",")[0].trim().toLowerCase().replace(/:\d+$/, "");

const getRequestHostname = (req) =>
  normalizeHostname(req.headers["x-forwarded-host"] || req.headers.host || req.hostname);

const enforceTenantHost = (req, res, next) => {
  const expected = normalizeHostname(process.env.TENANT_DOMAIN);
  if (!expected) {
    return res.status(503).json({ error: "Tenant domain is not configured" });
  }

  const actual = getRequestHostname(req);
  const allowLocal =
    process.env.NODE_ENV !== "production" &&
    ["localhost", "127.0.0.1", "::1"].includes(actual);

  if (actual !== expected && !allowLocal) {
    return res.status(403).json({ error: "Invalid tenant domain" });
  }

  return next();
};

module.exports = {
  enforceTenantHost,
  getRequestHostname,
  normalizeHostname,
};
