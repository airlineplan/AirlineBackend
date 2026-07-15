const normalizeHostname = (value = "") =>
  String(value).split(",")[0].trim().toLowerCase().replace(/:\d+$/, "");

const getRequestHostname = (req) =>
  normalizeHostname(req.headers["x-forwarded-host"] || req.headers.host || req.hostname);

const getAllowedTenantHostnames = () => {
  const tenantDomain = normalizeHostname(process.env.TENANT_DOMAIN);
  const rootDomain = normalizeHostname(process.env.ROOT_DOMAIN || "airlineplan.com");

  return new Set(
    [tenantDomain, rootDomain, rootDomain && `www.${rootDomain}`].filter(Boolean)
  );
};

const enforceTenantHost = (req, res, next) => {
  const expected = normalizeHostname(process.env.TENANT_DOMAIN);
  if (!expected) {
    return res.status(503).json({ error: "Tenant domain is not configured" });
  }

  const actual = getRequestHostname(req);
  const allowedHostnames = getAllowedTenantHostnames();
  const allowLocal =
    process.env.NODE_ENV !== "production" &&
    ["localhost", "127.0.0.1", "::1"].includes(actual);

  if (!allowedHostnames.has(actual) && !allowLocal) {
    return res.status(403).json({ error: "Invalid tenant domain" });
  }

  return next();
};

module.exports = {
  enforceTenantHost,
  getAllowedTenantHostnames,
  getRequestHostname,
  normalizeHostname,
};
