const RESERVED_SUBDOMAINS = new Set([
  "admin",
  "api",
  "app",
  "assets",
  "cdn",
  "ftp",
  "mail",
  "mx",
  "ns1",
  "ns2",
  "smtp",
  "support",
  "www",
]);

const normalizeSubdomain = (value) => String(value || "").trim().toLowerCase();

const validateSubdomain = (value) => {
  const subdomain = normalizeSubdomain(value);

  if (!subdomain) {
    return { valid: false, subdomain, error: "Subdomain is required" };
  }
  if (subdomain.length < 2 || subdomain.length > 63) {
    return { valid: false, subdomain, error: "Subdomain must be 2 to 63 characters" };
  }
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(subdomain)) {
    return {
      valid: false,
      subdomain,
      error: "Use lowercase letters, numbers, and hyphens. Hyphens cannot start or end the subdomain.",
    };
  }
  if (RESERVED_SUBDOMAINS.has(subdomain)) {
    return { valid: false, subdomain, error: "This subdomain is reserved" };
  }

  return { valid: true, subdomain };
};

module.exports = {
  RESERVED_SUBDOMAINS,
  normalizeSubdomain,
  validateSubdomain,
};
