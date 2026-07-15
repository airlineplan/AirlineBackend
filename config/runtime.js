const {
  createDefaultFeatures,
  normalizeFeatureMap,
} = require("./featureCatalog");
const { getRedisUrl } = require("./redisUrl");

const parseJsonEnv = (name, fallback) => {
  const raw = process.env[name];
  if (!raw) return fallback;

  try {
    return JSON.parse(raw);
  } catch (error) {
    const configError = new Error(`${name} must contain valid JSON`);
    configError.cause = error;
    configError.code = "INVALID_RUNTIME_CONFIG";
    throw configError;
  }
};

const getAppMode = () => {
  const mode = String(process.env.APP_MODE || "tenant").trim().toLowerCase();
  if (!["control-plane", "tenant"].includes(mode)) {
    throw new Error("APP_MODE must be control-plane or tenant");
  }
  return mode;
};

const getTenantRuntimeConfig = () => {
  const features = normalizeFeatureMap(
    parseJsonEnv("TENANT_FEATURES_JSON", createDefaultFeatures(true))
  );
  const branding = parseJsonEnv("TENANT_BRANDING_JSON", {});
  const domain = String(process.env.TENANT_DOMAIN || "").trim().toLowerCase();
  const tenantId = String(process.env.TENANT_ID || process.env.TENANT_SLUG || "").trim();

  return {
    tenantId,
    slug: String(process.env.TENANT_SLUG || tenantId).trim().toLowerCase(),
    companyName: String(
      process.env.TENANT_COMPANY_NAME || branding.companyName || "Airlineplan"
    ).trim(),
    domain,
    appVersion: String(process.env.APP_VERSION || "development").trim(),
    imageDigest: String(process.env.IMAGE_DIGEST || "").trim(),
    features,
    branding: {
      companyName: String(
        branding.companyName || process.env.TENANT_COMPANY_NAME || "Airlineplan"
      ).trim(),
      logoUrl: String(branding.logoUrl || "").trim(),
      primaryColor: String(branding.primaryColor || "#0B3B75").trim(),
    },
  };
};

const validateRuntimeConfig = () => {
  const mode = getAppMode();
  if (process.env.NODE_ENV !== "production" || mode !== "tenant") return;

  const config = getTenantRuntimeConfig();
  const missing = [];
  if (!config.tenantId) missing.push("TENANT_ID");
  if (!config.domain) missing.push("TENANT_DOMAIN");
  if (!process.env.REDIS_URL) missing.push("REDIS_URL");
  if (!process.env.JWT_SECRET) missing.push("JWT_SECRET");
  if (missing.length > 0) {
    throw new Error(`Missing tenant runtime configuration: ${missing.join(", ")}`);
  }
  getRedisUrl();
};

module.exports = {
  getAppMode,
  getTenantRuntimeConfig,
  parseJsonEnv,
  validateRuntimeConfig,
};
