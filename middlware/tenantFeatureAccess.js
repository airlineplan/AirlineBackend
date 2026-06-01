const fs = require("fs");
const path = require("path");
const { TENANT_ADMIN_ROLES } = require("./auth");

const FEATURE_CONFIG_PATH = path.join(__dirname, "../config/tenantAdminFeatures.json");

const getTenantAdminFeatures = () => {
  const config = JSON.parse(fs.readFileSync(FEATURE_CONFIG_PATH, "utf8"));
  return Array.isArray(config.features) ? config.features : [];
};

const isFeatureAllowedForTenantAdmin = (featureId) => {
  const featureAccess = new Map(
    getTenantAdminFeatures().map((feature) => [feature.id, feature])
  );
  const feature = featureAccess.get(featureId);
  return feature?.isAllowed === true || feature?.isAlllowed === true;
};

const requireTenantFeatureAccess = (featureId) => (req, res, next) => {
  if (!TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return next();
  }

  if (isFeatureAllowedForTenantAdmin(featureId)) {
    return next();
  }

  return res.status(403).json({
    error: "This feature is not enabled for tenant admins",
    featureId,
  });
};

module.exports = {
  getTenantAdminFeatures,
  isFeatureAllowedForTenantAdmin,
  requireTenantFeatureAccess,
};
