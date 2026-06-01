const fs = require("fs");
const path = require("path");
const { TENANT_ADMIN_ROLES } = require("./auth");
const { getHighestAccessLevel, hasPageAccess, normalizeFeatureIds } = require("../config/pageAccess");

const FEATURE_CONFIG_PATH = path.join(__dirname, "../config/tenantAdminFeatures.json");

const getTenantAdminFeatures = () => {
  const config = JSON.parse(fs.readFileSync(FEATURE_CONFIG_PATH, "utf8"));
  return Array.isArray(config.features) ? config.features : [];
};

const isFeatureAllowedForTenantAdmin = (featureIdOrIds) => {
  const featureAccess = new Map(
    getTenantAdminFeatures().map((feature) => [feature.id, feature])
  );
  return normalizeFeatureIds(featureIdOrIds).some((featureId) => {
    const feature = featureAccess.get(featureId);
    return feature?.isAllowed === true || feature?.isAlllowed === true;
  });
};

const requireTenantFeatureAccess = (featureIdOrIds) => (req, res, next) => {
  if (!TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return next();
  }

  if (isFeatureAllowedForTenantAdmin(featureIdOrIds)) {
    return next();
  }

  return res.status(403).json({
    error: "This feature is not enabled for tenant admins",
    featureId: featureIdOrIds,
  });
};

const getPageAccessError = (req, featureIdOrIds, requiredAccess) => {
  const highestLevel = getHighestAccessLevel(req.user, featureIdOrIds);

  if (highestLevel === "read" && requiredAccess === "edit") {
    return "You have read-only access for this page. Edit access is required.";
  }

  return requiredAccess === "edit"
    ? "You do not have edit access for this page."
    : "You do not have access to this page.";
};

const requireUserPageAccess = (featureIdOrIds, requiredAccess = "read") => (req, res, next) => {
  if (TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return next();
  }

  try {
    if (hasPageAccess(req.user, featureIdOrIds, requiredAccess)) {
      return next();
    }
  } catch (error) {
    return res.status(error.statusCode || 500).json({ error: error.message });
  }

  return res.status(403).json({
    error: getPageAccessError(req, featureIdOrIds, requiredAccess),
    featureId: featureIdOrIds,
    requiredAccess,
  });
};

const requireFeatureAccess = (featureIdOrIds, requiredAccess = "read") => (req, res, next) => {
  if (TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return requireTenantFeatureAccess(featureIdOrIds)(req, res, next);
  }

  return requireUserPageAccess(featureIdOrIds, requiredAccess)(req, res, next);
};

module.exports = {
  getTenantAdminFeatures,
  isFeatureAllowedForTenantAdmin,
  requireFeatureAccess,
  requireTenantFeatureAccess,
  requireUserPageAccess,
};
