const { TENANT_ADMIN_ROLES } = require("./auth");
const { getHighestAccessLevel, hasPageAccess, normalizeFeatureIds } = require("../config/pageAccess");
const {
  FEATURE_CATALOG,
  canonicalizeFeatureIds,
} = require("../config/featureCatalog");
const { getTenantRuntimeConfig } = require("../config/runtime");

const getTenantAdminFeatures = () => {
  const { features } = getTenantRuntimeConfig();
  const canonical = FEATURE_CATALOG.map((feature) => ({
    id: feature.id,
    label: feature.label,
    isAllowed: features[feature.id] === true,
  }));

  return [
    ...canonical,
    { id: "view", label: "View", isAllowed: features.network === true },
    { id: "list", label: "List", isAllowed: features.network === true },
  ];
};

const isFeatureAllowedForTenantAdmin = (featureIdOrIds) => {
  const { features } = getTenantRuntimeConfig();
  return canonicalizeFeatureIds(featureIdOrIds).some(
    (featureId) => features[featureId] === true
  );
};

const requireTenantFeatureAccess = (featureIdOrIds) => (req, res, next) => {
  if (isFeatureAllowedForTenantAdmin(featureIdOrIds)) {
    return next();
  }

  return res.status(403).json({
    error: "This feature is not enabled for this tenant",
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
  if (!isFeatureAllowedForTenantAdmin(featureIdOrIds)) {
    return res.status(403).json({
      error: "This feature is not enabled for this tenant",
      featureId: featureIdOrIds,
    });
  }

  if (TENANT_ADMIN_ROLES.has(req.user?.role)) {
    return next();
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
