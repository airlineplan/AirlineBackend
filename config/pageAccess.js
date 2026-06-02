const ASSIGNABLE_PAGE_FEATURES = [
  { id: "network", label: "Network" },
  { id: "sectors", label: "Sectors" },
  { id: "stations", label: "Stations" },
  { id: "rotations", label: "Rotations" },
  { id: "flgts", label: "FLGTs" },
  { id: "view", label: "View" },
  { id: "dashboard", label: "Dashboard" },
  { id: "list", label: "List" },
  { id: "connections", label: "Connections" },
  { id: "assignment", label: "Assignment" },
  { id: "fleet", label: "Fleet" },
  { id: "maintenance", label: "Maintenance" },
  { id: "poo", label: "POO" },
  { id: "revenue", label: "Revenue" },
  { id: "cost", label: "Cost" },
  { id: "crew", label: "Crew" },
  { id: "routeEconomics", label: "Route Economics" },
];

const PAGE_ACCESS_LEVELS = ["none", "read", "edit"];
const PAGE_ACCESS_RANK = {
  none: 0,
  read: 1,
  edit: 2,
};
const ASSIGNABLE_PAGE_IDS = new Set(ASSIGNABLE_PAGE_FEATURES.map((feature) => feature.id));

const toPlainPageAccess = (pageAccess) => {
  if (!pageAccess) return {};
  if (pageAccess instanceof Map) return Object.fromEntries(pageAccess.entries());
  if (typeof pageAccess.toObject === "function") return pageAccess.toObject();
  return { ...pageAccess };
};

const isEmptyPageAccess = (pageAccess) => Object.keys(toPlainPageAccess(pageAccess)).length === 0;

const buildPageAccess = (level) => Object.fromEntries(
  ASSIGNABLE_PAGE_FEATURES.map((feature) => [feature.id, level])
);

const createDefaultPageAccess = () => buildPageAccess("edit");
const createLegacyPageAccess = () => buildPageAccess("edit");
const isAllNonePageAccess = (pageAccess) => {
  const source = toPlainPageAccess(pageAccess);
  const values = Object.entries(source).filter(([featureId]) => ASSIGNABLE_PAGE_IDS.has(featureId));
  return values.length > 0 && values.every(([, level]) => level === "none");
};

const normalizePageAccessInput = (input = {}) => {
  const source = input == null ? {} : toPlainPageAccess(input);

  if (Array.isArray(source) || typeof source !== "object") {
    const error = new Error("Page access must be an object");
    error.statusCode = 400;
    throw error;
  }

  const normalized = createDefaultPageAccess();

  Object.entries(source).forEach(([featureId, level]) => {
    if (!ASSIGNABLE_PAGE_IDS.has(featureId)) {
      const error = new Error(`Unknown page access feature: ${featureId}`);
      error.statusCode = 400;
      throw error;
    }

    if (!PAGE_ACCESS_LEVELS.includes(level)) {
      const error = new Error(`Invalid access level for ${featureId}`);
      error.statusCode = 400;
      throw error;
    }

    normalized[featureId] = level;
  });

  return normalized;
};

const getEffectivePageAccess = (user = {}) => {
  if (
    user.pageAccess === undefined ||
    user.pageAccess === null ||
    (user.pageAccessConfigured !== true && (
      isEmptyPageAccess(user.pageAccess) ||
      isAllNonePageAccess(user.pageAccess)
    ))
  ) {
    return createLegacyPageAccess();
  }

  return normalizePageAccessInput(user.pageAccess);
};

const normalizeFeatureIds = (featureIdOrIds) => (
  Array.isArray(featureIdOrIds) ? featureIdOrIds : [featureIdOrIds]
).filter(Boolean);

const getHighestAccessLevel = (user = {}, featureIdOrIds) => {
  const pageAccess = getEffectivePageAccess(user);
  return normalizeFeatureIds(featureIdOrIds).reduce((highest, featureId) => {
    const level = pageAccess[featureId] || "none";
    return PAGE_ACCESS_RANK[level] > PAGE_ACCESS_RANK[highest] ? level : highest;
  }, "none");
};

const hasPageAccess = (user = {}, featureIdOrIds, requiredAccess = "read") => {
  if (!["read", "edit"].includes(requiredAccess)) {
    const error = new Error("Invalid required page access");
    error.statusCode = 500;
    throw error;
  }

  const highestLevel = getHighestAccessLevel(user, featureIdOrIds);
  return PAGE_ACCESS_RANK[highestLevel] >= PAGE_ACCESS_RANK[requiredAccess];
};

module.exports = {
  ASSIGNABLE_PAGE_FEATURES,
  ASSIGNABLE_PAGE_IDS,
  PAGE_ACCESS_LEVELS,
  PAGE_ACCESS_RANK,
  createDefaultPageAccess,
  createLegacyPageAccess,
  getEffectivePageAccess,
  getHighestAccessLevel,
  hasPageAccess,
  isAllNonePageAccess,
  isEmptyPageAccess,
  normalizeFeatureIds,
  normalizePageAccessInput,
  toPlainPageAccess,
};
