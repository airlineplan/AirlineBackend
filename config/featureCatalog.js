const FEATURE_CATALOG = Object.freeze([
  { id: "network", label: "Network", defaultEnabled: true },
  { id: "sectors", label: "Sectors", defaultEnabled: true },
  { id: "stations", label: "Stations", defaultEnabled: true },
  { id: "rotations", label: "Rotations", defaultEnabled: true },
  { id: "flgts", label: "FLGTs", defaultEnabled: true },
  { id: "dashboard", label: "Dashboard", defaultEnabled: true },
  { id: "connections", label: "Connections", defaultEnabled: true },
  { id: "assignment", label: "Assignment", defaultEnabled: true },
  { id: "fleet", label: "Fleet", defaultEnabled: true },
  { id: "maintenance", label: "Maintenance", defaultEnabled: true },
  { id: "poo", label: "POO", defaultEnabled: true },
  { id: "revenue", label: "Revenue", defaultEnabled: true },
  { id: "cost", label: "Cost", defaultEnabled: true },
  { id: "crew", label: "Crew", defaultEnabled: true },
  { id: "routeEconomics", label: "Route Economics", defaultEnabled: true },
  { id: "users", label: "Users", defaultEnabled: true },
]);

const FEATURE_IDS = Object.freeze(FEATURE_CATALOG.map((feature) => feature.id));
const FEATURE_ID_SET = new Set(FEATURE_IDS);

// These legacy pages remain available, but their tenant entitlement follows
// the core network module rather than creating additional public feature IDs.
const FEATURE_ALIASES = Object.freeze({
  view: "network",
  list: "network",
});

const createDefaultFeatures = (enabled = true) =>
  Object.fromEntries(FEATURE_IDS.map((featureId) => [featureId, enabled]));

const normalizeFeatureMap = (input, { defaultEnabled = true } = {}) => {
  const source = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  const normalized = createDefaultFeatures(defaultEnabled);

  Object.entries(source).forEach(([featureId, enabled]) => {
    const canonicalId = FEATURE_ALIASES[featureId] || featureId;
    if (FEATURE_ID_SET.has(canonicalId)) {
      normalized[canonicalId] = enabled === true;
    }
  });

  return normalized;
};

const canonicalizeFeatureIds = (featureIdOrIds) =>
  (Array.isArray(featureIdOrIds) ? featureIdOrIds : [featureIdOrIds])
    .filter(Boolean)
    .map((featureId) => FEATURE_ALIASES[featureId] || featureId);

module.exports = {
  FEATURE_ALIASES,
  FEATURE_CATALOG,
  FEATURE_IDS,
  FEATURE_ID_SET,
  canonicalizeFeatureIds,
  createDefaultFeatures,
  normalizeFeatureMap,
};
