const CATEGORIES = Object.freeze({
  PLANNED: "Planned",
  ACTUAL: "Actual",
  INVOICED: "Invoiced",
});

function normalizeCategory(value) {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return Object.values(CATEGORIES).find((category) => category.toLowerCase() === normalized)
    || CATEGORIES.PLANNED;
}

function categoryFilter(category) {
  return { category: normalizeCategory(category) };
}

module.exports = { CATEGORIES, normalizeCategory, categoryFilter };
