// Read-only data shared by multiple pages. Keep these policies separate from
// write routes so a page can load its supporting data without gaining edit
// access to the module that owns that data.
const SUPPORTING_PAGE_READ_ACCESS = Object.freeze({
  networkData: Object.freeze(["network", "sectors"]),
  sectorData: Object.freeze(["sectors", "cost"]),
  dashboardDropdowns: Object.freeze([
    "dashboard",
    "cost",
    "revenue",
    "network",
    "list",
    "connections",
  ]),
  stationData: Object.freeze([
    "stations",
    "network",
    "sectors",
    "poo",
    "view",
    "cost",
  ]),
  fleetData: Object.freeze(["fleet", "maintenance", "cost"]),
  maintenanceRotables: Object.freeze(["maintenance", "cost"]),
  financialConfig: Object.freeze(["revenue", "cost", "poo", "dashboard", "stations"]),
  apuFuelRows: Object.freeze(["cost", "dashboard"]),
});

module.exports = { SUPPORTING_PAGE_READ_ACCESS };
