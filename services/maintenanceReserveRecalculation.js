const Flight = require("../model/flight");
const CostConfig = require("../model/costConfigSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const Fleet = require("../model/fleet");
const Matrix = require("../model/severityMatrixSchema");
const { buildCtdTimeline } = require("./ctdService");
const { getUtilisationFlights } = require("../utils/utilisationFlights");
const { buildMaintenanceReserveContext } = require("../utils/maintenanceReserveContext");
const { generateMaintenanceReserveScheduleWithContributions, normalizeCostConfig, computeFlightCostsBatch } = require("../utils/costLogic");
const { COST_PERSIST_FIELDS } = require("../utils/costFields");

async function loadSeverityContext(userId, config, flights) {
  const accounts = (config.leasedReserve || []).filter((row) => row.setRateType === "SM");
  if (!accounts.length || !flights.length) return {};
  const dates = flights.map((row) => new Date(row.date).getTime()).filter(Number.isFinite);
  if (!dates.length) return {};
  const to = new Date(Math.max(...dates));
  to.setUTCMonth(to.getUTCMonth() + 1, 0);
  const [matrices, timeline] = await Promise.all([
    Matrix.find({ userId, mrAccId: { $in: accounts.map((row) => String(row.mrAccId).trim().toUpperCase()) } }).lean(),
    buildCtdTimeline({ userId, esns: [...new Set(accounts.map((row) => row.sn))], from: new Date(Math.min(...dates)), to }),
  ]);
  return { severityMatrices: Object.fromEntries(matrices.map((matrix) => [matrix.mrAccId, matrix])), ctdByEsn: timeline };
}
async function loadCostContext(userId, flights, config) {
  const [rawConfig, revenue, fleet, mr] = await Promise.all([
    config ? Promise.resolve(config) : CostConfig.findOne({ userId }).lean(),
    RevenueConfig.findOne({ userId }).lean(), Fleet.find({ userId }).lean(), buildMaintenanceReserveContext(userId, flights),
  ]);
  const normalized = normalizeCostConfig({ ...(rawConfig || {}), reportingCurrency: revenue?.reportingCurrency || rawConfig?.reportingCurrency,
    fxRates: revenue?.fxRates || rawConfig?.fxRates, fleet });
  return { ...normalized, aircraftOnwing: [...(mr.aircraftOnwing || []), ...normalized.aircraftOnwing],
    maintenanceReserveSchedule: rawConfig?.maintenanceReserveSchedule || [], fleet };
}
async function recalculateActualCosts(userId, { mrOnly = false } = {}) {
  const flights = await Flight.find({ userId, category: "Actual" }).lean();
  if (!flights.length) return 0;
  const config = await loadCostContext(userId, flights);
  const rows = computeFlightCostsBatch(flights, config);
  const fields = mrOnly ? require("../utils/costFields").MR_FIELDS : COST_PERSIST_FIELDS;
  await Flight.bulkWrite(rows.map((row) => ({ updateOne: {
    filter: { _id: row._id, userId, category: "Actual" },
    update: { $set: Object.fromEntries(fields.filter((field) => row[field] !== undefined).map((field) => [field, row[field]])) },
  } })));
  return rows.length;
}
async function refreshMaintenanceReserves(userId, { mrAccId, esns, recalculate = true } = {}) {
  const config = await CostConfig.findOne({ userId }).lean();
  if (!config) return [];
  const flights = await getUtilisationFlights({ userId });
  const context = await buildMaintenanceReserveContext(userId, flights);
  const severity = await loadSeverityContext(userId, config, flights);
  const selected = (config.leasedReserve || []).filter((row) => (!mrAccId || String(row.mrAccId).trim().toUpperCase() === mrAccId)
    && (!esns || (row.setRateType === "SM" && esns.includes(row.sn))));
  const ids = new Set(selected.map((row) => String(row.mrAccId).trim().toUpperCase()));
  const previous = config.maintenanceReserveSchedule || [];
  const rows = generateMaintenanceReserveScheduleWithContributions(selected, previous.filter((row) => ids.has(String(row.mrAccId).trim().toUpperCase())), flights,
    { ...severity, aircraftOnwing: [...(context.aircraftOnwing || []), ...(config.aircraftOnwing || [])], schMxEvents: config.schMxEvents || [] });
  const merged = [...previous.filter((row) => !ids.has(String(row.mrAccId).trim().toUpperCase())), ...rows];
  await CostConfig.updateOne({ userId }, { $set: { maintenanceReserveSchedule: merged } });
  if (recalculate) await recalculateActualCosts(userId, { mrOnly: true });
  return merged;
}
module.exports = { loadSeverityContext, loadCostContext, refreshMaintenanceReserves, recalculateActualCosts };
