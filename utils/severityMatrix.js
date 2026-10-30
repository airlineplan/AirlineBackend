const { DEFAULT_CTD, validateCtdValue } = require("./ctdValidation");
const round2 = (value) => Math.round((Number(value) + Number.EPSILON) * 100) / 100;
const defaultMatrix = () => ({ columns: [{ ...DEFAULT_CTD }], rows: [{ hc: 1, rates: [0] }, { hc: 3, rates: [0] }] });
function decimal(value, name, positive = false) {
  if (value === "" || value == null || !Number.isFinite(Number(value)) || Number(value) < 0 || (positive && Number(value) === 0)
      || Math.abs(Number(value) * 100 - Math.round(Number(value) * 100)) > 1e-7) {
    throw new Error(`${name} must be ${positive ? "positive" : "non-negative"}, with at most two decimal places.`);
  }
  return Number(value);
}
function validateMatrix(input = {}) {
  if (!Array.isArray(input.columns) || input.columns.length < 1 || !Array.isArray(input.rows) || input.rows.length < 2) {
    throw new Error("A matrix needs at least one column and two H:C rows.");
  }
  const columns = input.columns.map((column) => Object.fromEntries(Object.keys(DEFAULT_CTD).map((metric) => [metric, validateCtdValue(metric, column[metric])])));
  const identities = columns.map((column) => `${column.condition.toUpperCase()}|${column.thrust}|${column.derate}`);
  if (new Set(identities).size !== columns.length) throw new Error("Duplicate Condition/Thrust/Derate columns.");
  const rows = input.rows.map((row) => {
    if (!Array.isArray(row.rates) || row.rates.length !== columns.length) throw new Error("Every H:C row needs one rate per column.");
    return { hc: decimal(row.hc, "H:C", true), rates: row.rates.map((rate) => decimal(rate, "Rate")) };
  }).sort((a, b) => a.hc - b.hc);
  if (new Set(rows.map((row) => row.hc)).size !== rows.length) throw new Error("Duplicate H:C values.");
  return { columns, rows };
}
function lookupSeverityRate(matrix, ctd, hours, departures) {
  const detail = { ...ctd, hc: departures > 0 ? round2(hours / departures) : null, hcLower: null, hcHigher: null, baseRate: 0 };
  if (!matrix || !ctd || departures <= 0) return detail;
  const column = matrix.columns.findIndex((item) => item.condition.trim().toUpperCase() === ctd.condition.trim().toUpperCase()
    && Number(item.thrust) === Number(ctd.thrust) && Math.abs(Number(item.derate) - Number(ctd.derate)) < 0.001);
  if (column < 0) return detail;
  const sorted = [...matrix.rows].sort((a, b) => a.hc - b.hc);
  const lower = [...sorted].reverse().find((row) => row.hc <= detail.hc);
  const higher = sorted.find((row) => row.hc >= detail.hc);
  detail.hcLower = lower?.hc ?? null;
  detail.hcHigher = higher?.hc ?? null;
  detail.baseRate = round2(lower && higher ? (lower.rates[column] + higher.rates[column]) / 2 : (lower || higher)?.rates[column] || 0);
  return detail;
}
module.exports = { defaultMatrix, validateMatrix, lookupSeverityRate };
