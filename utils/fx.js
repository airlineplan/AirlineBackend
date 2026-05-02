const moment = require("moment");

const parseNumber = (value, fallback = 0) => {
  if (value === null || value === undefined || value === "") return fallback;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : fallback;
};

const roundToTwo = (value) => Number(parseNumber(value).toFixed(2));

function normalizeCurrencyCode(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, "")
    .slice(0, 3);
}

function normalizeDateKey(value) {
  if (!value) return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value.trim())) {
    return value.trim();
  }
  const parsed = moment.utc(value);
  return parsed.isValid() ? parsed.format("YYYY-MM-DD") : String(value || "").trim();
}

function normalizeFxRateRow(row = {}) {
  const pair = String(row.pair || "").trim().toUpperCase();
  const dateKey = normalizeDateKey(row.dateKey || row.date || row.effectiveDate || row.period);
  const rate = parseNumber(row.rate ?? row.fxRate ?? row.value, 1);
  return { pair, dateKey, rate };
}

function getCarriedForwardFxRate(fxRates = [], pair, dateKey) {
  const normalizedPair = String(pair || "").trim().toUpperCase();
  const targetKey = normalizeDateKey(dateKey);
  if (!normalizedPair || !targetKey) return 1;

  const matches = (Array.isArray(fxRates) ? fxRates : [])
    .map(normalizeFxRateRow)
    .filter((row) => row.pair === normalizedPair && row.dateKey && row.rate > 0)
    .sort((a, b) => a.dateKey.localeCompare(b.dateKey));

  let selected = null;
  for (const row of matches) {
    if (row.dateKey <= targetKey) selected = row;
    if (row.dateKey > targetKey) break;
  }

  return selected ? selected.rate : 1;
}

function convertLocalToReporting(amount, localCcy, reportingCurrency, dateKey, fxRates = []) {
  const numeric = parseNumber(amount);
  const local = normalizeCurrencyCode(localCcy);
  const reporting = normalizeCurrencyCode(reportingCurrency) || "USD";
  if (!local || local === reporting) return roundToTwo(numeric);

  // FX direction is LOCAL/REPORTING. Local-to-reporting is multiplication only.
  const rate = getCarriedForwardFxRate(fxRates, `${local}/${reporting}`, dateKey);
  return roundToTwo(numeric * rate);
}

module.exports = {
  normalizeCurrencyCode,
  normalizeDateKey,
  getCarriedForwardFxRate,
  convertLocalToReporting,
};
