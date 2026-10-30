const moment = require("moment");
const cleanHeader = (value) => String(value).toLowerCase().replace(/[^a-z0-9]/g, "");
function parseDate(value) {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : moment.utc(value).startOf("day").toDate();
  if (typeof value === "number" && Number.isFinite(value)) return moment.utc(Date.UTC(1899, 11, 30) + Math.floor(value) * 86400000).toDate();
  const parsed = moment.utc(String(value || "").trim(), ["YYYY-MM-DD", "D-MMM-YY", "D-MMM-YYYY", "D/M/YYYY", "D-M-YYYY"], true);
  return parsed.isValid() ? parsed.startOf("day").toDate() : null;
}
function parseTime(value, duration = false) {
  if (value instanceof Date) value = `${value.getUTCHours()}:${String(value.getUTCMinutes()).padStart(2, "0")}`;
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    const minutes = Math.round(value * 1440);
    if (!duration && minutes >= 1440) return null;
    return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
  }
  const match = String(value ?? "").trim().match(/^(\d{1,3}):([0-5]\d)(?::[0-5]\d)?$/);
  if (!match || (!duration && Number(match[1]) > 23)) return null;
  return `${match[1].padStart(2, "0")}:${match[2]}`;
}
function parseActualRows(rawRows) {
  return rawRows.map((input, index) => {
    const cells = Object.fromEntries(Object.entries(input).map(([name, value]) => [cleanHeader(name), value]));
    const reasons = [];
    const date = parseDate(cells.date);
    const row = { rowNumber: input.__rowNum__ != null ? input.__rowNum__ + 1 : index + 2, date,
      flight: String(cells.flight ?? cells.flightno ?? cells.flightnumber ?? "").trim().toUpperCase(),
      depStn: String(cells.depstn ?? "").trim().toUpperCase(), arrStn: String(cells.arrstn ?? "").trim().toUpperCase(),
      acft: String(cells.acft ?? "").trim().toUpperCase(), std: parseTime(cells.atdlt ?? cells.atd), sta: parseTime(cells.atalt ?? cells.ata),
      bt: parseTime(cells.bt, true), ft: cells.ft == null || cells.ft === "" ? null : parseTime(cells.ft, true),
      domIntl: String(cells.domintl ?? "").trim().toLowerCase(), rotationNumber: String(cells.rotation ?? "").trim(),
    };
    if (!date) reasons.push("Invalid or missing Date");
    for (const field of ["flight", "depStn", "arrStn", "acft", "std", "sta", "bt"]) if (!row[field]) reasons.push(`Invalid or missing ${field}`);
    if (cells.ft != null && cells.ft !== "" && !row.ft) reasons.push("Invalid FT");
    for (const [field, header] of [["seats", "seats"], ["CargoCapT", "cargocap"], ["pax", "pax"], ["CargoT", "cargot"]]) {
      const value = cells[header];
      row[field] = value == null || value === "" ? 0 : Number(value);
      if (!Number.isFinite(row[field])) reasons.push(`${header} must be numeric`);
    }
    return { ...row, reasons };
  });
}
module.exports = { parseDate, parseTime, parseActualRows };
