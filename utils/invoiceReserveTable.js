// Parser for reserve invoices with Sub account / Rate / Utilisation / Amount
// columns. Additional client layouts can be added without changing the wizard.
const AMOUNT = "(?:\\d{1,3}(?:,\\d{3})+|\\d+)(?:\\.\\d+)?";
const ROW = new RegExp(`^(.+?)\\s+(${AMOUNT})\\s+(${AMOUNT})\\s+(APU\\s*(?:hours?|hrs?)|FH|BH|FC|AH|months?|cycles?|hours?|hrs?|departures?)\\s+(${AMOUNT})(?:\\s+([A-Z]{3}))?$`, "i");
const numeric = value => Number(value.replace(/,/g, ""));
const norm = value => String(value || "").trim().toUpperCase();

function eventKey(value) {
  const text = norm(value).replace(/[^A-Z0-9]+/g, " ").trim()
    .replace(/PERFORMANCE RESTORATION/g, "PR").replace(/LIFE LIMITED PARTS?/g, "LLP")
    .replace(/STRUCTURAL INSPECTION/g, "SI");
  if (/\b(?:APUN|APU)\b/.test(text) && /\bPR\b/.test(text)) return "APU PR";
  if (/\b(?:ESN|ENGINE|ENG)\b/.test(text) && /\bLLP\b/.test(text)) return "ENGINE LLP";
  if (/\b(?:ESN|ENGINE|ENG)\b/.test(text) && /\bPR\b/.test(text)) return "ENGINE PR";
  const interval = text.match(/\b(6|12)\s*(?:Y|YR|YRS|YEAR|YEARS)\b/);
  if (interval && /\bSI\b/.test(text)) return `${interval[1]}Y SI`;
  if (/\bLANDING GEAR\b|\bLG(?: SET)?\b/.test(text)) return "LG SET";
  return text;
}

function matchReserveSetting(candidate, settings, aircraft = []) {
  const registrations = new Set(aircraft.map(row => norm(row.regn)).filter(Boolean));
  const matches = settings.filter(row => (!registrations.size || registrations.has(norm(row.acftRegn)))
    && eventKey(row.schMxEvent) === eventKey(candidate.schMxEvent || candidate.description)
    && (!candidate.sn || norm(row.sn) === norm(candidate.sn)));
  return matches.length === 1 ? matches[0] : null;
}

function parseReserveTable(text) {
  const lines = text.split(/\r?\n/).map(line => line.trim());
  let currency = "", inTable = false;
  const rows = [];
  let total;
  for (const line of lines) {
    if (/\bRate\s*\([A-Z]{3}\).*\bUtili[sz]ation\b.*\bAmount\s*\([A-Z]{3}\)/i.test(line)) {
      currency = line.match(/\bRate\s*\(([A-Z]{3})\)/i)[1].toUpperCase();
      inTable = true;
      continue;
    }
    if (!inTable) continue;
    const sum = line.match(new RegExp(`^Total\\s+(?:([A-Z]{3})\\s+)?(${AMOUNT})(?:\\s+([A-Z]{3}))?$`, "i"));
    if (sum) { total = { amount: numeric(sum[2]), currency: norm(sum[1] || sum[3] || currency) }; inTable = false; continue; }
    const match = line.match(ROW);
    if (!match) continue;
    const [, description, rate, utilisation, unit, amount, rowCurrency] = match;
    const sn = description.match(/\b(?:ESN|APUN|S\/?N)\s*[:#]?\s*([A-Z0-9-]+)/i)?.[1] || "";
    const key = eventKey(description);
    const labels = { "ENGINE PR": "Engine PR", "ENGINE LLP": "Engine LLP", "APU PR": "APU PR", "LG SET": "LG set" };
    const metric = /^APU\s*(?:HOURS?|HRS?)$|^AH$/i.test(unit) ? "APUHR"
      : /^MONTH/i.test(unit) ? "MONTH" : /^(CYCLE|FC|DEPARTURE)/i.test(unit) ? "DEPARTURES" : /^(HOUR|HR)/i.test(unit) ? "FH" : norm(unit);
    rows.push({ description, schMxEvent: labels[key] || (key.endsWith("Y SI") ? key : description), sn: norm(sn),
      rate: numeric(rate), utilisation: numeric(utilisation), utilisationMetric: metric,
      amount: numeric(amount), currency: norm(rowCurrency || currency) });
  }
  return { lines: rows, ...(total && { total }) };
}
module.exports = { parseReserveTable, matchReserveSetting, eventKey };
