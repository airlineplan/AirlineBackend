const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const { parseReserveTable, matchReserveSetting } = require("./invoiceReserveTable");
const number = (value) => Number(String(value || "").replace(/,/g, ""));
function extractCandidates(text, { fleet = [], settings = [] } = {}) {
  if (!text.trim()) return { months: [], aircraft: [], lines: [] };
  const months = new Map();
  const pattern = new RegExp(`\\b(${MONTHS.map((month) => `${month}|${month.slice(0, 3)}`).join("|")})[-\\s/]+(20\\d{2}|\\d{2})\\b`, "gi");
  // Invoice and due dates can be in the following month. Prefer the explicitly
  // stated utilisation period instead of suggesting those as the billed month.
  const periodLines = text.split(/\r?\n/).filter(line => /(?:for\s+the\s+month|utili[sz]ation\s+(?:month|period)|(?:reserve|billing)\s+period)/i.test(line));
  const periodText = periodLines.length ? periodLines.join("\n") : text;
  for (const match of periodText.matchAll(pattern)) {
    const month = MONTHS.findIndex((name) => name.slice(0, 3).toLowerCase() === match[1].slice(0, 3).toLowerCase()) + 1;
    const year = Number(match[2]) + (match[2].length === 2 ? 2000 : 0);
    months.set(`${year}-${month}`, { month, year });
  }
  for (const match of periodText.matchAll(/\b(0?[1-9]|1[0-2])\/(20\d{2})\b/g)) months.set(`${match[2]}-${Number(match[1])}`, { month: Number(match[1]), year: Number(match[2]) });
  const upper = text.toUpperCase();
  const aircraft = fleet.filter((row) => row.category === "Aircraft" && ((row.regn && upper.includes(row.regn.toUpperCase())) || new RegExp(`\\bMSN\\s*[:#]?\\s*${row.sn}\\b`, "i").test(text)))
    .map((row) => ({ msn: row.sn, regn: row.regn }));
  if (!aircraft.length) {
    for (const match of upper.matchAll(/\b[A-Z]{1,3}-[A-Z][A-Z0-9]{1,5}\b/g)) {
      if (!aircraft.some(row => row.regn === match[0])) aircraft.push({ regn: match[0], msn: upper.match(/\bMSN\s*[:#]?\s*(\d+)/)?.[1] || "" });
    }
  }
  const table = parseReserveTable(text);
  if (table.lines.length) return { months: [...months.values()], aircraft,
    lines: table.lines.map(candidate => {
      const setting = matchReserveSetting(candidate, settings, aircraft);
      return { ...candidate, ...(setting && { mrAccId: setting.mrAccId, schMxEvent: setting.schMxEvent }) };
    }), ...(table.total && { total: table.total }) };
  const lines = [];
  for (const line of text.split(/\r?\n/).filter((value) => value.trim())) {
    const setting = settings.map((row) => ({ row, score: String(row.schMxEvent || "").toUpperCase().split(/\W+/).filter((word) => word.length > 1 && line.toUpperCase().includes(word)).length }))
      .sort((a, b) => b.score - a.score)[0];
    if (!/\b(ESN|SN|FH|FC|HOURS?|CYCLES?|RATE|CONTRIBUTION|RESERVE)\b/i.test(line) && !setting?.score) continue;
    const currency = line.match(/\b(USD|EUR|GBP|INR|AED)\b/i)?.[1]?.toUpperCase() || (line.includes("$") ? "USD" : line.includes("\u20ac") ? "EUR" : "");
    const rate = line.match(/\brate\s*[:=]?\s*(?:[A-Z]{3}|[$\u20ac])?\s*([\d,]+(?:\.\d+)?)/i)?.[1];
    const usage = line.match(/([\d,]+(?:\.\d+)?)\s*(FH|FC|BH|HOURS?|CYCLES?|MONTHS?)\b/i);
    const amount = line.match(/\b(?:amount|contribution|total)\s*[:=]?\s*(?:[A-Z]{3}|[$\u20ac])?\s*([\d,]+(?:\.\d+)?)/i)?.[1];
    lines.push({ description: line.trim(), schMxEvent: setting?.score ? setting.row.schMxEvent : "",
      sn: line.match(/\b(?:ESN|SN)\s*[:#]?\s*([A-Z0-9-]+)/i)?.[1] || "", currency,
      ...(rate ? { rate: number(rate) } : {}), ...(usage ? { utilisation: number(usage[1]), utilisationMetric: usage[2].toUpperCase() } : {}),
      ...(amount ? { amount: number(amount) } : {}),
    });
  }
  return { months: [...months.values()], aircraft, lines };
}
async function extractInvoice(buffer, context) {
  if (buffer.subarray(0, 5).toString() !== "%PDF-") throw new Error("The uploaded file is not a PDF.");
  const { PDFParse } = require("pdf-parse");
  const parser = new PDFParse({ data: new Uint8Array(buffer), isEvalSupported: false });
  try {
    const result = await parser.getText();
    return { textPreview: result.text.slice(0, 20000), candidates: extractCandidates(result.text, context) };
  } finally { await parser.destroy(); }
}
module.exports = { extractInvoice, extractCandidates };
