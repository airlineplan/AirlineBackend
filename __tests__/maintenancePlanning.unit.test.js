const assert = require("node:assert/strict");
const { test } = require("node:test");
const { validateCtdValue } = require("../utils/ctdValidation");
const { resolveCtd } = require("../services/ctdService");
const { defaultMatrix, validateMatrix, lookupSeverityRate } = require("../utils/severityMatrix");
const { buildOwnershipCache, resolveFromCache } = require("../services/utilisationOwnership");
const { parseActualRows, parseTime } = require("../utils/actualsFormat");
const { extractCandidates, extractInvoice } = require("../utils/invoiceExtractor");
const { getEscalationFactor, generateMaintenanceReserveScheduleWithContributions, computeCategoryFlightCostsBatch } = require("../utils/costLogic");
const matrix = { columns: [{ condition: "Normal", thrust: 22, derate: 0 }, { condition: "25% Harsh", thrust: 24, derate: 10 }],
  rows: [{ hc: 1.2, rates: [278.25, 284.17] }, { hc: 1.3, rates: [277.68, 282.84] }, { hc: 1.4, rates: [276.45, 282.25] }, { hc: 1.5, rates: [275.28, 281.47] }, { hc: 2.2, rates: [273.02, 280.34] }] };
const ctd = { condition: "Normal", thrust: 22, derate: 0 };
test("CTD validates required text, integer thrust and percentage limits", () => {
  assert.equal(validateCtdValue("condition", " 25% Harsh "), "25% Harsh");
  for (const value of [501, -1, 22.5, "", null]) assert.throws(() => validateCtdValue("thrust", value));
  for (const value of [101, -1, "", null]) assert.throws(() => validateCtdValue("derate", value));
  assert.throws(() => validateCtdValue("condition", ""));
});
test("CTD resolves saved change points and blanks outside Fleet dates", () => {
  const points = [{ metric: "thrust", effectiveDate: "2026-10-10", value: 24 }, { metric: "thrust", effectiveDate: "2026-12-01", value: 26 }];
  assert.equal(resolveCtd(points, "2026-10-09", "2026-10-01").thrust, 0);
  assert.equal(resolveCtd(points, "2026-11-01", "2026-10-01").thrust, 24);
  assert.equal(resolveCtd(points, "2026-12-01", "2026-10-01").thrust, 26);
  assert.equal(resolveCtd(points, "2026-09-30", "2026-10-01"), null);
});
test("Severity Matrix applies the workbook exact, average, edge and CTD examples", () => {
  assert.equal(lookupSeverityRate(matrix, ctd, 260, 200).baseRate, 277.68);
  assert.equal(lookupSeverityRate(matrix, ctd, 270, 200).baseRate, 277.07);
  assert.equal(lookupSeverityRate(matrix, ctd, 110, 100).baseRate, 278.25);
  assert.equal(lookupSeverityRate(matrix, ctd, 250, 100).baseRate, 273.02);
  assert.equal(lookupSeverityRate(matrix, { condition: " 25% HARSH ", thrust: 24, derate: 10 }, 150, 100).baseRate, 281.47);
  assert.equal(lookupSeverityRate(matrix, { ...ctd, thrust: 23 }, 130, 100).baseRate, 0);
  assert.equal(lookupSeverityRate(matrix, ctd, 0, 0).baseRate, 0);
});
test("matrix validation rejects duplicates, missing rates and invalid precision", () => {
  assert.deepEqual(validateMatrix(defaultMatrix()), defaultMatrix());
  assert.throws(() => validateMatrix({ ...matrix, columns: [...matrix.columns, { ...matrix.columns[0], condition: " normal " }] }), /Duplicate/);
  assert.throws(() => validateMatrix({ ...matrix, rows: [{ hc: 1, rates: [1, 2] }] }));
  assert.throws(() => validateMatrix({ ...matrix, rows: [{ hc: 1, rates: ["", 2] }, { hc: 2, rates: [1, 2] }] }));
  assert.throws(() => validateMatrix({ ...matrix, rows: [{ hc: 1.111, rates: [1, 2] }, { hc: 2, rates: [1, 2] }] }));
});
test("escalation factor preserves annual anniversary logic", () => {
  assert.equal(getEscalationFactor({ asOnDate: "2025-11-01", anniversaryDate: "2026-10-15", annualEscalation: 3 }, "2026-11-01"), 1.03);
});
test("nested rotable ownership switches the day after a blank-position movement", () => {
  const cache = buildOwnershipCache({ fleet: [{ category: "Aircraft", sn: "1234" }], resets: [
    { snBn: "123", msnEsn: "1234", date: "2026-01-01" }, { snBn: "789", msnEsn: "123", date: "2026-01-01" }, { snBn: "567", msnEsn: "", date: "2026-01-01" },
  ], movements: [{ date: "2027-01-01", msn: "123", pn: "PQR", position: "", removedSN: "789", installedSN: "567" }] });
  assert.deepEqual(resolveFromCache({ sn: "789", date: "2027-01-01", cache }).chain, ["789", "123", "1234"]);
  assert.equal(resolveFromCache({ sn: "567", date: "2027-01-01", cache }).rootMsn, null);
  assert.equal(resolveFromCache({ sn: "789", date: "2027-01-02", cache }).rootMsn, null);
  assert.equal(resolveFromCache({ sn: "567", date: "2027-01-02", cache }).rootMsn, "1234");
});
test("actuals parser accepts workbook headers, serial dates, fractions and bad-row reasons", () => {
  const parsed = parseActualRows([{ " Date ": "16-Apr-26", "Flight #.": "a103", "Dep Stn": "CCU", "ATD(LT).": "07:30", BT: "02:50", FT: "2:35", "ATA(LT)": "10:20", "Arr Stn.": "BOM", ACFT: "vt-abc", "Seats.": 180 }]);
  assert.equal(parsed[0].date.toISOString().slice(0, 10), "2026-04-16");
  assert.equal(parsed[0].ft, "02:35"); assert.equal(parsed[0].acft, "VT-ABC"); assert.deepEqual(parsed[0].reasons, []);
  assert.equal(parseTime(0.5), "12:00"); assert.equal(parseTime("24:01"), null);
  assert.ok(parseActualRows([{ Date: "bad" }])[0].reasons.length);
});
test("invoice text extraction suggests values locally and supports empty text", () => {
  const candidates = extractCandidates("October 2026\nVT-ABC MSN 1234\nEngine PR ESN 994704 rate USD 300 utilisation 280 FH amount USD 84,000", {
    fleet: [{ category: "Aircraft", regn: "VT-ABC", sn: "1234" }], settings: [{ schMxEvent: "Engine PR" }],
  });
  assert.deepEqual(candidates.months, [{ month: 10, year: 2026 }]);
  assert.equal(candidates.aircraft[0].msn, "1234");
  const line = candidates.lines.find(row => row.sn === "994704");
  assert.equal(line.rate, 300); assert.equal(line.utilisation, 280); assert.equal(line.amount, 84000);
  assert.deepEqual(extractCandidates(""), { months: [], aircraft: [], lines: [] });
});

// Exercise the actual PDF parser as well as text heuristics. These are synthetic
// parser fixtures; lessor-specific extraction acceptance still needs real PDFs.
function textPdf(lines) {
  const stream = lines.length ? `BT /F1 12 Tf 50 750 Td ${lines.map((line, index) => `${index ? '0 -20 Td ' : ''}(${line.replace(/[\\()]/g, '\\$&')}) Tj`).join('\n')} ET` : 'q Q';
  const objects = ['<< /Type /Catalog /Pages 2 0 R >>', '<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
    '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>',
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>', `<< /Length ${Buffer.byteLength(stream)} >>\nstream\n${stream}\nendstream`];
  let text = '%PDF-1.4\n';
  const offsets = [0];
  objects.forEach((object, index) => { offsets.push(Buffer.byteLength(text)); text += `${index + 1} 0 obj\n${object}\nendobj\n`; });
  const xref = Buffer.byteLength(text);
  text += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n${offsets.slice(1).map(offset => `${String(offset).padStart(10, '0')} 00000 n \n`).join('')}trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return Buffer.from(text);
}
test("PDF extraction reads digital invoices and leaves textless PDFs for manual confirmation", async () => {
  const context = { fleet: [{ category: "Aircraft", regn: "VT-ABC", sn: "1234" }], settings: [{ schMxEvent: "Engine PR" }] };
  for (const period of ["Oct-26", "October 2026", "10/2026"]) {
    const extracted = await extractInvoice(textPdf([period, "VT-ABC MSN 1234", "Engine PR ESN 994704 rate USD 300 utilisation 280 FH amount USD 84,000"]), context);
    assert.ok(extracted.candidates.months.some(row => row.month === 10 && row.year === 2026));
    assert.equal(extracted.candidates.aircraft[0].regn, "VT-ABC");
    const line = extracted.candidates.lines.find(row => row.sn === "994704");
    assert.equal(line.rate, 300); assert.equal(line.utilisation, 280); assert.equal(line.amount, 84000);
  }
  assert.deepEqual((await extractInvoice(textPdf([]), context)).candidates, { months: [], aircraft: [], lines: [] });
  await assert.rejects(extractInvoice(Buffer.from("not a PDF"), context), /not a PDF/);
});
test("category reporting never recomputes stored Actual/Invoiced costs", () => {
  const rows = [{ category: "Actual", engineFuelCost: 77 }, { category: "Invoiced", maintenanceReserveContribution: 90 }];
  assert.deepEqual(computeCategoryFlightCostsBatch(rows, {}), rows);
});
test("invoice-locked schedule retains invoice amounts while generated values refresh", () => {
  const setting = { mrAccId: "ENG1", acftRegn: "VT-ABC", asOnDate: "2026-10-01", endDate: "2026-10-31", driver: "FH", setRate: 200 };
  const existing = [{ date: "2026-11-01", mrAccId: "ENG1", transactionType: "Monthly Contribution", source: "generated", acftRegn: "VT-ABC", invoiceLocked: true, rate: 300, driverValue: 280, contribution: 84150, invoiceId: "invoice" }];
  const flights = [{ date: "2026-10-03", aircraft: { registration: "VT-ABC" }, fh: 275 }];
  const rows = generateMaintenanceReserveScheduleWithContributions([setting], existing, flights);
  const row = rows.find(item => item.date === "2026-11-01");
  assert.equal(row.rate, 300); assert.equal(row.contribution, 84150);
  assert.equal(row.generatedRate, 200); assert.equal(row.generatedDriverValue, 275); assert.equal(row.generatedContribution, 55000);
  assert.equal(rows.filter(item => item.date === "2026-11-01").length, 1);
});
