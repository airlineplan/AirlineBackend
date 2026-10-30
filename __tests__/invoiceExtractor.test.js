const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const { extractInvoice, extractCandidates } = require("../utils/invoiceExtractor");
const expected = require("./fixtures/maintenance-reserve/sample-invoice.expected.json");
const sample = path.join(__dirname, "fixtures/maintenance-reserve/sample-invoice.pdf");
const settings = expected.lines.map((line, index) => ({ mrAccId: `SAMPLE-${index + 1}`, acftRegn: expected.aircraft.regn,
  schMxEvent: line.schMxEvent, sn: line.sn || expected.aircraft.msn }));

test("the supplied invoice PDF extracts September, aircraft, all eight lines and the exact total", async () => {
  const { candidates } = await extractInvoice(fs.readFileSync(sample));
  assert.deepEqual(candidates.months, [{ month: expected.month, year: expected.year }]);
  assert.deepEqual(candidates.aircraft, [expected.aircraft]);
  assert.deepEqual(candidates.total, expected.total);
  assert.equal(candidates.lines.length, 8);
  assert.deepEqual(candidates.lines.map(({ description, ...values }) => values), expected.lines);
  assert.equal(Math.round(candidates.lines.reduce((sum, line) => sum + line.amount, 0) * 100) / 100, expected.total.amount);
});

test("sample sub accounts match MR settings by aircraft, event and engine/APU serial", async () => {
  const { candidates } = await extractInvoice(fs.readFileSync(sample), {
    fleet: [{ category: "Aircraft", sn: expected.aircraft.msn, regn: expected.aircraft.regn }],
    settings: [{ ...settings[0], acftRegn: "VT-OTHER", mrAccId: "OTHER" }, ...settings],
  });
  assert.deepEqual(candidates.lines.map(line => line.mrAccId), settings.map(row => row.mrAccId));
  assert.equal(candidates.lines[3].sn, "749014");
  assert.equal(candidates.lines[5].sn, "749015");
  assert.equal(candidates.lines[7].sn, "P-4275");
});

test("table parsing handles whitespace, client event aliases and ambiguous MR accounts", () => {
  const text = "Date: 01-Oct-26\nAircraft MSN: 4902\nAircraft Regn: VT-ZYX\nMaintenance reserve contribution for the month Sep 2026\nSub account\tRate (EUR)\tUtilisation\tAmount (EUR)\nESN 749014 PR\t189.26\t160.91 FH\t30,453.83\nAPUN P-4275 PR 40.83 145 APU hours 5,920.35\nTotal 36,374.18\nDue date: 10-Oct-26";
  const aliases = [{ ...settings[3], schMxEvent: "Engine Performance Restoration" }, { ...settings[7], schMxEvent: "APU Performance Restoration" }];
  const result = extractCandidates(text, { settings: aliases });
  assert.deepEqual(result.months, [{ month: 9, year: 2026 }]);
  assert.equal(result.lines[0].schMxEvent, "Engine Performance Restoration");
  assert.equal(result.lines[0].mrAccId, settings[3].mrAccId);
  assert.equal(result.lines[1].utilisationMetric, "APUHR");
  assert.equal(result.lines[1].currency, "EUR");
  assert.equal(result.total.amount, 36374.18);
  const ambiguous = extractCandidates(text, { settings: [...aliases, { ...aliases[0], mrAccId: "DUPLICATE" }] });
  assert.equal(ambiguous.lines[0].mrAccId, undefined);
  assert.equal(ambiguous.lines[1].mrAccId, settings[7].mrAccId);
});
