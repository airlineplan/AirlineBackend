const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const mongoose = require("mongoose");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const Onwing = require("../model/aircraftOnwing");
const Assignment = require("../model/assignment");
const CostConfig = require("../model/costConfigSchema");
const Setting = require("../model/engineOperatingSettingSchema");
const Draft = require("../model/invoiceDraft");
const Invoice = require("../model/maintenanceReserveInvoice");
const Reset = require("../model/maintenanceReset");
const Utilisation = require("../model/utilisation");
const Movement = require("../model/rotableMovementSchema");
const GroundDay = require("../model/groundDay");
const { importActuals } = require("../services/actualsService");
const { refreshMaintenanceReserves, recalculateActualCosts } = require("../services/maintenanceReserveRecalculation");
const { applyInvoice } = require("../services/invoiceService");
const { resolveUtilisationOwner, validateOwnershipHistory } = require("../services/utilisationOwnership");
const { revalidateAssignmentsForUser, purgeStaleAssignmentsForUser } = require("../utils/assignmentSync");
const ctd = require("../controller/engineOperatingController");
const maintenance = require("../controller/maintenanceController");
const fleetController = require("../controller/fleetController");
const dataController = require("../controller/dataController");
const flightController = require("../controller/flightController");
const dashboardController = require("../controller/dashboardController");
const costController = require("../controller/costController");
const matrixController = require("../controller/severityMatrixController");
const invoiceController = require("../controller/invoiceController");
const Matrix = require("../model/severityMatrixSchema");
const { startMongoTestServer } = require("./helpers/mongoTestServer");
const USER = "65f000000000000000000001", OTHER = "65f000000000000000000002";
const date = day => new Date(Date.UTC(2026, 9, day));
const aircraft = { msn: 1234, registration: "VT-ABC" };
const response = () => ({ statusCode: 200, body: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } });
let close;
before(async () => { close = await startMongoTestServer({ replicaSet: true }); });
after(async () => { if (close) await close(); });
beforeEach(async () => { await Promise.all(Object.values(mongoose.connection.collections).map(collection => collection.deleteMany({}))); });
async function seedFleet() {
  await Fleet.create([{ userId: USER, category: "Aircraft", type: "A320", variant: "A320", sn: "1234", regn: "VT-ABC", entry: date(1), exit: date(92) },
    { userId: USER, category: "Engine", type: "CFM56", variant: "5B", sn: "994704", titled: "VT-ABC #1", entry: date(1), exit: date(92) }]);
  await Onwing.create({ userId: USER, msn: "1234", date: date(1), pos1Esn: "994704" });
}
async function seedConfig() {
  await CostConfig.create({ userId: USER, reportingCurrency: "USD", leasedReserve: [{ mrAccId: "ENG1", schMxEvent: "Engine PR", acftRegn: "VT-ABC", sn: "994704", setBalance: 0,
    setRate: 200, setRateType: "VALUE", asOnDate: "2026-10-01", endDate: "2026-12-31", ccy: "USD", driver: "FH" }] });
}
const raw = (day, overrides = {}) => ({ Date: `2026-10-${String(day).padStart(2, "0")}`, "Flight #.": "A103", "Dep Stn": "CCU", "ATD(LT).": "07:30", BT: "02:50", FT: "02:35", "ATA(LT)": "10:20", "Arr Stn.": "BOM", ACFT: "VT-ABC", "Seats.": 180, "Cargo Cap": 1.6, Pax: 171, "Cargo T": 0.64, ...overrides });

test("actuals import creates twins, protects Planned data, replaces only its aircraft/date and survives revalidation", async () => {
  await seedFleet(); await seedConfig();
  await Flight.create({ userId: USER, date: date(3), flight: "A103", aircraft, bh: 9, fh: 8, isComplete: true });
  await Assignment.create([{ userId: USER, date: date(3), flightNumber: "OLD", aircraft, isValid: true },
    { userId: USER, date: date(3), flightNumber: "OTHER", aircraft: { msn: 5555, registration: "VT-OTHER" }, isValid: true }]);
  const result = await importActuals({ userId: USER, rawRows: [raw(3)] });
  assert.equal(result.transactional, true); assert.equal(result.inserted, 2);
  const actual = await Flight.findOne({ userId: USER, category: "Actual" }).lean();
  const invoiced = await Flight.findOne({ userId: USER, category: "Invoiced" }).lean();
  assert.equal(actual.pairId, invoiced.pairId); assert.equal(actual.networkId, undefined);
  assert.equal(actual.std, "07:30"); assert.equal(actual.sta, "10:20"); assert.equal(actual.bh, 170 / 60); assert.equal(actual.fh, 155 / 60);
  assert.equal(actual.maintenanceReserveContribution, 516.67); assert.equal(invoiced.maintenanceReserveContribution, 0);
  assert.equal((await Flight.findOne({ userId: USER, category: "Planned" })).bh, 9);
  assert.ok(await Assignment.findOne({ userId: USER, flightNumber: "OTHER" }));
  assert.equal(await Assignment.countDocuments({ userId: USER, flightNumber: "OLD" }), 0);
  await revalidateAssignmentsForUser({ userId: USER }); await purgeStaleAssignmentsForUser({ userId: USER });
  assert.ok(await Assignment.findOne({ userId: USER, flightNumber: "A103", source: "ACTUALS" }));
  await importActuals({ userId: USER, rawRows: [raw(3, { FT: "02:00" })] });
  assert.equal(await Flight.countDocuments({ userId: USER, category: { $in: ["Actual", "Invoiced"] } }), 2);
  assert.equal((await Flight.findOne({ userId: USER, category: "Actual" })).fh, 2);
});

test("one malformed row preserves its whole aircraft/date group and imports other groups", async () => {
  await seedFleet();
  await importActuals({ userId: USER, rawRows: [raw(3)] });
  const old = await Flight.findOne({ userId: USER, category: "Actual" }).lean();
  const result = await importActuals({ userId: USER, rawRows: [raw(3), raw(3, { "Flight #.": "BAD", BT: "broken" }), raw(4)] });
  assert.equal(result.inserted, 2); assert.equal(result.rejected.length, 2);
  assert.ok(await Flight.findOne({ _id: old._id, category: "Actual" }));
  assert.equal(await Flight.countDocuments({ userId: USER, category: "Actual" }), 2);
});

test("an interrupted replacement rolls back twins and assignments together", async t => {
  await seedFleet();
  await importActuals({ userId: USER, rawRows: [raw(3)] });
  const previous = await Flight.find({ userId: USER, category: { $in: ["Actual", "Invoiced"] } }).sort({ category: 1 }).lean();
  const previousAssignments = await Assignment.find({ userId: USER }).lean();
  t.mock.method(Assignment, "insertMany", async () => { throw new Error("Simulated interrupted assignment write"); });
  await assert.rejects(importActuals({ userId: USER, rawRows: [raw(3, { FT: "01:00" })] }), /Simulated interrupted/);
  assert.deepEqual(await Flight.find({ userId: USER, category: { $in: ["Actual", "Invoiced"] } }).sort({ category: 1 }).lean(), previous);
  assert.deepEqual(await Assignment.find({ userId: USER }).lean(), previousAssignments);
});

test("Actual assignments override ground days in Fleet metrics", async () => {
  await seedFleet(); await Flight.create({ userId: USER, date: date(3), flight: "A103", aircraft, bh: 9 });
  await GroundDay.create({ userId: USER, date: date(3), msn: "1234", event: "Check" });
  await importActuals({ userId: USER, rawRows: [raw(3)] });
  const res = response(); await fleetController.getFleetScheduleMetrics({ user: { id: USER }, query: { month: "October 2026" } }, res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data["Aircraft:1234"]["03 Oct 26"].status, "aircraft-assigned");
  assert.equal(res.body.data["Aircraft:1234"]["03 Oct 26"].bh, 170 / 60);
});

test("CTD API persists fill-forward changes, rejects bad data and scopes reads by user", async () => {
  await seedFleet();
  await Flight.create([{ userId: USER, date: date(1) }, { userId: USER, date: date(92) }]);
  const put = response(); await ctd.put({ user: { id: USER }, body: { changes: [
    { esn: "994704", metric: "thrust", date: "2026-10-10", value: 24 }, { esn: "994704", metric: "thrust", date: "2026-12-01", value: 26 },
  ] } }, put);
  assert.equal(put.statusCode, 200);
  const get = response(); await ctd.get({ user: { id: USER }, query: { from: "2026-10-09", to: "2026-12-02" } }, get);
  assert.equal(get.body.rows[0].values.thrust["2026-10-09"], 0);
  assert.equal(get.body.rows[0].values.thrust["2026-11-30"], 24);
  assert.equal(get.body.rows[0].values.thrust["2026-12-01"], 26);
  const overwrite = response(); await ctd.put({ user: { id: USER }, body: { changes: [
    { esn: "994704", metric: "thrust", date: "2026-11-01", value: 23 },
  ] } }, overwrite);
  assert.equal(overwrite.statusCode, 200);
  const updated = response(); await ctd.get({ user: { id: USER }, query: { from: "2026-10-09", to: "2026-12-02" } }, updated);
  assert.equal(updated.body.rows[0].values.thrust["2026-10-31"], 24);
  assert.equal(updated.body.rows[0].values.thrust["2026-11-01"], 23);
  assert.equal(updated.body.rows[0].values.thrust["2026-12-01"], 23);
  assert.equal(await Setting.countDocuments({ userId: USER, effectiveDate: { $gte: new Date("2026-12-01") } }), 0);
  const bad = response(); await ctd.put({ user: { id: USER }, body: { changes: [{ esn: "994704", metric: "thrust", date: "2026-10-10", value: 501 }] } }, bad);
  assert.equal(bad.statusCode, 400); assert.equal(await Setting.countDocuments({ userId: USER }), 2);
  const other = response(); await ctd.get({ user: { id: OTHER }, query: {} }, other); assert.deepEqual(other.body.rows, []);
});

test("FLGTs, View, List, Cost, Dashboard and Excel isolate categories and preserve frozen costs", async () => {
  const { Writable } = require("node:stream");
  const xlsx = require("xlsx");
  await seedFleet();
  for (const [category, fh, cost] of [["Planned", 1, 0], ["Actual", 2, 77], ["Invoiced", 3, 0]]) {
    await Flight.create({ userId: USER, date: date(3), flight: "A103", aircraft, category, fh, bh: fh, depStn: "CCU", arrStn: "BOM", sector: "CCU-BOM", domIntl: "dom", isComplete: true, engineFuelCostRCCY: cost });
  }
  await CostConfig.create({ userId: USER, reportingCurrency: "USD", rotableChanges: [{ date: "2026-10-03", acftRegn: "VT-ABC", cost: 999, ccy: "USD" }] });
  for (const [category, expectedFh] of [["Planned", 1], ["Actual", 2], ["Invoiced", 3]]) {
    const search = response(); await flightController.searchFlights({ user: { id: USER }, body: { category, page: 1, limit: 20 } }, search);
    assert.equal(search.statusCode, 200); assert.equal(search.body.total, 1); assert.equal(search.body.data[0].category, category);
    const list = response(); await dataController.getListPageData({ user: { id: USER }, body: { category } }, list);
    assert.equal(list.body.flights.length, 1); assert.equal(list.body.flights[0].fh, expectedFh);
    const view = response(); await dataController.getViewData({ user: { id: USER }, query: { category, mode: "Sectors", weekStart: "2026-10-01" } }, view);
    assert.equal(view.body.rows.flatMap(row => row.flights).length, 1); assert.equal(view.body.rows[0].flights[0].category, category);
    const cost = response(); await costController.getCostPageData({ user: { id: USER }, query: {}, body: { category } }, cost);
    assert.equal(cost.statusCode, 200); assert.equal(cost.body.flights[0].category, category);
    const dashboard = response(); await dashboardController.getDashboardData({ user: { id: USER }, query: { category, label: "both", periodicity: "monthly" } }, dashboard);
    assert.equal(dashboard.statusCode, 200); assert.equal(dashboard.body.periods[0].data.departures, 1); assert.equal(dashboard.body.periods[0].data.fh, expectedFh);
    if (category !== "Planned") {
      assert.equal(search.body.data[0].engineFuelCostRCCY, category === "Actual" ? 77 : 0);
      assert.equal(dashboard.body.periods[0].data.rotableChangesRCCY, 0);
    }
    const buffers = [], headers = {};
    const download = new Writable({ write(chunk, encoding, callback) { buffers.push(Buffer.from(chunk)); callback(); } });
    download.setHeader = (key, value) => { headers[key] = value; };
    download.status = code => { throw new Error(`Excel export failed: ${code}`); };
    await dataController.downloadExpenses({ user: { id: USER }, query: { category } }, download);
    const workbook = xlsx.read(Buffer.concat(buffers));
    const rows = xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
    assert.equal(rows.length, 1); assert.equal(rows[0].Category, category);
    assert.ok(headers["Content-Disposition"].includes(`FLGTs_${category}.xlsx`));
    for (const field of ["Other Maintenance 1 CCY", "Other Mx Expenses 3 RCCY", "Nav Enr RCCY"]) assert.ok(Object.hasOwn(rows[0], field), `Export missing ${field}`);
  }
});

test("Severity Matrix Save recalculates Actual-based H:C and rates after a CTD change", async () => {
  await seedFleet(); await seedConfig();
  await CostConfig.updateOne({ userId: USER }, { $set: { "leasedReserve.0.setRateType": "SM" } });
  await Flight.create([{ userId: USER, category: "Planned", date: date(3), flight: "A103", aircraft, fh: 9 },
    { userId: USER, category: "Actual", date: date(3), flight: "A103", aircraft, fh: 2, bh: 2 }]);
  const matrix = { columns: [{ condition: "Normal", thrust: 0, derate: 0 }, { condition: "Normal", thrust: 22, derate: 0 }], rows: [{ hc: 1, rates: [100, 200] }, { hc: 3, rates: [200, 400] }] };
  const saved = response(); await matrixController.put({ user: { id: USER }, params: { mrAccId: "ENG1" }, body: matrix }, saved);
  assert.equal(saved.statusCode, 200);
  assert.equal(saved.body.schedule.find(row => row.date === "2026-11-01").rate, 150);
  assert.equal(saved.body.schedule.find(row => row.date === "2026-11-01").driverValue, 2);
  const changed = response(); await ctd.put({ user: { id: USER }, body: { changes: [{ esn: "994704", metric: "thrust", date: "2026-10-03", value: 22 }] } }, changed);
  assert.equal(changed.statusCode, 200);
  const config = await CostConfig.findOne({ userId: USER }).lean();
  assert.equal(config.maintenanceReserveSchedule.find(row => row.date === "2026-11-01").rate, 300);
  assert.equal((await Flight.findOne({ userId: USER, category: "Actual" })).maintenanceReserveContribution, 600);
  assert.equal(await Matrix.countDocuments({ userId: USER, mrAccId: "ENG1" }), 1);
});

test("invoice apply locks the schedule, fills Invoiced MR and retains generated/Actual amounts", async () => {
  await seedFleet(); await seedConfig();
  for (const day of [1, 2]) for (const category of ["Planned", "Actual", "Invoiced"]) await Flight.create({ userId: USER, date: date(day), flight: `F${day}`, aircraft, category, bh: 141, fh: 140 });
  await refreshMaintenanceReserves(USER); await recalculateActualCosts(USER);
  const draft = await Draft.create({ userId: USER, fileName: "synthetic-invoice.pdf", storedFileKey: "fixture.pdf" });
  const body = { draftId: draft._id, acftRegn: "VT-ABC", msn: "1234", month: 10, year: 2026,
    lines: [{ schMxEvent: "Engine PR", sn: "994704", rate: 300, utilisation: 280, contribution: 84000 }] };
  const applied = await applyInvoice({ userId: USER, body });
  assert.equal(applied.status, "Reconciled");
  const locked = applied.schedule.find(row => row.date === "2026-11-01");
  assert.equal(locked.invoiceLocked, true); assert.equal(locked.rate, 300); assert.equal(locked.contribution, 84000);
  assert.equal(locked.generatedRate, 200);
  assert.equal((await Flight.findOne({ userId: USER, category: "Actual" })).maintenanceReserveContribution, 28000);
  assert.equal((await Flight.findOne({ userId: USER, category: "Invoiced" })).maintenanceReserveContribution, 42000);
  const config = await CostConfig.findOne({ userId: USER }); config.leasedReserve[0].setRate = 250; config.markModified("leasedReserve"); await config.save();
  await refreshMaintenanceReserves(USER);
  const after = (await CostConfig.findOne({ userId: USER }).lean()).maintenanceReserveSchedule.find(row => row.date === "2026-11-01");
  assert.equal(after.rate, 300); assert.equal(after.generatedRate, 250); assert.equal(after.contribution, 84000);
  assert.equal((await Flight.findOne({ userId: USER, category: "Actual" })).maintenanceReserveContribution, 35000);
  assert.equal((await Flight.findOne({ userId: USER, category: "Invoiced" })).maintenanceReserveContribution, 42000);
  const replaced = await applyInvoice({ userId: USER, body: { ...body, lines: [{ ...body.lines[0], contribution: 84150 }] } });
  assert.equal(replaced.status, "Unreconciled"); assert.equal(replaced.checks[0].diff, 150);
  assert.equal(await Invoice.countDocuments({ userId: USER }), 1);
});

test("the supplied sample extracts through the API and applies all eight September reserve lines", async () => {
  const fs = require("node:fs");
  const path = require("node:path");
  const os = require("node:os");
  const expected = require("./fixtures/maintenance-reserve/sample-invoice.expected.json");
  const settings = expected.lines.map((line, index) => ({ mrAccId: `SAMPLE-${index + 1}`, acftRegn: expected.aircraft.regn,
    schMxEvent: line.schMxEvent, sn: line.sn || expected.aircraft.msn, driver: line.utilisationMetric, ccy: line.currency,
    setRate: 1, setRateType: "VALUE", asOnDate: "2026-09-01", endDate: "2026-09-30", setBalance: 0 }));
  await Fleet.create({ userId: USER, category: "Aircraft", type: "DEMO", sn: expected.aircraft.msn, regn: expected.aircraft.regn });
  await CostConfig.create({ userId: USER, reportingCurrency: "USD", leasedReserve: settings });
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "airline-invoice-sample-"));
  const upload = path.join(directory, "sample.pdf");
  fs.copyFileSync(path.join(__dirname, "fixtures/maintenance-reserve/sample-invoice.pdf"), upload);
  try {
    const extracted = response(); await invoiceController.extract({ user: { id: USER }, file: { path: upload, originalname: "MR invoice sample.pdf" } }, extracted);
    assert.equal(extracted.statusCode, 200);
    assert.deepEqual(extracted.body.candidates.months, [{ month: 9, year: 2026 }]);
    assert.deepEqual(extracted.body.candidates.total, expected.total);
    assert.deepEqual(extracted.body.candidates.lines.map(line => line.mrAccId), settings.map(row => row.mrAccId));
    assert.equal(await Invoice.countDocuments({ userId: USER }), 0, "Extraction must not apply an invoice before confirmation");
    const lines = extracted.body.candidates.lines.map(line => ({ ...line, contribution: line.amount, extracted: line }));
    const applied = response(); await invoiceController.apply({ user: { id: USER }, body: { draftId: extracted.body.draftId,
      acftRegn: expected.aircraft.regn, msn: expected.aircraft.msn, month: 9, year: 2026, lines } }, applied);
    assert.equal(applied.statusCode, 200);
    const locked = applied.body.schedule.filter(row => row.invoiceLocked);
    assert.equal(locked.length, 8);
    for (const [index, setting] of settings.entries()) {
      const row = locked.find(row => row.mrAccId === setting.mrAccId);
      assert.equal(row.date, "2026-10-01");
      assert.equal(row.rate, expected.lines[index].rate);
      assert.equal(row.driverValue, expected.lines[index].utilisation);
      assert.equal(row.contribution, expected.lines[index].amount);
      assert.equal(row.generatedRate, 1);
    }
    assert.equal(Math.round(locked.reduce((sum, row) => sum + row.contribution, 0) * 100) / 100, 187327.91);
    assert.equal(applied.body.status, "Unreconciled");
    assert.match(applied.body.warning, /No Invoiced flights for VT-ZYX Sep-26/);
    assert.equal(await Invoice.countDocuments({ userId: USER }), 1);
    await invoiceController.apply({ user: { id: USER }, body: { draftId: extracted.body.draftId,
      acftRegn: expected.aircraft.regn, msn: expected.aircraft.msn, month: 9, year: 2026, lines } }, response());
    assert.equal(await Invoice.countDocuments({ userId: USER }), 1);
    assert.equal((await CostConfig.findOne({ userId: USER }).lean()).maintenanceReserveSchedule.filter(row => row.invoiceLocked).length, 8);
  } finally {
    if (fs.existsSync(upload)) fs.unlinkSync(upload);
    fs.rmdirSync(directory);
  }
});

test("recursive ownership validation rejects cycles, double installation and invalid removals", async () => {
  await seedFleet();
  await Reset.create([{ userId: USER, date: date(1), msnEsn: "1234", pn: "ABC", snBn: "123" },
    { userId: USER, date: date(1), msnEsn: "123", pn: "PQR", snBn: "789" }, { userId: USER, date: date(1), msnEsn: "", pn: "PQR", snBn: "567" }]);
  const swap = { date: date(3), msn: "123", pn: "PQR", position: "", removedSN: "789", installedSN: "567" };
  await validateOwnershipHistory({ userId: USER, movements: [swap] }); await Movement.create({ ...swap, userId: USER });
  assert.equal((await resolveUtilisationOwner({ userId: USER, sn: "789", date: date(3) })).rootMsn, "1234");
  assert.equal((await resolveUtilisationOwner({ userId: USER, sn: "567", date: date(4) })).rootMsn, "1234");
  assert.equal((await resolveUtilisationOwner({ userId: USER, sn: "789", date: date(4) })).rootMsn, null);
  await assert.rejects(validateOwnershipHistory({ userId: USER, movements: [{ date: date(5), msn: "567", pn: "ABC", installedSN: "123" }] }), /parent|cycle/);
  await assert.rejects(validateOwnershipHistory({ userId: USER, movements: [{ date: date(5), msn: "9999", pn: "PQR", installedSN: "567" }] }), /already has parent/);
  await assert.rejects(validateOwnershipHistory({ userId: USER, movements: [{ date: date(5), msn: "123", pn: "PQR", removedSN: "999" }] }), /not installed/);
});

test("nested parts accrue through the hierarchy; removed parts stop and spare calendar days continue", async () => {
  await seedFleet();
  for (let day = 1; day <= 4; day += 1) {
    await Flight.create({ userId: USER, date: date(day), flight: `F${day}`, aircraft, bh: 3, fh: 2 });
    await Assignment.create({ userId: USER, date: date(day), flightNumber: `F${day}`, aircraft, metrics: { blockHours: 3, flightHours: 2, cycles: 1 }, isValid: true });
  }
  for (const [parent, pn, snBn] of [["1234", "ABC", "123"], ["123", "PQR", "789"], ["", "PQR", "567"]]) await Reset.create({ userId: USER, date: date(0), msnEsn: parent, pn, snBn, tsn: 0, csn: 0, dsn: 0, timeMetric: "BH" });
  await Movement.create({ userId: USER, date: date(2), msn: "123", pn: "PQR", removedSN: "789", installedSN: "567" });
  await maintenance.recomputeMaintenanceTimeline({ userId: USER });
  assert.equal((await Utilisation.findOne({ userId: USER, snBn: "789", date: date(4) })).tsn, 6);
  assert.equal((await Utilisation.findOne({ userId: USER, snBn: "567", date: date(4) })).tsn, 6);
  assert.equal((await Utilisation.findOne({ userId: USER, snBn: "567", date: date(4) })).dsn, 4);
  const first = await Utilisation.find({ userId: USER }).sort({ snBn: 1, date: 1 }).select("snBn date tsn csn dsn -_id").lean();
  await maintenance.recomputeMaintenanceTimeline({ userId: USER });
  assert.deepEqual(await Utilisation.find({ userId: USER }).sort({ snBn: 1, date: 1 }).select("snBn date tsn csn dsn -_id").lean(), first);
  // Force the dashboard to project from Reset instead of relying on saved utilisation.
  await Utilisation.deleteMany({ userId: USER });
  const dashboard = response(); await maintenance.getMaintenanceDashboard({ user: { id: USER }, query: { date: "2026-10-04" } }, dashboard);
  assert.equal(dashboard.statusCode, 200);
  const projected = new Map(dashboard.body.data.maintenanceData.map(row => [row.snBn, row]));
  assert.equal(projected.get("789").tsn, 6);
  assert.equal(projected.get("567").tsn, 6);
  assert.equal(projected.get("567").dsn, 4);
  const saved = response(); await fleetController.bulkUpsertFleet({ user: { id: USER }, body: { fleetData: await Fleet.find({ userId: USER }).lean() } }, saved);
  assert.equal(saved.statusCode, 200);
  assert.equal(await Reset.countDocuments({ userId: USER, snBn: { $in: ["123", "789", "567"] } }), 3);
  assert.equal(await Movement.countDocuments({ userId: USER, installedSN: "567" }), 1);
});
