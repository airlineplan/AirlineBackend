const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const net = require("node:net");
const mongoose = require("mongoose");

const dashboardController = require("../controller/dashboardController");
const pooController = require("../controller/pooController");
const CostConfig = require("../model/costConfigSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const PooTable = require("../model/pooTable");

const USER_ID = new mongoose.Types.ObjectId().toString();

let mongodProcess;
let dbPath;
let port;
let dbName;

function resolveMongodBinary() {
  return fs.existsSync("/usr/bin/mongod") ? "/usr/bin/mongod" : "mongod";
}

function utcDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day));
}

function createMockResponse() {
  return {
    statusCode: null,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

function waitForPortOpen(targetPort, host = "127.0.0.1", timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const check = () => {
      const socket = net.createConnection({ port: targetPort, host }, () => {
        socket.end();
        resolve();
      });
      socket.on("error", () => {
        socket.destroy();
        if (Date.now() - startedAt > timeoutMs) return reject(new Error("MongoDB did not start"));
        setTimeout(check, 200);
      });
    };
    check();
  });
}

async function connectMongo() {
  dbName = `airline_dashboard_test_${Date.now()}`;
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-dashboard-mongo-"));
  await new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.unref();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      port = probe.address().port;
      probe.close(resolve);
    });
  });
  mongodProcess = spawn(resolveMongodBinary(), [
    "--dbpath", dbPath,
    "--port", String(port),
    "--bind_ip", "127.0.0.1",
    "--nounixsocket",
    "--quiet",
  ], { stdio: ["ignore", "ignore", "ignore"] });
  await waitForPortOpen(port);
  await mongoose.connect(`mongodb://127.0.0.1:${port}/${dbName}`, { serverSelectionTimeoutMS: 5000 });
}

async function resetDatabase() {
  if (mongoose.connection?.db) await mongoose.connection.db.dropDatabase();
}

before(async () => {
  await connectMongo();
});

after(async () => {
  await Promise.race([
    mongoose.connection.close(true),
    new Promise((resolve) => setTimeout(resolve, 2000)),
  ]);
  if (mongodProcess) {
    await new Promise((resolve) => {
      mongodProcess.once("exit", resolve);
      mongodProcess.kill("SIGKILL");
      mongodProcess.unref();
      setTimeout(resolve, 2000);
    });
    mongodProcess = null;
  }
  if (dbPath) fs.rmSync(dbPath, { recursive: true, force: true });
  setImmediate(() => process.exit(process.exitCode || 0));
});

beforeEach(resetDatabase);

async function seedDashboardFixture() {
  const [flightA100, flightA101] = await Flight.create([
    {
      userId: USER_ID,
      date: utcDate(2026, 3, 4),
      sector: "DEL-BOM",
      depStn: "DEL",
      arrStn: "BOM",
      flight: "A100",
      variant: "A320",
      acftType: "A320",
      aircraft: { registration: "VT-A100", msn: 100 },
      domIntl: "dom",
      seats: 180,
      pax: 153,
      CargoCapT: 1.2,
      CargoT: 0.6,
      bh: 2.5,
      fh: 2.3,
      dist: 1200,
      paxLF: 85,
      userTag1: "Label A",
      userTag2: "Group 1",
    },
    {
      userId: USER_ID,
      date: utcDate(2026, 4, 5),
      sector: "DEL-DXB",
      depStn: "DEL",
      arrStn: "DXB",
      flight: "A101",
      variant: "A320",
      acftType: "A320",
      aircraft: { registration: "VT-A101", msn: 101 },
      domIntl: "intl",
      seats: 180,
      pax: 160,
      CargoCapT: 1.4,
      CargoT: 0.7,
      bh: 3.5,
      fh: 3.2,
      dist: 2200,
      userTag1: "Label B",
      userTag2: "Group 1",
    },
  ]);

  await Promise.all([
    Fleet.create([
      { userId: USER_ID, category: "Aircraft", type: "A320", variant: "A320", sn: "100", regn: "VT-A100", mtow: 77000, entry: utcDate(2026, 1, 1) },
      { userId: USER_ID, category: "Aircraft", type: "A320", variant: "A320", sn: "101", regn: "VT-A101", mtow: 77000, entry: utcDate(2026, 1, 1) },
    ]),
    PooTable.create([
      {
        userId: USER_ID,
        sNo: 1,
        rowKey: "row-1",
        flightId: String(flightA100._id),
        trafficType: "leg",
        source: "system",
        depStn: "DEL",
        arrStn: "BOM",
        sector: "DEL-BOM",
        flightNumber: "A100",
        variant: "A320",
        userTag1: "Label A",
        userTag2: "Group 1",
        date: utcDate(2026, 3, 4),
        odDI: "Dom",
        legDI: "Dom",
        fnlRccyPaxRev: 144000,
        fnlRccyCargoRev: 10,
        fnlRccyTotalRev: 144010,
        odTotalRev: 144010,
        pooCcy: "INR",
      },
      {
        userId: USER_ID,
        sNo: 2,
        rowKey: "row-2",
        flightId: String(flightA101._id),
        trafficType: "leg",
        source: "system",
        depStn: "DEL",
        arrStn: "DXB",
        sector: "DEL-DXB",
        flightNumber: "A101",
        variant: "A320",
        userTag1: "Label B",
        userTag2: "Group 1",
        date: utcDate(2026, 4, 5),
        odDI: "Intl",
        legDI: "Intl",
        fnlRccyPaxRev: 83000,
        fnlRccyCargoRev: 0,
        fnlRccyTotalRev: 83000,
        odTotalRev: 1000,
        pooCcy: "USD",
      },
    ]),
    RevenueConfig.create({
      userId: USER_ID,
      reportingCurrency: "INR",
      currencyCodes: ["INR", "USD", "EUR"],
      fxRates: [{ pair: "USD/INR", dateKey: "2026-04-01", rate: 83 }],
    }),
    CostConfig.create({
      userId: USER_ID,
      allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
      fuelConsum: [
        { sectorOrGcd: "DEL-BOM", acftRegn: "VT-A100", month: "03/26", fuelConsumptionKg: 1000 },
        { sectorOrGcd: "DEL-DXB", acftRegn: "VT-A101", month: "04/26", fuelConsumptionKg: 1200 },
      ],
      fuelConsumIndex: [
        { acftRegn: "VT-A100", month: "03/26", fuelConsumptionIndex: 1 },
        { acftRegn: "VT-A101", month: "04/26", fuelConsumptionIndex: 1 },
      ],
      ccyFuel: [
        { station: "DEL", month: "03/26", kgPerLtr: 1, intoPlaneRate: 1, ccy: "INR", costRCCY: 50000 },
        { station: "DEL", month: "04/26", kgPerLtr: 1, intoPlaneRate: 1, ccy: "USD", costRCCY: 80000 },
      ],
      apuUsage: [
        { stn: "BOM", acftRegn: "VT-A100", apuHours: 1, consumptionPerApuHour: 50, ccy: "INR", costRCCY: 5000 },
        { stn: "DXB", acftRegn: "VT-A101", apuHours: 1, consumptionPerApuHour: 70, ccy: "USD", costRCCY: 7000 },
      ],
      leasedReserve: [
        { acftRegn: "VT-A100", sn: "100", setRate: 1, driver: "FH", asOnDate: "2026-01-01", ccy: "INR", costRCCY: 10000 },
        { acftRegn: "VT-A101", sn: "101", setRate: 1, driver: "FH", asOnDate: "2026-01-01", ccy: "USD", costRCCY: 15000 },
      ],
      navMtowTiers: [77000],
      navEnr: [
        { sector: "DEL-BOM", ccy: "INR", costRCCY: 3000, 77000: 1 },
        { sector: "DEL-DXB", ccy: "USD", costRCCY: 6000, 77000: 1 },
      ],
      airportLanding: [
        { arrStn: "BOM", variant: "A320", mtow: 77000, ccy: "INR", costRCCY: 7000, cost: 1 },
        { arrStn: "DXB", variant: "A320", mtow: 77000, ccy: "USD", costRCCY: 12000, cost: 1 },
      ],
      otherDoc: [
        { sector: "DEL-BOM", variantOrAcftRegn: "A320", per: "Departure", ccy: "INR", costRCCY: 2000, cost: 1 },
        { sector: "DEL-DXB", variantOrAcftRegn: "A320", per: "Departure", ccy: "USD", costRCCY: 3000, cost: 1 },
      ],
    }),
  ]);
}

async function getDashboard(query = {}) {
  const res = createMockResponse();
  await dashboardController.getDashboardData({
    user: { id: USER_ID },
    query: { label: "both", periodicity: "monthly", ...query },
  }, res);
  assert.equal(res.statusCode, 200);
  return res.body;
}

test("monthly dashboard combines operational, POO revenue, costs, filters, and risk exposure", async () => {
  await seedDashboardFixture();
  const body = await getDashboard();
  assert.deepEqual(body.periods.map((period) => period.dateLabel), ["31 Mar 26", "30 Apr 26"]);

  const march = body.periods[0].data;
  assert.equal(march.departures, 1);
  assert.equal(march.seats, 180);
  assert.equal(march.pax, 153);
  assert.equal(march.paxLF, 85);
  assert.equal(march.fnlRccyPaxRev, 144000);
  assert.equal(march.fnlRccyCargoRev, 10);
  assert.equal(march.fnlRccyTotalRev, 144010);
  assert.equal(march.totalFuelCostRCCY, 55000);
  assert.equal(march.totalDocRCCY, 77000);
  assert.equal(march.grossProfitLossRCCY, 67010);

  const april = body.periods[1].data;
  assert.equal(april.departures, 1);
  assert.equal(april.pax, 160);
  assert.equal(april.fnlRccyTotalRev, 83000);
  assert.ok(body.riskExposure.fuel[0].totalFuelKg > 0);
  assert.equal(body.riskExposure.currencies.USD[0].revenue, 1000);
  assert.ok(body.riskExposure.currencies.USD[0].cost < 0);
});

test("dashboard filters by tag, flight, and label without crashing on no data", async () => {
  await seedDashboardFixture();
  const tag = await getDashboard({ userTag1: "Label A" });
  assert.equal(tag.periods[0].data.departures, 1);
  assert.equal(tag.periods[1].data.departures, 0);

  const flight = await getDashboard({ flight: "A101" });
  assert.equal(flight.periods[0].data.departures, 0);
  assert.equal(flight.periods[1].data.departures, 1);

  const dom = await getDashboard({ label: "dom" });
  assert.equal(dom.periods[0].data.fnlRccyTotalRev, 144010);
  assert.equal(dom.periods[1].data.fnlRccyTotalRev, 0);

  const none = await getDashboard({ flight: "NOPE" });
  assert.equal(none.periods[0].data.departures, 0);
  assert.equal(none.periods[0].data.fnlRccyTotalRev, 0);
});

test("reporting currency reset regenerates pairs with master dates", async () => {
  await seedDashboardFixture();
  const res = createMockResponse();
  await pooController.saveReportingCurrency({
    user: { id: USER_ID },
    body: { reportingCurrency: "EUR" },
  }, res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.reportingCurrency, "EUR");
  assert.deepEqual(
    res.body.data.fxRates.map((row) => `${row.dateKey}:${row.pair}:${row.rate}`).sort(),
    [
      "2026-03-04:INR/EUR:1",
      "2026-03-04:USD/EUR:1",
      "2026-04-05:INR/EUR:1",
      "2026-04-05:USD/EUR:1",
    ]
  );
});
