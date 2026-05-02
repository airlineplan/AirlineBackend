const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const mongoose = require("mongoose");

const CostConfig = require("../model/costConfigSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const costController = require("../controller/costController");
const pooController = require("../controller/pooController");

const USER_ID = new mongoose.Types.ObjectId().toString();

let mongodProcess;
let dbPath;
let port;
let dbName;

function resolveMongodBinary() {
  return fs.existsSync("/usr/bin/mongod") ? "/usr/bin/mongod" : "mongod";
}

function utcDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
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
        if (Date.now() - startedAt > timeoutMs) {
          reject(new Error(`MongoDB did not start on ${host}:${targetPort} within ${timeoutMs}ms`));
          return;
        }
        setTimeout(check, 200);
      });
    };

    check();
  });
}

async function connectMongo() {
  dbName = `airline_cost_test_${Date.now()}`;
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-cost-mongo-"));

  await new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.unref();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      port = probe.address().port;
      probe.close(resolve);
    });
  });

  mongodProcess = spawn(
    resolveMongodBinary(),
    [
      "--dbpath",
      dbPath,
      "--port",
      String(port),
      "--bind_ip",
      "127.0.0.1",
      "--nounixsocket",
      "--quiet",
    ],
    {
      stdio: ["ignore", "ignore", "ignore"],
    }
  );

  await waitForPortOpen(port);
  await mongoose.connect(`mongodb://127.0.0.1:${port}/${dbName}`, {
    serverSelectionTimeoutMS: 5000,
  });
}

async function resetDatabase() {
  if (mongoose.connection?.db) {
    await mongoose.connection.db.dropDatabase();
  }
}

before(async () => {
  await connectMongo();
});

after(async () => {
  await mongoose.connection.close();
  if (mongodProcess) {
    await new Promise((resolve) => {
      const timeout = setTimeout(() => {
        mongodProcess.kill("SIGKILL");
      }, 5000);

      mongodProcess.once("exit", () => {
        clearTimeout(timeout);
        resolve();
      });

      mongodProcess.kill("SIGINT");
    });
  }
  if (dbPath) {
    fs.rmSync(dbPath, { recursive: true, force: true });
  }
});

beforeEach(async () => {
  await resetDatabase();
});

test("cost config controller round-trips the spreadsheet-style input sections", async () => {
  const payload = {
    allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
    fuelConsum: [
      { rowType: "sector", sectorOrGcd: "CCU-BOM", gcd: "895", month1: "APR-26" },
      { rowType: "aircraft", sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", m1: "8275", month1: "APR-26" },
    ],
    fuelConsumIndex: [{ acftRegn: "VT-ABC", month1: "APR-26", m1: "1.02" }],
    apuUsage: [{ arrStn: "BOM", variant: "A320", acftRegn: "VT-ABC", apuHours: "0.75", consumptionPerApuHour: "255" }],
    plfEffect: [
      { rowType: "header", p56: "", p80: "", p90: "", p95: "" },
      { rowType: "sector", sectorOrGcd: "CCU-BOM", gcd: "895" },
      { rowType: "aircraft", sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", p80: "0.98", p90: "1", p95: "1.02", p99: "1.03" },
    ],
    ccyFuel: [{ ccy: "INR", station: "CCU", kgPerLtr: "0.78", month1: "APR-26", m1: "92500" }],
    leasedReserve: [{ mrAccId: "MR-1", schMxEvent: "C-check", acftRegn: "VT-ABC", pn: "A320", sn: "5825", setBalance: "1000", setRate: "10", asOnDate: "2026-04-01", ccy: "INR", driver: "FH" }],
    schMxEvents: [{ date: "2026-04-20", msnEsnApun: "5825", event: "C-check", pn: "A320", snBn: "5825", cost: "500", ccy: "INR", capitalisation: "N" }],
    transitMx: [{ depStn: "CCU", variant: "A320", acftRegn: "VT-ABC", pn: "A320", sn: "5825", costPerDeparture: "20", ccy: "INR", fromDate: "2026-04-01", toDate: "2026-04-30" }],
    otherMx: [{ depStn: "CCU", acftRegn: "VT-ABC", costPerBh: "5", costPerDeparture: "3", costPerMonth: "30", ccy: "INR" }],
    rotableChanges: [{ label: "Rotable", date: "2026-04-01", pn: "PN-1", msn: "5825", acftRegn: "VT-ABC", position: "L1", removedSN: "OLD", installedSN: "NEW", cost: "40", ccy: "INR" }],
    navMtowTiers: ["73000", "77000"],
    navEnr: [{ ccy: "INR", sector: "CCU-BOM", 73000: "6", 77000: "7" }],
    navTerm: [{ ccy: "INR", arrStn: "BOM", 73000: "8", 77000: "9" }],
    airportLanding: [{ arrStn: "BOM", mtow: "77000", variant: "A320", cost: "11", ccy: "INR" }],
    airportDom: [{ arrStn: "BOM", mtow: "77000", variant: "A320", cost: "12", ccy: "INR" }],
    airportIntl: [{ arrStn: "BOM", mtow: "77000", variant: "A320", cost: "13", ccy: "INR" }],
    airportAvsec: [{ arrStn: "BOM", variant: "A320", cost: "14", ccy: "INR" }],
    airportOther: [{ arrStn: "BOM", ccy: "INR", 73000: "15", 77000: "16" }],
    otherDoc: [{ label: "Doc", sector: "CCU-BOM", depStn: "CCU", arrStn: "BOM", variantOrAcftRegn: "A320", per: "BH", cost: "2", ccy: "INR" }],
  };

  const saveRes = createMockResponse();
  await costController.saveCostConfig({ user: { id: USER_ID }, body: payload }, saveRes);

  assert.equal(saveRes.statusCode, 200);
  assert.equal(saveRes.body.success, true);

  const savedConfig = await CostConfig.findOne({ userId: USER_ID }).lean();
  assert.equal(savedConfig.plfEffect[0].rowType, "header");
  assert.ok(Object.prototype.hasOwnProperty.call(savedConfig.plfEffect[0], "p56"));
  assert.equal(savedConfig.ccyFuel[0].station, "CCU");
  assert.equal(savedConfig.ccyFuel[0].month, "04/26");
  assert.equal(savedConfig.ccyFuel[0].intoPlaneRate, 92500);

  const loadRes = createMockResponse();
  await costController.getCostConfig({ user: { id: USER_ID } }, loadRes);

  assert.equal(loadRes.statusCode, 200);
  const data = loadRes.body.data;
  [
    "allocationTable", "fuelConsum", "fuelConsumIndex", "apuUsage", "plfEffect", "ccyFuel",
    "leasedReserve", "schMxEvents", "transitMx", "otherMx", "rotableChanges",
    "navEnr", "navTerm", "airportLanding", "airportDom", "airportIntl", "airportAvsec", "airportOther", "otherDoc",
  ].forEach((key) => assert.ok(Array.isArray(data[key]), `${key} should round-trip as an array`));

  assert.deepEqual(data.navMtowTiers, [73000, 77000]);
  assert.equal(data.allocationTable[0].costCode, "APUFUELCOST");
  assert.equal(data.allocationTable[0].basis, "BH");
  assert.equal(data.apuUsage[0].stn, "BOM");
  assert.equal(data.apuUsage[0].apuHrPerDay, 0.75);
  assert.equal(data.apuUsage[0].kgPerApuHr, 255);
  assert.equal(data.leasedReserve[0].mrAccId, "MR-1");
  assert.equal(data.transitMx[0].sn, "5825");
  assert.equal(data.otherDoc[0].label, "Doc");
  assert.equal(data.otherDoc[0].variantOrAcftRegn, "A320");
  assert.equal(data.plfEffect[0].rowType, "header");
  assert.ok(Object.prototype.hasOwnProperty.call(data.plfEffect[0], "p56"));
  assert.equal(data.plfEffect.find((row) => row.acftRegn === "VT-ABC").p99, 1.03);
  assert.equal(data.ccyFuel[0].station, "CCU");
  assert.equal(data.ccyFuel[0].month1, "04/26");
  assert.equal(data.ccyFuel[0].m1, 92500);
});

test("cost config rejects additional APU usage rows without Stn", async () => {
  const res = createMockResponse();

  await costController.saveCostConfig({
    user: { id: USER_ID },
    body: {
      apuUsage: [
        {
          addlnUse: "Y",
          acftRegn: "VT-ABC",
          fromDate: "2026-04-20",
          apuHrPerDay: "1",
          kgPerApuHr: "100",
        },
      ],
    },
  }, res);

  assert.equal(res.statusCode, 400);
  assert.equal(res.body.success, false);
  assert.match(res.body.message, /Stn is required/);
  assert.equal(await CostConfig.countDocuments({ userId: USER_ID }), 0);
});

test("revenue config preserves reporting currency, entered CCYs, and FX rates", async () => {
  const payload = {
    reportingCurrency: "inr",
    currencyCodes: ["usd", "INR", "aed", "USD"],
    fxRates: [
      { pair: "USD/INR", dateKey: "2026-04-16", rate: 83.25 },
      { pair: "AED/INR", dateKey: "2026-04-16", rate: 22.68 },
    ],
  };

  const saveRes = createMockResponse();
  await pooController.saveRevenueConfig({ user: { id: USER_ID }, body: payload }, saveRes);

  assert.equal(saveRes.statusCode, 200);
  assert.equal(saveRes.body.success, true);
  assert.equal(saveRes.body.data.reportingCurrency, "INR");
  assert.deepEqual(saveRes.body.data.currencyCodes, ["INR", "USD", "AED"]);
  assert.deepEqual(saveRes.body.data.fxRates, payload.fxRates);

  const loadRes = createMockResponse();
  await pooController.getRevenueConfig({ user: { id: USER_ID } }, loadRes);

  assert.equal(loadRes.statusCode, 200);
  assert.equal(loadRes.body.data.reportingCurrency, "INR");
  assert.deepEqual(loadRes.body.data.currencyCodes, ["INR", "USD", "AED"]);
  assert.deepEqual(loadRes.body.data.fxRates, payload.fxRates);
});

test("reporting currency endpoint saves newly entered reporting CCY and resets FX pairs", async () => {
  await RevenueConfig.create({
    userId: USER_ID,
    reportingCurrency: "USD",
    currencyCodes: ["USD"],
  });

  const payload = {
    reportingCurrency: "inr",
    currencyCodes: ["usd", "inr"],
    fxRates: [
      { pair: "USD/INR", dateKey: "2026-04-16", rate: 83.25 },
    ],
  };

  const saveRes = createMockResponse();
  await pooController.saveReportingCurrency({ user: { id: USER_ID }, body: payload }, saveRes);

  assert.equal(saveRes.statusCode, 200);
  assert.equal(saveRes.body.success, true);
  assert.equal(saveRes.body.data.reportingCurrency, "INR");
  assert.deepEqual(saveRes.body.data.currencyCodes, ["INR", "USD"]);
  assert.deepEqual(saveRes.body.data.fxRates, []);

  const saved = await RevenueConfig.findOne({ userId: USER_ID }).lean();
  assert.equal(saved.reportingCurrency, "INR");
  assert.deepEqual(saved.currencyCodes, ["INR", "USD"]);
  assert.deepEqual(saved.fxRates, []);
});

test("cost page controller computes representative cost inputs into flight cost fields", async () => {
  await Promise.all([
    RevenueConfig.create({ userId: USER_ID, reportingCurrency: "INR" }),
    Fleet.create({
      userId: USER_ID,
      category: "Aircraft",
      type: "A320",
      variant: "A320",
      sn: "5825",
      regn: "VT-ABC",
      mtow: 77000,
      entry: utcDate(2026, 1, 1),
    }),
    Flight.create({
      userId: USER_ID,
      date: utcDate(2026, 4, 16),
      flight: "AI101",
      sector: "CCU-BOM",
      depStn: "CCU",
      arrStn: "BOM",
      variant: "A320",
      acftType: "A320",
      aircraft: { registration: "VT-ABC", msn: 5825 },
      bh: 2,
      fh: 1.5,
      paxLF: 95,
      domIntl: "dom",
    }),
    CostConfig.create({
      userId: USER_ID,
      allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
      fuelConsum: [{ sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 1000 }],
      fuelConsumIndex: [{ acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1 }],
      plfEffect: [{ sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", p95: 1 }],
      ccyFuel: [
        { station: "CCU", month: "04/26", kgPerLtr: 1, intoPlaneRate: 1000, ccy: "INR" },
        { station: "BOM", month: "04/26", kgPerLtr: 1, intoPlaneRate: 1000, ccy: "INR" },
      ],
      apuUsage: [
        { arrStn: "BOM", acftRegn: "VT-ABC", apuHours: 1, consumptionPerApuHour: 100, ccy: "INR" },
        { arrStn: "BOM", addlnUse: "Y", acftRegn: "VT-ABC", fromDate: "2026-04-20", apuHours: 1, consumptionPerApuHour: 50, ccy: "INR" },
      ],
      leasedReserve: [{ acftRegn: "VT-ABC", sn: "5825", setRate: 10, driver: "FH", asOnDate: "2026-01-01", ccy: "INR" }],
      transitMx: [{ depStn: "CCU", sn: "5825", costPerDeparture: 20, ccy: "INR" }],
      otherMx: [{ acftRegn: "VT-ABC", costPerBh: 5, costPerDeparture: 3, costPerMonth: 30, ccy: "INR" }],
      rotableChanges: [{ date: "2026-04-01", acftRegn: "VT-ABC", cost: 40, ccy: "INR" }],
      navMtowTiers: [73000, 77000],
      navEnr: [{ sector: "CCU-BOM", ccy: "INR", 73000: 6, 77000: 7 }],
      navTerm: [{ arrStn: "BOM", ccy: "INR", 73000: 8, 77000: 8 }],
      airportLanding: [{ arrStn: "BOM", mtow: 77000, variant: "A320", cost: 11, ccy: "INR" }],
      airportDom: [{ arrStn: "BOM", mtow: 77000, variant: "A320", cost: 12, ccy: "INR" }],
      airportAvsec: [{ arrStn: "BOM", variant: "A320", cost: 13, ccy: "INR" }],
      airportOther: [{ arrStn: "BOM", ccy: "INR", 73000: 15, 77000: 14 }],
    otherDoc: [{ label: "Doc", sector: "CCU-BOM", variantOrAcftRegn: "A320", per: "BH", cost: 2, ccy: "INR" }],
  }),
]);

  const res = createMockResponse();
  await costController.getCostPageData({
    user: { id: USER_ID },
    body: { label: { value: "both" } },
    query: {},
  }, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.flights.length, 1);

  const [flight] = res.body.flights;
  assert.equal(flight.reportingCurrency, "INR");
  assert.equal(flight.engineFuelConsumptionKg, 1000);
  assert.equal(flight.engineFuelCost, 1000);
  assert.equal(flight.apuFuelCostDirect, 100);
  assert.equal(flight.apuFuelCostAllocated, 50);
  assert.equal(flight.apuFuelCost, 150);
  assert.equal(flight.maintenanceReserveContribution, 15);
  assert.equal(flight.transitMaintenance, 20);
  assert.equal(flight.otherMaintenance, 13);
  assert.equal(flight.otherMxExpenses, 30);
  assert.equal(flight.rotableChanges, 40);
  assert.equal(flight.navigation, 15);
  assert.equal(flight.airport, 50);
  assert.equal(flight.otherDoc, 4);
  assert.ok(flight.totalCost > 0);
});
