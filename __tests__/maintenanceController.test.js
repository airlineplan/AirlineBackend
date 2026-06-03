const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const mongoose = require("mongoose");

const Flight = require("../model/flight");
const Assignment = require("../model/assignment");
const Fleet = require("../model/fleet");
const AircraftOnwing = require("../model/aircraftOnwing");
const MaintenanceReset = require("../model/maintenanceReset");
const MaintenanceTarget = require("../model/maintenanceTargetSchema");
const MaintenanceCalendar = require("../model/maintenanceCalendarSchema");
const Utilisation = require("../model/utilisation");
const UtilisationAssumption = require("../model/utilisationAssumptionSchema");
const RotableMovement = require("../model/rotableMovementSchema");
const maintenanceController = require("../controller/maintenanceController");

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
  dbName = `airline_maintenance_test_${Date.now()}`;
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-maintenance-mongo-"));

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

async function seedFlightDays(days, flightPrefix = "FL") {
  for (const day of days) {
    await Flight.create({
      userId: USER_ID,
      date: day,
      flight: `${flightPrefix}${day.getUTCDate()}`,
      bh: 0,
      fh: 0,
    });
  }
}

async function seedFleetAsset({ msn, regn, entry, exit, titled = "" }) {
  await Fleet.create({
    userId: USER_ID,
    category: "Aircraft",
    type: "A320",
    variant: "A320",
    sn: String(msn),
    regn,
    entry,
    exit,
    titled,
  });
}

async function seedAssignment({ date, flightNumber, msn, registration, bh = 0, fh = 0, includeMetrics = true }) {
  const assignment = {
    userId: USER_ID,
    date,
    flightNumber,
    aircraft: {
      msn,
      registration,
    },
    metrics: {
      blockHours: bh,
      flightHours: fh,
      cycles: 1,
    },
    isValid: true,
    validationErrors: [],
  };

  if (!includeMetrics) {
    delete assignment.metrics;
  }

  await Assignment.create(assignment);
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

test("maintenance compute uses the aircraft ownership valid on the reset date", async () => {
  const resetDate = utcDate(2026, 4, 12);
  const nextDate = utcDate(2026, 4, 13);

  await seedFlightDays([resetDate, nextDate]);
  await seedFleetAsset({
    msn: 4120,
    regn: "VT-AAB",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 4, 30),
  });
  await seedFleetAsset({
    msn: 4210,
    regn: "VT-BBC",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 4, 30),
  });

  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 1),
    msn: "4120",
    pos1Esn: "ENG-1",
  });
  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 10),
    msn: "4210",
    pos1Esn: "ENG-1",
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "ENG-1",
    pn: "PN-1",
    snBn: "SN-1",
    tsn: 10,
    csn: 10,
    dsn: 10,
    tsoTsr: 10,
    csoCsr: 10,
    dsoDsr: 10,
    tsRplmt: 10,
    csRplmt: 10,
    dsRplmt: 10,
    timeMetric: "BH",
  });

  await seedAssignment({
    date: nextDate,
    flightNumber: "FL13A",
    msn: 4210,
    registration: "VT-BBC",
    bh: 2,
  });
  await seedAssignment({
    date: nextDate,
    flightNumber: "FL13B",
    msn: 4120,
    registration: "VT-AAB",
    bh: 9,
  });

  const req = { user: { id: USER_ID } };
  const res = createMockResponse();

  await maintenanceController.computeMaintenanceLogic(req, res);

  assert.equal(res.statusCode, 200);
  assert.match(res.body.message, /Maintenance logic computed/i);

  const nextDayUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: nextDate,
    msnEsn: "ENG-1",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  assert.equal(nextDayUtil?.tsn, 12);
});

test("maintenance target deltas include aircraft and on-wing utilisation through the target date", async () => {
  const resetDate = utcDate(2026, 4, 20);
  const targetDate = utcDate(2026, 5, 10);
  const utilisationDays = [
    [utcDate(2026, 4, 21), "FL21", 2.5],
    [utcDate(2026, 4, 24), "FL24", 3],
    [utcDate(2026, 4, 27), "FL27", 2.5],
    [utcDate(2026, 5, 3), "FL03", 3],
    [utcDate(2026, 5, 6), "FL06", 3],
    [utcDate(2026, 5, 8), "FL08", 3],
  ];

  await seedFlightDays([resetDate, targetDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 3, 1),
    exit: utcDate(2026, 5, 31),
  });
  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 3, 1),
    msn: "5340",
    pos1Esn: "635799",
  });

  for (const [date, flightNumber, bh] of utilisationDays) {
    await seedAssignment({
      date,
      flightNumber,
      msn: 5340,
      registration: "VT-AAA",
      bh,
    });
  }

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 201,
    tsoTsr: 2500,
    csoCsr: 1251,
    dsoDsr: 251,
    tsRplmt: 2750,
    csRplmt: 1401,
    dsRplmt: 276,
    timeMetric: "FH",
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "635799",
    pn: "CFM56",
    snBn: "635799",
    tsn: 3000,
    csn: 1501,
    dsn: 301,
    tsoTsr: 2500,
    csoCsr: 1251,
    dsoDsr: 251,
    tsRplmt: 2750,
    csRplmt: 1401,
    dsRplmt: 276,
    timeMetric: "FH",
  });
  await MaintenanceTarget.create({
    userId: USER_ID,
    label: "SI check",
    msnEsn: "635799",
    pn: "CFM56",
    snBn: "635799",
    category: "Conserve",
    date: targetDate,
    tsn: "3100",
    csn: "1600",
    dsn: "350",
    tsRplmt: "2800",
    csRplmt: "1450",
    dsRplmt: "333",
  });

  const computeReq = { user: { id: USER_ID } };
  const computeRes = createMockResponse();
  await maintenanceController.computeMaintenanceLogic(computeReq, computeRes);

  const forecast = await Utilisation.findOne({
    userId: USER_ID,
    date: targetDate,
    msnEsn: "635799",
    pn: "CFM56",
    snBn: "635799",
  }).lean();

  assert.equal(computeRes.statusCode, 200);
  assert.equal(forecast?.tsn, 3017);
  assert.equal(forecast?.csn, 1507);
  assert.equal(forecast?.dsn, 321);
  assert.equal(forecast?.tsRplmt, 2767);
  assert.equal(forecast?.csRplmt, 1407);
  assert.equal(forecast?.dsRplmt, 296);

  const targetReq = { user: { id: USER_ID }, query: { date: "2026-05-10", msnEsn: "635799" } };
  const targetRes = createMockResponse();
  await maintenanceController.getTargets(targetReq, targetRes);

  assert.equal(targetRes.statusCode, 200);
  assert.equal(targetRes.body.data[0].fTsn, 83);
  assert.equal(targetRes.body.data[0].fCsn, 93);
  assert.equal(targetRes.body.data[0].fDsn, 29);
  assert.equal(targetRes.body.data[0].fTsr, 33);
  assert.equal(targetRes.body.data[0].fCsr, 43);
  assert.equal(targetRes.body.data[0].fDsr, 37);
});

test("maintenance compute uses utilisation assumptions only when assignments are absent", async () => {
  const day1 = utcDate(2026, 4, 1);
  const day2 = utcDate(2026, 4, 2);
  const day3 = utcDate(2026, 4, 3);

  await seedFlightDays([day1, day2, day3]);
  await seedFleetAsset({
    msn: 4120,
    regn: "VT-AAA",
    entry: day1,
    exit: utcDate(2026, 4, 30),
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: day1,
    msnEsn: "4120",
    pn: "PN-1",
    snBn: "SN-1",
    tsn: 100,
    csn: 100,
    dsn: 100,
    tsoTsr: 100,
    csoCsr: 100,
    dsoDsr: 100,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });

  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4120",
    fromDate: day2,
    toDate: day3,
    hours: 5,
    cycles: 2,
  });

  await seedAssignment({
    date: day3,
    flightNumber: "FL3",
    msn: 4120,
    registration: "VT-AAA",
    bh: 3,
  });

  const req = { user: { id: USER_ID } };
  const res = createMockResponse();

  await maintenanceController.computeMaintenanceLogic(req, res);

  const day2Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day2,
    msnEsn: "4120",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  const day3Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day3,
    msnEsn: "4120",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(day2Util?.tsn, 105);
  assert.equal(day2Util?.csn, 102);
  assert.equal(day3Util?.tsn, 108);
  assert.equal(day3Util?.csn, 103);
});

test("maintenance dashboard shows computed status before and after reset date", async () => {
  const day1 = utcDate(2026, 5, 1);
  const day2 = utcDate(2026, 5, 2);
  const resetDate = utcDate(2026, 5, 3);
  const day4 = utcDate(2026, 5, 4);

  await seedFlightDays([day1, day2, resetDate, day4]);
  await seedFleetAsset({
    msn: 5961,
    regn: "VT-MAY",
    entry: day1,
    exit: utcDate(2026, 5, 31),
  });

  for (const date of [day1, day2, resetDate, day4]) {
    await seedAssignment({
      date,
      flightNumber: `FL${date.getUTCDate()}`,
      msn: 5961,
      registration: "VT-MAY",
      bh: 2.5,
    });
  }

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5961",
    pn: "U92",
    snBn: "805",
    tsn: 300,
    csn: 300,
    dsn: 300,
    tsoTsr: 200,
    csoCsr: 200,
    dsoDsr: 200,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });

  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  await Utilisation.updateOne({
    userId: USER_ID,
    date: day4,
    msnEsn: "5961",
    pn: "U92",
    snBn: "805",
  }, {
    $set: {
      tsn: 300,
      csn: 302,
      dsn: 302,
      tsoTsr: 200,
      csoCsr: 202,
      dsoDsr: 202,
      tsRplmt: 100,
      csRplmt: 102,
      dsRplmt: 102,
    }
  });

  const previousRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-02", msnEsn: "5961" },
  }, previousRes);

  const nextRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-04", msnEsn: "5961" },
  }, nextRes);

  const previous = previousRes.body.data.maintenanceData[0];
  const next = nextRes.body.data.maintenanceData[0];

  assert.equal(previousRes.statusCode, 200);
  assert.equal(previous.tsn, 297.5);
  assert.equal(previous.csn, 299);
  assert.equal(previous.dsn, 299);
  assert.equal(previous.tso, 197.5);
  assert.equal(previous.cso, 199);
  assert.equal(previous.dso, 199);
  assert.equal(previous.tsr, 97.5);
  assert.equal(previous.csr, 99);
  assert.equal(previous.dsr, 99);

  assert.equal(nextRes.statusCode, 200);
  assert.equal(next.tsn, 302.5);
  assert.equal(next.csn, 301);
  assert.equal(next.dsn, 301);
  assert.equal(next.tso, 202.5);
  assert.equal(next.cso, 201);
  assert.equal(next.dso, 201);
  assert.equal(next.tsr, 102.5);
  assert.equal(next.csr, 101);
  assert.equal(next.dsr, 101);
});

test("maintenance dashboard uses flight utilisation when assignments do not embed metrics", async () => {
  const day1 = utcDate(2026, 5, 1);
  const day2 = utcDate(2026, 5, 2);
  const resetDate = utcDate(2026, 5, 3);
  const day4 = utcDate(2026, 5, 4);

  await seedFlightDays([day1, day2, resetDate, day4]);
  await Flight.updateMany(
    { userId: USER_ID, date: { $in: [day1, day2, resetDate, day4] } },
    { $set: { bh: 2.5, fh: 1.5 } }
  );
  await seedFleetAsset({
    msn: 5961,
    regn: "VT-MAY",
    entry: day1,
    exit: utcDate(2026, 5, 31),
  });

  for (const date of [day1, day2, resetDate, day4]) {
    await seedAssignment({
      date,
      flightNumber: `FL${date.getUTCDate()}`,
      msn: 5961,
      registration: "VT-MAY",
      includeMetrics: false,
    });
  }

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5961",
    pn: "U92",
    snBn: "805",
    tsn: 300,
    csn: 300,
    dsn: 300,
    tsoTsr: 200,
    csoCsr: 200,
    dsoDsr: 200,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });

  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  const previousRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-02", msnEsn: "5961" },
  }, previousRes);

  const nextRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-04", msnEsn: "5961" },
  }, nextRes);

  const previous = previousRes.body.data.maintenanceData[0];
  const next = nextRes.body.data.maintenanceData[0];

  assert.equal(previous.tsn, 297.5);
  assert.equal(previous.csn, 299);
  assert.equal(previous.dsn, 299);
  assert.equal(previous.tso, 197.5);
  assert.equal(previous.tsr, 97.5);

  assert.equal(next.tsn, 302.5);
  assert.equal(next.csn, 301);
  assert.equal(next.dsn, 301);
  assert.equal(next.tso, 202.5);
  assert.equal(next.tsr, 102.5);
});

test("saving reset status on a new date replaces the prior anchor and recomputes the full range", async () => {
  const days = [7, 8, 9, 10, 11].map(day => utcDate(2026, 5, day));

  await seedFlightDays(days);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-RESET",
    entry: days[0],
    exit: utcDate(2026, 5, 31),
  });

  for (const date of days) {
    await seedAssignment({
      date,
      flightNumber: `MX${date.getUTCDate()}`,
      msn: 5340,
      registration: "VT-RESET",
      bh: 10,
    });
  }

  await MaintenanceReset.create({
    userId: USER_ID,
    date: days[0],
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 1000,
    csn: 1000,
    dsn: 1000,
    tsoTsr: 1000,
    csoCsr: 1000,
    dsoDsr: 1000,
    tsRplmt: 1000,
    csRplmt: 1000,
    dsRplmt: 1000,
    timeMetric: "BH",
  });
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  const saveRes = createMockResponse();
  await maintenanceController.bulkSaveResetRecords({
    user: { id: USER_ID },
    body: {
      resetDate: "2026-05-10",
      resetData: [{
        msnEsn: "5340",
        pn: "A320",
        snBn: "5340",
        tsn: 5000,
        csn: 1000,
        dsn: 1000,
        tso: 5000,
        cso: 1000,
        dso: 1000,
        tsr: 5000,
        csr: 1000,
        dsr: 1000,
        metric: "BH",
      }],
    },
  }, saveRes);

  const remainingResets = await MaintenanceReset.find({
    userId: USER_ID,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
  }).sort({ date: 1 }).lean();
  const beforeReset = await Utilisation.findOne({
    userId: USER_ID,
    date: days[0],
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
  }).lean();
  const afterReset = await Utilisation.findOne({
    userId: USER_ID,
    date: days[4],
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
  }).lean();

  assert.equal(saveRes.statusCode, 200);
  assert.equal(remainingResets.length, 1);
  assert.equal(remainingResets[0].date.toISOString().slice(0, 10), "2026-05-10");
  assert.equal(beforeReset?.tsn, 4970);
  assert.equal(beforeReset?.csn, 997);
  assert.equal(beforeReset?.dsn, 997);
  assert.equal(afterReset?.tsn, 5010);
  assert.equal(afterReset?.csn, 1001);
  assert.equal(afterReset?.dsn, 1001);
});

test("maintenance compute advances day counters outside assumption dates", async () => {
  const days = Array.from({ length: 15 }, (_, index) => utcDate(2026, 5, 7 + index));
  const resetDate = days[0];
  const idleDate = utcDate(2026, 5, 10);
  const assumptionStart = utcDate(2026, 5, 11);
  const assumptionPenultimate = utcDate(2026, 5, 20);
  const assumptionEnd = utcDate(2026, 5, 21);

  await seedFlightDays(days);
  await seedFleetAsset({
    msn: 6125,
    regn: "VT-MAY",
    entry: resetDate,
    exit: utcDate(2026, 5, 31),
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "6125",
    pn: "U93",
    snBn: "801",
    tsn: 150,
    csn: 150,
    dsn: 150,
    tsoTsr: 100,
    csoCsr: 100,
    dsoDsr: 100,
    tsRplmt: 50,
    csRplmt: 50,
    dsRplmt: 50,
    timeMetric: "BH",
  });

  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "6125",
    fromDate: assumptionStart,
    toDate: assumptionEnd,
    hours: 4.5,
    cycles: 2,
  });

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const idleUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: idleDate,
    msnEsn: "6125",
    pn: "U93",
    snBn: "801",
  }).lean();
  const penultimateUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: assumptionPenultimate,
    msnEsn: "6125",
    pn: "U93",
    snBn: "801",
  }).lean();
  const endUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: assumptionEnd,
    msnEsn: "6125",
    pn: "U93",
    snBn: "801",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(idleUtil?.tsn, 150);
  assert.equal(idleUtil?.csn, 150);
  assert.equal(idleUtil?.dsn, 153);
  assert.equal(idleUtil?.dsoDsr, 103);
  assert.equal(penultimateUtil?.tsn, 195);
  assert.equal(penultimateUtil?.csn, 170);
  assert.equal(penultimateUtil?.dsn, 163);
  assert.equal(penultimateUtil?.dsoDsr, 113);
  assert.equal(endUtil?.tsn, 199.5);
  assert.equal(endUtil?.csn, 172);
  assert.equal(endUtil?.dsn, 164);
  assert.equal(endUtil?.dsoDsr, 114);
});

test("deleting the only reset setting removes all computed maintenance status for that part", async () => {
  const days = [1, 2, 3].map(day => utcDate(2026, 5, day));

  await seedFlightDays(days);
  await seedFleetAsset({
    msn: 7200,
    regn: "VT-DEL",
    entry: days[0],
    exit: days[2],
  });

  for (const date of days) {
    await seedAssignment({
      date,
      flightNumber: `DL${date.getUTCDate()}`,
      msn: 7200,
      registration: "VT-DEL",
      bh: 2,
    });
  }

  const reset = await MaintenanceReset.create({
    userId: USER_ID,
    date: days[0],
    msnEsn: "7200",
    pn: "A320",
    snBn: "7200",
    tsn: 100,
    csn: 100,
    dsn: 100,
    tsoTsr: 100,
    csoCsr: 100,
    dsoDsr: 100,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });

  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  const beforeDeleteCount = await Utilisation.countDocuments({
    userId: USER_ID,
    msnEsn: "7200",
    pn: "A320",
    snBn: "7200",
  });
  assert.equal(beforeDeleteCount, 3);

  const deleteRes = createMockResponse();
  await maintenanceController.deleteResetRecord({
    user: { id: USER_ID },
    params: { id: reset._id.toString() },
  }, deleteRes);

  const afterDeleteCount = await Utilisation.countDocuments({
    userId: USER_ID,
    msnEsn: "7200",
    pn: "A320",
    snBn: "7200",
  });
  const dashboardRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-02", msnEsn: "7200" },
  }, dashboardRes);

  assert.equal(deleteRes.statusCode, 200);
  assert.equal(afterDeleteCount, 0);
  assert.deepEqual(dashboardRes.body.data.maintenanceData, []);
});

test("saving duplicate reset settings for the same part is rejected", async () => {
  const day1 = utcDate(2026, 5, 1);
  await seedFlightDays([day1]);

  const saveRes = createMockResponse();
  await maintenanceController.bulkSaveResetRecords({
    user: { id: USER_ID },
    body: {
      resetDate: "2026-05-01",
      resetData: [
        {
          msnEsn: "8100",
          pn: "A320",
          snBn: "8100",
          tsn: 100,
          csn: 100,
          dsn: 100,
          tso: 100,
          cso: 100,
          dso: 100,
          tsr: 100,
          csr: 100,
          dsr: 100,
          metric: "BH",
        },
        {
          msnEsn: "8100",
          pn: "A320",
          snBn: "8100",
          tsn: 200,
          csn: 200,
          dsn: 200,
          tso: 200,
          cso: 200,
          dso: 200,
          tsr: 200,
          csr: 200,
          dsr: 200,
          metric: "BH",
        },
      ],
    },
  }, saveRes);

  assert.equal(saveRes.statusCode, 400);
  assert.match(saveRes.body.message, /Only one maintenance status setting/i);
});

test("maintenance calendar since-new threshold triggers once inside the master date range", async () => {
  const day1 = utcDate(2026, 4, 1);
  const day2 = utcDate(2026, 4, 2);
  const day3 = utcDate(2026, 4, 3);
  const day4 = utcDate(2026, 4, 4);
  const day5 = utcDate(2026, 4, 5);

  await seedFlightDays([day1, day2, day3, day4, day5]);
  await seedFleetAsset({
    msn: 4100,
    regn: "VT-SN1",
    entry: day1,
    exit: day5,
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: day1,
    msnEsn: "4100",
    pn: "PN-SN",
    snBn: "SN-SN",
    tsn: 100,
    csn: 50,
    dsn: 10,
    tsoTsr: 20,
    csoCsr: 20,
    dsoDsr: 20,
    tsRplmt: 20,
    csRplmt: 20,
    dsRplmt: 20,
    timeMetric: "BH",
  });
  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4100",
    fromDate: day2,
    toDate: day5,
    hours: 5,
    cycles: 1,
  });
  await MaintenanceCalendar.create({
    userId: USER_ID,
    calMsn: "4100",
    calPn: "PN-SN",
    snBn: "SN-SN",
    schEvent: "Performance restoration",
    eTsn: 110,
    downDays: 2,
  });

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const triggerDay = await Utilisation.findOne({
    userId: USER_ID,
    date: day3,
    msnEsn: "4100",
    pn: "PN-SN",
    snBn: "SN-SN",
  }).lean();
  const laterDay = await Utilisation.findOne({
    userId: USER_ID,
    date: day5,
    msnEsn: "4100",
    pn: "PN-SN",
    snBn: "SN-SN",
  }).lean();
  const calendar = await MaintenanceCalendar.findOne({ userId: USER_ID, calMsn: "4100" }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(triggerDay?.remarks, "Maintenance Check Triggered");
  assert.equal(laterDay?.remarks, undefined);
  assert.equal(calendar?.occurrence, 1);
  assert.equal(calendar?.lastOccurre.toISOString().slice(0, 10), "2026-04-03");
});

test("maintenance compute deletes affected assignments and clears matching flights", async () => {
  const day1 = utcDate(2026, 4, 1);
  const day2 = utcDate(2026, 4, 2);

  await seedFlightDays([day1, day2], "MX");
  await seedFleetAsset({
    msn: 4150,
    regn: "VT-MX1",
    entry: day1,
    exit: utcDate(2026, 4, 30),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: day1,
    msnEsn: "4150",
    pn: "PN-MX",
    snBn: "SN-MX",
    tsn: 100,
    csn: 50,
    dsn: 10,
    tsoTsr: 20,
    csoCsr: 20,
    dsoDsr: 20,
    tsRplmt: 20,
    csRplmt: 20,
    dsRplmt: 20,
    timeMetric: "BH",
  });
  await MaintenanceCalendar.create({
    userId: USER_ID,
    calMsn: "4150",
    calPn: "PN-MX",
    snBn: "SN-MX",
    schEvent: "Performance restoration",
    eTsn: 105,
    downDays: 1,
  });
  await seedAssignment({
    date: day2,
    flightNumber: "MX2",
    msn: 4150,
    registration: "VT-MX1",
    bh: 10,
  });
  await Flight.updateOne(
    { userId: USER_ID, date: day2, flight: "MX2" },
    { $set: { aircraft: { msn: 4150, registration: "VT-MX1" } } }
  );

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const assignment = await Assignment.findOne({
    userId: USER_ID,
    date: day2,
    flightNumber: "MX2",
  }).lean();
  const flight = await Flight.findOne({ userId: USER_ID, date: day2, flight: "MX2" }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.assignmentImpact.deletedCount, 1);
  assert.equal(assignment, null);
  assert.ok(!flight?.aircraft?.registration);
});

test("maintenance calendar restoration intervals reset and repeat", async () => {
  const days = [1, 2, 3, 4, 5].map(day => utcDate(2026, 4, day));

  await seedFlightDays(days);
  await seedFleetAsset({
    msn: 4200,
    regn: "VT-RST",
    entry: days[0],
    exit: days[4],
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: days[0],
    msnEsn: "4200",
    pn: "PN-RST",
    snBn: "SN-RST",
    tsn: 100,
    csn: 100,
    dsn: 100,
    tsoTsr: 0,
    csoCsr: 0,
    dsoDsr: 0,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });
  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4200",
    fromDate: days[1],
    toDate: days[4],
    hours: 5,
    cycles: 1,
  });
  await MaintenanceCalendar.create({
    userId: USER_ID,
    calMsn: "4200",
    calPn: "PN-RST",
    snBn: "SN-RST",
    schEvent: "Restoration interval",
    eTso: 10,
    downDays: 1,
    postTso: 0,
    postCso: 0,
    postDso: 0,
  });

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const firstTrigger = await Utilisation.findOne({ userId: USER_ID, date: days[2], msnEsn: "4200" }).lean();
  const secondTrigger = await Utilisation.findOne({ userId: USER_ID, date: days[4], msnEsn: "4200" }).lean();
  const betweenTriggers = await Utilisation.findOne({ userId: USER_ID, date: days[3], msnEsn: "4200" }).lean();
  const calendar = await MaintenanceCalendar.findOne({ userId: USER_ID, calMsn: "4200" }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(firstTrigger?.remarks, "Maintenance Check Triggered");
  assert.equal(firstTrigger?.tsoTsr, 0);
  assert.equal(betweenTriggers?.tsoTsr, 5);
  assert.equal(secondTrigger?.remarks, "Maintenance Check Triggered");
  assert.equal(secondTrigger?.tsoTsr, 0);
  assert.equal(calendar?.occurrence, 2);
  assert.equal(calendar?.nextEstima.toISOString().slice(0, 10), "2026-04-03");
  assert.equal(calendar?.lastOccurre.toISOString().slice(0, 10), "2026-04-05");
});

test("maintenance calendar replacement intervals reset replacement counters", async () => {
  const day1 = utcDate(2026, 4, 1);
  const day2 = utcDate(2026, 4, 2);
  const day3 = utcDate(2026, 4, 3);

  await seedFlightDays([day1, day2, day3]);
  await seedFleetAsset({
    msn: 4300,
    regn: "VT-RPL",
    entry: day1,
    exit: day3,
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: day1,
    msnEsn: "4300",
    pn: "PN-RPL",
    snBn: "SN-RPL",
    tsn: 100,
    csn: 100,
    dsn: 100,
    tsoTsr: 100,
    csoCsr: 100,
    dsoDsr: 100,
    tsRplmt: 0,
    csRplmt: 0,
    dsRplmt: 0,
    timeMetric: "BH",
  });
  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4300",
    fromDate: day2,
    toDate: day3,
    hours: 5,
    cycles: 1,
  });
  await MaintenanceCalendar.create({
    userId: USER_ID,
    calMsn: "4300",
    calPn: "PN-RPL",
    snBn: "SN-RPL",
    schEvent: "Replacement interval",
    eTsr: 10,
    avgDownda: 1,
    postTsr: 0,
    postCsr: 0,
    postDsr: 0,
  });

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const triggerDay = await Utilisation.findOne({
    userId: USER_ID,
    date: day3,
    msnEsn: "4300",
    pn: "PN-RPL",
    snBn: "SN-RPL",
  }).lean();
  const calendar = await MaintenanceCalendar.findOne({ userId: USER_ID, calMsn: "4300" }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(triggerDay?.remarks, "Maintenance Check Triggered");
  assert.equal(triggerDay?.tsRplmt, 0);
  assert.equal(triggerDay?.csRplmt, 0);
  assert.equal(triggerDay?.dsRplmt, 0);
  assert.equal(calendar?.occurrence, 1);
  assert.equal(calendar?.soTsr, 10);
});

test("saving calendar inputs recomputes next estimated date and occurrence count", async () => {
  const may1 = utcDate(2026, 5, 1);
  const may31 = utcDate(2026, 5, 31);

  await seedFlightDays([may1, may31]);
  await seedFleetAsset({
    msn: 4000,
    regn: "VT-AAB",
    entry: may1,
    exit: may31,
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: may1,
    msnEsn: "4000",
    pn: "A320",
    snBn: "4000",
    tsn: 3000,
    csn: 3000,
    dsn: 3000,
    timeMetric: "BH",
  });
  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4000",
    fromDate: may1,
    toDate: may31,
    hours: 5,
    cycles: 1,
  });

  const res = createMockResponse();
  await maintenanceController.bulkSaveCalendar({
    user: { id: USER_ID },
    body: {
      calendarData: [
        {
          calLabel: "C check",
          lineBase: "DEL",
          calMsn: "4000",
          schEvent: "C check",
          calPn: "A320",
          snBn: "4000",
          eTsn: 3025,
          nextEstima: "2026-06-15",
          occurrence: 99,
        },
      ],
    },
  }, res);

  const calendar = await MaintenanceCalendar.findOne({
    userId: USER_ID,
    calMsn: "4000",
    calPn: "A320",
    snBn: "4000",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(calendar?.occurrence, 1);
  assert.equal(calendar?.nextEstima.toISOString().slice(0, 10), "2026-05-06");
  assert.equal(calendar?.lastOccurre.toISOString().slice(0, 10), "2026-05-06");
});

test("saving calendar inputs retains multiple scheduled events for the same part", async () => {
  const res = createMockResponse();
  await maintenanceController.bulkSaveCalendar({
    user: { id: USER_ID },
    body: {
      calendarData: [
        {
          calLabel: "Airframe",
          lineBase: "Base",
          calMsn: "1600",
          schEvent: "A4",
          calPn: "ATR72",
          snBn: "1600",
          eTsn: 2000,
          downDays: 3,
        },
        {
          calLabel: "Airframe",
          lineBase: "Base",
          calMsn: "1600",
          schEvent: "4C",
          calPn: "ATR72",
          snBn: "1600",
          downDays: 7,
          avgDownda: 11,
        },
      ],
    },
  }, res);

  const records = await MaintenanceCalendar.find({
    userId: USER_ID,
    calMsn: "1600",
    calPn: "ATR72",
    snBn: "1600",
  }).sort({ schEvent: 1 }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(records.length, 2);
  assert.deepEqual(records.map(record => record.schEvent), ["4C", "A4"]);
});

test("post-event calendar values are blank no-ops and filled values apply on last ground day", async () => {
  const days = [1, 2, 3, 4, 5].map(day => utcDate(2026, 1, day));

  await seedFlightDays(days);
  await seedFleetAsset({
    msn: 4400,
    regn: "VT-PEV",
    entry: days[0],
    exit: days[4],
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: days[0],
    msnEsn: "4400",
    pn: "PN-PEV",
    snBn: "SN-PEV",
    tsn: 100,
    csn: 100,
    dsn: 100,
    tsoTsr: 42,
    csoCsr: 0,
    dsoDsr: 50,
    tsRplmt: 100,
    csRplmt: 100,
    dsRplmt: 100,
    timeMetric: "BH",
  });
  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4400",
    fromDate: days[1],
    toDate: days[4],
    hours: 5,
    cycles: 1,
  });
  await MaintenanceCalendar.create({
    userId: USER_ID,
    calMsn: "4400",
    calPn: "PN-PEV",
    snBn: "SN-PEV",
    schEvent: "Post event restoration",
    eCso: 1,
    downDays: 3,
    postDso: 0,
  });

  const res = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, res);

  const triggerDay = await Utilisation.findOne({ userId: USER_ID, date: days[1], msnEsn: "4400" }).lean();
  const lastGroundDay = await Utilisation.findOne({ userId: USER_ID, date: days[3], msnEsn: "4400" }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(triggerDay?.remarks, "Maintenance Check Triggered");
  assert.equal(triggerDay?.tsoTsr, 42);
  assert.equal(triggerDay?.dsoDsr, 51);
  assert.equal(lastGroundDay?.remarks, "Maintenance Downtime");
  assert.equal(lastGroundDay?.tsoTsr, 42);
  assert.equal(lastGroundDay?.dsoDsr, 0);
});

test("utilisation assumptions persist avg downdays", async () => {
  const req = {
    user: { id: USER_ID },
    body: {
      utilisationAssumptions: [
        {
          msn: "4120",
          fromDate: "2025-10-01",
          toDate: "2025-10-12",
          hours: 5,
          cycles: 2,
          avgDowndays: 1.5,
        },
      ],
    },
  };
  const saveRes = createMockResponse();

  await maintenanceController.bulkSaveUtilisationAssumptions(req, saveRes);

  const getRes = createMockResponse();
  await maintenanceController.getUtilisationAssumptions({ user: { id: USER_ID } }, getRes);

  assert.equal(saveRes.statusCode, 200);
  assert.equal(getRes.statusCode, 200);
  assert.equal(getRes.body.data[0].msn, "4120");
  assert.equal(getRes.body.data[0].avgDowndays, 1.5);
});

test("maintenance backfill writes the first boundary day", async () => {
  const firstDay = utcDate(2026, 4, 1);
  const secondDay = utcDate(2026, 4, 2);
  const resetDate = utcDate(2026, 4, 3);

  await seedFlightDays([firstDay, secondDay, resetDate]);
  await seedFleetAsset({
    msn: 5000,
    regn: "VT-CCC",
    entry: firstDay,
    exit: utcDate(2026, 4, 30),
  });
  await seedAssignment({
    date: firstDay,
    flightNumber: "FL1",
    msn: 5000,
    registration: "VT-CCC",
    bh: 1,
  });
  await seedAssignment({
    date: secondDay,
    flightNumber: "FL2",
    msn: 5000,
    registration: "VT-CCC",
    bh: 2,
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5000",
    pn: "PN-1",
    snBn: "SN-1",
    tsn: 30,
    csn: 30,
    dsn: 30,
    tsoTsr: 30,
    csoCsr: 30,
    dsoDsr: 30,
    tsRplmt: 30,
    csRplmt: 30,
    dsRplmt: 30,
    timeMetric: "BH",
  });

  const req = { user: { id: USER_ID } };
  const res = createMockResponse();

  await maintenanceController.computeMaintenanceLogic(req, res);

  const firstDayUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: firstDay,
    msnEsn: "5000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  const secondDayUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: secondDay,
    msnEsn: "5000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(firstDayUtil?.tsn, 28);
  assert.equal(secondDayUtil?.tsn, 30);
});

test("maintenance compute uses network effective dates when dated flights are not expanded", async () => {
  const viewDate = utcDate(2026, 4, 10);
  const resetDate = utcDate(2026, 4, 20);

  await Flight.create({
    userId: USER_ID,
    flight: "T100",
    depStn: "DEL",
    arrStn: "BOM",
    variant: "A320",
    effFromDt: utcDate(2026, 4, 1),
    effToDt: utcDate(2026, 5, 10),
    dow: "12567",
    bh: 0,
    fh: 0,
  });
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 3, 1),
    exit: utcDate(2026, 5, 31),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 201,
    timeMetric: "BH",
  });

  const computeRes = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, computeRes);

  const viewDayUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: viewDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
  }).lean();

  assert.equal(computeRes.statusCode, 200);
  assert.match(computeRes.body.message, /Maintenance logic computed/i);
  assert.equal(viewDayUtil?.tsn, 2000);
  assert.equal(viewDayUtil?.csn, 1001);
  assert.equal(viewDayUtil?.dsn, 191);

  const dashboardRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-04-10" },
  }, dashboardRes);

  assert.equal(dashboardRes.statusCode, 200);
  assert.equal(dashboardRes.body.data.maintenanceData.length, 1);
  assert.equal(dashboardRes.body.data.maintenanceData[0].msnEsn, "5340");
  assert.equal(dashboardRes.body.data.maintenanceData[0].dsn, 191);
});

test("maintenance forward fill stops at a later reset and resumes from that reset", async () => {
  const day1 = utcDate(2026, 4, 3);
  const day2 = utcDate(2026, 4, 4);
  const day3 = utcDate(2026, 4, 5);
  const day4 = utcDate(2026, 4, 6);
  const day5 = utcDate(2026, 4, 7);

  await seedFlightDays([day1, day2, day3, day4, day5]);
  await seedFleetAsset({
    msn: 6000,
    regn: "VT-DDD",
    entry: day1,
    exit: utcDate(2026, 4, 30),
  });

  await seedAssignment({
    date: day2,
    flightNumber: "FL4",
    msn: 6000,
    registration: "VT-DDD",
    bh: 2,
  });
  await seedAssignment({
    date: day3,
    flightNumber: "FL5",
    msn: 6000,
    registration: "VT-DDD",
    bh: 3,
  });
  await seedAssignment({
    date: day5,
    flightNumber: "FL7",
    msn: 6000,
    registration: "VT-DDD",
    bh: 5,
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: day1,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
    tsn: 10,
    csn: 10,
    dsn: 10,
    tsoTsr: 10,
    csoCsr: 10,
    dsoDsr: 10,
    tsRplmt: 10,
    csRplmt: 10,
    dsRplmt: 10,
    timeMetric: "BH",
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: day4,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
    tsn: 50,
    csn: 50,
    dsn: 50,
    tsoTsr: 50,
    csoCsr: 50,
    dsoDsr: 50,
    tsRplmt: 50,
    csRplmt: 50,
    dsRplmt: 50,
    timeMetric: "BH",
  });

  const req = { user: { id: USER_ID } };
  const res = createMockResponse();

  await maintenanceController.computeMaintenanceLogic(req, res);

  const day2Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day2,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();
  const day3Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day3,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();
  const day4Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day4,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();
  const day5Util = await Utilisation.findOne({
    userId: USER_ID,
    date: day5,
    msnEsn: "6000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(day2Util?.tsn, 12);
  assert.equal(day3Util?.tsn, 15);
  assert.equal(day4Util?.tsn, 50);
  assert.equal(day5Util?.tsn, 55);
});

test("saving reset records waits for recompute before responding", async () => {
  const resetDate = utcDate(2026, 4, 3);
  const nextDate = utcDate(2026, 4, 4);

  await seedFlightDays([resetDate, nextDate]);
  await seedFleetAsset({
    msn: 7000,
    regn: "VT-EEE",
    entry: resetDate,
    exit: utcDate(2026, 4, 30),
  });
  await seedAssignment({
    date: nextDate,
    flightNumber: "FL4",
    msn: 7000,
    registration: "VT-EEE",
    bh: 4,
  });

  const req = {
    user: { id: USER_ID },
    body: {
      resetData: [
        {
          date: "2026-04-03",
          msnEsn: "7000",
          pn: "PN-1",
          snBn: "SN-1",
          tsn: 20,
          csn: 20,
          dsn: 20,
          tso: 20,
          cso: 20,
          dso: 20,
          tsr: 20,
          csr: 20,
          dsr: 20,
          metric: "BH",
        },
      ],
    },
  };
  const res = createMockResponse();

  await maintenanceController.bulkSaveResetRecords(req, res);

  const nextDayUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: nextDate,
    msnEsn: "7000",
    pn: "PN-1",
    snBn: "SN-1",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(nextDayUtil?.tsn, 24);
});

test("saving reset records accepts modal fallback date", async () => {
  const resetDate = utcDate(2026, 5, 5);

  await seedFlightDays([resetDate]);
  await seedFleetAsset({
    msn: 8000,
    regn: "VT-FFF",
    entry: resetDate,
    exit: utcDate(2026, 5, 31),
  });
  await seedAssignment({
    date: resetDate,
    flightNumber: "FL5",
    msn: 8000,
    registration: "VT-FFF",
    bh: 3,
  });

  const req = {
    user: { id: USER_ID },
    body: {
      resetDate: "2026-05-05",
      resetData: [
        {
          msnEsn: "8000",
          pn: "PN-2",
          snBn: "SN-2",
          tsn: "30",
          csn: "12",
          metric: "FH",
        },
      ],
    },
  };
  const res = createMockResponse();

  await maintenanceController.bulkSaveResetRecords(req, res);

  const savedReset = await MaintenanceReset.findOne({
    userId: USER_ID,
    msnEsn: "8000",
    pn: "PN-2",
    snBn: "SN-2",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(savedReset?.date.toISOString().slice(0, 10), "2026-05-05");
  assert.equal(savedReset?.tsn, 30);
  assert.equal(savedReset?.csn, 12);
  assert.equal(savedReset?.timeMetric, "FH");
});

test("saving reset records uses modal reset date over effective row date", async () => {
  const staleEffectiveDate = utcDate(2026, 5, 1);
  const resetDate = utcDate(2026, 5, 5);

  await seedFlightDays([staleEffectiveDate, resetDate]);
  await seedFleetAsset({
    msn: 9700,
    regn: "VT-MOD",
    entry: staleEffectiveDate,
    exit: utcDate(2026, 5, 30),
  });

  const req = {
    user: { id: USER_ID },
    body: {
      resetDate: "2026-05-05",
      resetData: [
        {
          date: "2026-05-01",
          msnEsn: "9700",
          pn: "PN-MOD",
          snBn: "SN-MOD",
          tsn: 25,
          csn: 10,
          dsn: 5,
          metric: "BH",
        },
      ],
    },
  };
  const res = createMockResponse();

  await maintenanceController.bulkSaveResetRecords(req, res);

  const staleReset = await MaintenanceReset.findOne({
    userId: USER_ID,
    date: staleEffectiveDate,
    msnEsn: "9700",
    pn: "PN-MOD",
    snBn: "SN-MOD",
  }).lean();
  const savedReset = await MaintenanceReset.findOne({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "9700",
    pn: "PN-MOD",
    snBn: "SN-MOD",
  }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(staleReset, null);
  assert.equal(savedReset?.tsn, 25);
});

test("reset records modal returns only records matching selected date", async () => {
  await MaintenanceReset.create([
    {
      userId: USER_ID,
      date: utcDate(2026, 5, 1),
      msnEsn: "9000",
      pn: "PN-1",
      snBn: "SN-1",
      tsn: 10,
    },
    {
      userId: USER_ID,
      date: utcDate(2026, 5, 4),
      msnEsn: "9000",
      pn: "PN-1",
      snBn: "SN-1",
      tsn: 14,
    },
    {
      userId: USER_ID,
      date: utcDate(2026, 5, 5),
      msnEsn: "9000",
      pn: "PN-1",
      snBn: "SN-1",
      tsn: 15,
    },
    {
      userId: USER_ID,
      date: utcDate(2026, 5, 5),
      msnEsn: "9001",
      pn: "PN-2",
      snBn: "SN-2",
      tsn: 20,
    },
    {
      userId: USER_ID,
      date: utcDate(2026, 5, 8),
      msnEsn: "9000",
      pn: "PN-1",
      snBn: "SN-1",
      tsn: 18,
    },
  ]);

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-05-05",
      msnEsn: "9000",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getResetRecords(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.length, 1);
  assert.equal(res.body.data[0].date, "2026-05-05");
  assert.equal(res.body.data[0].msnEsn, "9000");
  assert.equal(res.body.data[0].tsn, 15);
});

test("maintenance dashboard autopopulates titled spare from active fleet asset", async () => {
  const selectedDate = utcDate(2026, 5, 5);

  await seedFlightDays([selectedDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-ABC",
    entry: utcDate(2026, 5, 1),
    exit: utcDate(2026, 5, 30),
    titled: "Spare",
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: selectedDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1000,
    dsn: 200,
    timeMetric: "BH",
  });

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-05-05",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getMaintenanceDashboard(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 1);
  assert.equal(res.body.data.maintenanceData[0].msnEsn, "5340");
  assert.equal(res.body.data.maintenanceData[0].titled, "Spare");
});

test("maintenance dashboard autopopulates titled value from fleet when status row is outside fleet window", async () => {
  const selectedDate = utcDate(2026, 5, 5);

  await seedFlightDays([selectedDate]);
  await seedFleetAsset({
    msn: 6125,
    regn: "VT-AAC",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 4, 30),
    titled: "VT-AAC",
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: selectedDate,
    msnEsn: "6125",
    pn: "U92",
    snBn: "805",
    tsn: 300,
    csn: 302,
    dsn: 302,
    timeMetric: "BH",
  });

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-05-05",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getMaintenanceDashboard(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 1);
  assert.equal(res.body.data.maintenanceData[0].msnEsn, "6125");
  assert.equal(res.body.data.maintenanceData[0].titled, "VT-AAC");
});

test("maintenance dashboard hides reset rows for assets no longer in fleet master", async () => {
  const selectedDate = utcDate(2026, 5, 5);

  await seedFlightDays([selectedDate]);
  await seedFleetAsset({
    msn: 5150,
    regn: "VT-ACT",
    entry: utcDate(2026, 5, 1),
    exit: utcDate(2026, 5, 30),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: selectedDate,
    msnEsn: "1001",
    pn: "ATR72-600",
    snBn: "1001",
    tsn: 1000,
    csn: 1000,
    dsn: 994,
    timeMetric: "BH",
  });

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-05-05",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getMaintenanceDashboard(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 0);
});

test("maintenance dashboard shows the exact reset-day metrics when viewing the reset date", async () => {
  const resetDate = utcDate(2026, 4, 20);

  await seedFlightDays([resetDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 3, 1),
    exit: utcDate(2026, 5, 31),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1000,
    dsn: 200,
    timeMetric: "BH",
  });
  await Utilisation.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 201,
    timeMetric: "BH",
  });

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-04-20",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getMaintenanceDashboard(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 1);
  assert.equal(res.body.data.maintenanceData[0].asOnDate, "2026-04-20");
  assert.equal(res.body.data.maintenanceData[0].savedResetDate, "2026-04-20");
  assert.equal(res.body.data.maintenanceData[0].tsn, 2000);
  assert.equal(res.body.data.maintenanceData[0].csn, 1000);
  assert.equal(res.body.data.maintenanceData[0].dsn, 200);
});

test("maintenance dashboard shows backfilled status before the reset date", async () => {
  const viewDate = utcDate(2026, 4, 10);
  const resetDate = utcDate(2026, 4, 20);

  await seedFlightDays([viewDate, resetDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 3, 1),
    exit: utcDate(2026, 5, 31),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 201,
    timeMetric: "BH",
  });
  await Utilisation.create({
    userId: USER_ID,
    date: viewDate,
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 191,
    timeMetric: "BH",
  });

  const req = {
    user: { id: USER_ID },
    query: {
      date: "2026-04-10",
    },
  };
  const res = createMockResponse();

  await maintenanceController.getMaintenanceDashboard(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 1);
  assert.equal(res.body.data.maintenanceData[0].msnEsn, "5340");
  assert.equal(res.body.data.maintenanceData[0].asOnDate, "2026-04-10");
  assert.equal(res.body.data.maintenanceData[0].savedResetDate, "2026-04-20");
  assert.equal(res.body.data.maintenanceData[0].tsn, 2000);
  assert.equal(res.body.data.maintenanceData[0].dsn, 191);
});

test("maintenance dashboard can seed a pre-reset row from a future reset inside the planning window", async () => {
  await seedFlightDays([utcDate(2026, 4, 8), utcDate(2026, 4, 10), utcDate(2026, 5, 10)]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 3, 1),
    exit: utcDate(2026, 5, 31),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 20),
    msnEsn: "5340",
    pn: "A320",
    snBn: "5340",
    tsn: 2000,
    csn: 1001,
    dsn: 201,
    timeMetric: "BH",
  });

  const res = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-04-10" },
  }, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.data.maintenanceData.length, 1);
  assert.equal(res.body.data.maintenanceData[0].msnEsn, "5340");
  assert.equal(res.body.data.maintenanceData[0].asOnDate, "2026-04-10");
  assert.equal(res.body.data.maintenanceData[0].savedResetDate, "2026-04-20");
});

test("maintenance dashboard only shows rows from opening balance day through planning end", async () => {
  await seedFlightDays([utcDate(2026, 5, 1), utcDate(2026, 5, 31)]);
  await seedFleetAsset({
    msn: 5961,
    regn: "VT-BAL",
    entry: utcDate(2026, 5, 1),
    exit: utcDate(2026, 5, 31),
  });
  await MaintenanceReset.create({
    userId: USER_ID,
    date: utcDate(2026, 5, 5),
    msnEsn: "5961",
    pn: "U92",
    snBn: "805",
    tsn: 300,
    csn: 298,
    dsn: 298,
    timeMetric: "BH",
  });

  const beforeOpeningRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-04-29" },
  }, beforeOpeningRes);

  const openingRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-04-30" },
  }, openingRes);

  const afterEndRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-06-01" },
  }, afterEndRes);

  assert.equal(beforeOpeningRes.statusCode, 200);
  assert.equal(beforeOpeningRes.body.data.maintenanceData.length, 0);
  assert.equal(openingRes.statusCode, 200);
  assert.equal(openingRes.body.data.maintenanceData.length, 1);
  assert.equal(openingRes.body.data.maintenanceData[0].msnEsn, "5961");
  assert.equal(afterEndRes.statusCode, 200);
  assert.equal(afterEndRes.body.data.maintenanceData.length, 0);
});

test("target dashboard returns rendered aliases, deltas, and highlight flags", async () => {
  const targetDate = utcDate(2026, 4, 13);

  await seedFlightDays([targetDate]);
  await MaintenanceTarget.create({
    userId: USER_ID,
    label: "DEF",
    msnEsn: "685912",
    pn: "CFM56-5B6",
    snBn: "685912",
    category: "Conserve",
    date: targetDate,
    tsn: "19385",
    csn: "9800",
    cso: "9900",
  });
  await Utilisation.create({
    userId: USER_ID,
    date: targetDate,
    msnEsn: "685912",
    pn: "CFM56-5B6",
    snBn: "685912",
    tsn: 19381.48,
    csn: 9916,
    csoCsr: 9916,
  });

  const req = { user: { id: USER_ID }, query: { date: "2026-04-13" } };
  const res = createMockResponse();

  await maintenanceController.getTargets(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.success, true);
  assert.equal(res.body.data.length, 1);
  assert.equal(res.body.data[0].targetLabel, "DEF");
  assert.equal(res.body.data[0].targetMsn, "685912");
  assert.equal(res.body.data[0].date, "2026-04-13");
  assert.equal(res.body.data[0].displayDate, "13 Apr 26");
  assert.equal(res.body.data[0].fTsn, 3.52);
  assert.equal(res.body.data[0].fCsn, -116);
  assert.equal(res.body.data[0].fCso, -16);
  assert.deepEqual(res.body.data[0].highlights.sort(), ["cso", "csn"].sort());
});

test("saving target maintenance status is scoped by target date", async () => {
  const req = {
    user: { id: USER_ID },
    body: {
      targetData: [
        {
          label: "ABC",
          msnEsn: "685912",
          pn: "CFM56-5B6",
          snBn: "685912",
          category: "Conserve",
          date: "2026-10-12",
          csn: "9800",
        },
        {
          label: "DEF",
          msnEsn: "685912",
          pn: "CFM56-5B6",
          snBn: "685912",
          category: "Conserve",
          date: "2026-10-13",
          csn: "9900",
        },
      ],
    },
  };
  const res = createMockResponse();

  await maintenanceController.bulkSaveTargets(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(await MaintenanceTarget.countDocuments({ userId: USER_ID }), 2);

  const updateReq = {
    user: { id: USER_ID },
    body: {
      targetData: [
        {
          label: "ABC",
          msnEsn: "685912",
          pn: "CFM56-5B6",
          snBn: "685912",
          category: "Run-down",
          date: "2026-10-12",
          csn: "9850",
        },
      ],
    },
  };
  const updateRes = createMockResponse();

  await maintenanceController.bulkSaveTargets(updateReq, updateRes);

  const records = await MaintenanceTarget.find({ userId: USER_ID }).sort({ date: 1 }).lean();

  assert.equal(updateRes.statusCode, 200);
  assert.equal(records.length, 2);
  assert.equal(records[0].date.toISOString().slice(0, 10), "2026-10-12");
  assert.equal(records[0].category, "Run-down");
  assert.equal(records[0].csn, "9850");
  assert.equal(records[1].date.toISOString().slice(0, 10), "2026-10-13");
  assert.equal(records[1].category, "Conserve");
  assert.equal(records[1].csn, "9900");
});

test("saving rotable movement scopes on-wing updates to the current user", async () => {
  const otherUserId = new mongoose.Types.ObjectId().toString();

  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 12),
    msn: "4120",
    pos2Esn: "OLD-USER",
  });
  await AircraftOnwing.create({
    userId: otherUserId,
    date: utcDate(2026, 4, 12),
    msn: "4120",
    pos2Esn: "OLD-OTHER",
  });

  const req = {
    user: { id: USER_ID },
    body: {
      rotablesData: [
        {
          label: "Engine change",
          date: "2026-04-11",
          pn: "CFM56-5B6",
          msn: "4120",
          acftRegn: "VT-DKU",
          position: "#2",
          removedSN: "OLD-USER",
          installedSN: "685782",
        },
      ],
    },
  };
  const res = createMockResponse();

  await maintenanceController.bulkSaveRotables(req, res);

  const userOnwing = await AircraftOnwing.findOne({ userId: USER_ID, msn: "4120", date: utcDate(2026, 4, 12) }).lean();
  const otherOnwing = await AircraftOnwing.findOne({ userId: otherUserId, msn: "4120", date: utcDate(2026, 4, 12) }).lean();

  assert.equal(res.statusCode, 200);
  assert.equal(userOnwing?.pos2Esn, "685782");
  assert.equal(otherOnwing?.pos2Esn, "OLD-OTHER");
});

test("maintenance compute switches engine utilisation after rotable movement", async () => {
  const resetDate = utcDate(2026, 4, 15);
  const movementDate = utcDate(2026, 5, 10);
  const effectiveDate = utcDate(2026, 5, 11);

  await seedFlightDays([resetDate, effectiveDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 12, 31),
  });

  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 1),
    msn: "5340",
    pos1Esn: "635799",
    pos2Esn: "635800",
  });

  await MaintenanceReset.insertMany([
    {
      userId: USER_ID,
      date: resetDate,
      msnEsn: "5340",
      pn: "A320",
      snBn: "5340",
      tsn: 100,
      csn: 40,
      dsn: 30,
      timeMetric: "BH",
    },
    {
      userId: USER_ID,
      date: resetDate,
      msnEsn: "635799",
      pn: "CFM56",
      snBn: "635799",
      tsn: 100,
      csn: 40,
      dsn: 31,
      tsoTsr: 80,
      csoCsr: 20,
      dsoDsr: 21,
      tsRplmt: 90,
      csRplmt: 25,
      dsRplmt: 26,
      timeMetric: "BH",
    },
    {
      userId: USER_ID,
      date: resetDate,
      msnEsn: "721576",
      pn: "CFM56",
      snBn: "721576",
      tsn: 5000,
      csn: 3000,
      dsn: 1000,
      tsoTsr: 2000,
      csoCsr: 1000,
      dsoDsr: 500,
      tsRplmt: 2500,
      csRplmt: 1500,
      dsRplmt: 700,
      timeMetric: "BH",
    },
  ]);

  await seedAssignment({
    date: effectiveDate,
    flightNumber: "FL11",
    msn: 5340,
    registration: "VT-AAA",
    bh: 4,
  });

  const saveRotableRes = createMockResponse();
  await maintenanceController.bulkSaveRotables({
    user: { id: USER_ID },
    body: {
      rotablesData: [
        {
          label: "Engine change",
          date: movementDate.toISOString().slice(0, 10),
          pn: "CFM56",
          msn: "5340",
          acftRegn: "VT-AAA",
          position: "#1",
          removedSN: "635799",
          installedSN: "721576",
        },
      ],
    },
  }, saveRotableRes);

  const computeRes = createMockResponse();
  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, computeRes);

  const removedEngineUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: effectiveDate,
    msnEsn: "635799",
    pn: "CFM56",
    snBn: "635799",
  }).lean();
  const installedEngineUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: effectiveDate,
    msnEsn: "721576",
    pn: "CFM56",
    snBn: "721576",
  }).lean();
  const effectiveOnwing = await AircraftOnwing.findOne({
    userId: USER_ID,
    msn: "5340",
    date: effectiveDate,
  }).lean();

  assert.equal(saveRotableRes.statusCode, 200);
  assert.equal(computeRes.statusCode, 200);
  assert.equal(effectiveOnwing?.pos1Esn, "721576");
  assert.equal(effectiveOnwing?.pos2Esn, "635800");
  assert.equal(removedEngineUtil?.tsn, 100);
  assert.equal(removedEngineUtil?.csn, 40);
  assert.equal(installedEngineUtil?.tsn, 5004);
  assert.equal(installedEngineUtil?.csn, 3001);
});

test("maintenance dashboard applies aircraft utilisation to engine installed by prior-day rotable movement", async () => {
  const resetDate = utcDate(2026, 5, 18);
  const movementDate = utcDate(2026, 5, 19);
  const effectiveDate = utcDate(2026, 5, 20);

  await seedFlightDays([resetDate, movementDate, effectiveDate]);
  await seedFleetAsset({
    msn: 4950,
    regn: "VT-AAA",
    entry: utcDate(2026, 5, 1),
    exit: utcDate(2026, 5, 31),
  });
  await seedFleetAsset({
    msn: 721576,
    regn: "",
    entry: utcDate(2026, 5, 1),
    exit: utcDate(2026, 5, 31),
    titled: "Spare",
  });

  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 5, 1),
    msn: "4950",
    pos1Esn: "635799",
    pos2Esn: "635800",
  });

  await MaintenanceReset.create({
    userId: USER_ID,
    date: resetDate,
    msnEsn: "721576",
    pn: "CFM56",
    snBn: "721576",
    tsn: 2000,
    csn: 1000,
    dsn: 10,
    tsoTsr: 500,
    csoCsr: 250,
    dsoDsr: 5,
    tsRplmt: 400,
    csRplmt: 200,
    dsRplmt: 4,
    timeMetric: "BH",
  });

  await UtilisationAssumption.create({
    userId: USER_ID,
    msn: "4950",
    fromDate: effectiveDate,
    toDate: effectiveDate,
    hours: 3.5,
    cycles: 1,
  });

  await RotableMovement.create({
    userId: USER_ID,
    label: "Engine change",
    date: movementDate,
    pn: "CFM56",
    msn: "4950",
    acftReg: "VT-AAA",
    position: "#2",
    removedSN: "635800",
    installedSN: "721576",
  });

  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  const movementDayRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-19", msnEsn: "721576" },
  }, movementDayRes);

  const effectiveDayRes = createMockResponse();
  await maintenanceController.getMaintenanceDashboard({
    user: { id: USER_ID },
    query: { date: "2026-05-20", msnEsn: "721576" },
  }, effectiveDayRes);

  const movementDay = movementDayRes.body.data.maintenanceData[0];
  const effectiveDay = effectiveDayRes.body.data.maintenanceData[0];

  assert.equal(movementDayRes.statusCode, 200);
  assert.equal(effectiveDayRes.statusCode, 200);
  assert.equal(movementDay.tsn, 2000);
  assert.equal(movementDay.csn, 1000);
  assert.equal(effectiveDay.tsn, 2003.5);
  assert.equal(effectiveDay.csn, 1001);
  assert.equal(effectiveDay.tsr, 403.5);
  assert.equal(effectiveDay.csr, 201);
});

test("deleting rotable movement restores on-wing ownership and recomputes utilisation", async () => {
  const resetDate = utcDate(2026, 4, 15);
  const movementDate = utcDate(2026, 5, 10);
  const effectiveDate = utcDate(2026, 5, 11);

  await seedFlightDays([resetDate, effectiveDate]);
  await seedFleetAsset({
    msn: 5340,
    regn: "VT-AAA",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 12, 31),
  });

  await AircraftOnwing.create({
    userId: USER_ID,
    date: utcDate(2026, 4, 1),
    msn: "5340",
    pos1Esn: "635799",
    pos2Esn: "635800",
  });

  await MaintenanceReset.insertMany([
    {
      userId: USER_ID,
      date: resetDate,
      msnEsn: "635799",
      pn: "CFM56",
      snBn: "635799",
      tsn: 100,
      csn: 40,
      dsn: 31,
      timeMetric: "BH",
    },
    {
      userId: USER_ID,
      date: resetDate,
      msnEsn: "721576",
      pn: "CFM56",
      snBn: "721576",
      tsn: 5000,
      csn: 3000,
      dsn: 1000,
      timeMetric: "BH",
    },
  ]);

  await seedAssignment({
    date: effectiveDate,
    flightNumber: "FL11",
    msn: 5340,
    registration: "VT-AAA",
    bh: 4,
  });

  const saveRotableRes = createMockResponse();
  await maintenanceController.bulkSaveRotables({
    user: { id: USER_ID },
    body: {
      rotablesData: [
        {
          label: "Engine change",
          date: movementDate.toISOString().slice(0, 10),
          pn: "CFM56",
          msn: "5340",
          acftRegn: "VT-AAA",
          position: "#1",
          removedSN: "635799",
          installedSN: "721576",
        },
      ],
    },
  }, saveRotableRes);

  await maintenanceController.computeMaintenanceLogic({ user: { id: USER_ID } }, createMockResponse());

  const savedMovement = await RotableMovement.findOne({
    userId: USER_ID,
    msn: "5340",
    position: "#1",
    installedSN: "721576",
  }).lean();

  const deleteRotableRes = createMockResponse();
  await maintenanceController.deleteRotable({
    user: { id: USER_ID },
    params: { id: String(savedMovement._id) },
  }, deleteRotableRes);

  const restoredOnwing = await AircraftOnwing.findOne({
    userId: USER_ID,
    msn: "5340",
    date: effectiveDate,
  }).lean();
  const removedEngineUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: effectiveDate,
    msnEsn: "635799",
    pn: "CFM56",
    snBn: "635799",
  }).lean();
  const deletedInstalledEngineUtil = await Utilisation.findOne({
    userId: USER_ID,
    date: effectiveDate,
    msnEsn: "721576",
    pn: "CFM56",
    snBn: "721576",
  }).lean();

  assert.equal(saveRotableRes.statusCode, 200);
  assert.equal(deleteRotableRes.statusCode, 200);
  assert.equal(restoredOnwing?.pos1Esn, "635799");
  assert.equal(removedEngineUtil?.tsn, 104);
  assert.equal(removedEngineUtil?.csn, 41);
  assert.equal(deletedInstalledEngineUtil?.tsn, 5000);
  assert.equal(deletedInstalledEngineUtil?.csn, 3000);
});
