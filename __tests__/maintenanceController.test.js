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

async function seedAssignment({ date, flightNumber, msn, registration, bh = 0, fh = 0 }) {
  await Assignment.create({
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
  });
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
