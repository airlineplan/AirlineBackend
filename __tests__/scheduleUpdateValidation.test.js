const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const mongoose = require("mongoose");
const xlsx = require("xlsx");

const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const Flight = require("../model/flight");
const Assignment = require("../model/assignment");
const Fleet = require("../model/fleet");
const { uploadAssignments } = require("../controller/assignmentController");
const { buildAssignmentSyncPlan } = require("../utils/assignmentSync");

const USER_ID = "test-user";
const BASE_FLIGHT = "AB123";
const BASE_VARIANT = "A320";
const BASE_DEP = "DEL";
const BASE_ARR = "BOM";
const BASE_STD = "08:00";
const BASE_BT = "02:00";
const BASE_STA = "10:00";
const BASE_FROM = utcDate(2026, 4, 6);
const BASE_TO = utcDate(2026, 4, 12);
const BASE_DOW = "1357";

let mongodProcess;
let dbPath;
let port;
let dbName;

function utcDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
}

function formatDate(date) {
  return new Date(date).toISOString().slice(0, 10);
}

function expectedDates(fromDate, toDate, dowString) {
  const allowed = new Set(String(dowString).split("").map(Number));
  const dates = [];
  const cursor = new Date(fromDate);
  cursor.setUTCHours(0, 0, 0, 0);
  const end = new Date(toDate);
  end.setUTCHours(0, 0, 0, 0);

  while (cursor <= end) {
    const jsDow = cursor.getUTCDay();
    const scheduleDow = jsDow === 0 ? 7 : jsDow;
    if (allowed.has(scheduleDow)) {
      dates.push(formatDate(cursor));
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return dates;
}

function collectFlightDates(flights) {
  return flights
    .slice()
    .sort((a, b) => new Date(a.date) - new Date(b.date))
    .map((flight) => formatDate(flight.date));
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
  dbName = `airline_test_${Date.now()}`;
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-mongo-"));

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
    "mongod",
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

async function seedNetwork(overrides = {}) {
  const data = await Data.create({
    flight: overrides.flight || BASE_FLIGHT,
    depStn: overrides.depStn || BASE_DEP,
    std: overrides.std || BASE_STD,
    bt: overrides.bt || BASE_BT,
    sta: overrides.sta || BASE_STA,
    arrStn: overrides.arrStn || BASE_ARR,
    variant: overrides.variant || BASE_VARIANT,
    effFromDt: overrides.effFromDt || BASE_FROM,
    effToDt: overrides.effToDt || BASE_TO,
    dow: overrides.dow || BASE_DOW,
    domINTL: overrides.domINTL || "dom",
    userTag1: overrides.userTag1 || "Tag-1",
    userTag2: overrides.userTag2 || "Tag-2",
    remarks1: overrides.remarks1 || "Remark-1",
    remarks2: overrides.remarks2 || "Remark-2",
    userId: USER_ID,
  });
  const networkId = data._id.toString();

  const sector = await Sector.create({
    sector1: overrides.depStn || BASE_DEP,
    sector2: overrides.arrStn || BASE_ARR,
    acftType: overrides.acftType || BASE_VARIANT,
    variant: overrides.variant || BASE_VARIANT,
    bt: overrides.bt || BASE_BT,
    gcd: overrides.gcd || "1000",
    paxCapacity: overrides.paxCapacity || "150",
    CargoCapT: overrides.CargoCapT || "20",
    paxLF: overrides.paxLF || "80",
    cargoLF: overrides.cargoLF || "70",
    fromDt: overrides.effFromDt || BASE_FROM,
    toDt: overrides.effToDt || BASE_TO,
    flight: overrides.flight || BASE_FLIGHT,
    std: overrides.std || BASE_STD,
    sta: overrides.sta || BASE_STA,
    dow: overrides.dow || BASE_DOW,
    domINTL: overrides.domINTL || "dom",
    userTag1: overrides.userTag1 || "Tag-1",
    userTag2: overrides.userTag2 || "Tag-2",
    remarks1: overrides.remarks1 || "Remark-1",
    remarks2: overrides.remarks2 || "Remark-2",
    userId: USER_ID,
    networkId,
  });

  return { data, sector, networkId };
}

async function seedValidAssignment({ date, flightNumber = BASE_FLIGHT, regn = "VT-ABC", msn = 4120 }) {
  await Fleet.create({
    userId: USER_ID,
    category: "Aircraft",
    type: BASE_VARIANT,
    variant: BASE_VARIANT,
    sn: String(msn),
    regn,
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 4, 30),
  });

  await Assignment.collection.insertOne({
    userId: USER_ID,
    date,
    flightNumber,
    aircraft: {
      registration: regn,
      msn,
    },
    isValid: true,
    validationErrors: [],
    removedReason: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  });
}

function createAssignmentWorkbook(rows) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "assignment-upload-"));
  const filePath = path.join(tempDir, "assignments.xlsx");
  const workbook = xlsx.utils.book_new();
  const worksheet = xlsx.utils.json_to_sheet(rows);
  xlsx.utils.book_append_sheet(workbook, worksheet, "Assignments");
  xlsx.writeFile(workbook, filePath);
  return { tempDir, filePath };
}

function createMockResponse() {
  return {
    statusCode: 200,
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

test("generates one flight occurrence per valid date in the effective range", async () => {
  const { networkId } = await seedNetwork();

  const flights = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  const expected = expectedDates(BASE_FROM, BASE_TO, BASE_DOW);

  assert.equal(flights.length, expected.length);
  assert.deepEqual(collectFlightDates(flights), expected);
  assert.ok(flights.every((flight) => flight.flight === BASE_FLIGHT));
});

test("sector updates flow through to linked flight rows without changing the occurrence count", async () => {
  const { networkId, sector } = await seedNetwork();
  const beforeFlights = await Flight.find({ networkId }).lean();

  await Sector.findByIdAndUpdate(
    sector._id,
    {
      $set: {
        acftType: "A321",
        gcd: "1200",
        paxCapacity: "180",
        CargoCapT: "30",
        paxLF: "85",
        cargoLF: "75",
      },
    },
    { new: true }
  );

  const afterFlights = await Flight.find({ networkId }).sort({ date: 1 }).lean();

  assert.equal(afterFlights.length, beforeFlights.length);
  assert.equal(afterFlights[0].acftType, "A321");
  assert.equal(afterFlights[0].seats, 180);
  assert.equal(afterFlights[0].dist, 1200);
  assert.equal(afterFlights[0].CargoCapT, 30);
});

test("schedule field updates regenerate the flight rows and delete existing assignments", async () => {
  const { data, networkId } = await seedNetwork();
  const originalFlights = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  await seedValidAssignment({ date: originalFlights[1].date });

  const nextFrom = utcDate(2026, 4, 7);
  const nextTo = utcDate(2026, 4, 11);
  const nextDow = "246";

  await Data.findByIdAndUpdate(
    data._id,
    {
      $set: {
        effFromDt: nextFrom,
        effToDt: nextTo,
        dow: nextDow,
      },
    },
    { new: true, runValidators: true }
  );

  const flights = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  const expected = expectedDates(nextFrom, nextTo, nextDow);
  const assignments = await Assignment.find({ userId: USER_ID }).lean();

  assert.equal(flights.length, expected.length);
  assert.deepEqual(collectFlightDates(flights), expected);
  assert.equal(assignments.length, 0);
  assert.ok(!flights.some((flight) => formatDate(flight.date) === formatDate(originalFlights[0].date)));
});

test("schedule field updates keep sector-derived numeric values on regenerated flights", async () => {
  const { data, networkId } = await seedNetwork({
    paxCapacity: "180",
    CargoCapT: "30",
    gcd: "1200",
    paxLF: "85",
    cargoLF: "75",
  });

  await Data.findByIdAndUpdate(
    data._id,
    {
      $set: {
        effFromDt: utcDate(2026, 4, 7),
        effToDt: utcDate(2026, 4, 11),
        dow: "246",
      },
    },
    { new: true, runValidators: true }
  );

  const flights = await Flight.find({ networkId }).sort({ date: 1 }).lean();

  assert.ok(flights.length > 0);
  assert.equal(flights[0].seats, 180);
  assert.equal(flights[0].CargoCapT, 30);
  assert.equal(flights[0].dist, 1200);
  assert.equal(flights[0].pax, 153);
  assert.equal(flights[0].CargoT, 22.5);
  assert.equal(flights[0].ask, 216000);
  assert.equal(flights[0].rsk, 183600);
  assert.equal(flights[0].cargoAtk, 36000);
  assert.equal(flights[0].cargoRtk, 27000);
});

test("non-schedule field updates keep the same flight rows and revalidate assignments", async () => {
  const { data, networkId } = await seedNetwork();
  const flightsBefore = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  await seedValidAssignment({ date: flightsBefore[1].date });

  await Data.findByIdAndUpdate(
    data._id,
    {
      $set: {
        bt: "03:00",
        remarks1: "Updated remark",
      },
    },
    { new: true, runValidators: true }
  );

  const flightsAfter = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  const assignment = await Assignment.findOne({ userId: USER_ID }).lean();

  assert.equal(flightsAfter.length, flightsBefore.length);
  assert.equal(flightsAfter[0].bt, "03:00");
  assert.equal(flightsAfter[0].remarks1, "Updated remark");
  assert.equal(assignment?.isValid, true);
  assert.equal(flightsAfter[1].aircraft?.registration, "VT-ABC");
  assert.equal(flightsAfter[1].aircraft?.msn, 4120);
});

test("assignment sync rejects ACFT values that are not fleet registrations", async () => {
  await seedNetwork();
  await Fleet.create({
    userId: USER_ID,
    category: "Aircraft",
    type: BASE_VARIANT,
    variant: BASE_VARIANT,
    sn: "4120",
    regn: "VT-AAB",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 6, 30),
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [
      {
        assignDate: BASE_FROM,
        dateKey: formatDate(BASE_FROM),
        flight: BASE_FLIGHT,
        acft: "A320",
      },
    ],
  });

  assert.equal(result.assignmentBulkOps.length, 1);
  assert.equal(result.flightBulkOps.length, 1);
  assert.equal(result.diagnostics.rejections.missingFromFleetDB, 1);
  assert.equal(result.diagnostics.rejectedRows[0].acft, "A320");
  assert.match(result.diagnostics.rejectedRows[0].errors[0], /not found in Fleet master/i);
});

test("assignment sync treats base variant before hyphen as a match", async () => {
  await seedNetwork({ variant: "A320" });
  await Fleet.create({
    userId: USER_ID,
    category: "Aircraft",
    type: "A320ceo",
    variant: "A320-214",
    sn: "4120",
    regn: "VT-AAB",
    entry: utcDate(2026, 4, 1),
    exit: utcDate(2026, 6, 30),
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [
      {
        assignDate: BASE_FROM,
        dateKey: formatDate(BASE_FROM),
        flight: BASE_FLIGHT,
        acft: "VT-AAB",
      },
    ],
  });

  assert.equal(result.diagnostics.rejections.variantMismatches, 0);
  assert.equal(result.assignmentBulkOps.length, 1);
  assert.equal(result.flightBulkOps.length, 1);
  assert.equal(result.assignmentBulkOps[0].updateOne.update.$set.isValid, true);
});

test("assignment upload rejects the whole file when any row fails validation", async () => {
  await seedNetwork({ variant: BASE_VARIANT });
  await Fleet.create([
    {
      userId: USER_ID,
      category: "Aircraft",
      type: BASE_VARIANT,
      variant: BASE_VARIANT,
      sn: "4120",
      regn: "VT-AAA",
      entry: utcDate(2026, 4, 1),
      exit: utcDate(2026, 6, 30),
    },
    {
      userId: USER_ID,
      category: "Aircraft",
      type: "B737",
      variant: "B737",
      sn: "7370",
      regn: "VT-BAD",
      entry: utcDate(2026, 4, 1),
      exit: utcDate(2026, 6, 30),
    },
  ]);

  const { tempDir, filePath } = createAssignmentWorkbook([
    { Date: "06-Apr-26", "Flight #": BASE_FLIGHT, ACFT: "VT-AAA" },
    { Date: "08-Apr-26", "Flight #": BASE_FLIGHT, ACFT: "VT-BAD" },
  ]);
  const res = createMockResponse();

  try {
    await uploadAssignments(
      {
        user: { id: USER_ID },
        file: { path: filePath },
      },
      res
    );
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }

  const assignments = await Assignment.find({ userId: USER_ID }).lean();
  const flights = await Flight.find({ userId: USER_ID }).lean();

  assert.equal(res.statusCode, 422);
  assert.equal(res.body?.success, false);
  assert.equal(res.body?.diagnostics?.rejections?.variantMismatches, 1);
  assert.equal(assignments.length, 0);
  assert.ok(flights.every((flight) => !flight.aircraft?.registration));
});

test("assignment and flight tenant indexes reject duplicate rows for the same user scope", async () => {
  await Assignment.syncIndexes();
  await Flight.syncIndexes();

  const sharedDate = utcDate(2026, 4, 6);

  await Assignment.create({
    userId: USER_ID,
    date: sharedDate,
    flightNumber: BASE_FLIGHT,
    aircraft: { registration: "VT-AAB", msn: 4120 },
  });

  await assert.rejects(
    () =>
      Assignment.create({
        userId: USER_ID,
        date: sharedDate,
        flightNumber: BASE_FLIGHT,
        aircraft: { registration: "VT-AAB", msn: 4120 },
      }),
    /duplicate key/i
  );

  await Flight.create({
    userId: USER_ID,
    networkId: "network-1",
    date: sharedDate,
    flight: BASE_FLIGHT,
  });

  await assert.rejects(
    () =>
      Flight.create({
        userId: USER_ID,
        networkId: "network-1",
        date: sharedDate,
        flight: BASE_FLIGHT,
      }),
    /duplicate key/i
  );
});

test("mixed schedule and non-schedule updates follow the schedule branch and regenerate from the new values", async () => {
  const { data, networkId } = await seedNetwork();
  const originalFlights = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  await seedValidAssignment({ date: originalFlights[2].date });

  const nextDow = "24";

  await Data.findByIdAndUpdate(
    data._id,
    {
      $set: {
        dow: nextDow,
        remarks1: "Mixed update remark",
      },
    },
    { new: true, runValidators: true }
  );

  const flights = await Flight.find({ networkId }).sort({ date: 1 }).lean();
  const assignments = await Assignment.find({ userId: USER_ID }).lean();
  const expected = expectedDates(BASE_FROM, BASE_TO, nextDow);

  assert.equal(flights.length, expected.length);
  assert.deepEqual(collectFlightDates(flights), expected);
  assert.equal(assignments.length, 0);
  assert.equal(flights[0].remarks1, "Mixed update remark");
});

test("sector save tolerates blank load factor values without writing NaN into flights", async () => {
  const data = await Data.create({
    flight: BASE_FLIGHT,
    depStn: BASE_DEP,
    std: BASE_STD,
    bt: BASE_BT,
    sta: BASE_STA,
    arrStn: BASE_ARR,
    variant: BASE_VARIANT,
    effFromDt: BASE_FROM,
    effToDt: BASE_TO,
    dow: BASE_DOW,
    domINTL: "dom",
    userId: USER_ID,
  });

  await Sector.create({
    sector1: BASE_DEP,
    sector2: BASE_ARR,
    acftType: BASE_VARIANT,
    variant: BASE_VARIANT,
    bt: BASE_BT,
    gcd: "1000",
    paxCapacity: "150",
    CargoCapT: "20",
    paxLF: "",
    cargoLF: "",
    fromDt: BASE_FROM,
    toDt: BASE_TO,
    flight: BASE_FLIGHT,
    std: BASE_STD,
    sta: BASE_STA,
    dow: BASE_DOW,
    domINTL: "dom",
    userId: USER_ID,
    networkId: data._id.toString(),
  });

  const flights = await Flight.find({ networkId: data._id.toString() }).lean();

  assert.ok(flights.length > 0);
  assert.ok(flights.every((flight) => Number.isFinite(flight.pax)));
  assert.ok(flights.every((flight) => Number.isFinite(flight.CargoT)));
  assert.ok(flights.every((flight) => Number.isFinite(flight.cargoRtk)));
});
