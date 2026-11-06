const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const net = require("node:net");
const mongoose = require("mongoose");
const Flight = require("../model/flight");
const { getActualDateKeys, getUtilisationFlights } = require("../utils/utilisationFlights");

const USER_ID = "65f000000000000000000001";
const OTHER_USER_ID = "65f000000000000000000002";
const from = "2026-10-01";
const to = "2026-10-31";
const day = (number) => new Date(Date.UTC(2026, 9, number));
const aircraft = (msn) => ({ msn, registration: `VT-${msn}` });
let mongodProcess;
let dbPath;

before(async () => {
  const port = await new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const selected = probe.address().port;
      probe.close(() => resolve(selected));
    });
  });
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-utilisation-mongo-"));
  const binary = fs.existsSync("/usr/bin/mongod") ? "/usr/bin/mongod" : "mongod";
  mongodProcess = spawn(binary, [
    "--dbpath", dbPath, "--port", String(port), "--bind_ip", "127.0.0.1", "--quiet",
    ...(process.platform === "win32" ? [] : ["--nounixsocket"]),
  ], { stdio: "ignore", windowsHide: true });
  await Promise.race([
    mongoose.connect(`mongodb://127.0.0.1:${port}/airline_utilisation_test`, { serverSelectionTimeoutMS: 15000 }),
    new Promise((resolve, reject) => {
      mongodProcess.once("error", reject);
      mongodProcess.once("exit", (code) => reject(new Error(`MongoDB exited (${code})`)));
    }),
  ]);
  await Flight.init();
});

after(async () => {
  await mongoose.disconnect();
  if (mongodProcess?.pid && mongodProcess.exitCode === null && mongodProcess.signalCode === null) {
    await new Promise((resolve) => {
      mongodProcess.once("exit", resolve);
      mongodProcess.kill("SIGKILL");
    });
  }
  if (dbPath) {
    const target = fs.realpathSync(dbPath);
    assert.equal(path.dirname(target).toLowerCase(), fs.realpathSync(os.tmpdir()).toLowerCase());
    assert.ok(path.basename(target).startsWith("airline-utilisation-mongo-"));
    fs.rmSync(target, { recursive: true, force: true });
  }
});

beforeEach(async () => {
  await Flight.collection.deleteMany({});
});

async function seedFlight(date, category, overrides = {}) {
  return Flight.create({ userId: USER_ID, date, category, flight: "AB123", aircraft: aircraft(100), bh: 2, ...overrides });
}

test("October switches only covered aircraft/dates to Actual and never returns Invoiced", async () => {
  for (let date = 1; date <= 31; date += 1) {
    await seedFlight(day(date), "Planned");
    await seedFlight(day(date), "Planned", { aircraft: aircraft(200) });
    await seedFlight(day(date), "Invoiced", { bh: 999 });
  }
  for (const date of [3, 4]) {
    await seedFlight(day(date), "Actual", { bh: 3 });
  }
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.equal(flights.length, 62);
  for (let date = 1; date <= 31; date += 1) {
    const rows = flights.filter((flight) => flight.date.getUTCDate() === date);
    assert.equal(rows.length, 2);
    assert.equal(rows.find((flight) => flight.aircraft.msn === 100).category, [3, 4].includes(date) ? "Actual" : "Planned");
    assert.equal(rows.find((flight) => flight.aircraft.msn === 200).category, "Planned");
  }
  assert.ok(flights.every((flight) => flight.category !== "Invoiced"));
  assert.equal(flights.reduce((sum, flight) => sum + flight.bh, 0), 126);
});

test("getActualDateKeys returns distinct sorted UTC dates for this user only", async () => {
  await seedFlight(day(4), "Actual");
  await seedFlight(day(3), "Actual");
  await seedFlight(new Date("2026-10-03T23:59:59Z"), "Actual");
  await seedFlight(day(5), "Invoiced");
  await seedFlight(day(6), "Planned");
  await seedFlight(day(7), "Actual", { userId: OTHER_USER_ID });
  await Flight.collection.insertOne({ userId: USER_ID, category: "Actual", date: null });
  assert.deepEqual(await getActualDateKeys(USER_ID), ["2026-10-03", "2026-10-04"]);
  assert.deepEqual(await getActualDateKeys("no-flights"), []);
});

test("Actual coverage replaces every Planned flight for the aircraft/day, not just matching flight numbers", async () => {
  await seedFlight(day(3), "Planned", { flight: "P1" });
  await seedFlight(day(3), "Planned", { flight: "P2" });
  await seedFlight(day(3), "Actual", { flight: "A1" });
  await seedFlight(day(3), "Actual", { flight: "A2" });
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.deepEqual(flights.map((flight) => flight.flight), ["A1", "A2"]);
});

test("bounds include the entire final UTC date, and a timed Actual replaces midnight Planned", async () => {
  await seedFlight(day(2), "Planned");
  await seedFlight(day(3), "Planned");
  await seedFlight(new Date("2026-10-03T23:59:59.999Z"), "Actual");
  await seedFlight(day(4), "Actual");
  const flights = await getUtilisationFlights({ userId: USER_ID, from: "2026-10-03", to: "2026-10-03" });
  assert.equal(flights.length, 1);
  assert.equal(flights[0].category, "Actual");
});

test("legacy flights saved without a category count as Planned utilisation", async () => {
  await Flight.collection.insertOne({ userId: USER_ID, date: day(5), flight: "L1", aircraft: aircraft(100), fh: 2 });
  await Flight.collection.insertOne({ userId: USER_ID, date: day(6), flight: "L2", aircraft: aircraft(100), fh: 2 });
  await seedFlight(day(6), "Actual", { flight: "A1" });
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.deepEqual(flights.map((flight) => flight.flight), ["L1", "A1"]);
});

test("other users' Actual rows do not suppress this user's Planned flights", async () => {
  await seedFlight(day(3), "Planned");
  await seedFlight(day(3), "Actual", { userId: OTHER_USER_ID });
  await seedFlight(day(4), "Invoiced");
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.equal(flights.length, 1);
  assert.equal(flights[0].category, "Planned");
});

test("MSN matches across Master/nested fields and registration changes", async () => {
  await seedFlight(day(3), "Planned", { aircraft: {}, msn: "00100" });
  await seedFlight(day(3), "Planned", { aircraft: { msn: 100, registration: "OLD" } });
  await seedFlight(day(3), "Actual", { aircraft: { msn: 100, registration: "NEW" } });
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.deepEqual(flights.map((flight) => flight.category), ["Actual"]);
});

test("registration fallback is trimmed and case-insensitive when an MSN is absent", async () => {
  await seedFlight(day(3), "Planned", { aircraft: { registration: " vt-100 " } });
  await seedFlight(day(3), "Actual");
  await seedFlight(day(4), "Planned");
  await seedFlight(day(4), "Actual", { aircraft: { registration: " vt-100 " } });
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to });
  assert.deepEqual(flights.map((flight) => flight.category), ["Actual", "Actual"]);
});

test("different known MSNs sharing a registration are treated as different aircraft", async () => {
  await seedFlight(day(3), "Planned", { aircraft: { msn: 200, registration: "VT-100" } });
  await seedFlight(day(3), "Actual");
  assert.equal((await getUtilisationFlights({ userId: USER_ID, from, to })).length, 2);
});

test("unassigned Planned rows remain, while unidentified Actual rows fail explicitly", async () => {
  await seedFlight(day(3), "Planned", { aircraft: {} });
  await seedFlight(day(3), "Actual");
  assert.equal((await getUtilisationFlights({ userId: USER_ID, from, to })).length, 2);
  await seedFlight(day(4), "Actual", { aircraft: {} });
  await assert.rejects(getUtilisationFlights({ userId: USER_ID, from, to }), /must identify its aircraft/);
});

test("select can omit all coverage fields without changing which rows are selected", async () => {
  await seedFlight(day(3), "Planned", { bh: 2 });
  await seedFlight(day(3), "Planned", { aircraft: aircraft(200), bh: 4 });
  await seedFlight(day(3), "Actual", { bh: 3 });
  const projected = await getUtilisationFlights({ userId: USER_ID, from, to, select: "bh -_id" });
  assert.deepEqual(projected.sort((a, b) => a.bh - b.bh), [{ bh: 3 }, { bh: 4 }]);
  const flights = await getUtilisationFlights({ userId: USER_ID, from, to,
    select: { date: 0, category: 0, aircraft: 0, msn: 0 } });
  assert.deepEqual(flights.map((flight) => flight.bh).sort((a, b) => a - b), [3, 4]);
  assert.ok(flights.every((flight) => flight.date === undefined && flight.aircraft === undefined));
});

test("empty windows, no Actual rows, and open bounds are supported", async () => {
  assert.deepEqual(await getUtilisationFlights({ userId: USER_ID, from, to }), []);
  await seedFlight(day(1), "Planned");
  await seedFlight(day(31), "Planned");
  assert.equal((await getUtilisationFlights({ userId: USER_ID })).length, 2);
  assert.equal((await getUtilisationFlights({ userId: USER_ID, from: "2026-10-31" })).length, 1);
  assert.equal((await getUtilisationFlights({ userId: USER_ID, to: "2026-10-01" })).length, 1);
});

test("missing user and invalid or reversed date bounds fail before querying", async () => {
  await assert.rejects(getActualDateKeys(), /userId is required/);
  await assert.rejects(getUtilisationFlights(), /userId is required/);
  await assert.rejects(getUtilisationFlights({ userId: USER_ID, from: "invalid" }), /from must be a valid date/);
  await assert.rejects(getUtilisationFlights({ userId: USER_ID, to: "invalid" }), /to must be a valid date/);
  await assert.rejects(getUtilisationFlights({ userId: USER_ID, from: to, to: from }), /from must be on or before to/);
});
