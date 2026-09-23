const assert = require("node:assert/strict");
const { afterEach, test } = require("node:test");

const Assignment = require("../model/assignment");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const GroundDay = require("../model/groundDay");
const Station = require("../model/stationSchema");
const User = require("../model/userSchema");
const { buildAssignmentSyncPlan } = require("../utils/assignmentSync");

const USER_ID = "65f000000000000000000001";
const OTHER_USER_ID = "65f000000000000000000002";
const originals = new Map();

const date = (day) => new Date(Date.UTC(2026, 3, day));
const key = (day) => `2026-04-${String(day).padStart(2, "0")}`;
const query = (value) => ({
  select() { return this; },
  sort() { return this; },
  lean() { return Promise.resolve(value); },
});

const saveOriginal = (model, method) => {
  const mapKey = `${model.modelName}:${method}`;
  if (!originals.has(mapKey)) originals.set(mapKey, { model, method, value: model[method] });
};

const matchesRegexList = (value, list) => (
  !Array.isArray(list) || list.some((matcher) => matcher instanceof RegExp && matcher.test(String(value || "")))
);

const installValidationData = ({
  flights = [],
  fleet = [],
  stations = [],
  users = [{ _id: USER_ID, hometimeZone: "UTC+0:00" }],
  groundDays = [],
}) => {
  for (const [model, methods] of [
    [Flight, ["find", "aggregate"]],
    [Fleet, ["find"]],
    [GroundDay, ["find"]],
    [Station, ["find"]],
    [User, ["findById"]],
  ]) {
    methods.forEach((method) => saveOriginal(model, method));
  }

  Flight.find = (filter) => query(flights.filter((flight) => (
    flight.userId === String(filter.userId) &&
    (!filter.date || (flight.date >= filter.date.$gte && flight.date <= filter.date.$lte)) &&
    matchesRegexList(flight.flight, filter.flight?.$in)
  )));
  Flight.aggregate = async (pipeline) => {
    const requestedUserId = pipeline[0].$match.userId;
    const dates = flights.filter((flight) => flight.userId === requestedUserId).map((flight) => flight.date);
    return dates.length === 0 ? [] : [{
      _id: null,
      minDate: new Date(Math.min(...dates.map((value) => value.getTime()))),
      maxDate: new Date(Math.max(...dates.map((value) => value.getTime()))),
    }];
  };
  Fleet.find = (filter) => query(fleet.filter((asset) => (
    asset.userId === String(filter.userId) &&
    asset.category === filter.category &&
    matchesRegexList(asset.regn, filter.regn?.$in)
  )));
  GroundDay.find = (filter) => query(groundDays.filter((row) => row.userId === String(filter.userId)));
  Station.find = (filter) => query(stations.filter((row) => row.userId === String(filter.userId)));
  User.findById = (userId) => query(users.find((row) => String(row._id) === String(userId)) || null);
};

afterEach(() => {
  for (const { model, method, value } of originals.values()) model[method] = value;
  originals.clear();
});

const makeStation = (stationName, stdtz, userId = USER_ID, overrides = {}) => ({
  userId,
  stationName,
  stdtz,
  dsttz: overrides.dsttz || stdtz,
  nextDSTStart: overrides.nextDSTStart || "",
  nextDSTEnd: overrides.nextDSTEnd || "",
});

const makeFlight = ({
  day,
  flight,
  std,
  sta,
  depStn = "DEP",
  arrStn = depStn,
  variant = "A320",
  userId = USER_ID,
}) => ({
  _id: `${userId}-${day}-${flight}`,
  userId,
  date: date(day),
  flight,
  std,
  sta,
  depStn,
  arrStn,
  variant,
});

const makeAircraft = ({
  regn = "VT-AAA",
  variant = "A320",
  entry = date(1),
  exit = date(30),
  userId = USER_ID,
  sn = "1001",
} = {}) => ({
  userId,
  category: "Aircraft",
  regn,
  variant,
  entry,
  exit,
  sn,
});

const row = (day, flight, acft = "VT-AAA", sourceOrder) => ({
  assignDate: date(day),
  dateKey: key(day),
  flight,
  acft,
  ...(sourceOrder === undefined ? {} : { sourceOrder }),
});

test("master range rejects dates outside its endpoints while retaining both boundaries", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 10, flight: "FIRST", std: "08:00", sta: "09:00" }),
      makeFlight({ day: 30, flight: "LAST", std: "10:00", sta: "11:00" }),
    ],
    fleet: [makeAircraft()],
    stations: [makeStation("DEP", "UTC+0:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(9, "BEFORE"), row(10, "FIRST"), row(30, "LAST"), row(31, "AFTER")],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.rejections.outsideMasterDateRange, 2);
  assert.equal(result.diagnostics.successfullyAssigned, 2);
  assert.deepEqual(
    result.assignmentBulkOps.filter((op) => op.updateOne).map((op) => op.updateOne.filter.flightNumber),
    ["FIRST", "LAST"]
  );
});

test("fleet existence, inclusive entry/exit dates, and variant matching remain enforced", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 9, flight: "PRE", std: "08:00", sta: "09:00" }),
      makeFlight({ day: 10, flight: "ENTRY", std: "10:00", sta: "11:00" }),
      makeFlight({ day: 20, flight: "BADVAR", std: "12:00", sta: "13:00" }),
      makeFlight({ day: 20, flight: "MISSING", std: "13:00", sta: "14:00" }),
      makeFlight({ day: 21, flight: "POST", std: "13:00", sta: "14:00" }),
      makeFlight({ day: 30, flight: "EXIT", std: "14:00", sta: "15:00" }),
    ],
    fleet: [
      makeAircraft({ entry: date(10), exit: date(30) }),
      makeAircraft({ regn: "VT-BAD", variant: "B737", sn: "2002" }),
      makeAircraft({ regn: "VT-SHORT", entry: date(10), exit: date(20), sn: "3003" }),
    ],
    stations: [makeStation("DEP", "UTC+0:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [
      row(9, "PRE"),
      row(10, "ENTRY"),
      row(20, "BADVAR", "VT-BAD"),
      row(30, "EXIT"),
      row(20, "MISSING", "VT-MISSING"),
      row(21, "POST", "VT-SHORT"),
    ],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.duplicateComboCount, 0);
  assert.equal(result.diagnostics.rejections.preEntryDates, 1);
  assert.equal(result.diagnostics.rejections.postExitDates, 1);
  assert.equal(result.diagnostics.rejections.variantMismatches, 1);
  assert.equal(result.diagnostics.rejections.missingFromFleetDB, 1);
  assert.equal(result.diagnostics.successfullyAssigned, 2);
});

test("a later duplicate never replaces the first spreadsheet row even when the first is invalid", async () => {
  installValidationData({
    flights: [makeFlight({ day: 10, flight: "AB123", std: "08:00", sta: "09:00" })],
    fleet: [makeAircraft()],
    stations: [makeStation("DEP", "UTC+0:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(10, "AB123", "VT-MISSING"), row(10, "AB123", "VT-AAA")],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.duplicateComboCount, 1);
  assert.equal(result.diagnostics.rejections.missingFromFleetDB, 1);
  assert.equal(result.diagnostics.successfullyAssigned, 0);
  assert.ok(result.assignmentBulkOps[0].deleteOne);
});

test("spreadsheet order controls overlap precedence and back-to-back rows remain valid", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 15, flight: "LATE-DATE-FIRST", std: "10:00", sta: "12:00" }),
      makeFlight({ day: 10, flight: "EARLY-DATE-SECOND", std: "11:00", sta: "13:00" }),
      makeFlight({ day: 15, flight: "BACK-TO-BACK", std: "12:00", sta: "14:00" }),
    ],
    fleet: [makeAircraft()],
    stations: [makeStation("DEP", "UTC+0:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(15, "LATE-DATE-FIRST"), row(10, "EARLY-DATE-SECOND"), row(15, "BACK-TO-BACK")],
    includeGroundDays: false,
  });

  // The dates differ, so only the same-date back-to-back pair shares a day.
  assert.equal(result.diagnostics.rejections.acftOverlaps, 0);
  assert.equal(result.diagnostics.successfullyAssigned, 3);
});

test("same-aircraft overlaps across different source dates reject the later spreadsheet row", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 10, flight: "ROW1", std: "23:00", sta: "02:00", depStn: "PLUS14" }),
      makeFlight({ day: 9, flight: "ROW2", std: "22:00", sta: "01:00", depStn: "MINUS12" }),
    ],
    fleet: [makeAircraft()],
    stations: [
      makeStation("PLUS14", "UTC+14:00"),
      makeStation("MINUS12", "UTC-12:00"),
    ],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(10, "ROW1"), row(9, "ROW2")],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.rejections.acftOverlaps, 1);
  assert.equal(result.diagnostics.successfullyAssigned, 1);
  assert.equal(result.assignmentBulkOps[0].updateOne.filter.flightNumber, "ROW1");
  assert.equal(result.assignmentBulkOps[1].deleteOne.filter.flightNumber, "ROW2");
});

test("timezone normalization avoids a false overlap from similar raw local clocks", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 10, flight: "EAST", std: "10:00", sta: "12:00", depStn: "EAST" }),
      makeFlight({ day: 10, flight: "WEST", std: "11:00", sta: "13:00", depStn: "WEST" }),
    ],
    fleet: [makeAircraft()],
    stations: [makeStation("EAST", "UTC+10:00"), makeStation("WEST", "UTC-5:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(10, "EAST"), row(10, "WEST")],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.rejections.acftOverlaps, 0);
  assert.equal(result.diagnostics.successfullyAssigned, 2);
});

test("malformed or cross-tenant timezone/fleet data cannot validate an assignment", async () => {
  installValidationData({
    flights: [
      makeFlight({ day: 10, flight: "BADTZ", std: "10:00", sta: "12:00" }),
      makeFlight({ day: 10, flight: "OTHERFLEET", std: "13:00", sta: "14:00" }),
      makeFlight({ day: 10, flight: "FOREIGN", std: "15:00", sta: "16:00", userId: OTHER_USER_ID }),
    ],
    fleet: [
      makeAircraft(),
      makeAircraft({ regn: "VT-OTHER", userId: OTHER_USER_ID }),
    ],
    stations: [
      makeStation("DEP", "invalid"),
      makeStation("DEP", "UTC+0:00", OTHER_USER_ID),
    ],
    users: [
      { _id: USER_ID, hometimeZone: "UTC+5:30" },
      { _id: OTHER_USER_ID, hometimeZone: "UTC-5:00" },
    ],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(10, "BADTZ"), row(10, "OTHERFLEET", "VT-OTHER"), row(10, "FOREIGN")],
    includeGroundDays: false,
  });

  assert.equal(result.diagnostics.rejections.invalidTimezoneData, 1);
  assert.equal(result.diagnostics.rejections.missingFromFleetDB, 1);
  assert.equal(result.diagnostics.rejections.flightNotFound, 1);
  assert.equal(result.diagnostics.successfullyAssigned, 0);
});

test("sync operations can update existing assignments without enabling revalidation upserts", async () => {
  installValidationData({
    flights: [makeFlight({ day: 10, flight: "AB123", std: "08:00", sta: "09:00" })],
    fleet: [makeAircraft()],
    stations: [makeStation("DEP", "UTC+0:00")],
  });

  const result = await buildAssignmentSyncPlan({
    userId: USER_ID,
    rows: [row(10, "AB123", "VT-AAA", 7)],
    includeGroundDays: false,
    allowCreate: false,
  });

  assert.equal(result.assignmentBulkOps[0].updateOne.upsert, false);
  assert.equal(result.assignmentBulkOps[0].updateOne.update.$set.sourceOrder, 7);
  assert.equal(Assignment.modelName, "Assignment");
});
