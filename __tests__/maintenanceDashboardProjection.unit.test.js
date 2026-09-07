const assert = require("node:assert/strict");
const { test } = require("node:test");

const Assignment = require("../model/assignment");
const AircraftOnwing = require("../model/aircraftOnwing");
const Fleet = require("../model/fleet");
const Flight = require("../model/flight");
const MaintenanceCalendar = require("../model/maintenanceCalendarSchema");
const MaintenanceReset = require("../model/maintenanceReset");
const MaintenanceStatus = require("../model/maintenanceStatusSchema");
const RotableMovement = require("../model/rotableMovementSchema");
const Utilisation = require("../model/utilisation");
const UtilisationAssumption = require("../model/utilisationAssumptionSchema");
const maintenanceController = require("../controller/maintenanceController");

const USER_ID = "64b000000000000000000001";
const utcDate = (day) => new Date(Date.UTC(2026, 0, day));

const queryReturning = (rows) => ({
  sort() { return this; },
  limit() { return this; },
  select() { return this; },
  lean() { return Promise.resolve(rows); },
});

const createResponse = () => ({
  statusCode: null,
  body: null,
  status(code) {
    this.statusCode = code;
    return this;
  },
  json(body) {
    this.body = body;
    return this;
  },
});

test("maintenance dashboard projection keeps database query count constant across assets and days", async () => {
  const resets = ["4101", "4102"].map((msnEsn, index) => ({
    _id: `reset-${index}`,
    userId: USER_ID,
    date: utcDate(1),
    msnEsn,
    pn: "A320",
    snBn: msnEsn,
    tsn: 100,
    csn: 50,
    dsn: 10,
    timeMetric: "BH",
  }));
  const fleet = resets.map(({ msnEsn }) => ({
    sn: msnEsn,
    titled: `VT-${msnEsn}`,
    entry: utcDate(1),
    exit: utcDate(31),
  }));
  const movements = [{
    _id: "movement-1",
    userId: USER_ID,
    date: utcDate(2),
    createdAt: utcDate(2),
    msn: "5100",
    position: "#1",
    removedSN: "OLD-ENGINE",
    installedSN: "4102",
  }];
  const assignments = [
    ...[2, 3, 4, 5].map((day) => ({
      date: utcDate(day),
      flightNumber: `FL-4101-${day}`,
      aircraft: { msn: 4101 },
      metrics: { blockHours: 2, flightHours: 1.5, cycles: 1 },
    })),
    {
      date: utcDate(2),
      flightNumber: "FL-4102-2",
      aircraft: { msn: 4102 },
      metrics: { blockHours: 2, flightHours: 1.5, cycles: 1 },
    },
    ...[3, 4, 5].map((day) => ({
      date: utcDate(day),
      flightNumber: `FL-5100-${day}`,
      aircraft: { msn: 5100 },
      metrics: { blockHours: 3, flightHours: 2.5, cycles: 1 },
    })),
  ];
  const flights = assignments.map((assignment) => ({
    date: assignment.date,
    flight: assignment.flightNumber,
    bh: 2,
    fh: 1.5,
  }));

  const originals = new Map();
  const replace = (model, method, replacement) => {
    originals.set(`${model.modelName}.${method}`, { model, method, value: model[method] });
    model[method] = replacement;
  };
  const queryCounts = { assignments: 0, flights: 0, rotables: 0 };

  replace(Utilisation, "find", () => queryReturning([]));
  replace(MaintenanceStatus, "find", () => queryReturning([]));
  replace(RotableMovement, "find", () => {
    queryCounts.rotables += 1;
    return queryReturning(movements);
  });
  replace(MaintenanceReset, "find", () => queryReturning(resets));
  replace(Fleet, "find", () => queryReturning(fleet));
  replace(UtilisationAssumption, "find", () => queryReturning([]));
  replace(MaintenanceCalendar, "find", () => queryReturning([]));
  replace(AircraftOnwing, "find", () => queryReturning([]));
  replace(Assignment, "find", () => {
    queryCounts.assignments += 1;
    return queryReturning(assignments);
  });
  replace(Flight, "find", () => {
    queryCounts.flights += 1;
    return queryReturning(flights);
  });
  replace(Flight, "aggregate", (pipeline) => Promise.resolve(
    pipeline[0]?.$match?.date
      ? [{ firstDate: utcDate(1), lastDate: utcDate(5) }]
      : []
  ));

  try {
    const response = createResponse();
    await maintenanceController.getMaintenanceDashboard({
      user: { id: USER_ID },
      query: { date: "2026-01-05" },
    }, response);

    assert.equal(response.statusCode, 200);
    assert.equal(response.body.data.maintenanceData.length, 2);
    const rowsByMsn = new Map(response.body.data.maintenanceData.map((row) => [row.msnEsn, row]));
    assert.equal(rowsByMsn.get("4101").tsn, 108);
    assert.equal(rowsByMsn.get("4102").tsn, 111);
    assert.equal(rowsByMsn.get("4101").csn, 54);
    assert.equal(rowsByMsn.get("4102").csn, 54);
    assert.equal(rowsByMsn.get("4101").dsn, 14);
    assert.equal(rowsByMsn.get("4102").dsn, 14);
    assert.deepEqual(queryCounts, { assignments: 1, flights: 1, rotables: 2 });
  } finally {
    for (const { model, method, value } of originals.values()) model[method] = value;
  }
});
