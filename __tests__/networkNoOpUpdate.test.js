const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const Data = require("../model/dataSchema");
const Flight = require("../model/flight");
const Assignment = require("../model/assignment");
const Sector = require("../model/sectorSchema");
const { startMongoTestServer } = require("./helpers/mongoTestServer");

const USER_ID = "network-noop-user";
const flightDate = new Date("2026-10-10T00:00:00Z");
const stored = { from: new Date("2026-10-01T05:30:00Z"), to: new Date("2026-10-31T05:30:00Z") };
let close;
let networkId;

before(async () => { close = await startMongoTestServer(); });
after(async () => { await close(); });

beforeEach(async () => {
  for (const model of [Data, Flight, Assignment, Sector]) await model.collection.deleteMany({});
  const row = { userId: USER_ID, flight: "BM532", depStn: "BEN", arrStn: "FCO", std: "08:00", bt: "02:30", sta: "10:30", variant: "195LR", dow: "1234567", domINTL: "intl" };
  const { insertedId } = await Data.collection.insertOne({ ...row, effFromDt: stored.from, effToDt: stored.to });
  networkId = String(insertedId);
  await Sector.collection.insertOne({ userId: USER_ID, networkId, flight: "BM532", sector1: "BEN", sector2: "FCO", std: "08:00", bt: "02:30", sta: "10:30", variant: "195LR", dow: "1234567", domINTL: "intl", fromDt: stored.from, toDt: stored.to });
  await Flight.create({ userId: USER_ID, networkId, date: flightDate, flight: "BM532", category: "Planned", aircraft: { msn: 391, registration: "SX-LUX" } });
  await Assignment.create({ userId: USER_ID, date: flightDate, flightNumber: "BM532", aircraft: { msn: 391, registration: "SX-LUX" } });
});

test("re-saving a Network row with the same dates keeps its flights and aircraft assignments", async () => {
  const before = await Flight.findOne({ userId: USER_ID, networkId }).lean();
  await Data.findOneAndUpdate({ _id: networkId }, { $set: { effFromDt: "2026-10-01", effToDt: "2026-10-31", dow: "1234567" } }, { new: true });
  const flight = await Flight.findOne({ userId: USER_ID, networkId }).lean();
  assert.equal(String(flight._id), String(before._id));
  assert.equal(flight.aircraft.msn, 391);
  assert.equal(await Assignment.countDocuments({ userId: USER_ID, flightNumber: "BM532" }), 1);
});
