const assert = require("node:assert/strict");
const { before, after, beforeEach, test } = require("node:test");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const net = require("node:net");
const mongoose = require("mongoose");
const Flight = require("../model/flight");
const Data = require("../model/dataSchema");
const { CATEGORIES, normalizeCategory, categoryFilter } = require("../utils/flightCategory");
const { backfillFlightCategory } = require("../scripts/backfillFlightCategory");

const USER_ID = new mongoose.Types.ObjectId().toString();
const DATE = new Date("2026-04-06T00:00:00Z");
const BASE = { userId: USER_ID, date: DATE, flight: "AB123" };
const ALL_CATEGORIES = Object.values(CATEGORIES).sort();
let mongodProcess;
let dbPath;
let rows;

before(async () => {
  const port = await new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const selectedPort = probe.address().port;
      probe.close(() => resolve(selectedPort));
    });
  });
  dbPath = fs.mkdtempSync(path.join(os.tmpdir(), "airline-category-mongo-"));
  const binary = fs.existsSync("/usr/bin/mongod") ? "/usr/bin/mongod" : "mongod";
  mongodProcess = spawn(binary, [
    "--dbpath", dbPath, "--port", String(port), "--bind_ip", "127.0.0.1", "--quiet",
    ...(process.platform === "win32" ? [] : ["--nounixsocket"]),
  ], { stdio: "ignore", windowsHide: true });

  await new Promise((resolve, reject) => {
    const onError = (error) => { clearTimeout(timer); reject(error); };
    const onExit = (code) => onError(new Error(`MongoDB exited before startup (${code})`));
    const timer = setTimeout(() => onError(new Error("MongoDB did not start")), 15000);
    mongodProcess.once("error", onError);
    mongodProcess.once("exit", onExit);
    const check = () => {
      const socket = net.createConnection({ port, host: "127.0.0.1" }, () => {
        socket.end();
        clearTimeout(timer);
        mongodProcess.removeListener("error", onError);
        mongodProcess.removeListener("exit", onExit);
        resolve();
      });
      socket.on("error", () => {
        socket.destroy();
        if (mongodProcess.exitCode === null && mongodProcess.pid) {
          const retry = setTimeout(check, 100);
          retry.unref();
        }
      });
    };
    check();
  });
  await mongoose.connect(`mongodb://127.0.0.1:${port}/airline_category_test`, { serverSelectionTimeoutMS: 5000 });
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
    assert.ok(path.basename(target).startsWith("airline-category-mongo-"));
    fs.rmSync(target, { recursive: true, force: true });
  }
});

beforeEach(async () => {
  // Raw collection access is intentional for test cleanup across categories.
  await Flight.collection.deleteMany({});
  rows = await Flight.create(Object.values(CATEGORIES).map((category) => ({ ...BASE, category })));
});

async function checkReads(filter, expected, options = {}) {
  const sorted = [...expected].sort();
  const found = await Flight.find(filter).setOptions(options).lean();
  assert.deepEqual(found.map((row) => row.category).sort(), sorted);
  const first = await Flight.findOne(filter).setOptions(options).lean();
  assert.equal(first !== null, sorted.length > 0);
  if (first) assert.ok(sorted.includes(first.category));
  assert.equal(await Flight.countDocuments(filter).setOptions(options), sorted.length);
  assert.deepEqual((await Flight.distinct("category", filter).setOptions(options)).sort(), sorted);
  const aggregated = await Flight.aggregate([{ $match: filter }, { $project: { category: 1 } }]).option(options);
  assert.deepEqual(aggregated.map((row) => row.category).sort(), sorted);
}

test("category helpers normalize values and default to Planned", () => {
  for (const value of [undefined, null, "", "unknown", 1, {}]) {
    assert.equal(normalizeCategory(value), "Planned");
    assert.deepEqual(categoryFilter(value), { category: "Planned" });
  }
  for (const category of ALL_CATEGORIES) {
    assert.equal(normalizeCategory(` ${category.toUpperCase()} `), category);
    assert.deepEqual(categoryFilter(category), { category });
  }
});

test("schema defaults, enum, linkage fields and category index are present", async () => {
  const flight = await Flight.create({ ...BASE, pairId: "pair", actualUploadId: "upload", invoiceId: "invoice" });
  assert.equal(flight.category, "Planned");
  assert.equal(flight.pairId, "pair");
  assert.equal(flight.actualUploadId, "upload");
  assert.equal(flight.invoiceId, "invoice");
  await assert.rejects(Flight.create({ category: "Other" }), /category/);
  assert.ok(Flight.schema.indexes().some(([keys]) =>
    JSON.stringify(keys) === JSON.stringify({ userId: 1, category: 1, date: 1, flight: 1 })
  ));
});

test("find, findOne, countDocuments, distinct and aggregate default to Planned", async () => {
  await checkReads(BASE, ["Planned"]);
  assert.equal(await Flight.findById(rows[1]._id), null);
  assert.equal(await Flight.findById(rows[2]._id), null);
  assert.deepEqual(await Flight.aggregate([{ $group: { _id: "$category", count: { $sum: 1 } } }]), [
    { _id: "Planned", count: 1 },
  ]);
});

test("explicit category filters win for every read operation", async () => {
  for (const [category, expected] of [
    ["Actual", ["Actual"]],
    [{ $in: ["Actual", "Invoiced"] }, ["Actual", "Invoiced"]],
  ]) {
    await checkReads({ ...BASE, category }, expected);
  }
  await checkReads({ ...BASE, $or: [{ category: "Actual" }, { category: "Invoiced" }] }, ["Actual", "Invoiced"]);
  await checkReads({ ...BASE, $and: [{ $or: [{ category: "Actual" }, { category: "Invoiced" }] }] }, ["Actual", "Invoiced"]);
  await checkReads({ ...BASE, $nor: [{ category: "Planned" }] }, ["Actual", "Invoiced"]);
  // An explicit category in one branch leaves other branches untouched too.
  await checkReads({ ...BASE, $or: [{ category: "Actual" }, { flight: BASE.flight }] }, ALL_CATEGORIES);
});

test("logical filters without category stay guarded", async () => {
  await checkReads({ userId: USER_ID, $or: [{ flight: BASE.flight }, { flight: "missing" }] }, ["Planned"]);
});

test("allCategories true bypasses every read guard, including category group-by", async () => {
  await checkReads(BASE, ALL_CATEGORIES, { allCategories: true });
  await checkReads(BASE, ["Planned"], { allCategories: false });
  await checkReads({ ...BASE, category: "Actual" }, ["Actual"], { allCategories: true });
  const grouped = await Flight.aggregate([{ $group: { _id: "$category", count: { $sum: 1 } } }])
    .option({ allCategories: true });
  assert.deepEqual(grouped.map((row) => row._id).sort(), ALL_CATEGORIES);
  assert.ok(grouped.every((row) => row.count === 1));
});

test("a category in a later aggregate match does not bypass the first-match rule", async () => {
  assert.deepEqual(await Flight.aggregate([{ $match: BASE }, { $match: { category: "Actual" } }]), []);
});

test("network updateMany and deleteMany affect only Planned", async () => {
  // Separate dates satisfy the existing unique network/date/flight index.
  for (const [index, row] of rows.entries()) {
    await Flight.collection.updateOne({ _id: row._id }, {
      $set: { networkId: "network", date: new Date(DATE.getTime() + index * 86400000) },
    });
  }
  const result = await Flight.updateMany({ networkId: "network" }, { $set: { remarks1: "changed" } });
  assert.equal(result.modifiedCount, 1);
  const unguarded = await Flight.collection.find({ networkId: "network" }).toArray();
  assert.equal(unguarded.find((row) => row.category === "Planned").remarks1, "changed");
  assert.ok(unguarded.filter((row) => row.category !== "Planned").every((row) => row.remarks1 === undefined));
  assert.equal((await Flight.deleteMany({ networkId: "network" })).deletedCount, 1);
  assert.deepEqual((await Flight.collection.find({}).toArray()).map((row) => row.category).sort(), ["Actual", "Invoiced"]);
});

for (const operation of ["updateOne", "findOneAndUpdate", "deleteOne"]) {
  test(`${operation} protects non-Planned rows even when addressed by ID`, async () => {
    for (const row of rows) {
      const result = operation === "deleteOne"
        ? await Flight.deleteOne({ _id: row._id })
        : operation === "findOneAndUpdate"
          ? await Flight.findOneAndUpdate({ _id: row._id }, { $set: { remarks1: "changed" } }, { new: true })
          : await Flight.updateOne({ _id: row._id }, { $set: { remarks1: "changed" } });
      const planned = row.category === "Planned";
      if (operation === "deleteOne") assert.equal(result.deletedCount, planned ? 1 : 0);
      else if (operation === "updateOne") assert.equal(result.modifiedCount, planned ? 1 : 0);
      else assert.equal(result?.category ?? null, planned ? "Planned" : null);
      if (!planned) {
        const unchanged = await Flight.collection.findOne({ _id: row._id });
        assert.ok(unchanged);
        assert.equal(unchanged.remarks1, undefined);
      }
    }
  });
}

test("explicit nested category filters and allCategories opt writes in", async () => {
  assert.equal((await Flight.updateMany({ $and: [{ $or: [{ category: "Actual" }] }] },
    { $set: { remarks1: "actual" } })).modifiedCount, 1);
  assert.equal((await Flight.updateOne({ category: "Invoiced" }, { $set: { remarks1: "invoice" } })).modifiedCount, 1);
  assert.equal((await Flight.findOneAndUpdate({ $nor: [{ category: { $in: ["Planned", "Invoiced"] } }] },
    { $set: { remarks1: "updated" } }, { new: true })).category, "Actual");
  assert.equal((await Flight.updateMany(BASE, { $set: { remarks2: "all" } }, { allCategories: true })).modifiedCount, 3);
  assert.equal((await Flight.updateOne({ _id: rows[1]._id }, { $set: { remarks1: "override" } }, { allCategories: true })).modifiedCount, 1);
  assert.equal((await Flight.findOneAndUpdate({ _id: rows[2]._id }, { $set: { remarks1: "override" } },
    { allCategories: true, new: true })).category, "Invoiced");
  assert.equal((await Flight.deleteOne({ category: "Invoiced" })).deletedCount, 1);
  assert.equal((await Flight.deleteMany({ $or: [{ category: "Actual" }] })).deletedCount, 1);
  assert.equal((await Flight.deleteMany({}, { allCategories: true })).deletedCount, 1);
});

test("allCategories allows deleteOne and deleteMany to remove non-Planned rows", async () => {
  assert.equal((await Flight.deleteOne({ _id: rows[1]._id }, { allCategories: true })).deletedCount, 1);
  assert.equal((await Flight.deleteMany(BASE, { allCategories: true })).deletedCount, 2);
});

test("legacy rows stay readable, writable and deletable before migration", async () => {
  const { insertedId } = await Flight.collection.insertOne({ ...BASE, remarks1: "legacy" });
  assert.equal(await Flight.countDocuments(BASE), 2);
  assert.equal((await Flight.find({ remarks1: "legacy" }).lean())[0].category, undefined);
  assert.equal((await Flight.findOne({ _id: insertedId }).lean()).category, undefined);
  assert.deepEqual(await Flight.distinct("remarks1", BASE), ["legacy"]);
  assert.equal((await Flight.aggregate([{ $match: { _id: insertedId } }])).length, 1);
  assert.equal((await Flight.updateOne({ _id: insertedId }, { $set: { remarks1: "updated" } })).modifiedCount, 1);
  assert.equal((await Flight.deleteOne({ _id: insertedId })).deletedCount, 1);
});

test("migration backfills only missing categories and is idempotent", async () => {
  await Flight.collection.insertMany([{ ...BASE }, { ...BASE }]);
  assert.equal((await backfillFlightCategory()).modifiedCount, 2);
  assert.equal(await Flight.collection.countDocuments({ category: { $exists: false } }), 0);
  assert.deepEqual((await Flight.collection.find({}).toArray()).map((row) => row.category).sort(),
    ["Actual", "Invoiced", "Planned", "Planned", "Planned"]);
  assert.equal((await backfillFlightCategory()).modifiedCount, 0);
});

test("migration of a legacy-only collection groups entirely as Planned", async () => {
  await Flight.collection.deleteMany({});
  await Flight.collection.insertMany([{ ...BASE }, { ...BASE }]);
  await backfillFlightCategory();
  assert.deepEqual(await Flight.aggregate([{ $group: { _id: "$category", count: { $sum: 1 } } }])
    .option({ allCategories: true }), [{ _id: "Planned", count: 2 }]);
});

test("createFlgts FLIGHT_LIMIT ignores Actual and Invoiced rows", async () => {
  await Flight.collection.deleteMany({ category: "Planned" });
  const previousLimit = process.env.FLIGHT_LIMIT;
  process.env.FLIGHT_LIMIT = "1";
  try {
    const network = await Data.create({
      ...BASE, depStn: "DEL", arrStn: "BOM", std: "08:00", sta: "10:00", bt: "02:00",
      variant: "A320", effFromDt: DATE, effToDt: DATE, dow: "1",
    });
    assert.equal(await Flight.countDocuments({ userId: USER_ID }), 1);
    const generated = await Flight.findOne({ networkId: String(network._id) }).lean();
    assert.equal(generated.category, "Planned");
    assert.equal(await Flight.collection.countDocuments({ userId: USER_ID }), 3);
  } finally {
    if (previousLimit === undefined) delete process.env.FLIGHT_LIMIT;
    else process.env.FLIGHT_LIMIT = previousLimit;
  }
});
