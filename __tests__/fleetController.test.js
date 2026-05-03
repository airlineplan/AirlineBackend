const assert = require("node:assert/strict");
const { afterEach, test } = require("node:test");

const Fleet = require("../model/fleet");
const AircraftOnwing = require("../model/aircraftOnwing");
const fleetController = require("../controller/fleetController");

const originalFleetBulkWrite = Fleet.bulkWrite;
const originalFleetDeleteMany = Fleet.deleteMany;
const originalAircraftOnwingBulkWrite = AircraftOnwing.bulkWrite;

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

afterEach(() => {
  Fleet.bulkWrite = originalFleetBulkWrite;
  Fleet.deleteMany = originalFleetDeleteMany;
  AircraftOnwing.bulkWrite = originalAircraftOnwingBulkWrite;
});

test("fleet bulk save preserves ownership for spare engine rows", async () => {
  let fleetOps;
  Fleet.deleteMany = async () => ({ deletedCount: 0 });
  Fleet.bulkWrite = async (ops) => {
    fleetOps = ops;
    return { modifiedCount: ops.length };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });

  const req = {
    user: { id: "user-1" },
    body: {
      fleetData: [
        {
          category: "Engine",
          type: "CFM56",
          variant: "CFM56-5B6",
          sn: "721576",
          regn: "vt-xxx",
          entry: "2026-04-16",
          exit: "2026-05-10",
          titled: "Spare",
          ownership: "Operating lease",
          status: "Available",
        },
      ],
    },
  };
  const res = createMockResponse();

  await fleetController.bulkUpsertFleet(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(fleetOps[0].updateOne.update.$set.ownership, "Operating lease");
  assert.equal(fleetOps[0].updateOne.update.$set.titled, "Spare");
});

test("fleet bulk save blanks ownership for non-spare titled component rows", async () => {
  let fleetOps;
  Fleet.deleteMany = async () => ({ deletedCount: 0 });
  Fleet.bulkWrite = async (ops) => {
    fleetOps = ops;
    return { modifiedCount: ops.length };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });

  const req = {
    user: { id: "user-1" },
    body: {
      fleetData: [
        {
          category: "Engine",
          type: "CFM56",
          variant: "CFM56-5B6",
          sn: "635799",
          regn: "vt-xxx",
          entry: "2026-03-01",
          exit: "2026-05-31",
          titled: "VT-AAA #1",
          ownership: "Operating lease",
          status: "Available",
        },
      ],
    },
  };
  const res = createMockResponse();

  await fleetController.bulkUpsertFleet(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(fleetOps[0].updateOne.update.$set.ownership, "");
});

test("fleet bulk save deletes rows missing from submitted table", async () => {
  let deleteFilter;
  Fleet.bulkWrite = async (ops) => ({ modifiedCount: ops.length });
  Fleet.deleteMany = async (filter) => {
    deleteFilter = filter;
    return { deletedCount: 1 };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });

  const req = {
    user: { id: "user-1" },
    body: {
      fleetData: [
        {
          category: "Aircraft",
          type: "A320",
          variant: "A320",
          sn: " 5150 ",
          regn: "vt-aaa",
          entry: "2026-03-01",
          exit: "2026-06-30",
          titled: "",
          ownership: "Operating lease",
          status: "Assigned",
        },
      ],
    },
  };
  const res = createMockResponse();

  await fleetController.bulkUpsertFleet(req, res);

  assert.equal(res.statusCode, 200);
  assert.deepEqual(deleteFilter, {
    userId: "user-1",
    sn: { $nin: ["5150"] },
  });
});
