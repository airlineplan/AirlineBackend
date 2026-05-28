const assert = require("node:assert/strict");
const { afterEach, test } = require("node:test");

const Fleet = require("../model/fleet");
const AircraftOnwing = require("../model/aircraftOnwing");
const Utilisation = require("../model/utilisation");
const MaintenanceStatus = require("../model/maintenanceStatusSchema");
const MaintenanceTarget = require("../model/maintenanceTargetSchema");
const MaintenanceReset = require("../model/maintenanceReset");
const MaintenanceCalendar = require("../model/maintenanceCalendarSchema");
const UtilisationAssumption = require("../model/utilisationAssumptionSchema");
const RotableMovement = require("../model/rotableMovementSchema");
const fleetController = require("../controller/fleetController");

const originalFleetBulkWrite = Fleet.bulkWrite;
const originalFleetDeleteMany = Fleet.deleteMany;
const originalFleetFind = Fleet.find;
const originalFleetFindOneAndDelete = Fleet.findOneAndDelete;
const originalAircraftOnwingBulkWrite = AircraftOnwing.bulkWrite;
const originalAircraftOnwingDeleteMany = AircraftOnwing.deleteMany;
const originalAircraftOnwingUpdateMany = AircraftOnwing.updateMany;
const maintenanceModels = [
  MaintenanceReset,
  Utilisation,
  MaintenanceTarget,
  MaintenanceCalendar,
  MaintenanceStatus,
  UtilisationAssumption,
  RotableMovement,
];
const originalMaintenanceDeleteMany = new Map(
  maintenanceModels.map((model) => [model, model.deleteMany])
);

function mockFleetFind(results = []) {
  Fleet.find = () => ({
    select: () => ({
      lean: async () => results,
    }),
  });
}

function stubMaintenanceDeletes(handler = async () => ({ deletedCount: 0 })) {
  maintenanceModels.forEach((model) => {
    model.deleteMany = handler;
  });
}

function stubOnwingCleanup() {
  AircraftOnwing.deleteMany = async () => ({ deletedCount: 0 });
  AircraftOnwing.updateMany = async () => ({ modifiedCount: 0 });
}

function getFieldRegex(filter, field) {
  const clause = (filter.$or || []).find((item) => item[field]);
  return clause?.[field]?.$regex;
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

afterEach(() => {
  Fleet.bulkWrite = originalFleetBulkWrite;
  Fleet.deleteMany = originalFleetDeleteMany;
  Fleet.find = originalFleetFind;
  Fleet.findOneAndDelete = originalFleetFindOneAndDelete;
  AircraftOnwing.bulkWrite = originalAircraftOnwingBulkWrite;
  AircraftOnwing.deleteMany = originalAircraftOnwingDeleteMany;
  AircraftOnwing.updateMany = originalAircraftOnwingUpdateMany;
  originalMaintenanceDeleteMany.forEach((deleteMany, model) => {
    model.deleteMany = deleteMany;
  });
});

test("fleet bulk save preserves ownership for spare engine rows", async () => {
  let fleetOps;
  Fleet.deleteMany = async () => ({ deletedCount: 0 });
  mockFleetFind([]);
  Fleet.bulkWrite = async (ops) => {
    fleetOps = ops;
    return { modifiedCount: ops.length };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });
  stubMaintenanceDeletes();
  stubOnwingCleanup();

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
  mockFleetFind([]);
  Fleet.bulkWrite = async (ops) => {
    fleetOps = ops;
    return { modifiedCount: ops.length };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });
  stubMaintenanceDeletes();
  stubOnwingCleanup();

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
  mockFleetFind([]);
  Fleet.deleteMany = async (filter) => {
    deleteFilter = filter;
    return { deletedCount: 1 };
  };
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });
  stubMaintenanceDeletes();
  stubOnwingCleanup();

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

test("fleet bulk save purges maintenance data for rows missing from submitted table", async () => {
  const deleteFiltersByModel = {};
  const onwingDeleteFilters = [];
  const onwingUpdateFilters = [];

  Fleet.bulkWrite = async (ops) => ({ modifiedCount: ops.length });
  mockFleetFind([{ sn: "1001" }]);
  Fleet.deleteMany = async () => ({ deletedCount: 1 });
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });
  stubMaintenanceDeletes(function captureMaintenanceDelete(filter) {
    if (!deleteFiltersByModel[this.modelName]) deleteFiltersByModel[this.modelName] = [];
    deleteFiltersByModel[this.modelName].push(filter);
    return { deletedCount: 1 };
  });
  AircraftOnwing.deleteMany = async (filter) => {
    onwingDeleteFilters.push(filter);
    return { deletedCount: 1 };
  };
  AircraftOnwing.updateMany = async (filter) => {
    onwingUpdateFilters.push(filter);
    return { modifiedCount: 1 };
  };

  const req = {
    user: { id: "user-1" },
    body: {
      fleetData: [
        {
          category: "Aircraft",
          type: "A320",
          variant: "A320",
          sn: "5150",
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
  assert.equal(getFieldRegex(deleteFiltersByModel.MaintenanceReset[0], "msnEsn"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.Utilisation[0], "snBn"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.MaintenanceTarget[0], "msnEsn"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.MaintenanceCalendar[0], "calMsn"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.MaintenanceStatus[0], "targetId"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.UtilisationAssumption[0], "msn"), "^1001$");
  assert.equal(getFieldRegex(deleteFiltersByModel.RotableMovement[0], "installedSN"), "^1001$");
  assert.equal(getFieldRegex(onwingDeleteFilters[0], "msn"), "^1001$");
  assert.deepEqual(onwingUpdateFilters.slice(0, 3).map((filter) => Object.keys(filter.$or[0])[0]), [
    "pos1Esn",
    "pos2Esn",
    "apun",
  ]);
  assert.deepEqual(res.body.maintenanceCleanup.assetSns, ["1001"]);
});

test("fleet bulk save purges existing orphan maintenance rows not present in saved fleet", async () => {
  let resetOrphanFilter;

  Fleet.bulkWrite = async (ops) => ({ modifiedCount: ops.length });
  mockFleetFind([]);
  Fleet.deleteMany = async () => ({ deletedCount: 0 });
  AircraftOnwing.bulkWrite = async () => ({ modifiedCount: 0 });
  stubMaintenanceDeletes(function captureMaintenanceDelete(filter) {
    if (this.modelName === "MaintenanceReset" && filter.$and) resetOrphanFilter = filter;
    return { deletedCount: 1 };
  });
  stubOnwingCleanup();

  const req = {
    user: { id: "user-1" },
    body: {
      fleetData: [
        {
          category: "Aircraft",
          type: "A320",
          variant: "A320",
          sn: "5150",
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
  assert.deepEqual(resetOrphanFilter, {
    userId: "user-1",
    $and: [
      { msnEsn: { $nin: ["5150"] } },
      { snBn: { $nin: ["5150"] } },
    ],
  });
  assert.equal(res.body.orphanMaintenanceCleanup.activeAssetCount, 1);
});

test("direct fleet asset delete purges maintenance data for the deleted asset", async () => {
  let resetDeleteFilter;

  Fleet.findOneAndDelete = async () => ({ _id: "asset-1", sn: "ED0050" });
  stubMaintenanceDeletes(function captureMaintenanceDelete(filter) {
    if (this.modelName === "MaintenanceReset") resetDeleteFilter = filter;
    return { deletedCount: 1 };
  });
  stubOnwingCleanup();

  const req = {
    user: { id: "user-1" },
    params: { id: "asset-1" },
  };
  const res = createMockResponse();

  await fleetController.deleteFleetAsset(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(getFieldRegex(resetDeleteFilter, "msnEsn"), "^ED0050$");
  assert.deepEqual(res.body.maintenanceCleanup.assetSns, ["ED0050"]);
});
