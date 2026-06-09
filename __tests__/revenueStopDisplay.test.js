const assert = require("node:assert/strict");
const test = require("node:test");

const {
  getRevenueStopDisplayValue,
  rowMatchesRevenueStopFilter,
} = require("../controller/pooController").__testables__;

const connectionRows = [
  {
    trafficType: "behind",
    stops: 1,
    poo: "BOM",
    odOrigin: "DEL",
    odDestination: "MAA",
    sector: "DEL-BOM",
  },
  {
    trafficType: "beyond",
    stops: 1,
    poo: "BOM",
    odOrigin: "DEL",
    odDestination: "MAA",
    sector: "BOM-MAA",
  },
];

test("revenue stop display uses 0 for leg traffic", () => {
  assert.equal(getRevenueStopDisplayValue({
    trafficType: "leg",
    stops: 0,
    poo: "DEL",
    odOrigin: "DEL",
    odDestination: "BOM",
    sector: "DEL-BOM",
  }), "0");
});

test("revenue stop display uses the common station for connection traffic", () => {
  assert.equal(getRevenueStopDisplayValue(connectionRows[0]), "BOM");
  assert.equal(getRevenueStopDisplayValue(connectionRows[1]), "BOM");
});

test("revenue stop display derives the common station from OD when endpoint fields are absent", () => {
  assert.equal(getRevenueStopDisplayValue({
    trafficType: "behind",
    stops: 1,
    poo: "BOM",
    od: "DEL-MAA",
    sector: "DEL-BOM",
  }), "BOM");
  assert.equal(getRevenueStopDisplayValue({
    trafficType: "beyond",
    stops: 1,
    poo: "BOM",
    od: "DEL-MAA",
    sector: "BOM-MAA",
  }), "BOM");
});

test("revenue stop filter matches derived stop station values", () => {
  assert.equal(rowMatchesRevenueStopFilter(connectionRows[0], ["BOM"]), true);
  assert.equal(rowMatchesRevenueStopFilter(connectionRows[1], ["BOM"]), true);
  assert.equal(rowMatchesRevenueStopFilter(connectionRows[0], ["0"]), false);
});
