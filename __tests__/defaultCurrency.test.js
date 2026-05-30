const assert = require("node:assert/strict");
const test = require("node:test");

const CostConfig = require("../model/costConfigSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const Station = require("../model/stationSchema");
const { normalizeCostConfig } = require("../utils/costLogic");
const { convertLocalToReporting } = require("../utils/fx");

test("new account financial config defaults to INR", () => {
  const revenueConfig = new RevenueConfig({ userId: "user-1" });
  const costConfig = new CostConfig({ userId: "user-1" });

  assert.equal(revenueConfig.reportingCurrency, "INR");
  assert.deepEqual(revenueConfig.currencyCodes, ["INR"]);
  assert.equal(costConfig.reportingCurrency, "INR");
});

test("new stations default their currency code to INR", () => {
  const station = new Station({ stationName: "DEL", userId: "user-1" });

  assert.equal(station.currencyCode, "INR");
});

test("currency helpers use INR when reporting currency is omitted", () => {
  assert.equal(normalizeCostConfig({}).reportingCurrency, "INR");
  assert.equal(
    convertLocalToReporting(100, "USD", "", "2026-05-01", [
      { pair: "USD/INR", dateKey: "2026-05-01", rate: 83 },
    ]),
    8300
  );
});
