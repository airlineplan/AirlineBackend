const test = require("node:test");
const assert = require("node:assert/strict");

const pooController = require("../controller/pooController");

const { calculateProrateRatio, recalculateRevenue, buildEditableResponse } = pooController.__testables__;

test("calculates proration from the field-specific ratio before GCD fallback", () => {
    const row = {
        stops: 1,
        sectorGcd: 50,
        odViaGcd: 100,
        fareProrateRatioL1L2: 0.75,
        rateProrateRatioL1L2: 0,
        trafficType: "behind",
    };

    assert.equal(calculateProrateRatio(row, "fareProrateRatioL1L2"), 0.75);
    assert.equal(calculateProrateRatio(row, "rateProrateRatioL1L2"), 0.5);
});

test("uses leg revenue as the OD basis when applySSPricing is enabled", () => {
    const row = recalculateRevenue({
        stops: 0,
        pax: 10,
        cargoT: 5,
        legFare: 1.5,
        legRate: 2,
        odFare: 9,
        odRate: 8,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: true,
    });

    assert.equal(row.odPaxRev, 15);
    assert.equal(row.odCargoRev, 10);
    assert.equal(row.odTotalRev, 25);
    assert.equal(row.fnlRccyTotalRev, 25);
});

test("buildEditableResponse strips legacy revenue fields from the payload", () => {
    const rows = buildEditableResponse([
        {
            _id: "row-1",
            sNo: 1,
            identifier: "Leg",
            trafficType: "leg",
            poo: "DEL",
            od: "DEL-BOM",
            sector: "DEL-BOM",
            flightNumber: "A100",
            flightId: "flight-1",
            variant: "V1",
            prorateRatioL1: 0.6,
            rccyPax: 1,
            rccyCargo: 2,
            rccyTotalRev: 3,
        },
    ]);

    assert.equal(rows[0].displayType, "Leg");
    assert.equal(rows[0].prorateRatioL1, undefined);
    assert.equal(rows[0].rccyPax, undefined);
    assert.equal(rows[0].rccyCargo, undefined);
    assert.equal(rows[0].rccyTotalRev, undefined);
});
