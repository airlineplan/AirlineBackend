const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
    applyMaintenanceEvent,
    getAssumptionUsageForDate,
    projectDailyState,
    selectAssumptionForDate
} = require("../services/maintenanceProjectionService");

const date = (value) => new Date(`${value}T00:00:00.000Z`);
const baseline = () => ({
    tsn: 14987.82,
    csn: 6921,
    dsn: 2594,
    tsoTsr: 100,
    csoCsr: 50,
    dsoDsr: 30,
    tsRplmt: 10,
    csRplmt: 5,
    dsRplmt: 3
});

const octoberAssumption = {
    _id: "october-assumption",
    msn: "43563",
    fromDate: date("2026-10-01"),
    toDate: date("2027-10-31"),
    hours: 15,
    cycles: 6
};

test("future utilisation assumptions have zero contribution before their effective date", () => {
    for (const value of ["2026-09-14", "2026-09-15", "2026-09-30"]) {
        assert.deepEqual(getAssumptionUsageForDate({
            assumptions: [octoberAssumption],
            effectiveMsn: "43563",
            date: date(value)
        }), {
            timeUsage: 0,
            cycleUsage: 0,
            hasUsage: false,
            source: "NONE",
            assumption: null,
            assumptionId: null
        });
    }
});

test("assumption date boundaries are inclusive and select only a matching MSN/range", () => {
    assert.equal(selectAssumptionForDate({ assumptions: [octoberAssumption], effectiveMsn: "43563", date: date("2026-09-30") }), null);
    assert.equal(getAssumptionUsageForDate({ assumptions: [octoberAssumption], effectiveMsn: "43563", date: date("2026-10-01") }).timeUsage, 15);
    assert.equal(getAssumptionUsageForDate({ assumptions: [octoberAssumption], effectiveMsn: "43563", date: date("2027-10-31") }).cycleUsage, 6);
    assert.equal(selectAssumptionForDate({ assumptions: [octoberAssumption], effectiveMsn: "43563", date: date("2027-11-01") }), null);
    assert.equal(selectAssumptionForDate({ assumptions: [octoberAssumption], effectiveMsn: "43564", date: date("2026-10-01") }), null);
});

test("forward daily projection maps hours to TSN, cycles to CSN, and elapsed day to DSN", () => {
    const next = projectDailyState(baseline(), { canOperate: true, timeUsage: 15, cycleUsage: 6 }).state;
    assert.equal(next.tsn, 15002.82);
    assert.equal(next.csn, 6927);
    assert.equal(next.dsn, 2595);
});

test("a future assumption cannot change a finalized September state", () => {
    const before = projectDailyState(baseline(), { canOperate: true, timeUsage: 0, cycleUsage: 0 }).state;
    const usage = getAssumptionUsageForDate({
        assumptions: [octoberAssumption],
        effectiveMsn: "43563",
        date: date("2026-09-15")
    });
    const after = projectDailyState(baseline(), {
        canOperate: true,
        timeUsage: usage.timeUsage,
        cycleUsage: usage.cycleUsage
    }).state;
    assert.deepEqual(after, before);
});

test("since-new counters remain monotonic during maintenance and after release", () => {
    const beforeMaintenance = baseline();
    const maintenanceDay = projectDailyState(beforeMaintenance, { canOperate: false }).state;
    const releasedDay = projectDailyState(maintenanceDay, { canOperate: true, timeUsage: 15, cycleUsage: 6 }).state;

    for (const key of ["tsn", "csn", "dsn"]) {
        assert.ok(maintenanceDay[key] >= beforeMaintenance[key], `${key} decreased in maintenance`);
        assert.ok(releasedDay[key] >= maintenanceDay[key], `${key} decreased after release`);
    }
    assert.equal(maintenanceDay.tsn, beforeMaintenance.tsn);
    assert.equal(maintenanceDay.csn, beforeMaintenance.csn);
    assert.equal(releasedDay.tsn, beforeMaintenance.tsn + 15);
    assert.equal(releasedDay.csn, beforeMaintenance.csn + 6);
});

test("maintenance post-event application preserves since-new counters for the same serial", () => {
    const before = baseline();
    const after = applyMaintenanceEvent(before, {
        tsoTsr: 0,
        csoCsr: 0,
        dsoDsr: 0,
        tsRplmt: 0,
        csRplmt: 0,
        dsRplmt: 0
    });
    assert.equal(after.tsn, before.tsn);
    assert.equal(after.csn, before.csn);
    assert.equal(after.dsn, before.dsn);
    assert.equal(after.tsoTsr, 0);
    assert.equal(after.tsRplmt, 0);
});

test("a deterministic forward run is idempotent when its inputs do not change", () => {
    const dailyInputs = [
        { canOperate: true, timeUsage: 2.82, cycleUsage: 1 },
        { canOperate: false },
        { canOperate: false },
        { canOperate: true, timeUsage: 15, cycleUsage: 6 },
        { canOperate: true, timeUsage: 15, cycleUsage: 6 }
    ];
    const run = () => dailyInputs.reduce((state, input) => projectDailyState(state, input).state, baseline());
    assert.deepEqual(run(), run());
});

test("an actual/assigned daily value is a complete input and is not double-counted with an assumption", () => {
    const state = projectDailyState(baseline(), {
        canOperate: true,
        timeUsage: 4.25,
        cycleUsage: 2
    }).state;
    assert.equal(state.tsn, 14992.07);
    assert.equal(state.csn, 6923);
});

test("overlapping ranges choose the most recent effective range, never an unrelated newest record", () => {
    const older = { ...octoberAssumption, _id: "older", fromDate: date("2026-10-01"), hours: 8, cycles: 3 };
    const newer = { ...octoberAssumption, _id: "newer", fromDate: date("2026-10-15"), hours: 15, cycles: 6 };
    assert.equal(getAssumptionUsageForDate({ assumptions: [older, newer], effectiveMsn: "43563", date: date("2026-10-10") }).assumptionId, "older");
    assert.equal(getAssumptionUsageForDate({ assumptions: [older, newer], effectiveMsn: "43563", date: date("2026-10-15") }).assumptionId, "newer");
});
