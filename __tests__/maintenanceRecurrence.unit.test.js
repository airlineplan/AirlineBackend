const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  addSuppressedThreshold,
  findCalendarTriggerCandidate,
  getNextUnsuppressedThreshold,
} = require("../controller/maintenanceController").__recurrenceTestables;

const makeState = () => ({
  triggeredSinceNewMetrics: new Set(),
  suppressedThresholdsByMetric: new Map(),
});

const nextTsnThreshold = (currentValue, interval, state = makeState()) =>
  getNextUnsuppressedThreshold({
    state,
    metricCode: "TSN",
    currentValue,
    interval,
    includeCurrentBoundary: true,
  });

const findTsnOccurrence = ({ currentTsn, projectedTsn, every = 5000, state = makeState() }) =>
  findCalendarTriggerCandidate({
    cal: { eTsn: every },
    state,
    currentValues: { tsn: currentTsn },
    projectedValues: { tsn: projectedTsn },
  });

test("TSN recurrence resolves the next lifetime multiple", () => {
  assert.equal(nextTsnThreshold(14290, 5000), 15000);
  assert.equal(nextTsnThreshold(14290.4, "5000"), 15000);
  assert.equal(nextTsnThreshold(10001, 5000), 15000);
  assert.equal(nextTsnThreshold(14999, 5000), 15000);
});

test("TSN recurrence is generic across supported interval sizes", () => {
  const cases = [
    [500, 14500],
    [1000, 15000],
    [2500, 15000],
    [5000, 15000],
    [10000, 20000],
    [50000, 50000],
  ];

  for (const [interval, expected] of cases) {
    assert.equal(nextTsnThreshold(14290, interval), expected, `Every=${interval}`);
  }
});

test("forecast triggers on the first row that crosses a TSN interval", () => {
  const forecast = [14987, 14996, 15004, 15015];
  const state = makeState();
  const occurrences = [];

  for (let index = 1; index < forecast.length; index += 1) {
    const trigger = findTsnOccurrence({
      currentTsn: forecast[index - 1],
      projectedTsn: forecast[index],
      state,
    });
    if (!trigger) continue;

    occurrences.push({ index, ...trigger });
    addSuppressedThreshold(state, trigger.metricCode, trigger.threshold);
  }

  assert.equal(occurrences.length, 1);
  assert.equal(occurrences[0].index, 2);
  assert.equal(occurrences[0].projectedValue, 15004);
  assert.equal(occurrences[0].threshold, 15000);
});

test("processed TSN recurrence advances to the following interval", () => {
  const state = makeState();
  addSuppressedThreshold(state, "TSN", 15000);

  assert.equal(nextTsnThreshold(15001, 5000, state), 20000);
});

test("an exact TSN boundary is retained until that threshold is processed", () => {
  const pendingState = makeState();
  assert.equal(nextTsnThreshold(15000, 5000, pendingState), 15000);

  addSuppressedThreshold(pendingState, "TSN", 15000);
  assert.equal(nextTsnThreshold(15000, 5000, pendingState), 20000);
});

test("one TSN threshold produces no duplicate while later forecasts stay above it", () => {
  const forecast = [14996, 15004, 15015, 15031, 15048];
  const state = makeState();
  const thresholds = [];

  for (let index = 1; index < forecast.length; index += 1) {
    const trigger = findTsnOccurrence({
      currentTsn: forecast[index - 1],
      projectedTsn: forecast[index],
      state,
    });
    if (!trigger) continue;

    thresholds.push(trigger.threshold);
    addSuppressedThreshold(state, trigger.metricCode, trigger.threshold);
  }

  assert.deepEqual(thresholds, [15000]);
});

test("fractional TSN triggers when a forecast jumps over the interval", () => {
  const trigger = findTsnOccurrence({
    currentTsn: 14999.8,
    projectedTsn: 15003.2,
  });

  assert.equal(trigger?.threshold, 15000);
  assert.equal(trigger?.projectedValue, 15003.2);
});

test("Earliest-of chooses the first configured metric interval reached", () => {
  const state = makeState();
  const trigger = findCalendarTriggerCandidate({
    cal: { eTsn: 5000, eCsn: "1000" },
    state,
    currentValues: { tsn: 14980, csn: 999 },
    projectedValues: { tsn: 14995, csn: 1001 },
  });

  assert.equal(trigger?.metricCode, "CSN");
  assert.equal(trigger?.threshold, 1000);
});

test("CSN and DSN keep their existing since-new scheduling behavior", () => {
  const state = makeState();
  state.triggeredSinceNewMetrics.add("CSN");

  const trigger = findCalendarTriggerCandidate({
    cal: { eCsn: 1000 },
    state,
    currentValues: { csn: 1999 },
    projectedValues: { csn: 2001 },
  });

  assert.equal(trigger, null);
});

test("non-since-new recurring metrics keep their existing interval behavior", () => {
  const trigger = findCalendarTriggerCandidate({
    cal: { eTso: 10 },
    state: makeState(),
    currentValues: { tsoTsr: 5 },
    projectedValues: { tsoTsr: 10.25 },
  });

  assert.equal(trigger?.metricCode, "TSO/TSRTRTN");
  assert.equal(trigger?.threshold, 10);
});
