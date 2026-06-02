const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  calculateCrewMemberEvents,
  calculateKpiResponse,
} = require("../services/crewCalculationService");

const d = (value) => new Date(`${value}Z`);

const approx = (actual, expected, tolerance = 0.02) => {
  assert.ok(Math.abs(Number(actual) - expected) <= tolerance, `${actual} not within ${tolerance} of ${expected}`);
};

const crewMember = {
  _id: "crew-121",
  crewCode: "121",
  name: "Himanshu",
  crewType: "FC",
  role: "Captain",
  baseStation: "DEL",
  dpAllowanceRate: 100,
  fdpAllowanceRate: 500,
  ftAllowanceRate: 1500,
  allowanceCurrency: "INR",
};

const dutySettings = {
  restThresholdMinutes: 420,
  breakThresholdMinutes: 180,
  preflightNewFdpMinutes: 90,
  preflightExistingDutyMinutes: 45,
  postflightMinutes: 30,
};

const positioningSettings = {
  returnToBaseAfterFdpEnabled: true,
  hotacCutoffEnabled: true,
  hotacCutoffLocalTime: "20:00",
  positioningWithinCurrentFdpEnabled: true,
  defaultPositioningMinutes: 150,
  hotacToAirportTransferMinutes: 60,
};

const layoverRules = [
  { ruleType: "CONVENIENCE", station: "DEL", role: "Captain", thresholdMinutes: 180, costAmount: 2000, costBasis: "PER_HOUR", currency: "INR" },
  { ruleType: "CONVENIENCE", station: "BOM", role: "Captain", thresholdMinutes: 150, costAmount: 2500, costBasis: "PER_HOUR", currency: "INR" },
  { ruleType: "CONVENIENCE", station: "CCU", role: "Captain", thresholdMinutes: 120, costAmount: 1000, costBasis: "PER_HOUR", currency: "INR" },
  { ruleType: "CONVENIENCE", station: "MAA", role: "Captain", thresholdMinutes: 120, costAmount: 1800, costBasis: "PER_HOUR", currency: "INR" },
  { ruleType: "HOTAC", station: "BOM", role: "Captain", thresholdMinutes: 360, costAmount: 11000, costBasis: "PER_24_HOURS", currency: "INR" },
  { ruleType: "HOTAC", station: "CCU", role: "Captain", thresholdMinutes: 360, costAmount: 8500, costBasis: "PER_24_HOURS", currency: "INR" },
  { ruleType: "HOTAC", station: "MAA", role: "Captain", thresholdMinutes: 360, costAmount: 9400, costBasis: "PER_24_HOURS", currency: "INR" },
];

const sampleFlights = [
  { _id: "f-a100", std: d("2026-03-20T09:00:00"), sta: d("2026-03-20T11:30:00"), flightNumber: "A100", departureStation: "DEL", arrivalStation: "BOM", sector: "DEL-BOM" },
  { _id: "f-a101", std: d("2026-03-20T12:30:00"), sta: d("2026-03-20T15:00:00"), flightNumber: "A101", departureStation: "BOM", arrivalStation: "DEL", sector: "BOM-DEL" },
  { _id: "f-a153", std: d("2026-03-20T20:00:00"), sta: d("2026-03-20T23:00:00"), flightNumber: "A153", departureStation: "DEL", arrivalStation: "CCU", sector: "DEL-CCU" },
  { _id: "f-a284", std: d("2026-03-22T04:40:00"), sta: d("2026-03-22T07:40:00"), flightNumber: "A284", departureStation: "MAA", arrivalStation: "DEL", sector: "MAA-DEL" },
  { _id: "f-a237", std: d("2026-03-22T17:30:00"), sta: d("2026-03-22T19:30:00"), flightNumber: "A237", departureStation: "DEL", arrivalStation: "MAA", sector: "DEL-MAA" },
];

const sampleOtherDuties = [
  { _id: "o1", startDateTime: d("2026-03-23T13:00:00"), endDateTime: d("2026-03-23T18:00:00"), location: "Flight", category: "Training", subCategory: "Travel to venue" },
  { _id: "o2", startDateTime: d("2026-03-23T18:01:00"), endDateTime: d("2026-03-23T20:00:00"), location: "BOM", category: "Training", subCategory: "Pre session" },
  { _id: "o3", startDateTime: d("2026-03-23T20:01:00"), endDateTime: d("2026-03-24T00:00:00"), location: "BOM", category: "Training", subCategory: "SIM session" },
  { _id: "o4", startDateTime: d("2026-03-24T00:01:00"), endDateTime: d("2026-03-24T02:00:00"), location: "BOM", category: "Training", subCategory: "Post session" },
  { _id: "o5", startDateTime: d("2026-03-24T02:01:00"), endDateTime: d("2026-03-24T06:00:00"), location: "Flight", category: "Training", subCategory: "Return to Base" },
];

const sampleEvents = () => calculateCrewMemberEvents({
  crewMember,
  flightAssignments: sampleFlights,
  otherDuties: sampleOtherDuties,
  dutySettings,
  positioningSettings,
  layoverRules,
  positioningCostRules: [],
});

test("crew diary calculates flight durations and allowance costs from real datetimes", () => {
  const events = sampleEvents();
  const a100 = events.find((event) => event.flightNumber === "A100");
  assert.equal(a100.dpMinutes, 150);
  assert.equal(a100.fdpMinutes, 150);
  assert.equal(a100.ftMinutes, 150);
  assert.equal(a100.dpCost, 250);
  assert.equal(a100.fdpCost, 1250);
  assert.equal(a100.ftCost, 3750);

  const a237 = events.find((event) => event.flightNumber === "A237");
  assert.equal(a237.ftMinutes, 120);
  assert.equal(a237.dpCost, 200);
  assert.equal(a237.fdpCost, 1000);
  assert.equal(a237.ftCost, 3000);
});

test("crew diary keeps training DP-only and inserts expected preflight/postflight rows", () => {
  const events = sampleEvents();
  const firstPreflight = events.find((event) => (
    event.sourceType === "SYSTEM_PRE_FLIGHT" &&
    event.startDateTime.toISOString() === "2026-03-20T07:30:00.000Z"
  ));
  assert.equal(firstPreflight.dpMinutes, 90);
  assert.match(firstPreflight.reasonText, /first operated flight after qualifying rest/i);

  const positionedPreflight = events.find((event) => (
    event.sourceType === "SYSTEM_PRE_FLIGHT" &&
    event.endDateTime.toISOString() === "2026-03-22T04:40:00.000Z"
  ));
  assert.equal(positionedPreflight.dpMinutes, 45);

  const postflight = events.find((event) => (
    event.sourceType === "SYSTEM_POST_FLIGHT" &&
    event.startDateTime.toISOString() === "2026-03-20T23:00:00.000Z"
  ));
  assert.equal(postflight.dpMinutes, 30);

  const training = events.find((event) => event.category === "Training" && event.subCategory === "Travel to venue");
  assert.equal(training.dpMinutes, 300);
  assert.equal(training.fdpMinutes, 0);
  assert.equal(training.ftMinutes, 0);
  assert.equal(training.dpCost, 500);
});

test("crew diary classifies gaps, applies convenience and HOTAC rules, and avoids double counting rest", () => {
  const events = sampleEvents();
  const bomGap = events.find((event) => event.sourceType === "SYSTEM_CONTINUING_DUTY" && event.location === "BOM");
  assert.equal(bomGap.dpMinutes, 60);
  assert.equal(bomGap.layoverCost, 0);

  const delBreak = events.find((event) => event.sourceType === "SYSTEM_BREAK" && event.location === "DEL");
  assert.equal(delBreak.dpMinutes, 300);
  approx(delBreak.layoverCost, 8966.67);
  assert.match(delBreak.reasonText, /269 minutes were cost eligible/);

  const ccuHotac = events.find((event) => event.sourceType === "SYSTEM_REST" && event.location === "CCU");
  assert.equal(ccuHotac.subCategory, "Layover HOTAC");
  assert.ok(ccuHotac.rpMinutes > 360);
  assert.ok(ccuHotac.layoverCost > 0);

  const delHomeRest = events.find((event) => event.sourceType === "SYSTEM_REST" && event.location === "DEL");
  assert.equal(delHomeRest.subCategory, "Rest at Base");
  assert.equal(delHomeRest.layoverCost, 0);

  const restEvents = events.filter((event) => event.sourceType === "SYSTEM_REST");
  for (let index = 1; index < restEvents.length; index += 1) {
    assert.ok(restEvents[index].startDateTime >= restEvents[index - 1].endDateTime);
  }
});

test("crew diary inserts required positioning without counting it as flight time", () => {
  const events = sampleEvents();
  const positioning = events.find((event) => (
    event.sourceType === "SYSTEM_POSITIONING" &&
    event.departureStation === "CCU" &&
    event.arrivalStation === "MAA"
  ));
  assert.equal(positioning.dpMinutes, 150);
  assert.equal(positioning.fdpMinutes, 150);
  assert.equal(positioning.ftMinutes, 0);

  const transfer = events.find((event) => event.subCategory === "HOTAC to Airport Transfer");
  assert.equal(transfer.dpMinutes, 60);
  assert.equal(transfer.ftMinutes, 0);
});

test("crew diary inserts return-to-base positioning when the next duty begins at base", () => {
  const events = calculateCrewMemberEvents({
    crewMember,
    flightAssignments: [
      { _id: "rtb-1", std: d("2026-04-01T09:00:00"), sta: d("2026-04-01T11:00:00"), flightNumber: "R100", departureStation: "DEL", arrivalStation: "MAA" },
      { _id: "rtb-2", std: d("2026-04-02T09:00:00"), sta: d("2026-04-02T11:00:00"), flightNumber: "R101", departureStation: "DEL", arrivalStation: "BOM" },
    ],
    dutySettings,
    positioningSettings,
    layoverRules,
    positioningCostRules: [{ departureStation: "MAA", arrivalStation: "DEL", role: "Captain", costAmount: 7000, currency: "INR" }],
  });

  const rtb = events.find((event) => event.sourceType === "SYSTEM_POSITIONING" && event.departureStation === "MAA" && event.arrivalStation === "DEL");
  assert.ok(rtb);
  assert.equal(rtb.positioningCost, 7000);
  assert.equal(rtb.ftMinutes, 0);
});

test("crew diary does not duplicate user-uploaded positioning duties", () => {
  const events = calculateCrewMemberEvents({
    crewMember,
    flightAssignments: [
      { _id: "dup-1", std: d("2026-05-01T12:00:00"), sta: d("2026-05-01T14:00:00"), flightNumber: "D100", departureStation: "MAA", arrivalStation: "DEL" },
    ],
    otherDuties: [
      { _id: "dup-pos", startDateTime: d("2026-05-01T08:00:00"), endDateTime: d("2026-05-01T10:30:00"), location: "Flight", category: "Positioning", subCategory: "Travel to MAA", isUserEnteredPositioning: true },
    ],
    dutySettings,
    positioningSettings,
    layoverRules,
    positioningCostRules: [],
  });

  const systemPositioning = events.filter((event) => event.sourceType === "SYSTEM_POSITIONING" && event.subCategory === "Deadheading");
  assert.equal(systemPositioning.length, 0);
  const uploadedPositioning = events.find((event) => event.sourceType === "OTHER_DUTY_ROSTER");
  assert.equal(uploadedPositioning.dpMinutes, 150);
});

test("crew KPI values aggregate from diary events without mixing currencies", () => {
  const events = sampleEvents().map((event) => ({
    ...event,
    userId: "user-1",
    calculationRunId: "run-1",
  }));
  events.push({
    ...events[0],
    currency: "USD",
    dpCost: 10,
    fdpCost: 0,
    ftCost: 0,
    layoverCost: 0,
    positioningCost: 0,
  });

  const response = calculateKpiResponse({
    events,
    targets: [{ role: "Captain", averageDpMinutesPerDay: 480, averageFdpMinutesPerDay: 420, averageFtMinutesPerDay: 240 }],
    periodicity: "MONTHLY",
    startDate: "2026-03-01",
    filters: { roles: ["Captain"] },
  });

  const ftMetric = response.metrics.find((metric) => metric.key === "totalFtMinutes");
  assert.equal(ftMetric.values[0].value, 780);
  const landingMetric = response.metrics.find((metric) => metric.key === "totalLandings");
  assert.equal(landingMetric.values[0].value, 5);
  const costMetric = response.metrics.find((metric) => metric.key === "positioningTotalCost");
  assert.equal(costMetric.values[0].currency, "MIXED");
});
