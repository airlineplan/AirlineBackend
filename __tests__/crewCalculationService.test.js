const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  DEFAULT_LAYOVER_RULES,
  DEFAULT_POSITIONING_COST_RULES,
  __testables__,
  buildMonthlyKpiSummaries,
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

  const ccuHotacRows = events.filter((event) => event.sourceType === "SYSTEM_REST" && event.location === "CCU");
  const ccuHotac = ccuHotacRows[0];
  assert.equal(ccuHotac.subCategory, "Layover HOTAC");
  assert.ok(ccuHotacRows.reduce((total, event) => total + event.rpMinutes, 0) > 360);
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

test("crew diary positions before rest when non-base cutoff is met at FDP end", () => {
  const events = calculateCrewMemberEvents({
    crewMember,
    flightAssignments: [
      { _id: "cutoff-1", std: d("2026-04-03T17:30:00"), sta: d("2026-04-03T19:30:00"), flightNumber: "C100", departureStation: "DEL", arrivalStation: "BOM" },
      { _id: "cutoff-2", std: d("2026-04-04T11:00:00"), sta: d("2026-04-04T13:00:00"), flightNumber: "C101", departureStation: "MAA", arrivalStation: "DEL" },
    ],
    dutySettings,
    positioningSettings: {
      ...positioningSettings,
      hotacCutoffEnabled: true,
      hotacCutoffLocalTime: "20:00",
    },
    layoverRules,
    positioningCostRules: [],
  });

  const postflight = events.find((event) => event.sourceType === "SYSTEM_POST_FLIGHT" && event.location === "BOM");
  assert.equal(postflight.endDateTime.toISOString(), "2026-04-03T20:00:00.000Z");

  const cutoffPositioning = events.find((event) => (
    event.sourceType === "SYSTEM_POSITIONING" &&
    event.departureStation === "BOM" &&
    event.arrivalStation === "MAA"
  ));
  assert.equal(cutoffPositioning.startDateTime.toISOString(), "2026-04-03T20:00:00.000Z");
  assert.equal(cutoffPositioning.fdpMinutes, 0);

  const hotelTransfer = events.find((event) => event.subCategory === "Airport to HOTAC Transfer" && event.location === "MAA");
  assert.equal(hotelTransfer.startDateTime.toISOString(), cutoffPositioning.endDateTime.toISOString());
  assert.equal(hotelTransfer.dpMinutes, 60);
  assert.equal(hotelTransfer.fdpMinutes, 0);

  const maaRest = events.find((event) => event.sourceType === "SYSTEM_REST" && event.location === "MAA");
  assert.equal(maaRest.startDateTime.toISOString(), hotelTransfer.endDateTime.toISOString());
  assert.equal(maaRest.subCategory, "Layover HOTAC");
  assert.equal(events.find((event) => event.sourceType === "SYSTEM_REST" && event.location === "BOM"), undefined);
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

test("crew diary does not auto-position after a non-flight duty period", () => {
  const events = calculateCrewMemberEvents({
    crewMember: { ...crewMember, baseStation: "BOM" },
    flightAssignments: [
      { _id: "nf-flight", std: d("2026-06-05T13:05:00"), sta: d("2026-06-05T14:50:00"), flightNumber: "9I611", departureStation: "BOM", arrivalStation: "AMD" },
    ],
    otherDuties: [
      { _id: "nf-duty", startDateTime: d("2026-06-03T16:00:00"), endDateTime: d("2026-06-03T17:00:00"), location: "DEL", category: "Training", subCategory: "CR session" },
    ],
    dutySettings,
    positioningSettings,
    layoverRules,
    positioningCostRules: [],
  });

  const automaticPositioning = events.find((event) => (
    event.sourceType === "SYSTEM_POSITIONING" &&
    event.departureStation === "DEL" &&
    event.arrivalStation === "BOM"
  ));
  assert.equal(automaticPositioning, undefined);
});

test("crew default layover rules classify zero-cost convenience and HOTAC rows", () => {
  const breakEvents = calculateCrewMemberEvents({
    crewMember,
    flightAssignments: [
      { _id: "default-break-1", std: d("2026-07-01T09:00:00"), sta: d("2026-07-01T10:00:00"), flightNumber: "DB100", departureStation: "DEL", arrivalStation: "BOM" },
      { _id: "default-break-2", std: d("2026-07-01T16:00:00"), sta: d("2026-07-01T17:00:00"), flightNumber: "DB101", departureStation: "BOM", arrivalStation: "DEL" },
    ],
    dutySettings,
    positioningSettings,
    layoverRules: DEFAULT_LAYOVER_RULES,
    positioningCostRules: [],
  });
  const convenience = breakEvents.find((event) => event.sourceType === "SYSTEM_BREAK" && event.location === "BOM");
  assert.equal(convenience.subCategory, "Convenience");
  assert.equal(convenience.layoverCost, 0);

  const hotacEvents = calculateCrewMemberEvents({
    crewMember,
    flightAssignments: [
      { _id: "default-hotac", std: d("2026-07-01T09:00:00"), sta: d("2026-07-01T11:00:00"), flightNumber: "DH100", departureStation: "DEL", arrivalStation: "BOM" },
    ],
    dutySettings,
    positioningSettings,
    layoverRules: DEFAULT_LAYOVER_RULES,
    positioningCostRules: [],
    coverageEnd: d("2026-07-02T00:00:00"),
  });
  const hotac = hotacEvents.find((event) => event.sourceType === "SYSTEM_REST" && event.location === "BOM");
  assert.equal(hotac.subCategory, "Layover HOTAC");
  assert.equal(hotac.layoverCost, 0);
});

test("crew positioning cost supports all-station defaults with specific overrides", () => {
  const { findPositioningCostRule } = __testables__;
  const specificRule = { departureStation: "BOM", arrivalStation: "DEL", sector: "BOM-DEL", role: "Captain", costAmount: 9000, currency: "INR" };
  const rules = [...DEFAULT_POSITIONING_COST_RULES, specificRule];

  assert.equal(findPositioningCostRule(rules, "BOM", "DEL", "Captain"), specificRule);
  assert.equal(findPositioningCostRule(rules, "CCU", "MAA", "First Officer").costAmount, 0);
});

test("crew diary splits rest by calendar day and covers the calculation window", () => {
  const events = calculateCrewMemberEvents({
    crewMember: { ...crewMember, baseStation: "BOM" },
    flightAssignments: [
      { _id: "cover-flight", std: d("2026-06-05T13:05:00"), sta: d("2026-06-05T14:50:00"), flightNumber: "9I611", departureStation: "BOM", arrivalStation: "AMD" },
    ],
    otherDuties: [
      { _id: "cover-duty", startDateTime: d("2026-06-03T16:00:00"), endDateTime: d("2026-06-03T17:00:00"), location: "DEL", category: "Training", subCategory: "CR session" },
    ],
    dutySettings,
    positioningSettings,
    layoverRules,
    positioningCostRules: [],
    coverageStart: d("2026-06-02T00:00:00"),
    coverageEnd: d("2026-06-06T00:00:00"),
  });

  const restRows = events.filter((event) => event.sourceType === "SYSTEM_REST");
  const fullJun4Rest = restRows.find((event) => (
    event.startDateTime.toISOString() === "2026-06-04T00:00:00.000Z" &&
    event.endDateTime.toISOString() === "2026-06-05T00:00:00.000Z"
  ));
  assert.ok(fullJun4Rest);
  assert.equal(fullJun4Rest.rpMinutes, 24 * 60);

  const preFirstDutyRest = restRows.find((event) => (
    event.startDateTime.toISOString() === "2026-06-02T00:00:00.000Z" &&
    event.endDateTime.toISOString() === "2026-06-03T00:00:00.000Z"
  ));
  assert.ok(preFirstDutyRest);

  const postLastDutyRest = restRows.find((event) => (
    event.startDateTime > d("2026-06-05T14:50:00") &&
    event.endDateTime.toISOString() === "2026-06-06T00:00:00.000Z"
  ));
  assert.ok(postLastDutyRest);
});

test("crew diary stores multi-day rest as one row per calendar date", () => {
  const { splitEventByUtcDay } = __testables__;
  const rows = splitEventByUtcDay({
    crewMemberId: "crew-2",
    crewCode: "2",
    crewName: "Vijay",
    role: "FO",
    startDateTime: d("2026-06-03T19:30:00"),
    endDateTime: d("2026-06-05T12:35:00"),
    displayDate: "2026-06-03",
    location: "BOM",
    category: "Rest",
    subCategory: "Rest at Base",
    sourceType: "SYSTEM_REST",
    dpMinutes: 0,
    fdpMinutes: 0,
    ftMinutes: 0,
    rpMinutes: 2465,
    dpCost: 0,
    fdpCost: 0,
    ftCost: 0,
    layoverCost: 0,
    positioningCost: 0,
    currency: "INR",
  });

  assert.deepEqual(rows.map((row) => [
    row.displayDate,
    row.startDateTime.toISOString(),
    row.endDateTime.toISOString(),
    row.rpMinutes,
  ]), [
    ["2026-06-03", "2026-06-03T19:30:00.000Z", "2026-06-04T00:00:00.000Z", 270],
    ["2026-06-04", "2026-06-04T00:00:00.000Z", "2026-06-05T00:00:00.000Z", 1440],
    ["2026-06-05", "2026-06-05T00:00:00.000Z", "2026-06-05T12:35:00.000Z", 755],
  ]);
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

test("crew KPI utilisation divides aggregate duty by aggregate rolewise targets", () => {
  const events = [
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-05T08:00:00"), endDateTime: d("2026-03-05T16:00:00"), category: "Flight Duty", subCategory: "Operated Flight", sourceType: "FLIGHT_ROSTER", dpMinutes: 480, fdpMinutes: 420, ftMinutes: 240, currency: "INR" },
    { crewMemberId: "c2", crewCode: "C2", role: "Captain", startDateTime: d("2026-03-06T08:00:00"), endDateTime: d("2026-03-06T16:00:00"), category: "Flight Duty", subCategory: "Operated Flight", sourceType: "FLIGHT_ROSTER", dpMinutes: 480, fdpMinutes: 420, ftMinutes: 240, currency: "INR" },
    { crewMemberId: "f1", crewCode: "F1", role: "First Officer", startDateTime: d("2026-03-07T08:00:00"), endDateTime: d("2026-03-07T14:00:00"), category: "Flight Duty", subCategory: "Operated Flight", sourceType: "FLIGHT_ROSTER", dpMinutes: 360, fdpMinutes: 300, ftMinutes: 180, currency: "INR" },
  ];
  const targets = [
    { role: "Captain", averageDpMinutesPerDay: 480, averageFdpMinutesPerDay: 420, averageFtMinutesPerDay: 240 },
    { role: "First Officer", averageDpMinutesPerDay: 360, averageFdpMinutesPerDay: 300, averageFtMinutesPerDay: 180 },
  ];

  const response = calculateKpiResponse({ events, targets, periodicity: "MONTHLY", startDate: "2026-03-01" });
  const dpMetric = response.metrics.find((metric) => metric.key === "dpUtilisationPercent");
  const fdpMetric = response.metrics.find((metric) => metric.key === "fdpUtilisationPercent");
  const ftMetric = response.metrics.find((metric) => metric.key === "ftUtilisationPercent");

  assert.equal(dpMetric.values[0].value, 3.23);
  assert.equal(fdpMetric.values[0].value, 3.23);
  assert.equal(ftMetric.values[0].value, 3.23);

  const monthlyRows = buildMonthlyKpiSummaries({
    userId: "user-1",
    calculationRunId: "run-1",
    events,
    targets,
  });
  const captainRow = monthlyRows.find((row) => row.role === "Captain");
  assert.equal(captainRow.dpUtilisationPercent, 3.23);
});

test("crew KPI weekly periods use master week endings without leaking prior month labels", () => {
  const events = [
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-06-01T08:00:00"), endDateTime: d("2026-06-01T16:00:00"), category: "Flight Duty", subCategory: "Operated Flight", sourceType: "FLIGHT_ROSTER", dpMinutes: 480, fdpMinutes: 420, ftMinutes: 240, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-06-30T08:00:00"), endDateTime: d("2026-06-30T12:00:00"), category: "Flight Duty", subCategory: "Operated Flight", sourceType: "FLIGHT_ROSTER", dpMinutes: 240, fdpMinutes: 240, ftMinutes: 120, currency: "INR" },
  ];

  const response = calculateKpiResponse({
    events,
    targets: [],
    periodicity: "WEEKLY",
    startDate: "2026-06-01",
    endDate: "2026-06-30",
  });

  assert.deepEqual(response.periods.map((period) => period.label), [
    "Week 07 Jun",
    "Week 14 Jun",
    "Week 21 Jun",
    "Week 28 Jun",
    "Week 05 Jul",
  ]);
  assert.ok(!response.periods.some((period) => period.label.includes("May")));

  const ftMetric = response.metrics.find((metric) => metric.key === "totalFtMinutes");
  assert.equal(ftMetric.values[0].value, 240);
  assert.equal(ftMetric.values[4].value, 120);
});

test("crew KPI average costs divide by matching diary occurrence counts", () => {
  const events = [
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-05T08:00:00"), endDateTime: d("2026-03-05T10:00:00"), category: "Positioning", subCategory: "Deadheading", sourceType: "SYSTEM_POSITIONING", positioningCost: 200, layoverCost: 0, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-06T08:00:00"), endDateTime: d("2026-03-06T10:00:00"), category: "Positioning", subCategory: "Uploaded travel", sourceType: "OTHER_DUTY_ROSTER", positioningCost: 0, layoverCost: 0, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-07T08:00:00"), endDateTime: d("2026-03-07T12:00:00"), category: "Break", subCategory: "Convenience", sourceType: "SYSTEM_BREAK", positioningCost: 0, layoverCost: 100, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-08T08:00:00"), endDateTime: d("2026-03-08T20:00:00"), category: "Rest", subCategory: "Layover HOTAC", sourceType: "SYSTEM_REST", positioningCost: 0, layoverCost: 300, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-09T08:00:00"), endDateTime: d("2026-03-09T12:00:00"), category: "Break", subCategory: "Convenience", sourceType: "SYSTEM_BREAK", positioningCost: 0, layoverCost: 0, currency: "INR" },
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-10T08:00:00"), endDateTime: d("2026-03-10T20:00:00"), category: "Rest", subCategory: "Layover HOTAC", sourceType: "SYSTEM_REST", positioningCost: 0, layoverCost: 0, currency: "INR" },
  ];

  const response = calculateKpiResponse({ events, targets: [], periodicity: "MONTHLY", startDate: "2026-03-01" });
  const value = (key) => response.metrics.find((metric) => metric.key === key).values[0].value;

  assert.equal(value("positioningCount"), 2);
  assert.equal(value("positioningAverageCost"), 100);
  assert.equal(value("layoverOccurrences"), 4);
  assert.equal(value("layoverDurationMinutes"), 1920);
  assert.equal(value("layoverTotalCost"), 400);
  assert.equal(value("layoverAverageCost"), 100);
  assert.equal(value("convenienceAverageCost"), 50);
  assert.equal(value("hotacAverageCost"), 150);

  const monthlyRows = buildMonthlyKpiSummaries({
    userId: "user-1",
    calculationRunId: "run-1",
    events,
    targets: [],
  });
  assert.equal(monthlyRows[0].layoverOccurrences, 4);
  assert.equal(monthlyRows[0].layoverDurationMinutes, 1920);
  assert.equal(monthlyRows[0].layoverAverageCost, 100);
});

test("crew KPI does not count HOTAC airport transfers as layover occurrences", () => {
  const events = [
    { crewMemberId: "c1", crewCode: "C1", role: "Captain", startDateTime: d("2026-03-05T08:00:00"), endDateTime: d("2026-03-05T09:00:00"), category: "Positioning", subCategory: "HOTAC to Airport Transfer", sourceType: "SYSTEM_POSITIONING", positioningCost: 0, layoverCost: 0, currency: "INR" },
  ];

  const response = calculateKpiResponse({ events, targets: [], periodicity: "MONTHLY", startDate: "2026-03-01" });
  const value = (key) => response.metrics.find((metric) => metric.key === key).values[0].value;

  assert.equal(value("positioningCount"), 1);
  assert.equal(value("layoverOccurrences"), 0);
  assert.equal(value("layoverDurationMinutes"), 0);
  assert.equal(value("hotacTotalCost"), 0);
});
