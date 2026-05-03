const assert = require("node:assert/strict");
const { test } = require("node:test");

const { computeFlightCosts, computeFlightCostsBatch } = require("../utils/costLogic");

const approx = (actual, expected, tolerance = 0.02) => {
  assert.ok(Math.abs(actual - expected) <= tolerance, `${actual} not within ${tolerance} of ${expected}`);
};

const baseFlight = {
  date: "2026-04-16",
  flight: "A103",
  sector: "CCU-BOM",
  depStn: "CCU",
  arrStn: "BOM",
  variant: "A320",
  acftType: "A320",
  aircraft: { registration: "VT-ABC", msn: "5825" },
  msn: "5825",
  bh: 2.8333,
  fh: 2.6167,
  ft: "",
  pax: 171,
  seats: 180,
  paxLF: 95,
  domIntl: "dom",
  dist: 895,
};

const baseConfig = {
  reportingCurrency: "USD",
  fxRates: [{ pair: "INR/USD", month: "04/26", rate: 0.012 }],
  fleet: [{ regn: "VT-ABC", sn: "5825", variant: "A320", mtow: 77000, entry: "2026-01-01" }],
  fuelConsum: [{ sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 8000 }],
  fuelConsumIndex: [{ acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1 }],
  plfEffect: [{ sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", p80: 1, p90: 1.02, p95: 1.034375, p98: 1.04, p100: 1.04 }],
  ccyFuel: [{ station: "CCU", month: "04/26", kgPerLtr: 0.78, intoPlaneRate: 92500, ccy: "INR" }],
};

test("engine fuel consumption and cost follow table x index x PLF and station fuel price", () => {
  const row = computeFlightCosts(baseFlight, baseConfig);
  assert.equal(row.engineFuelConsumptionKg, 8275);
  assert.equal(row.engineFuelKg, 8275);
  approx(row.engineFuelCost, (8275 / 0.78 / 1000) * 92500);
});

test("engine fuel cost applies May aircraft fuel index for DEL-BOM monthly schedule", () => {
  const flights = Array.from({ length: 31 }, (_, index) => ({
    date: `2026-05-${String(index + 1).padStart(2, "0")}`,
    flight: "A101",
    sector: "DEL-BOM",
    depStn: "DEL",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320",
    aircraft: { registration: "VT-AAB" },
    paxLF: 80,
  }));

  const rows = computeFlightCostsBatch(flights, {
    reportingCurrency: "INR",
    fuelConsum: [{ sectorOrGcd: "DEL-BOM", acftRegn: "VT-AAB", month: "05/26", fuelConsumptionKg: 1100 }],
    fuelConsumIndex: [{ acftRegn: "VT-AAB", month1: "04/26", month2: "05/26", value1: 1.03, value2: 1.05 }],
    plfEffect: [{ sectorOrGcd: "DEL-BOM", acftRegn: "VT-AAB", p75: 0.95, p90: 1.1 }],
    ccyFuel: [{ station: "DEL", month: "05/26", kgPerLtr: 0.78, intoPlaneRate: 70000, ccy: "INR" }],
  });

  approx(rows[0].engineFuelCost, 114019.23);
  approx(rows.reduce((total, row) => total + row.engineFuelCostRCCY, 0), 3534596.13);
});

test("PLF consumption supports additional percentage bands beyond the default set", () => {
  const row = computeFlightCosts({
    ...baseFlight,
    paxLF: 98.5,
  }, {
    ...baseConfig,
    fuelConsum: [{ sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 1000 }],
    fuelConsumIndex: [{ acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1 }],
    ccyFuel: [],
    plfEffect: [{
      sectorOrGcd: "CCU-BOM",
      acftRegn: "VT-ABC",
      p80: 1,
      p90: 1.02,
      p95: 1.04,
      p98: 1.05,
      p99: 1.08,
      p100: 1.08,
    }],
  });

  assert.equal(row.engineFuelConsumptionKg, 1080);
  assert.equal(row.engineFuelCost, 1080);
});

test("direct APU fuel uses the APU usage station fuel price", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    apuUsage: [{ arrStn: "BOM", acftRegn: "VT-ABC", apuHours: 0.75, consumptionPerApuHour: 255 }],
    ccyFuel: [{ station: "BOM", month: "04/26", kgPerLtr: 0.78, intoPlaneRate: 92500, ccy: "INR" }],
  });
  const expectedKg = 0.75 * 30 * 255;
  assert.equal(row.apuFuelConsumptionKg, expectedKg);
  approx(row.apuFuelLitres, expectedKg / 0.78);
  assert.equal(row.apuFuelCostDirect, 0);
  approx(row.apuFuelCostAllocated, (expectedKg / 0.78 / 1000) * 92500);
});

test("APU fuel requires matching Stn fuel price and does not use quantity as cost", () => {
  const row = computeFlightCosts({
    ...baseFlight,
    date: "2026-05-01",
    sector: "DEL-BOM",
    depStn: "DEL",
    arrStn: "BOM",
    aircraft: { registration: "VT-AAB" },
  }, {
    reportingCurrency: "INR",
    apuUsage: [{ stn: "BOM", acftRegn: "VT-AAB", fromDate: "2026-05-01", toDate: "2026-05-31", apuHrPerDay: 0.75, kgPerApuHr: 400 }],
    ccyFuel: [{ station: "DEL", month: "05/26", kgPerLtr: 0.79, intoPlaneRate: 72000, ccy: "INR" }],
  });

  assert.equal(row.apuFuelConsumptionKg, 0);
  assert.equal(row.apuFuelCost, 0);
  assert.equal(row.apuFuelCostRCCY, 0);
});

test("APU fuel pool is aircraft-month scoped and allocated by selected driver", () => {
  const flights = [
    {
      ...baseFlight,
      date: "2026-05-01",
      flight: "M101",
      sector: "DEL-BOM",
      depStn: "DEL",
      arrStn: "BOM",
      aircraft: { registration: "VT-AAB" },
      bh: 2,
    },
    {
      ...baseFlight,
      date: "2026-05-02",
      flight: "M102",
      sector: "BOM-DEL",
      depStn: "BOM",
      arrStn: "DEL",
      aircraft: { registration: "VT-AAB" },
      bh: 1,
    },
    {
      ...baseFlight,
      date: "2026-05-02",
      flight: "M201",
      sector: "DEL-BOM",
      depStn: "DEL",
      arrStn: "BOM",
      aircraft: { registration: "VT-OTHER" },
      bh: 9,
    },
  ];

  const rows = computeFlightCostsBatch(flights, {
    reportingCurrency: "INR",
    allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
    apuUsage: [{ stn: "BOM", acftRegn: "VT-AAB", fromDate: "2026-05-01", toDate: "2026-05-31", apuHrPerDay: 0.75, kgPerApuHr: 400 }],
    ccyFuel: [{ station: "BOM", month: "05/26", kgPerLtr: 0.79, intoPlaneRate: 72000, ccy: "INR" }],
  });

  const expectedPool = (0.75 * 31 * 400 / 0.79) * (72000 / 1000);
  approx(expectedPool, 847594.94);
  approx(rows[0].apuFuelCostAllocated, expectedPool * (2 / 3));
  approx(rows[1].apuFuelCostAllocated, expectedPool * (1 / 3));
  assert.equal(rows[2].apuFuelCostAllocated, 0);
  approx(rows.reduce((sum, row) => sum + row.apuFuelCostAllocated, 0), expectedPool);
});

test("additional APU usage allocates by configured departures and preserves pool total", () => {
  const flights = [
    baseFlight,
    { ...baseFlight, flight: "A104", date: "2026-04-17", bh: 1, fh: 1, aircraft: { registration: "VT-ABC", msn: "5825" } },
  ];
  const rows = computeFlightCostsBatch(flights, {
    ...baseConfig,
    allocationTable: [{ costCode: "APUFUELCOST", basis: "DEPARTURES" }],
    ccyFuel: [{ station: "BOM", month: "04/26", kgPerLtr: 0.78, intoPlaneRate: 92500, ccy: "INR" }],
    apuUsage: [{ arrStn: "BOM", addlnUse: "Y", acftRegn: "VT-ABC", fromDate: "2026-04-20", apuHours: 2, consumptionPerApuHour: 280 }],
  });
  const expectedPool = (560 / 0.78 / 1000) * 92500;
  approx(rows.reduce((sum, row) => sum + row.apuFuelCostAllocated, 0), expectedPool);
  approx(rows[0].apuFuelCostAllocated, expectedPool / 2);
});

test("additional APU usage prices from Stn even when flight stations differ", () => {
  const flights = [
    { ...baseFlight, flight: "A201", depStn: "DEL", arrStn: "BOM", bh: 2 },
    { ...baseFlight, flight: "A202", depStn: "CCU", arrStn: "BLR", bh: 1 },
  ];

  const rows = computeFlightCostsBatch(flights, {
    ...baseConfig,
    reportingCurrency: "INR",
    allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
    ccyFuel: [
      { station: "DEL", month: "04/26", kgPerLtr: 1, intoPlaneRate: 1000, ccy: "INR" },
      { station: "CCU", month: "04/26", kgPerLtr: 1, intoPlaneRate: 2000, ccy: "INR" },
      { station: "HYD", month: "04/26", kgPerLtr: 1, intoPlaneRate: 3000, ccy: "INR" },
    ],
    apuUsage: [{ stn: "HYD", addlnUse: "Y", acftRegn: "VT-ABC", fromDate: "2026-04-20", apuHours: 1, consumptionPerApuHour: 90 }],
  });

  assert.equal(rows.reduce((sum, row) => sum + row.apuFuelCostAllocated, 0), 270);
  assert.equal(rows[0].apuFuelCostAllocated, 180);
  assert.equal(rows[1].apuFuelCostAllocated, 90);
});

test("additional APU usage allocation is isolated by aircraft and month", () => {
  const flights = [
    { ...baseFlight, flight: "A301", date: "2026-04-16", bh: 2, aircraft: { registration: "VT-ABC", msn: "5825" } },
    { ...baseFlight, flight: "A302", date: "2026-04-17", bh: 1, aircraft: { registration: "VT-ABC", msn: "5825" } },
    { ...baseFlight, flight: "A401", date: "2026-04-17", bh: 9, aircraft: { registration: "VT-OTHER", msn: "9999" } },
    { ...baseFlight, flight: "A303", date: "2026-05-01", bh: 7, aircraft: { registration: "VT-ABC", msn: "5825" } },
  ];

  const rows = computeFlightCostsBatch(flights, {
    ...baseConfig,
    reportingCurrency: "INR",
    allocationTable: [{ costCode: "APUFUELCOST", basis: "BH" }],
    ccyFuel: [
      { station: "BOM", month: "04/26", kgPerLtr: 1, intoPlaneRate: 1000, ccy: "INR" },
      { station: "BOM", month: "05/26", kgPerLtr: 1, intoPlaneRate: 1000, ccy: "INR" },
    ],
    apuUsage: [
      { stn: "BOM", addlnUse: "Y", acftRegn: "VT-ABC", fromDate: "2026-04-20", apuHours: 1, consumptionPerApuHour: 300 },
    ],
  });

  assert.equal(rows[0].apuFuelCostAllocated, 200);
  assert.equal(rows[1].apuFuelCostAllocated, 100);
  assert.equal(rows[2].apuFuelCostAllocated, 0);
  assert.equal(rows[3].apuFuelCostAllocated, 0);
});

test("maintenance reserve FH contribution uses flight FH driver", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    leasedReserve: [{ acftRegn: "VT-ABC", sn: "5825", setRate: 290, driver: "FH", ccy: "USD", asOnDate: "2026-01-01" }],
  });
  approx(row.maintenanceReserveContribution, 758.84);
});

test("navigation uses nearest MTOW tier for ENR and terminal", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    navEnr: [{ sector: "CCU-BOM", ccy: "USD", "73000": 10, "77000": 20, "79000": 30 }],
    navTerm: [{ arrStn: "BOM", ccy: "USD", "73000": 4, "77000": 5, "79000": 6 }],
  });
  assert.equal(row.mtowUsed, 77000);
  assert.equal(row.navigation, 25);
});

test("domestic airport handling uses airportDom and landing MTOW tier", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    airportLanding: [{ arrStn: "BOM", ccy: "USD", "77000": 100 }],
    airportDom: [{ arrStn: "BOM", variant: "A320", ccy: "USD", cost: 25 }],
    airportIntl: [{ arrStn: "BOM", variant: "A320", ccy: "USD", cost: 999 }],
  });
  assert.equal(row.aptLandingCost, 100);
  assert.equal(row.aptHandlingCost, 25);
  assert.equal(row.airport, 125);
});

test("other DOC per BH uses flight block hours", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    otherDoc: [{ sector: "CCU-BOM", per: "BH", cost: 100, ccy: "USD" }],
  });
  approx(row.otherDoc, 283.33);
});

test("FX converts local currency to reporting currency", () => {
  const row = computeFlightCosts(baseFlight, {
    reportingCurrency: "USD",
    fxRates: [{ pair: "INR/USD", month: "04/26", rate: 0.012 }],
    transitMx: [{ depStn: "CCU", acftRegn: "VT-ABC", costPerDeparture: 1000, ccy: "INR" }],
  });
  assert.equal(row.transitMaintenanceRCCY, 12);
});

test("total RCCY equals the sum of component RCCY fields", () => {
  const row = computeFlightCosts(baseFlight, {
    ...baseConfig,
    transitMx: [{ depStn: "CCU", acftRegn: "VT-ABC", costPerDeparture: 100, ccy: "USD" }],
    otherDoc: [{ sector: "CCU-BOM", per: "BH", cost: 10, ccy: "USD" }],
  });
  const expected = [
    "engineFuelCostRCCY", "apuFuelCostRCCY", "maintenanceReserveContributionRCCY",
    "mrMonthlyRCCY", "qualifyingSchMxEventsRCCY", "transitMaintenanceRCCY",
    "otherMaintenanceRCCY", "otherMxExpensesRCCY", "rotableChangesRCCY",
    "navigationRCCY", "airportRCCY", "otherDocRCCY", "crewAllowancesRCCY",
    "layoverCostRCCY", "crewPositioningCostRCCY",
  ].reduce((sum, key) => Number((sum + (row[key] || 0)).toFixed(2)), 0);
  assert.equal(row.totalCostRCCY, expected);
});
