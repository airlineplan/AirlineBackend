const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  normalizeCostConfig,
  computeFlightCosts,
  computeFlightCostsBatch,
  serializeNavigationCostRows,
  getFlightSnContext,
} = require("../utils/costLogic");
const { __private__: apuFuelPrivate } = require("../controller/apuFuelController");

test("normalizeCostConfig preserves maintenance UI fields for round-trip save/load", () => {
  const normalized = normalizeCostConfig({
    leasedReserve: [
      {
        mrAccId: "1",
        schMxEvent: "6YSI",
        acftRegn: "VT-ABC",
        pn: "A320",
        sn: "5825",
        setBalance: "278455",
        setRate: "6558.00",
        asOnDate: "2026-04-01",
        ccy: "USD",
        driver: "Month",
        annualEscl: "3.50",
        anniversary: "2023-05-01",
        endDate: "2026-06-01",
      },
    ],
    schMxEvents: [
      {
        date: "2026-04-01",
        msnEsnApun: "6YSI",
        event: "C-check",
        pn: "PN-1",
        snBn: "SN-1",
        hours: "12",
        cycles: "3",
        days: "5",
        cost: "2500",
        ccy: "USD",
        mrAccId: "1",
        drawdownDate: "2026-05-01",
        mrDrawdown: "100",
        mrDrawdownCcy: "USD",
        openingBal: "3000",
        remaining: "2900",
        capitalisation: "N",
        _hydratedFields: ["hours", "cycles", "days", "openingBal", "remaining"],
      },
    ],
    rotableChanges: [
      {
        label: "Rotable",
        date: "2026-04-01",
        month: "2026-04-01",
        pn: "PN-2",
        msn: "MSN-1",
        acftRegn: "VT-ABC",
        position: "L1",
        removedSN: "R-1",
        installedSN: "I-1",
        cost: "450",
        ccy: "USD",
      },
    ],
    transitMx: [
      {
        depStn: "DEL",
        variant: "A320",
        acftRegn: "VT-ABC",
        pn: "A320ceo",
        sn: "5825",
        fromDate: "2026-04-01",
        toDate: "2026-05-31",
        costPerDeparture: "15400",
        ccy: "INR",
      },
    ],
    otherMx: [
      {
        depStn: "DEL",
        variant: "737",
        acftRegn: "VT-IJK",
        pn: "PN-1",
        sn: "SN-1",
        costPerBh: "125",
        costPerDeparture: "",
        costPerMonth: "",
        fromDate: "2026-04-16",
        toDate: "2026-04-30",
        ccy: "USD",
      },
    ],
  });

  assert.equal(normalized.leasedReserve[0].schMxEvent, "6YSI");
  assert.equal(normalized.leasedReserve[0].asOnDate, "2026-04-01");
  assert.equal(normalized.leasedReserve[0].annualEscl, 3.5);
  assert.equal(normalized.leasedReserve[0].anniversary, "2023-05-01");
  assert.equal(normalized.leasedReserve[0].endDate, "2026-06-01");

  assert.equal(normalized.schMxEvents[0].hours, 12);
  assert.equal(normalized.schMxEvents[0].cycles, 3);
  assert.equal(normalized.schMxEvents[0].days, 5);
  assert.equal(normalized.schMxEvents[0].mrDrawdown, 100);
  assert.equal(normalized.schMxEvents[0].mrDrawdownCcy, "USD");
  assert.equal(normalized.schMxEvents[0].drawdownDate, "2026-05-01");
  assert.equal(normalized.schMxEvents[0].hours, 12);
  assert.equal(normalized.schMxEvents[0].openingBal, 3000);
  assert.equal(normalized.schMxEvents[0].remaining, 2900);
  assert.equal(normalized.schMxEvents[0].capitalisation, "N");
  assert.deepEqual(normalized.schMxEvents[0]._hydratedFields, ["hours", "cycles", "days", "openingBal", "remaining"]);

  assert.equal(normalized.rotableChanges[0].position, "L1");
  assert.equal(normalized.rotableChanges[0].removedSN, "R-1");
  assert.equal(normalized.rotableChanges[0].installedSN, "I-1");

  assert.equal(normalized.transitMx[0].depStn, "DEL");
  assert.equal(normalized.transitMx[0].variant, "A320");
  assert.equal(normalized.transitMx[0].acftRegn, "VT-ABC");
  assert.equal(normalized.transitMx[0].pn, "A320CEO");
  assert.equal(normalized.transitMx[0].sn, "5825");
  assert.equal(normalized.transitMx[0].costPerDeparture, 15400);
  assert.equal(normalized.transitMx[0].ccy, "INR");

  assert.equal(normalized.otherMx[0].depStn, "DEL");
  assert.equal(normalized.otherMx[0].variant, "737");
  assert.equal(normalized.otherMx[0].acftRegn, "VT-IJK");
  assert.equal(normalized.otherMx[0].pn, "PN-1");
  assert.equal(normalized.otherMx[0].sn, "SN-1");
  assert.equal(normalized.otherMx[0].costPerBh, 125);
  assert.equal(normalized.otherMx[0].ccy, "USD");
});

test("cost enrichment populates MSN, engine ESNs, and APUN from aircraft on-wing rows", () => {
  const flight = {
    date: "2026-04-20",
    aircraft: { msn: "5825", registration: "VT-ABC" },
    flight: "AB123",
  };

  const onwing = [
    { date: "2026-04-01", msn: "5825", pos1Esn: "ENG-1", pos2Esn: "ENG-2", apun: "APU-1" },
  ];

  const context = getFlightSnContext(flight, onwing);
  const [enriched] = computeFlightCostsBatch([flight], { aircraftOnwing: onwing });

  assert.deepEqual(context.snList, ["5825", "ENG-1", "ENG-2", "APU-1"]);
  assert.equal(enriched.msn, "5825");
  assert.equal(enriched.eng1Esn, "ENG-1");
  assert.equal(enriched.eng2Esn, "ENG-2");
  assert.equal(enriched.apun, "APU-1");
  assert.equal(enriched.sn, "5825, ENG-1, ENG-2, APU-1");
});

test("normalizeCostConfig preserves airport other mtow rows", () => {
  const normalized = normalizeCostConfig({
    airportOther: [
      {
        arrStn: "BOM",
        ccy: "INR",
        73000: "17800",
        77000: "18100",
        78000: "18150",
        79000: "18200",
      },
    ],
  });

  assert.equal(normalized.airportOther[0].arrStn, "BOM");
  assert.equal(normalized.airportOther[0].ccy, "INR");
  assert.equal(normalized.airportOther[0]["77000"], 18100);
});

test("normalizeCostConfig preserves airport landing mtow rows", () => {
  const normalized = normalizeCostConfig({
    airportLanding: [
      {
        arrStn: "BOM",
        mtow: "77000",
        variant: "A320",
        month: "04/26",
        cost: "1090",
        ccy: "USD",
      },
    ],
  });

  assert.equal(normalized.airportLanding[0].arrStn, "BOM");
  assert.equal(normalized.airportLanding[0].mtow, 77000);
  assert.equal(normalized.airportLanding[0].cost, 1090);
  assert.equal(normalized.airportLanding[0].ccy, "USD");
});

test("normalizeCostConfig preserves airport handling mtow rows", () => {
  const normalized = normalizeCostConfig({
    airportDom: [
      {
        arrStn: "BOM",
        mtow: "77000",
        variant: "A320",
        month: "04/26",
        cost: "118",
        ccy: "USD",
      },
    ],
  });

  assert.equal(normalized.airportDom[0].arrStn, "BOM");
  assert.equal(normalized.airportDom[0].mtow, 77000);
  assert.equal(normalized.airportDom[0].cost, 118);
  assert.equal(normalized.airportDom[0].ccy, "USD");
});

test("airport other matches the row by arrival station and mtow tier", () => {
  const flight = {
    date: "2026-04-20",
    domIntl: "DOM",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    fleet: [
      {
        regn: "VT-ABC",
        mtow: 77000,
        entry: "2026-01-01",
        exit: "2026-12-31",
      },
    ],
    airportOther: [
      { arrStn: "BOM", ccy: "INR", 73000: 17800, 77000: 18100, 78000: 18150, 79000: 18200 },
    ],
  });

  assert.equal(enriched.aptOtherCost, 18100);
  assert.equal(enriched.airport, 18100);
  assert.equal(enriched.airportCCY, "INR");
});

test("normalizeCostConfig preserves custom navigation mtow tiers", () => {
  const normalized = normalizeCostConfig({
    navMtowTiers: ["71000", "76000", "81000"],
    navEnr: [
      {
        sector: "DEL-BOM",
        "71000": 100,
        "76000": 110,
        "81000": 120,
      },
    ],
  });

  assert.deepEqual(normalized.navMtowTiers, [71000, 76000, 81000]);
  assert.equal(normalized.navEnr[0]["71000"], 100);
  assert.equal(normalized.navEnr[0]["76000"], 110);
  assert.equal(normalized.navEnr[0]["81000"], 120);
});

test("transit maintenance applies SN, ACFT Regn, PN, Variant precedence", () => {
  const flight = {
    date: "2026-04-12",
    depStn: "DEL",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    transitMx: [
      { depStn: "DEL", variant: "A320", costPerDeparture: 10, ccy: "INR" },
      { depStn: "DEL", acftRegn: "VT-ABC", costPerDeparture: 20, ccy: "INR" },
      { depStn: "DEL", pn: "A320ceo", costPerDeparture: 30, ccy: "INR" },
      { depStn: "DEL", sn: "5825", costPerDeparture: 40, ccy: "INR" },
    ],
  });

  assert.equal(enriched.transitMaintenance, 40);
  assert.equal(enriched.transitMaintenanceCCY, "INR");
});

test("transit maintenance returns zero when the flight date is outside the matching date range", () => {
  const flight = {
    date: "2026-06-01",
    depStn: "DEL",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    transitMx: [
      { depStn: "DEL", variant: "A320", costPerDeparture: 15400, ccy: "INR", fromDate: "2026-04-01", toDate: "2026-05-31" },
    ],
  });

  assert.equal(enriched.transitMaintenance, 0);
  assert.equal(enriched.transitMaintenanceCCY, "");
});

test("transit maintenance prefers aircraft rows over variant rows and keeps the latest matching row", () => {
  const flight = {
    date: "2026-04-20",
    depStn: "DEL",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    transitMx: [
      { depStn: "DEL", variant: "A320", costPerDeparture: 14000, ccy: "INR", fromDate: "2026-04-01", toDate: "2026-05-31" },
      { depStn: "DEL", acftRegn: "VT-ABC", costPerDeparture: 15400, ccy: "INR", fromDate: "2026-04-01", toDate: "2026-05-31" },
      { depStn: "DEL", acftRegn: "VT-ABC", costPerDeparture: 15600, ccy: "INR", fromDate: "2026-04-15", toDate: "2026-05-31" },
    ],
  });

  assert.equal(enriched.transitMaintenance, 15600);
  assert.equal(enriched.transitMaintenanceCCY, "INR");
});

test("airport landing matches the row by arrival station and mtow", () => {
  const flight = {
    date: "2026-04-20",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "USD",
    fleet: [
      {
        regn: "VT-ABC",
        mtow: 77000,
        entry: "2026-01-01",
        exit: "2026-12-31",
      },
    ],
    airportLanding: [
      { arrStn: "BOM", mtow: 73000, cost: 982.6, ccy: "USD" },
      { arrStn: "BOM", mtow: 77000, cost: 1090, ccy: "USD" },
      { arrStn: "BOM", cost: 1200, ccy: "USD" },
    ],
  });

  assert.equal(enriched.aptLandingCost, 1090);
  assert.equal(enriched.airport, 1090);
  assert.equal(enriched.airportCCY, "USD");
});

test("airport handling matches the row by arrival station and mtow", () => {
  const flight = {
    date: "2026-04-20",
    domIntl: "DOM",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320ceo",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "USD",
    fleet: [
      {
        regn: "VT-ABC",
        mtow: 77000,
        entry: "2026-01-01",
        exit: "2026-12-31",
      },
    ],
    airportDom: [
      { arrStn: "BOM", mtow: 73000, cost: 102, ccy: "USD" },
      { arrStn: "BOM", mtow: 77000, cost: 118, ccy: "USD" },
      { arrStn: "BOM", cost: 125, ccy: "USD" },
    ],
  });

  assert.equal(enriched.aptHandlingCost, 118);
  assert.equal(enriched.airport, 118);
  assert.equal(enriched.airportCCY, "USD");
});

test("other maintenance sums all matching rows and keeps monthly charges additive", () => {
  const flight = {
    date: "2026-04-20",
    depStn: "DEL",
    variant: "737",
    acftType: "CFM56-5B",
    aircraft: {
      registration: "VT-IJK",
      msn: "SN-1",
    },
    bh: 10,
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "USD",
    otherMx: [
      { variant: "737", costPerBh: 100, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
      { acftRegn: "VT-IJK", costPerBh: 25, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
      { pn: "CFM56-5B", costPerDeparture: 50, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
      { sn: "SN-1", costPerDeparture: 10, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
      { depStn: "DEL", variant: "737", costPerMonth: 700, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
      { depStn: "DEL", acftRegn: "VT-IJK", costPerMonth: 300, ccy: "USD", fromDate: "2026-04-01", toDate: "2026-04-30" },
    ],
  });

  assert.equal(enriched.otherMaintenance, 1310);
  assert.equal(enriched.otherMaintenance1, 1250);
  assert.equal(enriched.otherMaintenance2, 60);
  assert.equal(enriched.otherMaintenance3, 1000);
  assert.equal(enriched.otherMxExpenses, 1000);
  assert.equal(enriched.otherMxExpensesCCY, "USD");
});

test("other mx monthly expenses follow the allocation-table basis within the aircraft-month group", () => {
  const flights = [
    {
      date: "2026-04-05",
      bh: 2,
      fh: 1,
      aircraft: {
        registration: "VT-IJK",
      },
    },
    {
      date: "2026-04-18",
      bh: 3,
      fh: 3,
      aircraft: {
        registration: "VT-IJK",
      },
    },
    {
      date: "2026-04-20",
      bh: 4,
      fh: 6,
      aircraft: {
        registration: "VT-XYZ",
      },
    },
    {
      date: "2026-05-01",
      bh: 2,
      fh: 2,
      aircraft: {
        registration: "VT-IJK",
      },
    },
  ];

  const baseConfig = {
    reportingCurrency: "EUR",
    otherMx: [
      {
        acftRegn: "VT-IJK",
        costPerMonth: 1000,
        ccy: "EUR",
        fromDate: "2026-04-01",
        toDate: "2026-04-30",
      },
    ],
  };

  const fhEnriched = computeFlightCostsBatch(flights, {
    ...baseConfig,
    costAllocation: [
      {
        label: "Other maintenance expences based on Variant/ACFT Regn/PN/SN performing the flight when such cost is on per Month basis",
        basisOfAllocation: "FH",
      },
    ],
  });
  assert.equal(fhEnriched[0].otherMxExpenses, 250);
  assert.equal(fhEnriched[1].otherMxExpenses, 750);

  const bhEnriched = computeFlightCostsBatch(flights, {
    ...baseConfig,
    costAllocation: [
      {
        label: "Other maintenance expences based on Variant/ACFT Regn/PN/SN performing the flight when such cost is on per Month basis",
        basisOfAllocation: "BH",
      },
    ],
  });
  assert.equal(bhEnriched[0].otherMxExpenses, 400);
  assert.equal(bhEnriched[1].otherMxExpenses, 600);

  const departuresEnriched = computeFlightCostsBatch(flights, {
    ...baseConfig,
    costAllocation: [
      {
        label: "Other maintenance expences based on Variant/ACFT Regn/PN/SN performing the flight when such cost is on per Month basis",
        basisOfAllocation: "Departures",
      },
    ],
  });
  assert.equal(departuresEnriched[0].otherMxExpenses, 500);
  assert.equal(departuresEnriched[1].otherMxExpenses, 500);

  assert.equal(fhEnriched[0].otherMxExpensesCCY, "EUR");
  assert.equal(fhEnriched[1].otherMxExpensesCCY, "EUR");
  assert.equal(fhEnriched[2].otherMxExpenses, 0);
  assert.equal(fhEnriched[3].otherMxExpenses, 0);
});

test("maintenance reserve contribution uses flight FH and the matched engine SN schedule rate", () => {
  const flight = {
    date: "2026-04-15",
    fh: 12,
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
    acftType: "A320",
    variant: "A320",
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "USD",
    leasedReserve: [
      {
        mrAccId: "1",
        acftRegn: "VT-ABC",
        pn: "A320",
        sn: "740811",
        setRate: 61.2733333333,
        ccy: "USD",
        driver: "FH",
        asOnDate: "2026-04-01",
        endDate: "2026-12-31",
      },
    ],
    aircraftOnwing: [
      {
        date: "2026-04-01",
        msn: "5825",
        pos1Esn: "740811",
        pos2Esn: "740812",
        apun: "990001",
      },
    ],
    maintenanceReserveSchedule: [
      {
        date: "2026-05-01",
        msn: "740811",
        mrAccId: "1",
        rate: 61.2733333333,
        ccy: "USD",
      },
    ],
  });

  assert.equal(enriched.maintenanceReserveContribution, 735.28);
  assert.equal(enriched.maintenanceReserveContributionCCY, "USD");
  assert.equal(enriched.mrContribution, 735.28);
});

test("maintenance reserve monthly contribution splits the schedule contribution by aircraft-month BH share", () => {
  const flights = [
    {
      date: "2026-04-03",
      bh: 2,
      aircraft: {
        registration: "VT-ABC",
        msn: "5825",
      },
    },
    {
      date: "2026-04-17",
      bh: 6,
      aircraft: {
        registration: "VT-ABC",
        msn: "5825",
      },
    },
    {
      date: "2026-04-21",
      bh: 4,
      aircraft: {
        registration: "VT-XYZ",
        msn: "9999",
      },
    },
    {
      date: "2026-05-02",
      bh: 3,
      aircraft: {
        registration: "VT-ABC",
        msn: "5825",
      },
    },
  ];

  const enriched = computeFlightCostsBatch(flights, {
    reportingCurrency: "USD",
    aircraftOnwing: [
      {
        date: "2026-04-01",
        msn: "5825",
        pos1Esn: "740811",
        pos2Esn: "740812",
        apun: "990001",
      },
      {
        date: "2026-04-01",
        msn: "9999",
        pos1Esn: "111111",
        pos2Esn: "111112",
        apun: "990002",
      },
    ],
    maintenanceReserveSchedule: [
      {
        date: "2026-04-01",
        msn: "740811",
        mrAccId: "1",
        contribution: 800,
        ccy: "USD",
        driver: "MONTH",
        driverVal: 4,
      },
      {
        date: "2026-04-01",
        msn: "111111",
        mrAccId: "2",
        contribution: 500,
        ccy: "USD",
        driver: "MONTH",
        driverVal: 4,
      },
      {
        date: "2026-05-01",
        msn: "740811",
        mrAccId: "1",
        contribution: 900,
        ccy: "USD",
        driver: "MONTH",
        driverVal: 5,
      },
    ],
  });

  assert.equal(enriched[0].mrMonthly, 200);
  assert.equal(enriched[1].mrMonthly, 600);
  assert.equal(enriched[2].mrMonthly, 500);
  assert.equal(enriched[3].mrMonthly, 900);
});

test("engine fuel consumption multiplies fuel consumption, fuel index, and the matched PLF band", () => {
  const flight = {
    date: "2026-04-16",
    sector: "CCU-BOM",
    depStn: "CCU",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320",
    aircraft: {
      registration: "VT-ABC",
    },
    ask: 306000,
    rsk: 290700,
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    fuelConsum: [
      { sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 8275, ccy: "INR" },
    ],
    fuelConsumIndex: [
      { acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1.0 },
    ],
    plfEffect: [
      { sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", p80: 1.0, p90: 1.0, p95: 1.0, p98: 1.0, p100: 1.0 },
    ],
  });

  assert.equal(enriched.engineFuelConsumption, 8275);
  assert.equal(enriched.engineFuelCost, 8275);
  assert.equal(enriched.engineFuelCostCCY, "");
});

test("engine fuel consumption picks the next available PLF threshold when load factor is between bands", () => {
  const flight = {
    date: "2026-04-16",
    sector: "DEL-BOM",
    depStn: "DEL",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320",
    aircraft: {
      registration: "VT-ABC",
    },
    ask: 100,
    rsk: 87,
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    fuelConsum: [
      { sectorOrGcd: "DEL-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 1000, ccy: "INR" },
    ],
    fuelConsumIndex: [
      { acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1.0 },
    ],
    plfEffect: [
      { sectorOrGcd: "DEL-BOM", acftRegn: "VT-ABC", p80: 0.96, p90: 0.98, p95: 1.0, p98: 1.01, p100: 1.01 },
    ],
  });

  assert.equal(enriched.engineFuelConsumption, 980);
  assert.equal(enriched.engineFuelCost, 980);
});

test("engine fuel cost uses departure-station fuel price and month-specific per-kLtr rate", () => {
  const flight = {
    date: "2026-04-16",
    sector: "CCU-BOM",
    depStn: "CCU",
    arrStn: "BOM",
    variant: "A320",
    acftType: "A320",
    aircraft: {
      registration: "VT-ABC",
    },
    ask: 306000,
    rsk: 290700,
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    fuelConsum: [
      { sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", month: "04/26", fuelConsumptionKg: 8275, ccy: "INR" },
    ],
    fuelConsumIndex: [
      { acftRegn: "VT-ABC", month: "04/26", fuelConsumptionIndex: 1.0 },
    ],
    ccyFuel: [
      { station: "CCU", ccy: "INR", kgPerLtr: 0.782, month: "04/26", intoPlaneRate: 92500 },
    ],
    plfEffect: [
      { sectorOrGcd: "CCU-BOM", acftRegn: "VT-ABC", p80: 1.0, p90: 1.0, p95: 1.0, p98: 1.0, p100: 1.0 },
    ],
  });

  assert.equal(enriched.engineFuelConsumption, 8275);
  assert.equal(enriched.engineFuelCost, 978820.33);
  assert.equal(enriched.engineFuelCostCCY, "INR");
});

test("navigation ENR prefers the converted amount for the flight master field", () => {
  const flight = {
    date: "2026-04-20",
    sector: "DEL-BOM",
    depStn: "DEL",
    arrStn: "BOM",
    variant: "A320",
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    navEnr: [
      {
        sector: "DEL-BOM",
        variant: "A320",
        cost: 73000,
        costRCCY: 23500,
        ccy: "ENR",
        fromDate: "2026-04-01",
        toDate: "2026-04-30",
      },
    ],
  });

  assert.equal(enriched.navEnr, 23500);
  assert.equal(enriched.navigation, 23500);
  assert.equal(enriched.navigationCCY, "ENR");
});

test("navigation costs use the aircraft MTOW tier from fleet data", () => {
  const flight = {
    date: "2026-04-20",
    sector: "DEL-BOM",
    depStn: "DEL",
    arrStn: "BOM",
    variant: "A320",
    aircraft: {
      registration: "VT-ABC",
      msn: "5825",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    fleet: [
      {
        regn: "VT-ABC",
        sn: "5825",
        mtow: 77000,
        entry: "2026-03-10",
        exit: "2026-06-15",
      },
    ],
    navEnr: [
      {
        sector: "DEL-BOM",
        variant: "A320",
        ccy: "INR",
        month: "04/26",
        "73000": 23500,
        "77000": 24150,
        "78000": 24225,
        "79000": 24300,
      },
    ],
    navTerm: [
      {
        arrStn: "BOM",
        variant: "A320",
        ccy: "USD",
        month: "04/26",
        "73000": 86.02,
        "77000": 86.02,
        "78000": 86.02,
        "79000": 86.02,
      },
    ],
  });

  assert.equal(enriched.navEnr, 24150);
  assert.equal(enriched.navTrml, 86.02);
  assert.equal(enriched.navigation, 24236.02);
  assert.equal(enriched.navigationCCY, "INR");
});

test("navigation tables serialize to the exact frontend shape", () => {
  const serialized = serializeNavigationCostRows(
    [
      {
        sector: "DEL-BOM",
        variant: "A320",
        month: "04/26",
        cost: 23500,
        costRCCY: 23500,
        ccy: "INR",
        "73000": 23500,
        "77000": 24150,
        "78000": 24225,
        "79000": 24300,
      },
    ],
    "sector"
  );

  assert.deepEqual(serialized, [
    {
      sector: "DEL-BOM",
      ccy: "INR",
      "73000": 23500,
      "77000": 24150,
      "78000": 24225,
      "79000": 24300,
    },
  ]);
});

test("APU fuel allocation follows the configured basis", () => {
  const flights = [
    {
      date: "2026-04-20",
      flight: "F1001",
      depStn: "DEL",
      arrStn: "BOM",
      variant: "737",
      bh: 2,
      fh: 3,
      aircraft: {
        registration: "VT-IJK",
      },
    },
    {
      date: "2026-04-20",
      flight: "F1002",
      depStn: "DEL",
      arrStn: "BOM",
      variant: "737",
      bh: 1,
      fh: 5,
      aircraft: {
        registration: "VT-IJK",
      },
    },
  ];

  const enriched = computeFlightCostsBatch(flights, {
    reportingCurrency: "INR",
    allocationTable: [
      { costCode: "APUFUELCOST", basis: "BH" },
    ],
    apuUsage: [
      {
        variant: "737",
        acftRegn: "VT-IJK",
        addlnUse: "Y",
        fromDate: "2026-04-20",
        apuHours: 1,
        consumptionPerApuHour: 300,
        ccy: "INR",
      },
    ],
  });

  assert.equal(enriched[0].apuFuelCost, 200);
  assert.equal(enriched[1].apuFuelCost, 100);
  assert.equal(enriched[0].apuFuelCostCCY, "INR");
  assert.equal(enriched[1].apuFuelCostCCY, "INR");
});

test("generated APU fuel rows use arrival-based APU usage and departure-month fuel price", () => {
  const row = apuFuelPrivate.buildGeneratedApuFuelRow(
    {
      _id: "flight-1",
      date: "2026-04-16",
      depStn: "CCU",
      arrStn: "BOM",
      variant: "A320",
      aircraft: {
        registration: "VT-ABC",
      },
    },
    {
      apuUsage: [
        {
          arrStn: "BOM",
          variant: "A320",
          acftRegn: "VT-ABC",
          apuHours: 0.75,
          consumptionPerApuHour: 255,
        },
      ],
      ccyFuel: [
        {
          station: "CCU",
          month: "04/26",
          kgPerLtr: 0.78,
          intoPlaneRate: 92500,
          ccy: "INR",
        },
      ],
    }
  );

  assert.equal(row.arrStn, "BOM");
  assert.equal(row.acftRegn, "VT-ABC");
  assert.equal(row.apuHr, 0.75);
  assert.equal(row.consumptionKgPerApuHr, 255);
  assert.equal(row.consumptionKg, 191.25);
  assert.equal(row.costPerLtr, 92.5);
  assert.equal(row.consumptionLitres, 191.25 / 0.78);
  assert.ok(Math.abs(row.totalFuelCost - 22680.28846153846) < 1e-9);
});

test("generated APU fuel rows keep arrStn blank for additional-use usage rows", () => {
  const row = apuFuelPrivate.buildGeneratedApuFuelRow(
    {
      _id: "flight-2",
      date: "2026-04-20",
      depStn: "CCU",
      arrStn: "BOM",
      variant: "737",
      aircraft: {
        registration: "VT-IJK",
      },
    },
    {
      apuUsage: [
        {
          arrStn: "",
          variant: "737",
          acftRegn: "VT-IJK",
          apuHours: 1.5,
          consumptionPerApuHour: 280,
          addlnUse: "Y",
          fromDate: "2026-04-20",
          toDate: "2026-04-20",
        },
      ],
      ccyFuel: [
        {
          station: "CCU",
          month: "04/26",
          kgPerLtr: 0.78,
          intoPlaneRate: 94000,
          ccy: "INR",
        },
      ],
    },
    [
      {
        _id: "flight-1",
        date: "2026-04-20",
        flight: "F1001",
        depStn: "DEL",
        arrStn: "BOM",
        variant: "737",
        aircraft: {
          registration: "VT-IJK",
        },
      },
      {
        _id: "flight-2",
        date: "2026-04-20",
        flight: "F1002",
        depStn: "CCU",
        arrStn: "BOM",
        variant: "737",
        aircraft: {
          registration: "VT-IJK",
        },
      },
    ]
  );

  assert.equal(row.arrStn, "");
  assert.equal(row.acftRegn, "VT-IJK");
  assert.equal(row.apuHr, 1.5);
  assert.equal(row.consumptionKgPerApuHr, 280);
  assert.equal(row.consumptionKg, 420);
  assert.equal(row.costPerLtr, 94);
  assert.equal(row.costSourceType, "LAST_DEP_STN_RCCY");
  assert.equal(row.costSourceStation, "CCU");
  assert.equal(row.sourceFlightId, "flight-2");
  assert.equal(row.consumptionLitres, 420 / 0.78);
  assert.ok(Math.abs(row.totalFuelCost - 50615.38461538462) < 1e-9);
});

test("apu fuel allocation for additional-use rows uses the latest performed flight departure station", () => {
  const flights = [
    {
      date: "2026-04-20",
      flight: "F1001",
      depStn: "DEL",
      arrStn: "BOM",
      variant: "737",
      acftType: "737",
      aircraft: {
        registration: "VT-IJK",
      },
    },
    {
      date: "2026-04-20",
      flight: "F1002",
      depStn: "CCU",
      arrStn: "BOM",
      variant: "737",
      acftType: "737",
      aircraft: {
        registration: "VT-IJK",
      },
    },
  ];

  const enriched = computeFlightCostsBatch(flights, {
    reportingCurrency: "INR",
    apuUsage: [
      {
        arrStn: "",
        variant: "737",
        acftRegn: "VT-IJK",
        apuHours: 1.5,
        consumptionPerApuHour: 280,
        addlnUse: "Y",
        fromDate: "2026-04-20",
        toDate: "2026-04-20",
      },
    ],
    ccyFuel: [
      {
        station: "DEL",
        month: "04/26",
        kgPerLtr: 0.78,
        intoPlaneRate: 93000,
        ccy: "INR",
      },
      {
        station: "CCU",
        month: "04/26",
        kgPerLtr: 0.78,
        intoPlaneRate: 94000,
        ccy: "INR",
      },
    ],
  });

  assert.equal(enriched[0].apuFuelCost, 25307.69);
  assert.equal(enriched[1].apuFuelCost, 25307.69);
  assert.equal(enriched[0].apuFuelCostCCY, "INR");
  assert.equal(enriched[1].apuFuelCostCCY, "INR");
  assert.equal(enriched[0].apuFuelCost + enriched[1].apuFuelCost, 50615.38);
});

test("apu fuel allocation applies additional-use rows without an arrival station", () => {
  const flight = {
    date: "2026-04-20",
    depStn: "CCU",
    arrStn: "BOM",
    variant: "737",
    acftType: "737",
    aircraft: {
      registration: "VT-IJK",
    },
  };

  const enriched = computeFlightCosts(flight, {
    reportingCurrency: "INR",
    apuUsage: [
      {
        arrStn: "",
        variant: "737",
        acftRegn: "VT-IJK",
        apuHours: 1.5,
        consumptionPerApuHour: 280,
        addlnUse: "Y",
        fromDate: "2026-04-20",
        toDate: "2026-04-20",
      },
    ],
    ccyFuel: [
      {
        station: "CCU",
        month: "04/26",
        kgPerLtr: 0.78,
        intoPlaneRate: 93000,
        ccy: "INR",
      },
    ],
  });

  assert.equal(enriched.apuFuelCost, 50076.92);
  assert.equal(enriched.apuFuelCostCCY, "INR");
});
