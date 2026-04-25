const assert = require("node:assert/strict");
const { test } = require("node:test");

const { normalizeCostConfig, computeFlightCosts } = require("../utils/costLogic");

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

test("transit maintenance prefers the most specific matching identifier", () => {
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
  assert.equal(enriched.otherMxExpenses, 1000);
  assert.equal(enriched.otherMxExpensesCCY, "USD");
});
