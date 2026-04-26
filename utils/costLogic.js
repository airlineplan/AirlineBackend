/**
 * Shared cost enrichment for the FLIGHT master rows.
 *
 * The app has multiple generations of cost-input row shapes in the DB, so this
 * module normalizes config rows first and then computes direct plus allocated
 * costs in one batch so every consumer sees the same output.
 */

const COST_FIELDS = [
  "engineFuelCost",
  "maintenanceReserveContribution",
  "transitMaintenance",
  "otherMaintenance",
  "navigation",
  "airport",
  "otherDoc",
  "crewAllowances",
  "layoverCost",
  "crewPositioningCost",
  "apuFuelCost",
  "mrMonthly",
  "qualifyingSchMxEvents",
  "otherMxExpenses",
  "rotableChanges",
];

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
};

const round2 = (value) => Number(toNumber(value).toFixed(2));

const normalize = (value) => String(value ?? "").trim().toUpperCase();

const normalizeMonthKey = (value) => {
  if (!value) return "";
  const raw = String(value).trim();
  const direct = raw.match(/^(\d{1,2})[/-](\d{2,4})$/);
  if (direct) {
    return `${String(Number(direct[1])).padStart(2, "0")}/${direct[2].slice(-2)}`;
  }

  const monthText = raw.match(/^([A-Z]{3})[- ]?(\d{2,4})$/i);
  if (monthText) {
    const monthMap = {
      JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
      JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12",
    };
    const mm = monthMap[monthText[1].toUpperCase()];
    if (mm) return `${mm}/${monthText[2].slice(-2)}`;
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return `${String(parsed.getMonth() + 1).padStart(2, "0")}/${String(parsed.getFullYear()).slice(-2)}`;
  }

  return normalize(raw);
};

const parseDate = (value) => {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const direct = new Date(value);
  if (!Number.isNaN(direct.getTime())) return direct;

  if (typeof value === "string") {
    const ddmmyyyy = value.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$/);
    if (ddmmyyyy) {
      const [, dd, mm, yyyy] = ddmmyyyy;
      const year = yyyy.length === 2 ? `20${yyyy}` : yyyy;
      const parsed = new Date(`${year}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`);
      if (!Number.isNaN(parsed.getTime())) return parsed;
    }

    const ddMonYY = value.match(/^(\d{1,2})\s+([A-Z]{3})\s+(\d{2,4})$/i);
    if (ddMonYY) {
      const [, dd, mon, yy] = ddMonYY;
      const monthMap = {
        JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
        JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12",
      };
      const mm = monthMap[mon.toUpperCase()];
      const year = yy.length === 2 ? `20${yy}` : yy;
      if (mm) {
        const parsed = new Date(`${year}-${mm}-${String(dd).padStart(2, "0")}`);
        if (!Number.isNaN(parsed.getTime())) return parsed;
      }
    }
  }

  return null;
};

const isWithinRange = (targetDate, fromValue, toValue) => {
  if (!targetDate) return true;
  const from = parseDate(fromValue);
  const to = parseDate(toValue);
  if (from && targetDate < from) return false;
  if (to && targetDate > to) return false;
  return true;
};

const pick = (row, keys) => {
  for (const key of keys) {
    if (row?.[key] !== undefined && row?.[key] !== null && row?.[key] !== "") {
      return row[key];
    }
  }
  return "";
};

const normalizeMetric = (value) => {
  const raw = normalize(value);
  if (!raw) return "";
  if (["DEPARTURE", "DEPARTURES", "CYCLE", "CYCLES"].includes(raw)) return "DEPARTURES";
  if (["BH", "BLOCKHOURS", "BLOCK HOURS"].includes(raw)) return "BH";
  if (["FH", "FLIGHTHOURS", "FLIGHT HOURS"].includes(raw)) return "FH";
  if (["MONTH", "MONTHLY"].includes(raw)) return "MONTH";
  return raw;
};

const normalizeCostCodeId = (value) => normalize(value).replace(/[^A-Z0-9]/g, "");

const DEFAULT_NAV_MTOW_TIERS = [73000, 77000, 78000, 79000];

const normalizeNavMtowTiers = (value = []) => {
  const source = Array.isArray(value) && value.length > 0 ? value : DEFAULT_NAV_MTOW_TIERS;
  const tiers = [];
  source.forEach((tier) => {
    const numeric = toNumber(tier);
    if (numeric > 0 && !tiers.includes(numeric)) tiers.push(numeric);
  });
  return tiers.length > 0 ? tiers : [...DEFAULT_NAV_MTOW_TIERS];
};

const inferAllocationCostCode = (row = {}) => {
  const explicit = normalizeCostCodeId(pick(row, ["costCode", "cost"]));
  if (explicit) return explicit;

  const label = normalize(pick(row, ["label", "basisOfAllocation", "description"]));
  if (!label) return "";
  if (label.includes("APU") && label.includes("FUEL")) return "APUFUELCOST";
  if (label.includes("MAINTENANCE RESERVE")) return "MRMONTHLY";
  if (label.includes("OTHER MAINTENANCE") || label.includes("OTHER MX")) return "OTHERMXEXPENSES";
  if (label.includes("ROTABLE")) return "ROTABLECHANGES";
  if (label.includes("SCHEDULE") && label.includes("MX")) return "QUALIFYINGSCHMXEVENTS";
  return normalizeCostCodeId(label);
};

const getFlightDate = (flight) => parseDate(flight?.date);
const getFlightMonthKey = (flight) => normalizeMonthKey(flight?.date);
const getFlightMonthNumber = (flight) => {
  const date = getFlightDate(flight);
  return date ? date.getUTCMonth() + 1 : 0;
};
const getFlightRegistration = (flight) => normalize(flight?.aircraft?.registration || flight?.acftRegn || flight?.registration);
const getFlightMsn = (flight) => normalize(flight?.aircraft?.msn ?? flight?.msn);
const getFlightVariant = (flight) => normalize(flight?.variant);
const getFlightAircraftKey = (flight) => (
  getFlightRegistration(flight) ||
  getFlightMsn(flight) ||
  getFlightVariant(flight)
);
const getFlightPartNumber = (flight) => normalize(flight?.acftType || flight?.variant);
const getFlightSector = (flight) => normalize(flight?.sector);
const getFlightDep = (flight) => normalize(flight?.depStn);
const getFlightArr = (flight) => normalize(flight?.arrStn);
const getFlightDomIntl = (flight) => normalize(flight?.domIntl);
const getFlightMtow = (flight, fleetRows = []) => {
  const flightDate = getFlightDate(flight);
  const regn = getFlightRegistration(flight);
  const msn = getFlightMsn(flight);
  if (!flightDate || (!regn && !msn)) return 0;

  const candidates = (Array.isArray(fleetRows) ? fleetRows : []).filter((row) => {
    const rowRegn = normalize(row?.regn);
    const rowSn = normalize(row?.sn);
    if (regn && rowRegn && rowRegn === regn) return true;
    if (!regn && msn && rowSn && rowSn === msn) return true;
    return false;
  }).filter((row) => {
    const entry = parseDate(row?.entry);
    const exit = parseDate(row?.exit);
    if (entry && flightDate < entry) return false;
    if (exit && flightDate > exit) return false;
    return true;
  });

  const best = candidates.sort((a, b) => {
    const aEntry = parseDate(a?.entry)?.getTime() || 0;
    const bEntry = parseDate(b?.entry)?.getTime() || 0;
    return bEntry - aEntry;
  })[0];

  return toNumber(best?.mtow);
};
const getFlightLoadFactor = (flight) => {
  const explicit =
    flight?.paxLF ??
    flight?.plf ??
    flight?.loadFactor ??
    flight?.loadFactorPct;

  const explicitValue = toNumber(explicit);
  if (explicitValue > 0) return explicitValue;

  const rsk = toNumber(flight?.rsk);
  const ask = toNumber(flight?.ask);
  if (ask > 0 && rsk >= 0) return round2((rsk / ask) * 100);

  const pax = toNumber(flight?.pax);
  const seats = toNumber(flight?.seats);
  if (seats > 0 && pax >= 0) return round2((pax / seats) * 100);

  return 0;
};

const getLatestFlightForAircraft = (flights = [], aircraftFlight, {
  requireBeforeOrOnDate = false,
} = {}) => {
  const aircraftKey = getFlightRegistration(aircraftFlight);
  if (!aircraftKey) return null;

  const currentDate = getFlightDate(aircraftFlight);
  let latest = null;

  flights.forEach((candidate) => {
    if (getFlightRegistration(candidate) !== aircraftKey) return;

    const candidateDate = getFlightDate(candidate);
    if (!candidateDate) return;
    if (requireBeforeOrOnDate && currentDate && candidateDate > currentDate) return;

    if (!latest) {
      latest = candidate;
      return;
    }

    const latestDate = getFlightDate(latest);
    if (!latestDate || candidateDate > latestDate) {
      latest = candidate;
      return;
    }

    if (candidateDate.getTime() === latestDate.getTime()) {
      const candidateFlight = String(candidate?.flight || "");
      const latestFlight = String(latest?.flight || "");
      if (candidateFlight > latestFlight) latest = candidate;
    }
  });

  return latest;
};

const getApuFuelPriceSourceFlight = (flight, flights = [], apuUsageRow = {}) => {
  const isAdditionalUse = isAdditionalApuUseRow(apuUsageRow);
  const hasArrivalStation = Boolean(normalize(flight?.arrStn));
  if (!isAdditionalUse || hasArrivalStation) return flight;

  const sourceFlight = getLatestFlightForAircraft(flights, flight, {
    requireBeforeOrOnDate: true,
  });
  return sourceFlight || flight;
};

const getConvertedRuleAmount = (rule) => {
  if (!rule) return 0;
  return toNumber(rule.costRCCY) > 0 ? toNumber(rule.costRCCY) : toNumber(rule.cost || 0);
};

const scoreTransitRule = (row) => (
  (row?.depStn ? 10000 : 0) +
  (row?.sn ? 1000 : 0) +
  (row?.acftRegn ? 400 : 0) +
  (row?.pn ? 250 : 0) +
  (row?.variant ? 100 : 0) +
  (row?.fromDate || row?.toDate ? 10 : 0)
);

const matchesOtherMxRow = (row, {
  flightDate,
  depStn,
  variant,
  acftReg,
  pn,
  msn,
}) => {
  if (!matchesOptional(row.depStn, depStn)) return false;
  if (!matchesOptional(row.variant, variant)) return false;
  if (!matchesOptional(row.acftRegn, acftReg)) return false;
  if (!matchesOptional(row.pn, pn)) return false;
  if (!matchesOptional(row.sn, msn)) return false;
  return isWithinRange(flightDate, row.fromDate, row.toDate);
};

const buildLegacyMonthRecords = (row, valueKeys = []) => {
  const records = [];
  const pushIfPresent = (monthValue, amount, extra = {}) => {
    const month = normalizeMonthKey(monthValue);
    const numeric = toNumber(amount);
    if (month && numeric !== 0) {
      records.push({ ...row, ...extra, month, amount: numeric });
    }
  };

  const primary = valueKeys.length ? pick(row, valueKeys) : pick(row, ["value", "cost", "fuelConsumption", "consumption", "setRate", "rate"]);
  if (primary !== "" && !pick(row, ["m1", "m2", "value1", "value2", "apr", "may"])) {
    pushIfPresent(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"]), primary);
  }

  pushIfPresent(pick(row, ["m1Label", "month1", "mmmYy1", "period1", "aprLabel", "aprMonth", "monthApr", "monthA"]), pick(row, ["m1", "value1", "apr"]));
  pushIfPresent(pick(row, ["m2Label", "month2", "mmmYy2", "period2", "mayLabel", "mayMonth", "monthMay", "monthB"]), pick(row, ["m2", "value2", "may"]));

  if (records.length === 0) {
    const explicitMonth = pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"]);
    if (explicitMonth) {
      pushIfPresent(explicitMonth, primary);
    }
  }

  return records;
};

const buildThresholdMap = (row) => {
  const thresholds = [];
  const directPairs = [
    ["80", ["p80", "t80", "threshold80"]],
    ["90", ["p90", "t90", "threshold90"]],
    ["95", ["p95", "t95", "threshold95"]],
    ["98", ["p98", "t98", "threshold98"]],
    ["100", ["p100", "t100", "threshold100"]],
    ["75", ["p75", "t75", "threshold75"]],
    ["60", ["p60", "t60", "threshold60"]],
    ["50", ["p50", "t50", "threshold50"]],
  ];

  directPairs.forEach(([threshold, keys]) => {
    const value = pick(row, keys);
    if (value !== "") thresholds.push({ threshold: Number(threshold), factor: toNumber(value) });
  });

  Object.entries(row || {}).forEach(([key, value]) => {
    const match = String(key).match(/^(\d{2,3})$/);
    if (match && value !== "") {
      thresholds.push({ threshold: Number(match[1]), factor: toNumber(value) });
    }
  });

  return thresholds
    .filter((entry) => Number.isFinite(entry.threshold))
    .sort((a, b) => a.threshold - b.threshold);
};

const PLF_POINT_KEYS = ["p80", "p90", "p95", "p98", "p100"];

const getPlfCarryForwardValue = (row) => {
  let lastValue = null;
  for (const key of PLF_POINT_KEYS) {
    const raw = row?.[key];
    if (raw === "" || raw === null || raw === undefined) continue;
    lastValue = round2(toNumber(raw));
  }
  return lastValue ?? 1;
};

const extractPlfEffectRecords = (rows = []) => {
  const records = [];
  let currentSectorOrGcd = "";
  let currentGcd = "";

  rows.forEach((row) => {
    if (!row) return;

    const sectorOrGcd = normalizeFuelValue(pick(row, ["sectorOrGcd", "sector", "type"]));
    const gcd = normalizeFuelValue(pick(row, ["gcd"]));
    const acftRegn = normalizeFuelValue(pick(row, ["acftRegn", "acftReg", "acft", "regn"]));

    if (row.rowType === "sector" || (!acftRegn && (sectorOrGcd || gcd))) {
      if (sectorOrGcd) currentSectorOrGcd = sectorOrGcd;
      if (gcd) currentGcd = gcd;
      return;
    }

    const effectiveSector = sectorOrGcd || currentSectorOrGcd;
    const effectiveGcd = gcd || currentGcd;
    if (!effectiveSector && !acftRegn) return;

    const p80 = pick(row, ["p80", "t80", "threshold80"]);
    const p90 = pick(row, ["p90", "t90", "threshold90"]);
    const p95 = pick(row, ["p95", "t95", "threshold95"]);
    const p98 = pick(row, ["p98", "t98", "threshold98"]);
    const p100 = getPlfCarryForwardValue(row);

    records.push({
      sectorOrGcd: effectiveSector,
      gcd: effectiveGcd,
      acftRegn,
      p80: p80 === "" ? "" : toNumber(p80),
      p90: p90 === "" ? "" : toNumber(p90),
      p95: p95 === "" ? "" : toNumber(p95),
      p98: p98 === "" ? "" : toNumber(p98),
      p100,
    });
  });

  return records;
};

const flattenPlfEffectRows = (rows = []) => extractPlfEffectRecords(rows);

const groupPlfEffectRows = (rows = []) => {
  const grouped = [];
  const sectors = new Map();

  rows.forEach((row) => {
    const sectorKey = normalizeFuelValue(row?.sectorOrGcd);
    const acftRegn = normalizeFuelValue(row?.acftRegn);
    if (!sectorKey && !acftRegn) return;

    if (!sectors.has(sectorKey)) {
      const sectorRow = {
        rowType: "sector",
        sectorOrGcd: sectorKey,
        gcd: normalizeFuelValue(row?.gcd),
      };
      sectors.set(sectorKey, {
        sectorRow,
        aircraft: new Map(),
        order: [],
      });
      grouped.push(sectorRow);
    }

    const group = sectors.get(sectorKey);
    if (!acftRegn) return;

    const p80 = row?.p80 ?? row?.t80 ?? row?.threshold80 ?? "";
    const p90 = row?.p90 ?? row?.t90 ?? row?.threshold90 ?? "";
    const p95 = row?.p95 ?? row?.t95 ?? row?.threshold95 ?? "";
    const p98 = row?.p98 ?? row?.t98 ?? row?.threshold98 ?? "";
    const p100 = getPlfCarryForwardValue(row);

    const aircraftRow = {
      rowType: "aircraft",
      sectorOrGcd: sectorKey,
      gcd: normalizeFuelValue(row?.gcd || group.sectorRow.gcd),
      acftRegn,
      p80: p80 === "" ? "" : toNumber(p80),
      p90: p90 === "" ? "" : toNumber(p90),
      p95: p95 === "" ? "" : toNumber(p95),
      p98: p98 === "" ? "" : toNumber(p98),
      p100,
    };

    group.aircraft.set(acftRegn, aircraftRow);
    group.order.push(acftRegn);
    grouped.push(aircraftRow);
  });

  sectors.forEach((group) => {
    if (!group.sectorRow.gcd) {
      const firstAircraft = group.order.length ? group.aircraft.get(group.order[0]) : null;
      group.sectorRow.gcd = firstAircraft?.gcd || "";
    }
    group.order.forEach((acftRegn) => {
      const aircraftRow = group.aircraft.get(acftRegn);
      if (aircraftRow && !aircraftRow.p100) aircraftRow.p100 = getPlfCarryForwardValue(aircraftRow);
    });
  });

  return grouped;
};

const normalizeFuelValue = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).trim();
};

const FUEL_MONTH_LABEL_KEYS = ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"];
const FUEL_CONSUMPTION_KEYS = ["fuelConsumptionKg", "fuelConsumption", "consumption", "fuelKg", "value", "rate"];
const FUEL_INDEX_MONTH_LABEL_KEYS = ["month1", "month2", "month3", "month4", "month5", "m1Label", "m2Label", "m3Label", "m4Label", "m5Label"];
const FUEL_INDEX_VALUE_KEYS = ["m1", "m2", "m3", "m4", "m5", "value1", "value2", "value3", "value4", "value5", "fuelConsumptionIndex", "index"];

const extractFuelConsumptionRecords = (rows = []) => {
  const records = [];
  let currentSectorOrGcd = "";
  let currentGcd = "";

  rows.forEach((row) => {
    if (!row) return;

    const sectorOrGcd = normalizeFuelValue(pick(row, ["sectorOrGcd", "sector", "type", "fuelBasis"]));
    const gcd = normalizeFuelValue(pick(row, ["gcd"]));
    const acftRegn = normalizeFuelValue(pick(row, ["acftRegn", "acftReg", "acft", "regn"]));
    const ccy = normalizeFuelValue(pick(row, ["ccy"]));
    const reportingAmount = toNumber(pick(row, ["reportingAmount", "costRCCY"]));

    if (row.rowType === "sector" || (!acftRegn && (sectorOrGcd || gcd))) {
      if (sectorOrGcd) currentSectorOrGcd = sectorOrGcd;
      if (gcd) currentGcd = gcd;
      return;
    }

    const effectiveSector = sectorOrGcd || currentSectorOrGcd;
    const effectiveGcd = gcd || currentGcd;
    const monthRecords = buildLegacyMonthRecords(row, FUEL_CONSUMPTION_KEYS);
    if (monthRecords.length > 0) {
      monthRecords.forEach((record) => {
        records.push({
          sectorOrGcd: effectiveSector,
          gcd: effectiveGcd,
          acftRegn,
          ccy,
          reportingAmount,
          month: normalizeFuelValue(record.month),
          fuelConsumptionKg: round2(record.amount),
          fuelPrice: toNumber(pick(record, ["fuelPrice", "price"])),
        });
      });
      return;
    }

    const month = normalizeMonthKey(pick(row, FUEL_MONTH_LABEL_KEYS));
    const fuelConsumptionKg = round2(pick(row, FUEL_CONSUMPTION_KEYS));

    if (!effectiveSector && !acftRegn && !month && fuelConsumptionKg === 0) return;
    if (!acftRegn && !fuelConsumptionKg && !month) return;

    records.push({
      sectorOrGcd: effectiveSector,
      gcd: effectiveGcd,
      acftRegn,
      ccy,
      reportingAmount,
      month,
      fuelConsumptionKg,
      fuelPrice: toNumber(pick(row, ["fuelPrice", "price"])),
    });
  });

  return records.filter((row) => row.sectorOrGcd || row.acftRegn || row.month || row.fuelConsumptionKg);
};

const normalizeFuelConsum = (rows = []) => extractFuelConsumptionRecords(rows).map((row) => ({
  sectorOrGcd: normalize(row.sectorOrGcd),
  gcd: normalize(row.gcd),
  acftRegn: normalize(row.acftRegn),
  ccy: normalize(row.ccy),
  reportingAmount: toNumber(row.reportingAmount),
  month: normalizeMonthKey(row.month),
  fuelConsumptionKg: round2(row.fuelConsumptionKg),
  fuelPrice: toNumber(row.fuelPrice),
}));

const flattenFuelConsumRows = (rows = []) => extractFuelConsumptionRecords(rows);

const extractFuelConsumIndexRecords = (rows = []) => {
  const records = [];
  let currentMonths = [];

  rows.forEach((row) => {
    if (!row) return;

    const acftRegn = normalizeFuelValue(pick(row, ["acftRegn", "acftReg", "acft", "regn"]));
    const labels = [
      normalizeMonthKey(pick(row, ["month1"])),
      normalizeMonthKey(pick(row, ["month2"])),
      normalizeMonthKey(pick(row, ["month3"])),
      normalizeMonthKey(pick(row, ["month4"])),
      normalizeMonthKey(pick(row, ["month5"])),
    ];
    const values = [
      row.m1,
      row.m2,
      row.m3,
      row.m4,
      row.m5,
    ];

    const hasLabelOnly = labels.some(Boolean) && !acftRegn && values.every((value) => value === "" || value === null || value === undefined);
    if (hasLabelOnly) {
      currentMonths = labels.filter(Boolean);
      return;
    }

    const monthLabels = labels.some(Boolean) ? labels.filter(Boolean) : currentMonths;
    if (monthLabels.length === 0 && !acftRegn) return;

    let carry = "";
    monthLabels.forEach((month, index) => {
      const raw = values[index];
      const parsed = raw === "" || raw === null || raw === undefined ? "" : toNumber(raw);
      if (parsed !== "") carry = parsed;
      if (carry === "") return;
      records.push({
        acftRegn,
        month1: labels[0] || "",
        month2: labels[1] || "",
        month3: labels[2] || "",
        month4: labels[3] || "",
        month5: labels[4] || "",
        month,
        fuelConsumptionIndex: round2(carry),
      });
    });
  });

  return records.filter((row) => row.acftRegn || row.month);
};

const normalizeFuelConsumIndex = (rows = []) => extractFuelConsumIndexRecords(rows).map((row) => ({
  acftRegn: normalize(row.acftRegn),
  month1: normalizeMonthKey(row.month1),
  month2: normalizeMonthKey(row.month2),
  month3: normalizeMonthKey(row.month3),
  month4: normalizeMonthKey(row.month4),
  month5: normalizeMonthKey(row.month5),
  month: normalizeMonthKey(row.month),
  fuelConsumptionIndex: round2(row.fuelConsumptionIndex),
}));

const flattenFuelConsumIndexRows = (rows = []) => extractFuelConsumIndexRecords(rows);

const groupFuelConsumIndexRows = (rows = []) => {
  const grouped = [];
  const aircraftMap = new Map();
  const monthOrder = [];
  let headerLabels = [];

  rows.forEach((row) => {
    const acftRegn = normalizeFuelValue(row?.acftRegn);
    const month = normalizeMonthKey(row?.month);
    const value = toNumber(row?.fuelConsumptionIndex ?? row?.value ?? row?.m1 ?? row?.m2 ?? row?.m3 ?? row?.m4 ?? row?.m5);
    if (!acftRegn || !month) return;

    if (headerLabels.length === 0) {
      headerLabels = [
        normalizeMonthKey(row?.month1),
        normalizeMonthKey(row?.month2),
        normalizeMonthKey(row?.month3),
        normalizeMonthKey(row?.month4),
        normalizeMonthKey(row?.month5),
      ].filter(Boolean);
    }
    if (!monthOrder.includes(month)) monthOrder.push(month);
    if (!aircraftMap.has(acftRegn)) {
      aircraftMap.set(acftRegn, {});
    }
    aircraftMap.get(acftRegn)[month] = value;
  });

  const monthLabels = headerLabels.length ? headerLabels.slice(0, 5) : monthOrder.slice(0, 5);
  aircraftMap.forEach((monthValues, acftRegn) => {
    const row = {
      acftRegn,
      month1: monthLabels[0] || "",
      month2: monthLabels[1] || "",
      month3: monthLabels[2] || "",
      month4: monthLabels[3] || "",
      month5: monthLabels[4] || "",
      m1: "",
      m2: "",
      m3: "",
      m4: "",
      m5: "",
    };

    monthLabels.forEach((month, index) => {
      row[`m${index + 1}`] = monthValues[month] !== undefined ? monthValues[month] : "";
    });
    grouped.push(row);
  });

  return grouped;
};

const groupFuelConsumRows = (rows = []) => {
  const grouped = [];
  const sectorMap = new Map();

  rows.forEach((row) => {
    const sectorKey = normalizeFuelValue(row?.sectorOrGcd);
    const acftRegn = normalizeFuelValue(row?.acftRegn);
    const month = normalizeFuelValue(row?.month || row?.mmmYy || row?.mmmYY || row?.period || row?.mth || row?.mmYY);
    const fuelConsumptionKg = row?.fuelConsumptionKg ?? row?.fuelConsumption ?? row?.consumption ?? row?.fuelKg ?? row?.value ?? row?.rate;
    const numericFuel = toNumber(fuelConsumptionKg);

    if (!sectorKey && !acftRegn && !month && numericFuel === 0) return;

    if (!sectorMap.has(sectorKey)) {
      const sectorRow = {
        rowType: "sector",
        sectorOrGcd: sectorKey,
        gcd: normalizeFuelValue(row?.gcd),
      };
      sectorMap.set(sectorKey, {
        sectorRow,
        months: [],
        aircraft: new Map(),
        order: [],
      });
      grouped.push(sectorRow);
    }

    const group = sectorMap.get(sectorKey);

    if (month && !group.months.includes(month)) {
      group.months.push(month);
    }

    if (!acftRegn) return;

    if (!group.aircraft.has(acftRegn)) {
      const aircraftRow = {
        rowType: "aircraft",
        sectorOrGcd: sectorKey,
        gcd: normalizeFuelValue(row?.gcd || group.sectorRow.gcd),
        acftRegn,
        m1: "",
        m2: "",
      };
      group.aircraft.set(acftRegn, aircraftRow);
      group.order.push(acftRegn);
      grouped.push(aircraftRow);
    }

    const aircraftRow = group.aircraft.get(acftRegn);
    const monthIndex = group.months.indexOf(month);
    if (monthIndex >= 0 && monthIndex < 2) {
      aircraftRow[monthIndex === 0 ? "m1" : "m2"] = row?.fuelConsumptionKg !== undefined && row?.fuelConsumptionKg !== null
        ? row.fuelConsumptionKg
        : numericFuel;
    }
  });

  sectorMap.forEach((group) => {
    group.sectorRow.month1 = group.months[0] || "";
    group.sectorRow.month2 = group.months[1] || "";
    if (!group.sectorRow.gcd) {
      const firstAircraft = group.order.length ? group.aircraft.get(group.order[0]) : null;
      group.sectorRow.gcd = firstAircraft?.gcd || "";
    }
    group.order.forEach((acftRegn) => {
      const aircraftRow = group.aircraft.get(acftRegn);
      aircraftRow.month1 = group.sectorRow.month1;
      aircraftRow.month2 = group.sectorRow.month2;
      if (!aircraftRow.gcd) aircraftRow.gcd = group.sectorRow.gcd || "";
    });
  });

  return grouped;
};

const normalizeApuUsage = (rows = []) => rows.map((row) => ({
  arrStn: normalize(pick(row, ["arrStn", "stn", "station"])),
  fromDate: pick(row, ["fromDate", "fromDt"]),
  toDate: pick(row, ["toDate", "toDt"]),
  variant: normalize(pick(row, ["variant", "var"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
  apuHours: toNumber(pick(row, ["apuHours", "apuHr", "hours"])),
  consumptionPerApuHour: toNumber(pick(row, ["consumptionPerApuHour", "consumption", "apuFuel", "cost", "value"])),
  basis: normalizeMetric(pick(row, ["basis"])),
  ccy: normalize(pick(row, ["ccy"])),
  addlnUse: normalize(pick(row, ["addlnUse", "addln", "additionalUse", "addlnUsage"])) || "N",
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).map((row) => {
  if (row.addlnUse === "Y") {
    return {
      ...row,
      arrStn: "",
      toDate: row.fromDate || row.toDate,
    };
  }
  return {
    ...row,
    addlnUse: row.addlnUse || "N",
  };
}).filter((row) => row.variant || row.acftRegn || row.fromDate || row.toDate || row.apuHours || row.consumptionPerApuHour);

const normalizePlfEffect = (rows = []) => rows.map((row) => ({
  sectorOrGcd: normalize(pick(row, ["sectorOrGcd", "sector", "type", "gcd"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
  thresholds: buildThresholdMap(row),
})).filter((row) => row.sectorOrGcd || row.acftRegn || row.thresholds.length);

const normalizeFuelPrice = (rows = []) => {
  const normalized = [];
  rows.forEach((row) => {
    const base = {
      station: normalize(pick(row, ["station", "intoPlane", "stn"])),
      ccy: normalize(pick(row, ["ccy", "currency"])),
      kgPerLtr: toNumber(pick(row, ["kgPerLtr", "kgLtr", "density"])),
      costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
    };

    const monthRecords = buildLegacyMonthRecords(row, ["intoPlaneRate", "cost", "value"]);
    if (monthRecords.length > 0) {
      monthRecords.forEach((record) => {
        normalized.push({
          ...base,
          month: record.month,
          intoPlaneRate: round2(record.amount),
        });
      });
    } else {
      normalized.push({
        ...base,
        month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
        intoPlaneRate: round2(pick(row, ["intoPlaneRate", "cost", "value", "m1", "m2"])),
      });
    }
  });
  return normalized.filter((row) => row.station || row.month || row.intoPlaneRate);
};

const FUEL_PRICE_MONTH_KEYS = ["m1", "m2"];
const FUEL_PRICE_HEADER_KEYS = ["month1", "month2"];

const extractFuelPriceRecords = (rows = []) => {
  const records = [];
  let currentMonth1 = "";
  let currentMonth2 = "";
  const parseMaybeNumber = (value) => {
    if (value === "" || value === null || value === undefined) return null;
    if (typeof value === "number") return Number.isFinite(value) ? value : null;
    const parsed = Number(String(value).replace(/,/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
  };

  rows.forEach((row) => {
    if (!row) return;

    const station = normalizeFuelValue(pick(row, ["station", "intoPlane", "stn"]));
    const ccy = normalizeFuelValue(pick(row, ["ccy", "currency"]));
    const kgPerLtr = pick(row, ["kgPerLtr", "kgLtr", "density"]);
    const costRCCY = toNumber(pick(row, ["costRCCY", "reportingAmount"]));
    const month1 = normalizeMonthKey(pick(row, ["month1", "m1Label"]));
    const month2 = normalizeMonthKey(pick(row, ["month2", "m2Label"]));
    const m1 = pick(row, ["m1", "value1", "apr"]);
    const m2 = pick(row, ["m2", "value2", "may"]);

    if (row.rowType === "header" || (!station && (month1 || month2))) {
      if (month1) currentMonth1 = month1;
      if (month2) currentMonth2 = month2;
      return;
    }

    const header1 = month1 || currentMonth1;
    const header2 = month2 || currentMonth2;
    const firstValue = parseMaybeNumber(m1);
    const secondValue = parseMaybeNumber(m2);
    const carryFirst = firstValue ?? secondValue;
    const carrySecond = secondValue ?? firstValue;

    if (!station && !header1 && !header2 && carryFirst === null && carrySecond === null) return;

    if (header1 && carryFirst !== null) {
      records.push({
        station,
        ccy,
        kgPerLtr: toNumber(kgPerLtr),
        costRCCY,
        month: header1,
        intoPlaneRate: round2(carryFirst),
        month1: header1,
        month2: header2,
      });
    }

    if (header2 && carrySecond !== null) {
      records.push({
        station,
        ccy,
        kgPerLtr: toNumber(kgPerLtr),
        costRCCY,
        month: header2,
        intoPlaneRate: round2(carrySecond),
        month1: header1,
        month2: header2,
      });
    }
  });

  return records.filter((row) => row.station || row.month || row.intoPlaneRate);
};

const flattenFuelPriceRows = (rows = []) => extractFuelPriceRecords(rows);

const groupFuelPriceRows = (rows = []) => {
  const grouped = [];
  const stationMap = new Map();
  const monthOrder = [];

  rows.forEach((row) => {
    const station = normalizeFuelValue(row?.station);
    const month = normalizeMonthKey(row?.month);
    if (!station || !month) return;

    if (!monthOrder.includes(month)) monthOrder.push(month);
    if (!stationMap.has(station)) {
      stationMap.set(station, {
        station,
        ccy: normalizeFuelValue(row?.ccy),
        kgPerLtr: row?.kgPerLtr !== undefined ? row.kgPerLtr : "",
        costRCCY: toNumber(row?.costRCCY),
        values: {},
      });
    }
    const group = stationMap.get(station);
    if (!group.ccy) group.ccy = normalizeFuelValue(row?.ccy);
    if (!group.kgPerLtr && row?.kgPerLtr !== undefined) group.kgPerLtr = row.kgPerLtr;
    if (!group.costRCCY && row?.costRCCY !== undefined) group.costRCCY = row.costRCCY;
    group.values[month] = row?.intoPlaneRate !== undefined && row?.intoPlaneRate !== null ? row.intoPlaneRate : "";
  });

  stationMap.forEach((group) => {
    const month1 = monthOrder[0] || "";
    const month2 = monthOrder[1] || "";
    grouped.push({
      station: group.station,
      ccy: group.ccy,
      kgPerLtr: group.kgPerLtr,
      costRCCY: group.costRCCY,
      month1,
      month2,
      m1: month1 ? group.values[month1] ?? "" : "",
      m2: month2 ? group.values[month2] ?? "" : "",
    });
  });

  return grouped;
};

const normalizeLeasedReserve = (rows = []) => rows.map((row) => ({
  ...row,
  mrAccId: pick(row, ["mrAccId"]),
  schMxEvent: pick(row, ["schMxEvent", "schEvent", "event"]),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn", "esn", "apun", "msn"])),
  setBalance: toNumber(pick(row, ["setBalance"])),
  setRate: toNumber(pick(row, ["setRate", "rate", "contribution"])),
  asOnDate: pick(row, ["asOnDate", "date", "fromDate"]),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  driver: normalizeMetric(pick(row, ["driver"])),
  annualEscl: toNumber(pick(row, ["annualEscl", "annualEscalation", "escl"])),
  anniversary: pick(row, ["anniversary", "anniversaryDt"]),
  endDate: pick(row, ["endDate", "toDate"]),
  basis: normalizeMetric(pick(row, ["basis"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.acftRegn || row.pn || row.sn || row.driver || row.schMxEvent);

const normalizeAircraftOnwing = (rows = []) => rows.map((row) => ({
  ...row,
  date: pick(row, ["date"]),
  msn: normalize(pick(row, ["msn"])),
  pos1Esn: normalize(pick(row, ["pos1Esn"])),
  pos2Esn: normalize(pick(row, ["pos2Esn"])),
  apun: normalize(pick(row, ["apun"])),
})).filter((row) => row.date || row.msn || row.pos1Esn || row.pos2Esn || row.apun);

const normalizeMaintenanceReserveSchedule = (rows = []) => rows.map((row) => ({
  ...row,
  date: pick(row, ["date"]),
  msn: normalize(pick(row, ["msn"])),
  mrAccId: normalize(pick(row, ["mrAccId"])),
  acftReg: normalize(pick(row, ["acftReg"])),
  rate: toNumber(pick(row, ["rate"])),
  contribution: toNumber(pick(row, ["contribution", "setRate"])),
  monthNumber: toNumber(pick(row, ["monthNumber", "monthNo", "month", "driverVal"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  driver: normalizeMetric(pick(row, ["driver"])),
})).filter((row) => row.date || row.msn || row.mrAccId || row.rate || row.contribution);

const normalizeSchMxEvents = (rows = []) => rows.map((row) => ({
  ...row,
  date: pick(row, ["date"]),
  event: normalize(pick(row, ["event", "schMxEvent", "schEvent", "label"])),
  msnEsnApun: normalize(pick(row, ["msnEsnApun", "msn", "msnEsn", "acftRegn"])),
  pn: normalize(pick(row, ["pn"])),
  snBn: normalize(pick(row, ["snBn", "sn", "bn"])),
  hours: toNumber(pick(row, ["hours"])),
  cycles: toNumber(pick(row, ["cycles"])),
  days: toNumber(pick(row, ["days"])),
  cost: toNumber(pick(row, ["cost", "eventTotalCost"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  mrAccId: pick(row, ["mrAccId"]),
  drawdownDate: pick(row, ["drawdownDate", "mrDrawdownDate"]),
  mrDrawdown: toNumber(pick(row, ["mrDrawdown"])),
  mrDrawdownCcy: normalize(pick(row, ["mrDrawdownCcy"])),
  openingBal: toNumber(pick(row, ["openingBal", "openBal"])),
  remaining: toNumber(pick(row, ["remaining"])),
  capitalisation: normalize(pick(row, ["capitalisation", "capitalization", "cap"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.date || row.event || row.msnEsnApun);

const isBlankValue = (value) => value === "" || value === null || value === undefined;

const toDayKey = (value) => {
  const date = parseDate(value);
  if (!date) return "";
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("-");
};

const buildUtilisationLookupKeys = (row = {}) => {
  const day = toDayKey(pick(row, ["date"]));
  const msn = normalize(pick(row, ["msnEsnApun", "msnEsn", "msn", "acftRegn"]));
  const pn = normalize(pick(row, ["pn"]));
  const sn = normalize(pick(row, ["snBn", "sn", "bn"]));
  return [
    [day, msn, pn, sn].join("|"),
    [day, msn, "", sn].join("|"),
    [day, msn, "", ""].join("|"),
  ];
};

const buildReserveLookupKeys = (row = {}) => {
  const day = toDayKey(pick(row, ["date", "drawdownDate", "mrDrawdownDate"]));
  const mrAccId = normalize(pick(row, ["mrAccId"]));
  const asset = normalize(pick(row, ["msn", "msnEsnApun", "acftReg", "acftRegn"]));
  return [
    [day, mrAccId, asset].join("|"),
    [day, mrAccId, ""].join("|"),
  ];
};

const buildUtilisationLookupKey = (row = {}) => [
  toDayKey(pick(row, ["date"])),
  normalize(pick(row, ["msnEsnApun", "msnEsn", "msn", "acftRegn"])),
  normalize(pick(row, ["pn"])),
  normalize(pick(row, ["snBn", "sn", "bn"])),
].join("|");

const hydrateSchMxEvents = (rows = [], {
  utilisationRows = [],
  maintenanceReserveRows = [],
} = {}) => {
  const utilisationLookup = new Map();
  utilisationRows.forEach((row) => {
    buildUtilisationLookupKeys(row).forEach((key) => {
      if (key && !utilisationLookup.has(key)) {
        utilisationLookup.set(key, row);
      }
    });
  });

  const reserveLookup = new Map();
  maintenanceReserveRows.forEach((row) => {
    buildReserveLookupKeys(row).forEach((key) => {
      if (key && !reserveLookup.has(key)) {
        reserveLookup.set(key, row);
      }
    });
  });

  return rows.map((row) => {
    const next = { ...row };
    const hydratedFields = new Set(Array.isArray(next._hydratedFields) ? next._hydratedFields : []);
    const utilKey = buildUtilisationLookupKey(next);
    const utilisation = utilisationLookup.get(utilKey);

    if (utilisation) {
      if (isBlankValue(next.hours) && !isBlankValue(utilisation.tsn)) {
        next.hours = toNumber(utilisation.tsn);
        hydratedFields.add("hours");
      }
      if (isBlankValue(next.cycles) && !isBlankValue(utilisation.csn)) {
        next.cycles = toNumber(utilisation.csn);
        hydratedFields.add("cycles");
      }
      if (isBlankValue(next.days) && !isBlankValue(utilisation.dsn)) {
        next.days = toNumber(utilisation.dsn);
        hydratedFields.add("days");
      }
    }

    const drawdownDateKey = toDayKey(next.drawdownDate);
    if (isBlankValue(next.openingBal) && drawdownDateKey && !isBlankValue(next.mrAccId)) {
      const reserveKeys = buildReserveLookupKeys({
        date: next.drawdownDate,
        mrAccId: next.mrAccId,
        msn: next.msnEsnApun,
        acftReg: next.acftRegn,
      });
      const reserve = reserveKeys.map((key) => reserveLookup.get(key)).find(Boolean);
      if (reserve && !isBlankValue(reserve.closingBal)) {
        next.openingBal = toNumber(reserve.closingBal);
        hydratedFields.add("openingBal");
      }
    }

    if (isBlankValue(next.remaining) && !isBlankValue(next.openingBal) && !isBlankValue(next.mrDrawdown)) {
      next.remaining = round2(toNumber(next.openingBal) - toNumber(next.mrDrawdown));
      hydratedFields.add("remaining");
    }

    next._hydratedFields = Array.from(hydratedFields);
    return next;
  });
};

const normalizeTransitMx = (rows = []) => rows.map((row) => ({
  ...row,
  depStn: normalize(pick(row, ["depStn", "stn", "station"])),
  variant: normalize(pick(row, ["variant", "var"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn", "msn"])),
  fromDate: pick(row, ["fromDate"]),
  toDate: pick(row, ["toDate"]),
  costPerDeparture: toNumber(pick(row, ["costPerDeparture", "costDep", "cost", "rate"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.depStn || row.variant || row.acftRegn || row.pn || row.sn);

const normalizeOtherMx = (rows = []) => rows.map((row) => ({
  ...row,
  depStn: normalize(pick(row, ["depStn", "stn", "station"])),
  variant: normalize(pick(row, ["variant", "var"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn", "msn"])),
  fromDate: pick(row, ["fromDate"]),
  toDate: pick(row, ["toDate"]),
  costPerBh: toNumber(pick(row, ["costPerBh", "costBh"])),
  costPerDeparture: toNumber(pick(row, ["costPerDeparture", "costDep"])),
  costPerMonth: toNumber(pick(row, ["costPerMonth", "costMonth"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.depStn || row.variant || row.acftRegn || row.pn || row.sn);

const normalizeStationCost = (rows = [], stationKey) => {
  const normalized = [];
  rows.forEach((row) => {
    const base = {
      [stationKey]: normalize(pick(row, [stationKey, "stn", "station", "arrStn"])),
      mtow: toNumber(pick(row, ["mtow"])),
      variant: normalize(pick(row, ["variant", "var"])),
      fromDate: pick(row, ["fromDate"]),
      toDate: pick(row, ["toDate"]),
      ccy: normalize(pick(row, ["ccy", "currency"])),
      costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
    };

    const monthRecords = buildLegacyMonthRecords(row, ["cost", "value"]);
    if (monthRecords.length > 0) {
      monthRecords.forEach((record) => {
        normalized.push({
          ...base,
          month: record.month,
          cost: round2(record.amount),
        });
      });
    } else {
      normalized.push({
        ...base,
        month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
        cost: round2(pick(row, ["cost", "value", "m1", "m2", "v1", "v2"])),
      });
    }
  });
  return normalized.filter((row) => row[stationKey] || row.cost);
};

const normalizeNavigationCost = (rows = [], stationKey, tiers = DEFAULT_NAV_MTOW_TIERS) => rows.map((row) => {
  const tierRates = {};
  const navTiers = normalizeNavMtowTiers(tiers);
  navTiers.forEach((tier) => {
    let value = pick(row, [
      String(tier),
      `mtow${tier}`,
      `mtow_${tier}`,
      `tier${tier}`,
    ]);
    if ((value === "" || value === undefined || value === null) && row?.tierRates?.[tier] !== undefined && row?.tierRates?.[tier] !== null && row?.tierRates?.[tier] !== "") {
      value = row.tierRates[tier];
    }
    if (value !== "") tierRates[tier] = round2(value);
  });

  return {
    [stationKey]: normalize(pick(row, [stationKey, "stn", "station", "arrStn", "sector"])),
    cost: round2(pick(row, ["cost", "value"])),
    ccy: normalize(pick(row, ["ccy", "currency"])),
    costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
    ...Object.fromEntries(
      navTiers.map((tier) => [String(tier), tierRates[tier] !== undefined ? tierRates[tier] : ""])
    ),
    tierRates,
  };
}).filter((row) => row[stationKey] || row.ccy || row.cost || Object.keys(row.tierRates || {}).length > 0);

const serializeNavigationCostRows = (rows = [], stationKey, tiers = DEFAULT_NAV_MTOW_TIERS) => normalizeNavigationCost(rows, stationKey, tiers).map((row) => {
  const serialized = {
    [stationKey]: row[stationKey] || "",
    ccy: row.ccy || "",
  };

  normalizeNavMtowTiers(tiers).forEach((tier) => {
    if (row[String(tier)] !== undefined && row[String(tier)] !== "") {
      serialized[String(tier)] = row[String(tier)];
    }
  });

  return serialized;
}).filter((row) => row[stationKey] || row.ccy || normalizeNavMtowTiers(tiers).some((tier) => row[String(tier)] !== undefined && row[String(tier)] !== ""));

const normalizeAirportMtowCost = (rows = [], stationKey, tiers = DEFAULT_NAV_MTOW_TIERS) => rows.map((row) => {
  const tierRates = {};
  const airportTiers = normalizeNavMtowTiers(tiers);

  airportTiers.forEach((tier) => {
    let value = pick(row, [
      String(tier),
      `mtow${tier}`,
      `mtow_${tier}`,
      `tier${tier}`,
    ]);
    if ((value === "" || value === undefined || value === null) && row?.tierRates?.[tier] !== undefined && row?.tierRates?.[tier] !== null && row?.tierRates?.[tier] !== "") {
      value = row.tierRates[tier];
    }
    if (value !== "") tierRates[tier] = round2(value);
  });

  return {
    [stationKey]: normalize(pick(row, [stationKey, "stn", "station", "arrStn"])),
    mtow: pick(row, ["mtow"]),
    variant: normalize(pick(row, ["variant", "var"])),
    month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
    fromDate: pick(row, ["fromDate"]),
    toDate: pick(row, ["toDate"]),
    ccy: normalize(pick(row, ["ccy", "currency"])),
    costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
    ...Object.fromEntries(
      airportTiers.map((tier) => [String(tier), tierRates[tier] !== undefined ? tierRates[tier] : ""])
    ),
    tierRates,
  };
}).filter((row) => row[stationKey] || row.ccy || row.mtow || Object.keys(row.tierRates || {}).length > 0);

const serializeAirportMtowCostRows = (rows = [], stationKey, tiers = DEFAULT_NAV_MTOW_TIERS) => normalizeAirportMtowCost(rows, stationKey, tiers).map((row) => {
  const serialized = {
    [stationKey]: row[stationKey] || "",
    ccy: row.ccy || "",
  };

  normalizeNavMtowTiers(tiers).forEach((tier) => {
    if (row[String(tier)] !== undefined && row[String(tier)] !== "") {
      serialized[String(tier)] = row[String(tier)];
    }
  });

  return serialized;
}).filter((row) => row[stationKey] || row.ccy || normalizeNavMtowTiers(tiers).some((tier) => row[String(tier)] !== undefined && row[String(tier)] !== ""));

const normalizeFleetRows = (rows = []) => rows.map((row) => ({
  ...row,
  regn: normalize(pick(row, ["regn", "registration"])),
  sn: normalize(pick(row, ["sn"])),
  mtow: toNumber(pick(row, ["mtow"])),
  entry: pick(row, ["entry"]),
  exit: pick(row, ["exit"]),
  variant: normalize(pick(row, ["variant"])),
  category: normalize(pick(row, ["category"])),
})).filter((row) => row.regn || row.sn || row.mtow);

const normalizeOtherDoc = (rows = []) => rows.map((row) => ({
  label: pick(row, ["label", "lbl"]),
  sector: normalize(pick(row, ["sector", "sec"])),
  depStn: normalize(pick(row, ["depStn", "dep"])),
  arrStn: normalize(pick(row, ["arrStn", "arr"])),
  variant: normalize(pick(row, ["variant", "var"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn", "msn"])),
  per: normalizeMetric(pick(row, ["per"])),
  cost: toNumber(pick(row, ["cost"])),
  fromDate: pick(row, ["fromDate", "fdate"]),
  toDate: pick(row, ["toDate", "tdate"]),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.label || row.sector || row.depStn || row.arrStn || row.variant || row.acftRegn || row.pn || row.sn);

const normalizeRotableChanges = (rows = []) => rows.map((row) => ({
  ...row,
  label: pick(row, ["label", "lbl"]),
  date: pick(row, ["date"]),
  month: normalizeMonthKey(pick(row, ["month"])),
  pn: normalize(pick(row, ["pn"])),
  msn: normalize(pick(row, ["msn"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acft"])),
  position: pick(row, ["position"]),
  removedSN: pick(row, ["removedSN"]),
  installedSN: pick(row, ["installedSN"]),
  cost: toNumber(pick(row, ["cost"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.acftRegn || row.date || row.month);

const normalizeAllocationTable = (rows = []) => rows.map((row) => ({
  costCode: inferAllocationCostCode(row),
  basis: normalizeMetric(pick(row, ["basis", "basisOfAllocation"])),
  scope: normalize(pick(row, ["scope"])),
})).filter((row) => row.costCode || row.basis);

const normalizeCostConfig = (config = {}) => {
  const navMtowTiers = normalizeNavMtowTiers(config.navMtowTiers);
  return {
    __normalized: true,
    reportingCurrency: normalize(pick(config, ["reportingCurrency"])) || "USD",
    fxRates: Array.isArray(config.fxRates) ? config.fxRates : [],
    allocationTable: normalizeAllocationTable(config.allocationTable || config.costAllocation || []),
    fuelConsum: normalizeFuelConsum(config.fuelConsum || []),
    fuelConsumIndex: normalizeFuelConsumIndex(config.fuelConsumIndex || []),
    apuUsage: normalizeApuUsage(config.apuUsage || []),
    plfEffect: normalizePlfEffect(config.plfEffect || []),
    ccyFuel: normalizeFuelPrice(config.ccyFuel || []),
    leasedReserve: normalizeLeasedReserve(config.leasedReserve || []),
    schMxEvents: normalizeSchMxEvents(config.schMxEvents || []),
    transitMx: normalizeTransitMx(config.transitMx || []),
    otherMx: normalizeOtherMx(config.otherMx || []),
    rotableChanges: normalizeRotableChanges(config.rotableChanges || []),
    navMtowTiers,
    navEnr: normalizeNavigationCost(config.navEnr || [], "sector", navMtowTiers),
    navTerm: normalizeNavigationCost(config.navTerm || [], "arrStn", navMtowTiers),
    airportLanding: normalizeStationCost(config.airportLanding || [], "arrStn"),
    airportAvsec: normalizeStationCost(config.airportAvsec || [], "arrStn"),
    airportDom: normalizeStationCost(config.airportDom || [], "arrStn"),
    airportIntl: normalizeStationCost(config.airportIntl || [], "arrStn"),
    airportOther: normalizeAirportMtowCost(config.airportOther || [], "arrStn", navMtowTiers),
    otherDoc: normalizeOtherDoc(config.otherDoc || []),
    aircraftOnwing: normalizeAircraftOnwing(config.aircraftOnwing || []),
    maintenanceReserveSchedule: normalizeMaintenanceReserveSchedule(config.maintenanceReserveSchedule || []),
    fleet: normalizeFleetRows(config.fleet || []),
  };
};

const getBasisValue = (flight, basis) => {
  switch (normalizeMetric(basis)) {
    case "BH":
      return toNumber(flight.bh);
    case "FH":
      return toNumber(flight.fh);
    case "DEPARTURES":
    default:
      return 1;
  }
};

const getDefaultAllocationBasis = (costCode) => {
  switch (costCode) {
    case "APUFUELCOST":
      return "DEPARTURES";
    case "MRMONTHLY":
      return "BH";
    case "QUALIFYINGSCHMXEVENTS":
      return "BH";
    case "OTHERMXEXPENSES":
      return "FH";
    case "ROTABLECHANGES":
      return "DEPARTURES";
    default:
      return "DEPARTURES";
  }
};

const getAllocationBasis = (config, costCode) => {
  const match = (config.allocationTable || []).find((row) => row.costCode === normalizeCostCodeId(costCode));
  return match?.basis || getDefaultAllocationBasis(normalize(costCode));
};

const getPricePerKg = (row) => {
  const rate = toNumber(row?.intoPlaneRate);
  const kgPerLtr = toNumber(row?.kgPerLtr);
  if (rate <= 0) return 0;
  if (kgPerLtr > 0) return rate / (kgPerLtr * 1000);
  return rate;
};

const calculateFuelCost = (fuelConsumptionKg, fuelPriceRow) => {
  const consumption = toNumber(fuelConsumptionKg);
  const kgPerLtr = toNumber(fuelPriceRow?.kgPerLtr);
  const intoPlanePerKLtr = toNumber(fuelPriceRow?.intoPlaneRate);

  if (consumption <= 0 || kgPerLtr <= 0 || intoPlanePerKLtr <= 0) return 0;

  return round2((consumption / kgPerLtr) * (intoPlanePerKLtr / 1000));
};

const convertToRccy = (amount, currency, reportingCurrency, explicitRccy) => {
  const numeric = round2(amount);
  if (toNumber(explicitRccy) > 0) return round2(explicitRccy);
  if (!currency || normalize(currency) === normalize(reportingCurrency)) return numeric;
  return numeric;
};

const scoreSpecificity = (pairs) => pairs.reduce((sum, entry) => sum + (entry ? 1 : 0), 0);

const matchesOptional = (target, actual) => !target || target === actual;
const matchesOptionalNumber = (target, actual) => {
  if (target === undefined || target === null || target === "") return true;
  return toNumber(target) === toNumber(actual);
};

const isAdditionalApuUseRow = (row = {}) => normalize(row.addlnUse) === "Y";

const getFirstDayOfNextMonth = (value) => {
  const date = parseDate(value);
  if (!date) return null;
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1));
};

const getLatestAircraftOnwingForFlight = (flight, onwingRows = []) => {
  const flightDate = getFlightDate(flight);
  const aircraftMsn = getFlightMsn(flight);
  if (!flightDate || !aircraftMsn || !Array.isArray(onwingRows) || onwingRows.length === 0) return null;

  let latest = null;
  onwingRows.forEach((row) => {
    if (normalize(row?.msn) !== aircraftMsn) return;
    const rowDate = parseDate(row?.date);
    if (!rowDate || rowDate > flightDate) return;

    if (!latest) {
      latest = row;
      return;
    }

    const latestDate = parseDate(latest?.date);
    if (!latestDate || rowDate > latestDate) {
      latest = row;
    }
  });

  return latest;
};

const getFlightSnContext = (flight, onwingRows = []) => {
  const onwing = getLatestAircraftOnwingForFlight(flight, onwingRows);
  const msn = normalize(onwing?.msn || getFlightMsn(flight));
  const eng1Esn = normalize(onwing?.pos1Esn || flight?.eng1Esn);
  const eng2Esn = normalize(onwing?.pos2Esn || flight?.eng2Esn);
  const apun = normalize(onwing?.apun || flight?.apun);
  return {
    msn,
    eng1Esn,
    eng2Esn,
    apun,
    snList: [msn, eng1Esn, eng2Esn, apun].filter(Boolean),
  };
};

const applySnContext = (flight, config = {}) => {
  const context = getFlightSnContext(flight, config.aircraftOnwing || []);
  flight.msn = context.msn;
  flight.eng1Esn = context.eng1Esn;
  flight.eng2Esn = context.eng2Esn;
  flight.apun = context.apun;
  flight.sn = context.snList.join(", ");
  flight.snList = context.snList;
};

const resolveMaintenanceReserveRate = (flight, config = {}) => {
  const flightDate = getFlightDate(flight);
  const fh = toNumber(flight?.fh);
  if (!flightDate || fh <= 0) {
    return { amount: 0, rate: 0, currency: "", reportingAmount: 0 };
  }

  const onwing = getLatestAircraftOnwingForFlight(flight, config.aircraftOnwing || []);
  const engineSns = [onwing?.pos1Esn, onwing?.pos2Esn]
    .map((value) => normalize(value))
    .filter(Boolean);

  if (engineSns.length === 0) {
    return { amount: 0, rate: 0, currency: "", reportingAmount: 0 };
  }

  const reserveSettings = Array.isArray(config.leasedReserve) ? config.leasedReserve : [];
  const scheduleRows = Array.isArray(config.maintenanceReserveSchedule) ? config.maintenanceReserveSchedule : [];
  const rateDate = getFirstDayOfNextMonth(flightDate);

  const settingsMatches = reserveSettings.filter((row) => {
    if (normalizeMetric(row?.driver) === "MONTH") return false;
    if (!matchesOptional(row?.sn, engineSns[0]) && !engineSns.includes(normalize(row?.sn))) return false;
    if (!matchesOptional(row?.acftRegn, getFlightRegistration(flight))) return false;
    if (!matchesOptional(row?.pn, getFlightPartNumber(flight))) return false;
    return isWithinRange(flightDate, row?.asOnDate, row?.endDate);
  });

  const buildScore = (row) => scoreSpecificity([row?.acftRegn, row?.pn, row?.sn, row?.mrAccId, row?.endDate, row?.driver]);
  const bestSettings = settingsMatches.sort((a, b) => buildScore(b) - buildScore(a))[0] || null;

  const candidateSnList = settingsMatches.length > 0
    ? settingsMatches.map((row) => normalize(row?.sn)).filter(Boolean)
    : engineSns;

  const matchedScheduleRows = scheduleRows.filter((row) => {
    if (!row || !rateDate) return false;
    const rowDate = parseDate(row.date);
    if (!rowDate || rowDate.getTime() !== rateDate.getTime()) return false;
    if (candidateSnList.length > 0 && !candidateSnList.includes(normalize(row.msn))) return false;
    if (bestSettings?.mrAccId && normalize(row.mrAccId) && normalize(row.mrAccId) !== normalize(bestSettings.mrAccId)) return false;
    return true;
  });

  const bestSchedule = matchedScheduleRows.sort((a, b) => scoreSpecificity([b?.mrAccId, b?.acftReg, b?.msn]) - scoreSpecificity([a?.mrAccId, a?.acftReg, a?.msn]))[0] || null;

  const derivedRate = toNumber(bestSchedule?.rate);
  const fallbackRate = toNumber(bestSettings?.setRate || bestSettings?.rate);
  const rate = derivedRate > 0 ? derivedRate : fallbackRate;
  const currency = bestSchedule?.ccy || bestSettings?.ccy || "";
  const reportingAmount = toNumber(bestSchedule?.costRCCY || bestSettings?.costRCCY);

  if (rate <= 0) {
    return { amount: 0, rate: 0, currency, reportingAmount };
  }

  return {
    amount: round2(fh * rate),
    rate: round2(rate),
    currency,
    reportingAmount,
  };
};

const resolveMaintenanceReserveMonthlyContribution = (flight, config = {}) => {
  const flightMonthNumber = getFlightMonthNumber(flight);
  const flightDate = getFlightDate(flight);
  if (!flightMonthNumber || !flightDate) return 0;

  const onwing = getLatestAircraftOnwingForFlight(flight, config.aircraftOnwing || []);
  if (!onwing) return 0;

  const engineSns = [onwing?.pos1Esn, onwing?.pos2Esn]
    .map((value) => normalize(value))
    .filter(Boolean);
  if (engineSns.length === 0) return 0;

  const monthlyReserveRows = (Array.isArray(config.maintenanceReserveSchedule) ? config.maintenanceReserveSchedule : [])
    .filter((row) => normalizeMetric(row?.driver) === "MONTH" && toNumber(row?.contribution) > 0);
  if (monthlyReserveRows.length === 0) return 0;

  return monthlyReserveRows.reduce((sum, row) => {
    const rowMonthNumber = toNumber(row?.monthNumber) || (parseDate(row?.date)?.getUTCMonth() + 1 || 0);
    if (rowMonthNumber !== flightMonthNumber) return sum;
    if (row?.msn && !engineSns.includes(normalize(row.msn))) return sum;
    if (row?.acftReg && normalize(row.acftReg) && normalize(row.acftReg) !== getFlightRegistration(flight)) return sum;
    return sum + toNumber(row.contribution);
  }, 0);
};

const getNavigationTieredCost = (row, mtow) => {
  const tierValue = toNumber(mtow);
  if (!row) return 0;

  const converted = toNumber(row.costRCCY);
  if (converted > 0) return round2(converted);

  if (tierValue > 0 && row.tierRates && row.tierRates[tierValue] !== undefined) {
    return round2(row.tierRates[tierValue]);
  }

  return round2(row.cost || 0);
};

const getAirportMtowTieredCost = (row, mtow) => {
  const tierValue = toNumber(mtow);
  if (!row) return null;

  if (toNumber(row.costRCCY) > 0) {
    return round2(row.costRCCY);
  }

  if (tierValue > 0 && row.tierRates && row.tierRates[tierValue] !== undefined) {
    return round2(row.tierRates[tierValue]);
  }

  if (tierValue > 0 && row[String(tierValue)] !== undefined && row[String(tierValue)] !== "") {
    return round2(row[String(tierValue)]);
  }

  if (row.cost !== undefined && row.cost !== null && row.cost !== "") {
    return round2(row.cost);
  }

  return null;
};

const pickBest = (rows, scorer) => {
  let best = null;
  let bestScore = -1;
  rows.forEach((row, index) => {
    const score = scorer(row, index);
    if (score > bestScore) {
      best = row;
      bestScore = score;
    }
  });
  return bestScore >= 0 ? best : null;
};

const applyCostField = (flight, field, amount, currency, reportingCurrency, explicitRccy) => {
  const numeric = round2(amount);
  flight[field] = numeric;
  flight[`${field}CCY`] = currency || "";
  flight[`${field}RCCY`] = convertToRccy(numeric, currency, reportingCurrency, explicitRccy);
};

const initializeFlight = (flight, reportingCurrency) => {
  const next = { ...flight };

  COST_FIELDS.forEach((field) => {
    next[field] = 0;
    next[`${field}CCY`] = "";
    next[`${field}RCCY`] = 0;
  });

  next.engineFuel = 0;
  next.engineFuelConsumption = 0;
  next.mrContribution = 0;
  next.transitMx = 0;
  next.otherMx = 0;
  next.majorSchMx = 0;
  next.apuFuel = 0;
  next.otherMaintenance1 = 0;
  next.otherMaintenance2 = 0;
  next.otherMaintenance3 = 0;
  next.navEnr = 0;
  next.navTrml = 0;
  next.aptLandingCost = 0;
  next.aptHandlingCost = 0;
  next.aptOtherCost = 0;
  next.otherDoc1 = 0;
  next.otherDoc2 = 0;
  next.otherDoc3 = 0;
  next.crewOverlay = 0;
  next.crewPositioning = 0;
  next.reportingCurrency = reportingCurrency;

  return next;
};

const addAllocation = (flight, field, amount, currency, reportingCurrency, explicitRccy) => {
  const numeric = round2((flight[field] || 0) + amount);
  flight[field] = numeric;
  flight[`${field}CCY`] = currency || flight[`${field}CCY`] || "";
  flight[`${field}RCCY`] = round2((flight[`${field}RCCY`] || 0) + convertToRccy(amount, currency, reportingCurrency, explicitRccy));
};

const distributePool = (eligibleFlights, field, totalAmount, currency, reportingCurrency, basis, explicitRccy) => {
  const amount = round2(totalAmount);
  if (!eligibleFlights.length || amount === 0) return;

  const weights = eligibleFlights.map((flight) => Math.max(getBasisValue(flight, basis), 0));
  const totalWeight = weights.reduce((sum, value) => sum + value, 0);
  const safeWeights = totalWeight > 0 ? weights : eligibleFlights.map(() => 1);
  const safeTotal = safeWeights.reduce((sum, value) => sum + value, 0);

  let allocated = 0;
  eligibleFlights.forEach((flight, index) => {
    const share = index === eligibleFlights.length - 1
      ? round2(amount - allocated)
      : round2((amount * safeWeights[index]) / safeTotal);
    allocated = round2(allocated + share);
    addAllocation(flight, field, share, currency, reportingCurrency, explicitRccy);
  });
};

const distributeMonthlyPoolByBasis = (eligibleFlights, field, totalAmount, currency, reportingCurrency, basis, explicitRccy) => {
  const amount = round2(totalAmount);
  if (!eligibleFlights.length || amount === 0) return;

  const groupedFlights = new Map();
  eligibleFlights.forEach((flight) => {
    const monthKey = getFlightMonthKey(flight);
    const aircraftKey = getFlightAircraftKey(flight);
    if (!monthKey || !aircraftKey) return;
    const groupKey = `${aircraftKey}|${monthKey}`;
    if (!groupedFlights.has(groupKey)) groupedFlights.set(groupKey, []);
    groupedFlights.get(groupKey).push(flight);
  });

  groupedFlights.forEach((flightsInGroup) => {
    const weights = flightsInGroup.map((flight) => Math.max(getBasisValue(flight, basis), 0));
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    const safeWeights = totalWeight > 0 ? weights : flightsInGroup.map(() => 1);
    const safeTotal = safeWeights.reduce((sum, value) => sum + value, 0);

    let allocated = 0;
    flightsInGroup.forEach((flight, index) => {
      const share = index === flightsInGroup.length - 1
        ? round2(amount - allocated)
        : round2((amount * safeWeights[index]) / safeTotal);
      allocated = round2(allocated + share);
      addAllocation(flight, field, share, currency, reportingCurrency, explicitRccy);
    });
  });
};

const selectPlfFactor = (rule, paxLf) => {
  if (!rule?.thresholds?.length) return 1;
  const pct = toNumber(paxLf);
  if (pct <= 0) return 1;

  const sorted = [...rule.thresholds].sort((a, b) => a.threshold - b.threshold);
  const nextThreshold = sorted.find((entry) => pct <= entry.threshold);
  const selected = nextThreshold || sorted[sorted.length - 1];
  return selected?.factor || 1;
};

const selectTransitRule = (rows = [], { flightDate, depStn, variant, acftReg }) => {
  const candidates = [];

  rows.forEach((row, index) => {
    if (!row) return;
    if (!matchesOptional(row.depStn, depStn)) return;
    if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return;

    const matchesAcft = matchesOptional(row.acftRegn, acftReg);
    const matchesVariant = matchesOptional(row.variant, variant);
    let priority = 0;

    if (row.acftRegn && matchesAcft) {
      priority = 2;
    } else if (row.variant && matchesVariant) {
      priority = 1;
    } else if (!row.acftRegn && !row.variant) {
      priority = 1;
    }

    if (priority === 0) return;

    candidates.push({
      row,
      priority,
      effectiveFrom: parseDate(row.fromDate)?.getTime() || 0,
      effectiveTo: parseDate(row.toDate)?.getTime() || 0,
      index,
    });
  });

  if (candidates.length === 0) return null;

  candidates.sort((a, b) => (
    b.priority - a.priority ||
    b.effectiveFrom - a.effectiveFrom ||
    b.effectiveTo - a.effectiveTo ||
    b.index - a.index
  ));

  return candidates[0].row;
};

const enrichDirectCosts = (flights, config) => {
  flights.forEach((flight) => {
    const flightDate = getFlightDate(flight);
    const flightMonthKey = getFlightMonthKey(flight);
    const acftReg = getFlightRegistration(flight);
    const msn = getFlightMsn(flight);
    const pn = getFlightPartNumber(flight);
    const sector = getFlightSector(flight);
    const depStn = getFlightDep(flight);
    const arrStn = getFlightArr(flight);
    const variant = getFlightVariant(flight);
    const domIntl = getFlightDomIntl(flight);
    const bh = toNumber(flight.bh);
    const fh = toNumber(flight.fh);
    const mtow = getFlightMtow(flight, config.fleet);
    const departures = 1;

    const fuelRule = pickBest(config.fuelConsum, (row) => {
      if (!matchesOptional(row.sectorOrGcd, sector) && !matchesOptional(row.sectorOrGcd, normalize(flight.dist))) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      return scoreSpecificity([row.sectorOrGcd, row.acftRegn, row.month]) * 10;
    });

    const fuelIndexRule = pickBest(config.fuelConsumIndex, (row) => {
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      return scoreSpecificity([row.acftRegn, row.month]) * 10;
    });

    const plfRule = pickBest(config.plfEffect, (row) => {
      if (!matchesOptional(row.sectorOrGcd, sector) && !matchesOptional(row.sectorOrGcd, normalize(flight.dist))) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      return scoreSpecificity([row.sectorOrGcd, row.acftRegn]);
    });

    const fuelPriceRule = pickBest(config.ccyFuel, (row) => {
      if (!matchesOptional(row.station, depStn)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      return scoreSpecificity([row.station, row.month]);
    });

    const plfFactor = selectPlfFactor(plfRule, getFlightLoadFactor(flight));
    const fuelIndexFactor = toNumber(fuelIndexRule?.fuelConsumptionIndex) || 1;
    const baseFuelConsumption = round2((fuelRule?.fuelConsumptionKg || 0) * fuelIndexFactor * plfFactor);
    const engineFuelCost = fuelPriceRule
      ? calculateFuelCost(baseFuelConsumption, fuelPriceRule)
      : baseFuelConsumption;

    flight.engineFuelConsumption = baseFuelConsumption;
    flight.engineFuel = baseFuelConsumption;
    applyCostField(
      flight,
      "engineFuelCost",
      engineFuelCost,
      fuelPriceRule?.ccy || "",
      config.reportingCurrency,
      fuelPriceRule?.costRCCY || 0
    );

    const mrDerived = resolveMaintenanceReserveRate(flight, config);
    if (mrDerived.amount > 0) {
      applyCostField(
        flight,
        "maintenanceReserveContribution",
        mrDerived.amount,
        mrDerived.currency,
        config.reportingCurrency,
        mrDerived.reportingAmount
      );
    }

    const mrDirectRule = pickBest(config.leasedReserve, (row) => {
      if (!["BH", "FH", "DEPARTURES"].includes(row.driver)) return -1;
      if (row.endDate && !isWithinRange(flightDate, null, row.endDate)) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (!matchesOptional(row.sn, msn) && !matchesOptional(row.sn, acftReg)) return -1;
      return scoreSpecificity([row.acftRegn, row.sn, row.pn, row.endDate]) * 10;
    });
    if (mrDirectRule && mrDerived.amount <= 0) {
      let mrAmount = mrDirectRule.setRate;
      if (mrDirectRule.driver === "BH") mrAmount *= bh;
      else if (mrDirectRule.driver === "FH") mrAmount *= fh;
      else mrAmount *= departures;
      applyCostField(
        flight,
        "maintenanceReserveContribution",
        mrAmount,
        mrDirectRule.ccy,
        config.reportingCurrency,
        mrDirectRule.costRCCY
      );
    }

    const transitRule = selectTransitRule(config.transitMx, {
      flightDate,
      depStn,
      variant,
      acftReg,
    });
    if (transitRule) {
      applyCostField(
        flight,
        "transitMaintenance",
        transitRule.costPerDeparture * departures,
        transitRule.ccy,
        config.reportingCurrency,
        transitRule.costRCCY
      );
    }

    const matchingOtherMxRows = (config.otherMx || []).filter((row) => (
      matchesOtherMxRow(row, { flightDate, depStn, variant, acftReg, pn, msn }) &&
      (row.costPerBh || row.costPerDeparture || row.costPerMonth)
    ));
    if (matchingOtherMxRows.length > 0) {
      const perBhTotal = matchingOtherMxRows.reduce(
        (sum, row) => sum + ((row.costPerBh || 0) * bh),
        0
      );
      const perDepTotal = matchingOtherMxRows.reduce(
        (sum, row) => sum + ((row.costPerDeparture || 0) * departures),
        0
      );
      const perMonthTotal = matchingOtherMxRows.reduce(
        (sum, row) => sum + (row.costPerMonth || 0),
        0
      );
      const ccy = matchingOtherMxRows.find((row) => row.ccy)?.ccy || "";
      const costRCCY = matchingOtherMxRows.find((row) => row.costRCCY)?.costRCCY || 0;
      flight.otherMaintenance1 = round2(perBhTotal);
      flight.otherMaintenance2 = round2(perDepTotal);
      flight.otherMaintenance3 = round2(perMonthTotal);
      applyCostField(
        flight,
        "otherMaintenance",
        perBhTotal + perDepTotal,
        ccy,
        config.reportingCurrency,
        costRCCY
      );
    }

    const enrRule = pickBest(config.navEnr, (row) => {
      if (!matchesOptional(row.sector, sector)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.sector, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const termRule = pickBest(config.navTerm, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    flight.navEnr = round2(getNavigationTieredCost(enrRule, mtow));
    flight.navTrml = round2(getNavigationTieredCost(termRule, mtow));
    applyCostField(
      flight,
      "navigation",
      flight.navEnr + flight.navTrml,
      enrRule?.ccy || termRule?.ccy || "",
      config.reportingCurrency,
      enrRule?.costRCCY || termRule?.costRCCY
    );

    const landingRule = pickBest(config.airportLanding, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptionalNumber(row.mtow, mtow)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.mtow, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const avsecRule = pickBest(config.airportAvsec, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const otherAirportRule = pickBest(config.airportOther || [], (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptionalNumber(row.mtow, mtow)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.mtow, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const handlingSource = domIntl === "INTL" ? config.airportIntl : config.airportDom;
    const handlingRule = pickBest(handlingSource, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptionalNumber(row.mtow, mtow)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.mtow, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    flight.aptLandingCost = round2(landingRule?.cost || 0);
    const otherAirportCost = getAirportMtowTieredCost(otherAirportRule, mtow);
    flight.aptOtherCost = round2(
      otherAirportCost ?? (avsecRule?.cost || 0)
    );
    flight.aptHandlingCost = round2(handlingRule?.cost || 0);
    applyCostField(
      flight,
      "airport",
      flight.aptLandingCost + flight.aptHandlingCost + flight.aptOtherCost,
      landingRule?.ccy || handlingRule?.ccy || otherAirportRule?.ccy || avsecRule?.ccy || "",
      config.reportingCurrency,
      landingRule?.costRCCY || handlingRule?.costRCCY || otherAirportRule?.costRCCY || avsecRule?.costRCCY
    );

    const matchingOtherDocs = (config.otherDoc || []).filter((row) => {
      if (!matchesOptional(row.sector, sector)) return false;
      if (!matchesOptional(row.depStn, depStn)) return false;
      if (!matchesOptional(row.arrStn, arrStn)) return false;
      if (!matchesOptional(row.variant, variant)) return false;
      if (!matchesOptional(row.acftRegn, acftReg)) return false;
      if (!matchesOptional(row.sn, msn)) return false;
      return isWithinRange(flightDate, row.fromDate, row.toDate);
    }).sort((a, b) => {
      const scoreA = scoreSpecificity([a.sector, a.depStn, a.arrStn, a.variant, a.acftRegn, a.pn, a.sn, a.fromDate || a.toDate]);
      const scoreB = scoreSpecificity([b.sector, b.depStn, b.arrStn, b.variant, b.acftRegn, b.pn, b.sn, b.fromDate || b.toDate]);
      return scoreB - scoreA;
    });

    flight.otherDoc1 = round2(matchingOtherDocs[0]?.cost || 0);
    flight.otherDoc2 = round2(matchingOtherDocs[1]?.cost || 0);
    flight.otherDoc3 = round2(matchingOtherDocs[2]?.cost || 0);
    applyCostField(
      flight,
      "otherDoc",
      matchingOtherDocs.reduce((sum, row) => sum + row.cost, 0),
      matchingOtherDocs[0]?.ccy || "",
      config.reportingCurrency,
      matchingOtherDocs[0]?.costRCCY
    );

    applyCostField(flight, "crewAllowances", 0, "", config.reportingCurrency, 0);
    applyCostField(flight, "layoverCost", 0, "", config.reportingCurrency, 0);
    applyCostField(flight, "crewPositioningCost", 0, "", config.reportingCurrency, 0);
  });
};

const enrichAllocatedCosts = (flights, config) => {
  const masterStart = flights.reduce((min, flight) => {
    const date = getFlightDate(flight);
    if (!date) return min;
    return !min || date < min ? date : min;
  }, null);
  const masterEnd = flights.reduce((max, flight) => {
    const date = getFlightDate(flight);
    if (!date) return max;
    return !max || date > max ? date : max;
  }, null);

  config.apuUsage.forEach((row) => {
    const isAdditionalUse = isAdditionalApuUseRow(row);
    const monthFlights = flights.filter((flight) => {
      const flightDate = getFlightDate(flight);
      return (isAdditionalUse || getFlightArr(flight) === row.arrStn) &&
        matchesOptional(row.variant, getFlightVariant(flight)) &&
        matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
        isWithinRange(flightDate, row.fromDate, row.toDate);
    });

    if (!monthFlights.length) return;

    const priceSourceFlight = isAdditionalUse
      ? getLatestFlightForAircraft(monthFlights, monthFlights[0] || row)
      : monthFlights[0];
    const priceRule = pickBest(config.ccyFuel, (price) => {
      if (!matchesOptional(price.station, priceSourceFlight ? getFlightDep(priceSourceFlight) : "")) return -1;
      if (price.month && price.month !== getFlightMonthKey(priceSourceFlight || monthFlights[0])) return -1;
      return scoreSpecificity([price.station, price.month]);
    });
    const pricePerKg = getPricePerKg(priceRule);
    const totalKg = row.apuHours > 0 ? row.apuHours * row.consumptionPerApuHour : row.consumptionPerApuHour;
    const poolAmount = pricePerKg > 0 ? totalKg * pricePerKg : totalKg;
    distributePool(
      monthFlights,
      "apuFuelCost",
      poolAmount,
      priceRule?.ccy || row.ccy,
      config.reportingCurrency,
      row.basis || getAllocationBasis(config, "APUFUELCOST"),
      priceRule?.costRCCY || row.costRCCY
    );
  });

  const monthlyReserveRows = (config.maintenanceReserveSchedule || []).filter((row) => normalizeMetric(row.driver) === "MONTH" && toNumber(row.contribution) > 0);
  if (monthlyReserveRows.length > 0) {
    const monthlyReserveGroups = new Map();

    monthlyReserveRows.forEach((row) => {
      const rowMonthNumber = toNumber(row.monthNumber) || (parseDate(row.date)?.getUTCMonth() + 1 || 0);
      if (!rowMonthNumber) return;

      const matchedFlights = [];
      flights.forEach((flight) => {
        if (getFlightMonthNumber(flight) !== rowMonthNumber) return;

        const flightOnwing = getLatestAircraftOnwingForFlight(flight, config.aircraftOnwing || []);
        if (!flightOnwing) return;

        const engineSns = [flightOnwing.pos1Esn, flightOnwing.pos2Esn]
          .map((value) => normalize(value))
          .filter(Boolean);
        if (engineSns.length === 0) return;
        if (row.msn && !engineSns.includes(normalize(row.msn))) return;
        if (row.acftReg && normalize(row.acftReg) && normalize(row.acftReg) !== getFlightRegistration(flight)) return;
        matchedFlights.push(flight);
      });

      if (matchedFlights.length === 0) return;

      const groupKey = `${getFlightAircraftKey(matchedFlights[0])}|${rowMonthNumber}`;
      if (!monthlyReserveGroups.has(groupKey)) {
        monthlyReserveGroups.set(groupKey, {
          flights: new Map(),
          amount: 0,
          currency: row.ccy || "",
          reportingAmount: row.costRCCY || 0,
        });
      }

      const group = monthlyReserveGroups.get(groupKey);
      matchedFlights.forEach((flight) => {
        const flightKey = `${String(flight.flight || "")}|${String(flight.date || "")}`;
        group.flights.set(flightKey, flight);
      });
      group.amount = round2(group.amount + toNumber(row.contribution));
      if (!group.currency && row.ccy) group.currency = row.ccy;
      if (!group.reportingAmount && row.costRCCY) group.reportingAmount = row.costRCCY;
    });

    monthlyReserveGroups.forEach((group) => {
      distributePool(
        Array.from(group.flights.values()),
        "mrMonthly",
        group.amount,
        group.currency,
        config.reportingCurrency,
        "BH",
        group.reportingAmount
      );
    });
  } else {
    config.leasedReserve.forEach((row) => {
      if (normalizeMetric(row.driver) !== "MONTH") return;
      const eligibleFlights = flights.filter((flight) => {
        const flightDate = getFlightDate(flight);
        if (row.endDate && !isWithinRange(flightDate, null, row.endDate)) return false;
        return matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
          matchesOptional(row.sn, getFlightMsn(flight));
      });
      distributePool(
        eligibleFlights,
        "mrMonthly",
        row.setRate,
        row.ccy,
        config.reportingCurrency,
        row.basis || getAllocationBasis(config, "MRMONTHLY"),
        row.costRCCY
      );
    });
  }

  const schGroups = {};
  config.schMxEvents.forEach((row) => {
    if (["Y", "YES", "TRUE", "1"].includes(row.capitalisation)) return;
    const key = [row.event, row.msnEsnApun || row.snBn || ""].join("|");
    if (!schGroups[key]) schGroups[key] = [];
    schGroups[key].push(row);
  });

  Object.values(schGroups).forEach((events) => {
    const sorted = [...events].sort((a, b) => (parseDate(a.date)?.getTime() || 0) - (parseDate(b.date)?.getTime() || 0));
    sorted.forEach((event, index) => {
      const startDate = parseDate(event.date) || masterStart;
      const endDate = parseDate(sorted[index + 1]?.date) || masterEnd;
      const eligibleFlights = flights.filter((flight) => {
        const flightDate = getFlightDate(flight);
        if (!flightDate || !startDate || !endDate) return false;
        if (flightDate < startDate || flightDate > endDate) return false;
        return matchesOptional(event.msnEsnApun, getFlightMsn(flight)) ||
          matchesOptional(event.msnEsnApun, getFlightRegistration(flight));
      });
      distributePool(
        eligibleFlights,
        "qualifyingSchMxEvents",
        event.cost,
        event.ccy,
        config.reportingCurrency,
        getAllocationBasis(config, "QUALIFYINGSCHMXEVENTS"),
        event.costRCCY
      );
    });
  });

  config.otherMx.forEach((row) => {
    if (!row.costPerMonth) return;
    const eligibleFlights = flights.filter((flight) => {
      const flightDate = getFlightDate(flight);
      return matchesOtherMxRow(row, {
        flightDate,
        depStn: getFlightDep(flight),
        variant: getFlightVariant(flight),
        acftReg: getFlightRegistration(flight),
        pn: getFlightPartNumber(flight),
        msn: getFlightMsn(flight),
      });
    });
    distributeMonthlyPoolByBasis(
      eligibleFlights,
      "otherMxExpenses",
      row.costPerMonth,
      row.ccy,
      config.reportingCurrency,
      row.basis || getAllocationBasis(config, "OTHERMXEXPENSES"),
      row.costRCCY
    );
  });

  config.rotableChanges.forEach((row) => {
    const rowMonth = row.month || normalizeMonthKey(row.date);
    const eligibleFlights = flights.filter((flight) => {
      return matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
        (!rowMonth || getFlightMonthKey(flight) === rowMonth);
    });
    distributePool(
      eligibleFlights,
      "rotableChanges",
      row.cost,
      row.ccy,
      config.reportingCurrency,
      getAllocationBasis(config, "ROTABLECHANGES"),
      row.costRCCY
    );
  });
};

const applyCompatibilityAliases = (flight) => {
  flight.mrContribution = flight.maintenanceReserveContribution;
  flight.transitMx = flight.transitMaintenance;
  flight.otherMx = flight.otherMaintenance;
  flight.majorSchMx = flight.qualifyingSchMxEvents;
  flight.apuFuel = flight.apuFuelCost;
  flight.crewPositioning = flight.crewPositioningCost;
};

const computeFlightCostsBatch = (inputFlights = [], rawConfig = {}) => {
  const config = rawConfig?.__normalized ? rawConfig : normalizeCostConfig(rawConfig);

  const flights = (inputFlights || []).map((flight) => initializeFlight(flight, config.reportingCurrency));

  flights.forEach((flight) => applySnContext(flight, config));
  enrichDirectCosts(flights, config);
  enrichAllocatedCosts(flights, config);
  flights.forEach(applyCompatibilityAliases);

  return flights;
};

const computeFlightCosts = (flight, rawConfig = {}) => computeFlightCostsBatch([flight], rawConfig)[0];

module.exports = {
  normalizeCostConfig,
  normalizeAllocationTable,
  flattenFuelConsumRows,
  flattenFuelConsumIndexRows,
  flattenPlfEffectRows,
  flattenFuelPriceRows,
  normalizeApuUsage,
  normalizeOtherMx,
  normalizeFleetRows,
  groupFuelConsumRows,
  groupFuelConsumIndexRows,
  groupPlfEffectRows,
  groupFuelPriceRows,
  normalizeTransitMx,
  normalizeAircraftOnwing,
  normalizeMaintenanceReserveSchedule,
  normalizeNavMtowTiers,
  serializeNavigationCostRows,
  serializeAirportMtowCostRows,
  hydrateSchMxEvents,
  getFlightSnContext,
  getLatestFlightForAircraft,
  getApuFuelPriceSourceFlight,
  computeFlightCostsBatch,
  computeFlightCosts,
};
