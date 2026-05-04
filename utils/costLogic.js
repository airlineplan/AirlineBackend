/**
 * Shared cost enrichment for the FLIGHT master rows.
 *
 * The app has multiple generations of cost-input row shapes in the DB, so this
 * module normalizes config rows first and then computes direct plus allocated
 * costs in one batch so every consumer sees the same output.
 */

const {
  normalizeCurrencyCode: normalizeFxCurrencyCode,
  normalizeDateKey: normalizeFxDateKey,
  getCarriedForwardFxRate,
} = require("./fx");

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
  "totalCost",
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

const getUtcMonthStart = (date) => new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));

const getDaysInUtcMonth = (date) => new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)).getUTCDate();

const isSameUtcMonth = (a, b) => (
  a && b &&
  a.getUTCFullYear() === b.getUTCFullYear() &&
  a.getUTCMonth() === b.getUTCMonth()
);

const getExactExampleMonthlyFactor = (monthDate, fromDateValue, toDateValue) => {
  const monthStart = getUtcMonthStart(monthDate);
  const startDate = parseDate(fromDateValue);
  const endDate = parseDate(toDateValue);

  if (startDate && !endDate && isSameUtcMonth(startDate, monthStart)) {
    if (startDate.getUTCDate() === 1) return 1;
    return startDate.getUTCDate() / getDaysInUtcMonth(monthStart);
  }

  if (!startDate && endDate && isSameUtcMonth(endDate, monthStart)) {
    if (endDate.getUTCDate() === getDaysInUtcMonth(monthStart)) return 1;
    return endDate.getUTCDate() / 31;
  }

  if (startDate && endDate && isSameUtcMonth(startDate, monthStart) && isSameUtcMonth(endDate, monthStart)) {
    if (startDate.getUTCDate() === 1 && endDate.getUTCDate() === getDaysInUtcMonth(monthStart)) {
      return 1;
    }
    if (endDate.getUTCDate() === getDaysInUtcMonth(monthStart)) {
      if (startDate.getUTCDate() === 1) return 1;
      return startDate.getUTCDate() / getDaysInUtcMonth(monthStart);
    }
    if (startDate.getUTCDate() === 1) return endDate.getUTCDate() / 31;
    return endDate.getUTCDate() / 31;
  }

  if (startDate && isSameUtcMonth(startDate, monthStart)) {
    if (startDate.getUTCDate() === 1) return 1;
    return startDate.getUTCDate() / getDaysInUtcMonth(monthStart);
  }

  if (endDate && isSameUtcMonth(endDate, monthStart)) {
    if (endDate.getUTCDate() === getDaysInUtcMonth(monthStart)) return 1;
    return endDate.getUTCDate() / 31;
  }

  return 1;
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
  if (["FT", "FLIGHTTIME", "FLIGHT TIME"].includes(raw)) return "FT";
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
const getFlightSector = (flight) => normalize(flight?.sector) || [flight?.depStn, flight?.arrStn].filter(Boolean).join("-").toUpperCase();
const getFlightDep = (flight) => normalize(flight?.depStn);
const getFlightArr = (flight) => normalize(flight?.arrStn);
const getFlightDomIntl = (flight) => normalize(flight?.domIntl);
const getFlightEffectiveFt = (flight) => {
  const ft = toNumber(flight?.ft);
  if (ft > 0) return ft;
  const fh = toNumber(flight?.fh);
  if (fh > 0) return fh;
  return toNumber(flight?.bh);
};
const getFlightMtow = (flight, fleetRows = []) => {
  const flightDate = getFlightDate(flight);
  const regn = getFlightRegistration(flight);
  const msn = getFlightMsn(flight);
  const variant = getFlightVariant(flight);
  if (!flightDate) return 0;

  const validRows = (Array.isArray(fleetRows) ? fleetRows : []).filter((row) => {
    const entry = parseDate(row?.entry);
    const exit = parseDate(row?.exit);
    if (entry && flightDate < entry) return false;
    if (exit && flightDate > exit) return false;
    if (toNumber(row?.mtow) <= 0) return false;
    return true;
  });

  const findBest = (predicate) => validRows.filter(predicate).sort((a, b) => {
    const aEntry = parseDate(a?.entry)?.getTime() || 0;
    const bEntry = parseDate(b?.entry)?.getTime() || 0;
    return bEntry - aEntry;
  })[0];

  const best =
    (regn && findBest((row) => normalize(row?.regn) === regn)) ||
    (msn && findBest((row) => normalize(row?.sn) === msn)) ||
    (variant && findBest((row) => normalize(row?.variant) === variant || normalize(row?.type) === variant));

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

const getTransitIdentifierRank = (row) => {
  if (row?.sn) return 4;
  if (row?.acftRegn) return 3;
  if (row?.pn) return 2;
  if (row?.variant) return 1;
  return 0;
};

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
  return getPlfThresholdEntries(row);
};

const PLF_PERCENT_KEY_RE = /^p(\d{1,3})$/i;

const getPlfThresholdNumber = (key) => {
  const match = String(key ?? "").match(PLF_PERCENT_KEY_RE);
  if (!match) return null;
  const threshold = Number(match[1]);
  if (!Number.isFinite(threshold) || threshold <= 0 || threshold > 100) return null;
  return threshold;
};

const getPlfThresholdKeys = (row, { includeCarryForward = true } = {}) => Object.keys(row || {})
  .filter((key) => {
    const threshold = getPlfThresholdNumber(key);
    if (threshold === null) return false;
    if (!includeCarryForward && threshold === 100) return false;
    return true;
  })
  .sort((a, b) => getPlfThresholdNumber(a) - getPlfThresholdNumber(b));

const getPlfCarryForwardValue = (row) => {
  let lastValue = null;
  for (const key of getPlfThresholdKeys(row, { includeCarryForward: false })) {
    const raw = row?.[key];
    if (raw === "" || raw === null || raw === undefined) continue;
    lastValue = round2(toNumber(raw));
  }
  return lastValue ?? 1;
};

const getPlfThresholdEntries = (row) => getPlfThresholdKeys(row).map((key) => ({
  threshold: getPlfThresholdNumber(key),
  factor: key.toLowerCase() === "p100" ? getPlfCarryForwardValue(row) : toNumber(row?.[key]),
})).filter((entry) => Number.isFinite(entry.threshold));

const extractPlfEffectRecords = (rows = []) => {
  const records = [];
  let currentSectorOrGcd = "";
  let currentGcd = "";

  rows.forEach((row) => {
    if (!row) return;

    if (row.rowType === "header") {
      const headerRow = { rowType: "header" };
      getPlfThresholdKeys(row, { includeCarryForward: false }).forEach((key) => {
        headerRow[key] = "";
      });
      records.push(headerRow);
      return;
    }

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

    const record = {
      sectorOrGcd: effectiveSector,
      gcd: effectiveGcd,
      acftRegn,
    };

    getPlfThresholdKeys(row).forEach((key) => {
      const value = key.toLowerCase() === "p100" ? getPlfCarryForwardValue(row) : row?.[key];
      record[key] = value === "" ? "" : toNumber(value);
    });

    records.push(record);
  });

  return records;
};

const flattenPlfEffectRows = (rows = []) => extractPlfEffectRecords(rows);

const groupPlfEffectRows = (rows = []) => {
  const grouped = [];
  const sectors = new Map();

  rows.forEach((row) => {
    if (row?.rowType === "header") {
      const headerRow = { rowType: "header" };
      getPlfThresholdKeys(row, { includeCarryForward: false }).forEach((key) => {
        headerRow[key] = "";
      });
      grouped.push(headerRow);
      return;
    }

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

    const aircraftRow = {
      rowType: "aircraft",
      sectorOrGcd: sectorKey,
      gcd: normalizeFuelValue(row?.gcd || group.sectorRow.gcd),
      acftRegn,
    };

    getPlfThresholdKeys(row).forEach((key) => {
      const value = key.toLowerCase() === "p100" ? getPlfCarryForwardValue(row) : row?.[key];
      aircraftRow[key] = value === "" ? "" : toNumber(value);
    });

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
      pick(row, ["m1", "value1", "index1"]),
      pick(row, ["m2", "value2", "index2"]),
      pick(row, ["m3", "value3", "index3"]),
      pick(row, ["m4", "value4", "index4"]),
      pick(row, ["m5", "value5", "index5"]),
    ];

    const explicitMonth = normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"]));
    const explicitValue = pick(row, ["fuelConsumptionIndex", "index", "value"]);
    if (explicitMonth && explicitValue !== "") {
      records.push({
        acftRegn,
        month1: labels[0] || "",
        month2: labels[1] || "",
        month3: labels[2] || "",
        month4: labels[3] || "",
        month5: labels[4] || "",
        month: explicitMonth,
        fuelConsumptionIndex: round2(explicitValue),
      });
      return;
    }

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

const normalizeApuUsage = (rows = []) => rows.map((row) => {
  const station = normalize(pick(row, ["stn", "arrStn", "station"]));
  return {
    arrStn: station,
    stn: station,
    fromDate: pick(row, ["fromDate", "fromDt"]),
    toDate: pick(row, ["toDate", "toDt"]),
    variant: normalize(pick(row, ["variant", "var"])),
    acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
    apuHours: toNumber(pick(row, ["apuHours", "apuHr", "apuHrPerDay", "hours"])),
    apuHrPerDay: toNumber(pick(row, ["apuHrPerDay", "apuHours", "apuHr", "hours"])),
    consumptionPerApuHour: toNumber(pick(row, ["consumptionPerApuHour", "consumptionKgPerApuHr", "kgPerApuHr", "consumption", "apuFuel", "cost", "value"])),
    kgPerApuHr: toNumber(pick(row, ["kgPerApuHr", "consumptionKgPerApuHr", "consumptionPerApuHour", "consumption", "apuFuel", "cost", "value"])),
    basis: normalizeMetric(pick(row, ["basis"])),
    ccy: normalize(pick(row, ["ccy"])),
    addlnUse: normalize(pick(row, ["addlnUse", "addln", "additionalUse", "addlnUsage"])) || "N",
    costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
  };
}).map((row) => {
  if (row.addlnUse === "Y") {
    return {
      ...row,
      toDate: row.fromDate || row.toDate,
    };
  }
  return {
    ...row,
    addlnUse: row.addlnUse || "N",
  };
}).filter((row) => row.stn || row.variant || row.acftRegn || row.fromDate || row.toDate || row.apuHours || row.consumptionPerApuHour);

const normalizePlfEffect = (rows = []) => rows.filter((row) => row?.rowType !== "header").map((row) => ({
  sectorOrGcd: normalize(pick(row, ["sectorOrGcd", "sector", "type", "gcd"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft"])),
  ...Object.fromEntries(getPlfThresholdKeys(row).map((key) => [
    key,
    key.toLowerCase() === "p100" ? getPlfCarryForwardValue(row) : toNumber(row?.[key]),
  ])),
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
  fromDate: pick(row, ["fromDate", "asOnDate"]),
  toDate: pick(row, ["toDate", "endDate"]),
  msn: normalize(pick(row, ["msn", "sn"])),
  mrAccId: normalize(pick(row, ["mrAccId"])),
  schMxEventAccount: normalize(pick(row, ["schMxEventAccount", "schMxEvent", "event"])),
  acftReg: normalize(pick(row, ["acftReg", "acftRegn"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn"])),
  rate: toNumber(pick(row, ["rate"])),
  contribution: toNumber(pick(row, ["contribution", "setRate"])),
  driverValue: toNumber(pick(row, ["driverValue", "driverVal"])),
  monthNumber: toNumber(pick(row, ["monthNumber", "monthNo", "month", "driverValue", "driverVal"])),
  drawdown: toNumber(pick(row, ["drawdown"])),
  balance: toNumber(pick(row, ["balance"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  driver: normalizeMetric(pick(row, ["driver"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.date || row.fromDate || row.toDate || row.msn || row.mrAccId || row.acftReg || row.rate || row.contribution || row.drawdown || row.balance);

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
  per: normalizeMetric(pick(row, ["per", "basis", "driver"])),
  cost: toNumber(pick(row, ["cost", "rate", "value"])),
  costPerBh: toNumber(pick(row, ["costPerBh", "costBh"])),
  costPerFh: toNumber(pick(row, ["costPerFh", "costFh"])),
  costPerDeparture: toNumber(pick(row, ["costPerDeparture", "costDep"])),
  costPerMonth: toNumber(pick(row, ["costPerMonth", "costMonth"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).map((row) => ({
  ...row,
  costPerBh: row.costPerBh || (row.per === "BH" ? row.cost : 0),
  costPerFh: row.costPerFh || (row.per === "FH" ? row.cost : 0),
  costPerDeparture: row.costPerDeparture || (["", "DEPARTURES"].includes(row.per) ? row.cost : 0),
  costPerMonth: row.costPerMonth || (row.per === "MONTH" ? row.cost : 0),
})).filter((row) => row.depStn || row.variant || row.acftRegn || row.pn || row.sn || row.cost);

const normalizeStationCost = (rows = [], stationKey) => {
  const normalized = [];
  rows.forEach((row) => {
    const base = {
      [stationKey]: normalize(pick(row, [stationKey, "stn", "station", "arrStn"])),
      mtow: pick(row, ["mtow"]) === "" ? "" : toNumber(pick(row, ["mtow"])),
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

const normalizeOtherDoc = (rows = []) => rows.map((row) => ({
  label: pick(row, ["label", "lbl"]),
  sector: normalize(pick(row, ["sector", "sec"])),
  depStn: normalize(pick(row, ["depStn", "dep"])),
  arrStn: normalize(pick(row, ["arrStn", "arr"])),
  variantOrAcftRegn: normalize(pick(row, ["variantOrAcftRegn", "variantAcftRegn", "variant", "acftRegn", "acftReg"])),
  per: normalizeMetric(pick(row, ["per"])),
  cost: toNumber(pick(row, ["cost"])),
  fromDate: pick(row, ["fromDate", "fdate"]),
  toDate: pick(row, ["toDate", "tdate"]),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.label || row.sector || row.depStn || row.arrStn || row.variantOrAcftRegn);

const matchesOtherDocAircraft = (row, { variant, acftRegn }) => {
  const combined = normalize(pick(row, ["variantOrAcftRegn", "variantAcftRegn"]));
  if (combined) {
    return combined === variant || combined === acftRegn;
  }

  const rowVariant = normalize(row?.variant);
  const rowAcftRegn = normalize(row?.acftRegn);
  if (rowVariant && rowAcftRegn) {
    return rowVariant === variant && rowAcftRegn === acftRegn;
  }
  if (rowVariant) return rowVariant === variant;
  if (rowAcftRegn) return rowAcftRegn === acftRegn;
  return true;
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
    variant: normalize(pick(row, ["variant", "var"])),
    month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
    fromDate: pick(row, ["fromDate"]),
    toDate: pick(row, ["toDate"]),
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
    mtow: pick(row, ["mtow"]) === "" ? "" : toNumber(pick(row, ["mtow"])),
    variant: normalize(pick(row, ["variant", "var"])),
    month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
    fromDate: pick(row, ["fromDate"]),
    toDate: pick(row, ["toDate"]),
    cost: round2(pick(row, ["cost", "value"])),
    ccy: normalize(pick(row, ["ccy", "currency"])),
    costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
    ...Object.fromEntries(
      airportTiers.map((tier) => [String(tier), tierRates[tier] !== undefined ? tierRates[tier] : ""])
    ),
    tierRates,
  };
}).filter((row) => row[stationKey] || row.ccy || row.mtow || row.cost || Object.keys(row.tierRates || {}).length > 0);

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
  regn: normalize(pick(row, ["acftRegn", "regn", "registration"])),
  sn: normalize(pick(row, ["sn", "msn"])),
  mtow: toNumber(pick(row, ["mtow"])),
  entry: pick(row, ["entryDate", "entry", "inductionDate"]),
  exit: pick(row, ["exitDate", "exit"]),
  variant: normalize(pick(row, ["variant", "type"])),
  type: normalize(pick(row, ["type"])),
  category: normalize(pick(row, ["category"])),
})).filter((row) => row.regn || row.sn || row.mtow);

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
    otherDoc: normalizeOtherDoc(config.otherDoc || []),
    rotableChanges: normalizeRotableChanges(config.rotableChanges || []),
    navMtowTiers,
    navEnr: normalizeNavigationCost(config.navEnr || [], "sector", navMtowTiers),
    navTerm: normalizeNavigationCost(config.navTerm || [], "arrStn", navMtowTiers),
    airportLanding: normalizeAirportMtowCost(config.airportLanding || [], "arrStn", navMtowTiers),
    airportAvsec: normalizeStationCost(config.airportAvsec || [], "arrStn"),
    airportDom: normalizeStationCost(config.airportDom || [], "arrStn"),
    airportIntl: normalizeStationCost(config.airportIntl || [], "arrStn"),
    airportOther: normalizeAirportMtowCost(config.airportOther || [], "arrStn", navMtowTiers),
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
    case "FT":
      return getFlightEffectiveFt(flight);
    case "MONTH":
      return 1;
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
    case "OTHERDOC":
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

const normalizeFxRates = (rates = []) => (Array.isArray(rates) ? rates : []).map((row) => {
  const from = normalize(pick(row, ["from", "ccy", "currency", "source"]));
  const to = normalize(pick(row, ["to", "rccy", "reportingCurrency", "target"]));
  const pair = normalize(pick(row, ["pair"]));
  const rawDateKey = pick(row, ["dateKey", "date", "effectiveDate", "period"]);
  const exactDate = toDayKey(rawDateKey);
  return {
    pair: pair || (from && to ? `${from}/${to}` : ""),
    dateKey: normalizeMonthKey(pick(row, ["month", "period"]) || rawDateKey),
    exactDate,
    rate: toNumber(pick(row, ["rate", "fxRate", "value"])),
  };
}).filter((row) => row.pair && row.rate > 0);

const findFxRate = (fxRates, currency, reportingCurrency, date) => {
  const pair = `${normalize(currency)}/${normalize(reportingCurrency)}`;
  const carriedRate = getCarriedForwardFxRate(fxRates, pair, normalizeFxDateKey(date));
  if (carriedRate > 0 && (Array.isArray(fxRates) ? fxRates : []).some((row) => {
    const rowPair = normalize(row?.pair);
    const rowDateKey = normalizeFxDateKey(row?.dateKey || row?.date || row?.effectiveDate);
    return rowPair === pair && rowDateKey && rowDateKey <= normalizeFxDateKey(date);
  })) {
    return carriedRate;
  }
  const dayKey = toDayKey(date);
  const monthKey = normalizeMonthKey(date);
  const rates = normalizeFxRates(fxRates)
    .filter((row) => row.pair === pair)
    .sort((a, b) => String(a.exactDate || a.dateKey).localeCompare(String(b.exactDate || b.dateKey)));
  const carriedForward = [...rates]
    .filter((row) => row.exactDate && dayKey && row.exactDate <= dayKey)
    .pop();
  return (
    rates.find((row) => row.exactDate && row.exactDate === dayKey)?.rate ||
    carriedForward?.rate ||
    rates.find((row) => row.dateKey && row.dateKey === monthKey)?.rate ||
    rates.find((row) => !row.dateKey && !row.exactDate)?.rate ||
    1
  );
};

const addMissingFxPair = (flight, currency, reportingCurrency) => {
  if (!flight || !currency || normalize(currency) === normalize(reportingCurrency)) return;
  const pair = `${normalize(currency)}/${normalize(reportingCurrency)}`;
  if (!flight.missingFxPairs) flight.missingFxPairs = [];
  if (!flight.missingFxPairs.includes(pair)) flight.missingFxPairs.push(pair);
  if (flight.costDebug?.missingLookups && !flight.costDebug.missingLookups.includes(`No FX rate for ${pair}`)) {
    flight.costDebug.missingLookups.push(`No FX rate for ${pair}`);
  }
};

const convertToRccy = (amount, currency, reportingCurrency, explicitRccy, fxRates = [], date, flight) => {
  const numeric = round2(amount);
  if (toNumber(explicitRccy) > 0) return round2(explicitRccy);
  if (!currency || normalizeFxCurrencyCode(currency) === normalizeFxCurrencyCode(reportingCurrency)) return numeric;
  const rate = findFxRate(fxRates, currency, reportingCurrency, date);
  if (rate > 0) return round2(numeric * rate);
  addMissingFxPair(flight, currency, reportingCurrency);
  return numeric;
};

const scoreSpecificity = (pairs) => pairs.reduce((sum, entry) => sum + (entry ? 1 : 0), 0);

const matchesOptional = (target, actual) => !target || target === actual;
const matchesOptionalNumber = (target, actual) => {
  if (target === undefined || target === null || target === "") return true;
  return toNumber(target) === toNumber(actual);
};

const isAdditionalApuUseRow = (row) => normalize(row?.addlnUse) === "Y";

const getFirstDayOfNextMonth = (value) => {
  const date = parseDate(value);
  if (!date) return null;
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1));
};

const getUtcMonthEnd = (date) => new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));

const getMonthKeyFromDate = (date) => `${String(date.getUTCMonth() + 1).padStart(2, "0")}/${String(date.getUTCFullYear()).slice(-2)}`;

const getApuUsageDateRange = (row = {}) => {
  const from = parseDate(row.fromDate);
  const to = parseDate(row.toDate);
  if (from && to) return from <= to ? { from, to } : { from: to, to: from };
  if (from) return { from, to: getUtcMonthEnd(from) };
  if (to) return { from: getUtcMonthStart(to), to };
  return { from: null, to: null };
};

const getOverlappedApuUsageMonths = (row = {}, flights = []) => {
  const range = getApuUsageDateRange(row);
  const fallbackDate = range.from || range.to || flights.map(getFlightDate).find(Boolean);
  if (!fallbackDate) return [];

  const start = getUtcMonthStart(range.from || fallbackDate);
  const end = getUtcMonthStart(range.to || fallbackDate);
  const months = [];
  for (
    let cursor = new Date(start);
    cursor <= end;
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1))
  ) {
    const monthStart = getUtcMonthStart(cursor);
    const monthEnd = getUtcMonthEnd(cursor);
    const overlapStart = range.from && range.from > monthStart ? range.from : monthStart;
    const overlapEnd = range.to && range.to < monthEnd ? range.to : monthEnd;
    if (overlapStart <= overlapEnd) {
      months.push({
        monthKey: getMonthKeyFromDate(cursor),
        monthStart,
        days: Math.floor((overlapEnd - overlapStart) / 86400000) + 1,
      });
    }
  }
  return months;
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
  if (!flightDate) {
    return { amount: 0, rate: 0, currency: "", reportingAmount: 0 };
  }

  const snContext = getFlightSnContext(flight, config.aircraftOnwing || []);
  const snList = snContext.snList || [];

  const reserveSettings = Array.isArray(config.leasedReserve) ? config.leasedReserve : [];
  const scheduleRows = Array.isArray(config.maintenanceReserveSchedule) ? config.maintenanceReserveSchedule : [];
  const rateDate = getFirstDayOfNextMonth(flightDate);

  const settingsMatches = reserveSettings.filter((row) => {
    if (normalizeMetric(row?.driver) === "MONTH") return false;
    if (row?.sn && !snList.includes(normalize(row?.sn))) return false;
    if (!matchesOptional(row?.acftRegn, getFlightRegistration(flight))) return false;
    if (!matchesOptional(row?.pn, getFlightPartNumber(flight))) return false;
    return isWithinRange(flightDate, row?.asOnDate, row?.endDate);
  });

  const buildScore = (row) => scoreSpecificity([row?.acftRegn, row?.pn, row?.sn, row?.mrAccId, row?.endDate, row?.driver]);
  const bestSettings = settingsMatches.sort((a, b) => buildScore(b) - buildScore(a))[0] || null;

  const candidateSnList = settingsMatches.length > 0
    ? settingsMatches.map((row) => normalize(row?.sn)).filter(Boolean)
    : snList;

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
  let rate = derivedRate > 0 ? derivedRate : fallbackRate;
  const currency = bestSchedule?.ccy || bestSettings?.ccy || "";
  const reportingAmount = toNumber(bestSchedule?.costRCCY || bestSettings?.costRCCY);

  if (!bestSchedule && rate > 0 && bestSettings?.annualEscl) {
    const anniversary = parseDate(bestSettings.anniversary || bestSettings.asOnDate);
    if (anniversary) {
      let anniversaries = flightDate.getUTCFullYear() - anniversary.getUTCFullYear();
      const anniversaryThisYear = new Date(Date.UTC(flightDate.getUTCFullYear(), anniversary.getUTCMonth(), anniversary.getUTCDate()));
      if (flightDate < anniversaryThisYear) anniversaries -= 1;
      if (anniversaries > 0) rate *= ((1 + toNumber(bestSettings.annualEscl) / 100) ** anniversaries);
    }
  }

  if (rate <= 0) {
    return { amount: 0, rate: 0, currency, reportingAmount };
  }

  const driver = normalizeMetric(bestSchedule?.driver || bestSettings?.driver);
  const driverValue = driver === "BH"
    ? toNumber(flight.bh)
    : driver === "DEPARTURES"
      ? 1
      : driver === "FT"
        ? getFlightEffectiveFt(flight)
        : toNumber(flight.fh);

  return {
    amount: round2(driverValue * rate),
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

  const rates = Object.entries(row.tierRates || {})
    .map(([tier, cost]) => ({ tier: toNumber(tier), cost: toNumber(cost) }))
    .filter((entry) => entry.tier > 0 && entry.cost !== 0)
    .sort((a, b) => a.tier - b.tier);

  if (tierValue > 0 && rates.length > 0) {
    const exact = rates.find((entry) => entry.tier === tierValue);
    const nearestHigher = rates.find((entry) => entry.tier >= tierValue);
    return round2((exact || nearestHigher || rates[rates.length - 1]).cost);
  }

  return round2(row.cost || 0);
};

const getAirportMtowTieredCost = (row, mtow) => {
  const tierValue = toNumber(mtow);
  if (!row) return null;

  if (toNumber(row.costRCCY) > 0) {
    return round2(row.costRCCY);
  }

  const rates = Object.entries(row.tierRates || {})
    .map(([tier, cost]) => ({ tier: toNumber(tier), cost: toNumber(cost) }))
    .filter((entry) => entry.tier > 0 && entry.cost !== 0)
    .sort((a, b) => a.tier - b.tier);

  if (tierValue > 0 && rates.length > 0) {
    const exact = rates.find((entry) => entry.tier === tierValue);
    const nearestHigher = rates.find((entry) => entry.tier >= tierValue);
    return round2((exact || nearestHigher || rates[rates.length - 1]).cost);
  }

  if (tierValue > 0 && toNumber(row.mtow) > 0 && toNumber(row.mtow) === tierValue) {
    return round2(row.cost || 0);
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

const applyCostField = (flight, field, amount, currency, reportingCurrency, explicitRccy, fxRates = [], date) => {
  const numeric = round2(amount);
  const effectiveFxRates = fxRates.length ? fxRates : (flight.__costFxRates || []);
  flight[field] = numeric;
  flight[`${field}CCY`] = currency || "";
  flight[`${field}RCCY`] = convertToRccy(numeric, currency, reportingCurrency, explicitRccy, effectiveFxRates, date || flight?.date, flight);
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
  next.apuFuelConsumptionKg = 0;
  next.apuFuelKg = 0;
  next.apuFuelLitres = 0;
  next.apuFuelCostDirect = 0;
  next.apuFuelCostAllocated = 0;
  next.engineFuelKg = 0;
  next.engineFuelConsumptionKg = 0;
  next.engineFuelLitres = 0;
  next.ftEffective = getFlightEffectiveFt(next);
  next.mtowUsed = 0;
  next.otherMaintenance1 = 0;
  next.otherMaintenance2 = 0;
  next.otherMaintenance3 = 0;
  next.navEnr = 0;
  next.navTrml = 0;
  next.aptLandingCost = 0;
  next.aptHandlingCost = 0;
  next.aptAvsecCost = 0;
  next.aptOtherCost = 0;
  next.otherDoc1 = 0;
  next.otherDoc2 = 0;
  next.otherDoc3 = 0;
  next.crewOverlay = 0;
  next.crewPositioning = 0;
  next.reportingCurrency = reportingCurrency;

  return next;
};

const addAllocation = (flight, field, amount, currency, reportingCurrency, explicitRccy, fxRates = [], date) => {
  const effectiveFxRates = fxRates.length ? fxRates : (flight.__costFxRates || []);
  const numeric = round2((flight[field] || 0) + amount);
  flight[field] = numeric;
  flight[`${field}CCY`] = currency || flight[`${field}CCY`] || "";
  flight[`${field}RCCY`] = round2((flight[`${field}RCCY`] || 0) + convertToRccy(amount, currency, reportingCurrency, explicitRccy, effectiveFxRates, date || flight?.date, flight));
};

const distributePool = (eligibleFlights, field, totalAmount, currency, reportingCurrency, basis, explicitRccy, fxRates = [], date) => {
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
    addAllocation(flight, field, share, currency, reportingCurrency, explicitRccy, fxRates, date);
  });
};

const distributeMonthlyPoolByBasis = (eligibleFlights, field, totalAmount, currency, reportingCurrency, basis, explicitRccy, fxRates = [], amountForMonth = null) => {
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
    const sampleFlight = flightsInGroup[0];
    const monthAmount = round2(typeof amountForMonth === "function"
      ? amountForMonth(getFlightDate(sampleFlight), amount)
      : amount);
    if (monthAmount === 0) return;

    const weights = flightsInGroup.map((flight) => Math.max(getBasisValue(flight, basis), 0));
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    const safeWeights = totalWeight > 0 ? weights : flightsInGroup.map(() => 1);
    const safeTotal = safeWeights.reduce((sum, value) => sum + value, 0);

    let allocated = 0;
    flightsInGroup.forEach((flight, index) => {
      const share = index === flightsInGroup.length - 1
        ? round2(monthAmount - allocated)
        : round2((monthAmount * safeWeights[index]) / safeTotal);
      allocated = round2(allocated + share);
      addAllocation(flight, field, share, currency, reportingCurrency, explicitRccy, fxRates);
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

const selectTransitRule = (rows = [], { flightDate, depStn, variant, acftReg, pn, msn }) => {
  const candidates = [];

  rows.forEach((row, index) => {
    if (!row) return;
    if (!matchesOptional(row.depStn, depStn)) return;
    if (!matchesOptional(row.pn, pn)) return;
    if (!matchesOptional(row.sn, msn)) return;
    if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return;

    if (!matchesOptional(row.acftRegn, acftReg)) return;
    if (!matchesOptional(row.variant, variant)) return;

    candidates.push({
      row,
      identifierRank: getTransitIdentifierRank(row),
      effectiveFrom: parseDate(row.fromDate)?.getTime() || 0,
      effectiveTo: parseDate(row.toDate)?.getTime() || 0,
      specificity: scoreSpecificity([row.depStn, row.sn, row.acftRegn, row.pn, row.variant, row.fromDate || row.toDate]),
      index,
    });
  });

  if (candidates.length === 0) return null;

  candidates.sort((a, b) => (
    b.identifierRank - a.identifierRank ||
    b.effectiveFrom - a.effectiveFrom ||
    b.effectiveTo - a.effectiveTo ||
    b.specificity - a.specificity ||
    b.index - a.index
  ));

  return candidates[0].row;
};

const addDebugMissing = (flight, message) => {
  if (!flight?.costDebug) return;
  if (!flight.costDebug.missingLookups.includes(message)) {
    flight.costDebug.missingLookups.push(message);
  }
};

const scoreApuUsageRule = (row, flight) => {
  if (!row || isAdditionalApuUseRow(row)) return -1;
  const flightDate = getFlightDate(flight);
  const rowStation = row.stn || row.arrStn;
  if (!matchesOptional(rowStation, getFlightArr(flight))) return -1;
  if (!matchesOptional(row.variant, getFlightVariant(flight))) return -1;
  if (!matchesOptional(row.acftRegn, getFlightRegistration(flight))) return -1;
  if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
  return scoreSpecificity([rowStation, row.acftRegn, row.variant, row.fromDate || row.toDate]) * 10;
};

const findFuelPriceForStations = (config, stations = [], monthKey) => {
  for (const station of stations.map(normalize).filter(Boolean)) {
    const match = pickBest(config.ccyFuel, (row) => {
      if (!matchesOptional(row.station, station)) return -1;
      if (row.month && row.month !== monthKey) return -1;
      return scoreSpecificity([row.station, row.month]);
    });
    if (match) return match;
  }
  return null;
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
    const ftEffective = getFlightEffectiveFt(flight);
    const mtow = getFlightMtow(flight, config.fleet);
    const departures = 1;
    flight.ftEffective = ftEffective;
    flight.mtowUsed = mtow;

    const fuelRule = pickBest(config.fuelConsum, (row) => {
      const rowSector = normalize(row.sectorOrGcd);
      const sectorMatch = rowSector && rowSector === sector;
      const gcdMatch = rowSector && rowSector === normalize(flight.dist);
      if (!sectorMatch && !gcdMatch) return -1;
      if (row.acftRegn && row.acftRegn !== acftReg) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      return (sectorMatch ? 1000 : 500) + scoreSpecificity([row.acftRegn, row.month]) * 10;
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

    const fuelPriceRule = findFuelPriceForStations(config, [depStn, arrStn], flightMonthKey);

    const plfFactor = selectPlfFactor(plfRule, getFlightLoadFactor(flight));
    const fuelIndexFactor = toNumber(fuelIndexRule?.fuelConsumptionIndex) || 1;
    const baseFuelConsumption = round2((fuelRule?.fuelConsumptionKg || 0) * fuelIndexFactor * plfFactor);
    const engineFuelCost = fuelPriceRule
      ? calculateFuelCost(baseFuelConsumption, fuelPriceRule)
      : baseFuelConsumption;

    flight.engineFuelConsumption = baseFuelConsumption;
    flight.engineFuel = baseFuelConsumption;
    flight.engineFuelConsumptionKg = baseFuelConsumption;
    flight.engineFuelKg = baseFuelConsumption;
    flight.engineFuelLitres = fuelPriceRule?.kgPerLtr ? round2(baseFuelConsumption / toNumber(fuelPriceRule.kgPerLtr)) : 0;
    if (flight.costDebug) {
      flight.costDebug.fuel = {
        baseFuelRow: fuelRule || null,
        fuelIndexRow: fuelIndexRule || null,
        plfEffectRow: plfRule || null,
        fuelPriceRow: fuelPriceRule || null,
        baseFuelKg: round2(fuelRule?.fuelConsumptionKg || 0),
        fuelIndex: fuelIndexFactor,
        plfEffect: plfFactor,
        engineFuelKg: baseFuelConsumption,
      };
      if (!fuelRule) addDebugMissing(flight, `No fuel consumption row for ${sector} / ${acftReg} / ${flightMonthKey}`);
      if (baseFuelConsumption > 0 && !fuelPriceRule) addDebugMissing(flight, `No fuel price row for ${depStn} / ${flightMonthKey}`);
    }
    applyCostField(
      flight,
      "engineFuelCost",
      engineFuelCost,
      fuelPriceRule?.ccy || "",
      config.reportingCurrency,
      fuelPriceRule?.costRCCY || 0
    );

    const apuRule = pickBest(config.apuUsage || [], (row) => scoreApuUsageRule(row, flight));
    if (apuRule) {
      const apuStation = apuRule.stn || apuRule.arrStn;
      const apuPriceRule = findFuelPriceForStations(config, [apuStation], flightMonthKey);
      if (flight.costDebug) {
        flight.costDebug.apu = {
          matchedApuRow: apuRule,
          fuelPriceRow: apuPriceRule || null,
          apuFuelKg: 0,
          apuFuelLitres: 0,
          directCost: 0,
          allocatedCost: flight.apuFuelCostAllocated || 0,
        };
        if (!apuPriceRule) addDebugMissing(flight, `No APU fuel price row for ${apuStation} / ${flightMonthKey}`);
      }
    } else if (flight.costDebug) {
      flight.costDebug.apu = { matchedApuRow: null, fuelPriceRow: null, apuFuelKg: 0, apuFuelLitres: 0, directCost: 0, allocatedCost: 0 };
      addDebugMissing(flight, `No APU usage row for ${arrStn} / ${acftReg}`);
    }

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
      pn,
      msn,
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
      (row.costPerBh || row.costPerFh || row.costPerDeparture || row.costPerMonth)
    ));
    if (matchingOtherMxRows.length > 0) {
      const perBhTotal = matchingOtherMxRows.reduce(
        (sum, row) => sum + ((row.costPerBh || 0) * bh),
        0
      );
      const perFhTotal = matchingOtherMxRows.reduce(
        (sum, row) => sum + ((row.costPerFh || 0) * fh),
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
      flight.otherMaintenance1 = round2(perBhTotal + perFhTotal);
      flight.otherMaintenance2 = round2(perDepTotal);
      flight.otherMaintenance3 = round2(perMonthTotal);
      applyCostField(
        flight,
        "otherMaintenance",
        perBhTotal + perFhTotal + perDepTotal,
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
    const landingCost = getAirportMtowTieredCost(landingRule, mtow);
    const otherAirportCost = getAirportMtowTieredCost(otherAirportRule, mtow);
    flight.aptLandingCost = round2(landingCost ?? 0);
    flight.aptHandlingCost = round2(handlingRule?.cost || 0);
    flight.aptAvsecCost = round2(avsecRule?.cost || 0);
    flight.aptOtherCost = round2(otherAirportCost ?? 0);
    applyCostField(
      flight,
      "airport",
      flight.aptLandingCost + flight.aptHandlingCost + flight.aptAvsecCost + flight.aptOtherCost,
      landingRule?.ccy || handlingRule?.ccy || otherAirportRule?.ccy || avsecRule?.ccy || "",
      config.reportingCurrency,
      landingRule?.costRCCY || handlingRule?.costRCCY || otherAirportRule?.costRCCY || avsecRule?.costRCCY
    );
    if (flight.costDebug) {
      flight.costDebug.navigation = { enrRule: enrRule || null, termRule: termRule || null, mtowUsed: mtow, navEnr: flight.navEnr, navTrml: flight.navTrml };
      flight.costDebug.airport = { landingRule: landingRule || null, handlingRule: handlingRule || null, avsecRule: avsecRule || null, otherAirportRule: otherAirportRule || null, mtowUsed: mtow };
      if (!mtow) addDebugMissing(flight, `No MTOW found for ${acftReg || msn || variant} on ${flight.date}`);
      if (!enrRule) addDebugMissing(flight, `No nav ENR row for sector ${sector}`);
      if (!termRule) addDebugMissing(flight, `No nav terminal row for ${arrStn}`);
      if (!landingRule) addDebugMissing(flight, `No airport landing row for ${arrStn} / MTOW ${mtow}`);
    }

    const matchingOtherDocs = (config.otherDoc || []).filter((row) => {
      if (!matchesOptional(row.sector, sector)) return false;
      if (!matchesOptional(row.depStn, depStn)) return false;
      if (!matchesOptional(row.arrStn, arrStn)) return false;
      if (!matchesOtherDocAircraft(row, { variant, acftRegn: acftReg })) return false;
      return isWithinRange(flightDate, row.fromDate, row.toDate);
    }).sort((a, b) => {
      const scoreA = scoreSpecificity([a.sector, a.depStn, a.arrStn, a.variantOrAcftRegn || a.variant || a.acftRegn, a.fromDate || a.toDate]);
      const scoreB = scoreSpecificity([b.sector, b.depStn, b.arrStn, b.variantOrAcftRegn || b.variant || b.acftRegn, b.fromDate || b.toDate]);
      return scoreB - scoreA;
    });

    const calcOtherDoc = (row) => {
      if (!row || row.per === "MONTH") return 0;
      if (row.per === "BH") return row.cost * bh;
      if (row.per === "FH") return row.cost * fh;
      if (row.per === "FT") return row.cost * ftEffective;
      return row.cost * departures;
    };
    const directOtherDocRows = matchingOtherDocs.filter((row) => row.per !== "MONTH");
    const directOtherDocAmounts = directOtherDocRows.map(calcOtherDoc);
    flight.otherDoc1 = round2(directOtherDocAmounts[0] || 0);
    flight.otherDoc2 = round2(directOtherDocAmounts[1] || 0);
    flight.otherDoc3 = round2(directOtherDocAmounts[2] || 0);
    applyCostField(
      flight,
      "otherDoc",
      directOtherDocAmounts.reduce((sum, value) => sum + value, 0),
      directOtherDocRows[0]?.ccy || "",
      config.reportingCurrency,
      directOtherDocRows[0]?.costRCCY
    );
    if (flight.costDebug) {
      flight.costDebug.otherDoc = { matchingRows: directOtherDocRows, amounts: directOtherDocAmounts.map(round2) };
    }

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
    const apuStation = row.stn || row.arrStn;
    const apuHrPerDay = toNumber(row.apuHrPerDay || row.apuHours);
    const kgPerApuHr = toNumber(row.kgPerApuHr || row.consumptionPerApuHour);
    if (!apuStation || apuHrPerDay <= 0 || kgPerApuHr <= 0) return;

    getOverlappedApuUsageMonths(row, flights).forEach(({ monthKey, monthStart, days }) => {
      const priceRule = findFuelPriceForStations(config, [apuStation], monthKey);
      const totalKg = round2(apuHrPerDay * days * kgPerApuHr);
      const poolAmount = priceRule ? calculateFuelCost(totalKg, priceRule) : 0;
      const basis = row.basis || getAllocationBasis(config, "APUFUELCOST");

      const candidateFlights = flights.filter((flight) => {
        if (getFlightMonthKey(flight) !== monthKey) return false;
        if (!matchesOptional(row.variant, getFlightVariant(flight))) return false;
        if (row.acftRegn) return row.acftRegn === getFlightRegistration(flight);
        if (!isAdditionalApuUseRow(row) && apuStation !== getFlightArr(flight)) return false;
        return true;
      });

      const aircraftGroups = new Map();
      candidateFlights.forEach((flight) => {
        const aircraftKey = getFlightRegistration(flight);
        if (!aircraftKey) return;
        if (!aircraftGroups.has(aircraftKey)) aircraftGroups.set(aircraftKey, []);
        aircraftGroups.get(aircraftKey).push(flight);
      });

      aircraftGroups.forEach((monthFlights) => {
        if (!monthFlights.length) return;
        const weights = monthFlights.map((flight) => Math.max(getBasisValue(flight, basis), 0));
        const totalWeight = weights.reduce((sum, value) => sum + value, 0);
        if (totalWeight <= 0) {
          monthFlights.forEach((flight) => addDebugMissing(flight, `No positive APU allocation driver for ${getFlightRegistration(flight)} / ${monthKey}`));
          return;
        }

        const beforeValues = new Map(monthFlights.map((flight) => [flight, flight.apuFuelCostAllocated || 0]));
        let allocated = 0;
        monthFlights.forEach((flight, index) => {
          const share = index === monthFlights.length - 1
            ? round2(poolAmount - allocated)
            : round2((poolAmount * weights[index]) / totalWeight);
          allocated = round2(allocated + share);
          if (share > 0) {
            addAllocation(flight, "apuFuelCost", share, priceRule?.ccy || row.ccy, config.reportingCurrency, 0, config.fxRates || [], monthStart);
          }
        });

        monthFlights.forEach((flight) => {
          flight.apuFuelCostAllocated = round2((flight.apuFuelCost || 0) - (flight.apuFuelCostDirect || 0));
          const allocatedCost = round2((flight.apuFuelCostAllocated || 0) - (beforeValues.get(flight) || 0));
          const kgShare = poolAmount > 0 && allocatedCost > 0 ? round2((totalKg * allocatedCost) / poolAmount) : 0;
          if (kgShare > 0) {
            flight.apuFuelConsumptionKg = round2((flight.apuFuelConsumptionKg || 0) + kgShare);
            flight.apuFuelKg = round2((flight.apuFuelKg || 0) + kgShare);
            flight.apuFuelLitres = round2((flight.apuFuelLitres || 0) + (priceRule?.kgPerLtr ? kgShare / toNumber(priceRule.kgPerLtr) : 0));
          }
          if (flight.costDebug?.apu) {
            flight.costDebug.apu.matchedApuRow = row;
            flight.costDebug.apu.fuelPriceRow = priceRule || null;
            flight.costDebug.apu.apuFuelKg = round2((flight.costDebug.apu.apuFuelKg || 0) + kgShare);
            flight.costDebug.apu.apuFuelLitres = round2((flight.costDebug.apu.apuFuelLitres || 0) + (priceRule?.kgPerLtr && kgShare > 0 ? kgShare / toNumber(priceRule.kgPerLtr) : 0));
            flight.costDebug.apu.allocatedCost = flight.apuFuelCostAllocated;
            if (!priceRule) addDebugMissing(flight, `No APU fuel price row for ${apuStation} / ${monthKey}`);
          }
        });
      });
    });
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

      const groupMonthDate = parseDate(row.date) || getFlightDate(matchedFlights[0]);
      const groupMonthKey = groupMonthDate ? getFlightMonthKey({ date: groupMonthDate }) : String(rowMonthNumber);
      const groupKey = `${getFlightAircraftKey(matchedFlights[0])}|${groupMonthKey}`;
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
      group.amount = round2(group.amount + (toNumber(row.contribution) * getExactExampleMonthlyFactor(groupMonthDate, row.fromDate, row.toDate)));
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
        getAllocationBasis(config, "MRMONTHLY"),
        group.reportingAmount
      );
    });
  } else {
    config.leasedReserve.forEach((row) => {
      if (normalizeMetric(row.driver) !== "MONTH") return;
      const eligibleFlights = flights.filter((flight) => {
        const flightDate = getFlightDate(flight);
        if (!isWithinRange(flightDate, row.asOnDate, row.endDate)) return false;
        return matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
          matchesOptional(row.sn, getFlightMsn(flight));
      });
      distributeMonthlyPoolByBasis(
        eligibleFlights,
        "mrMonthly",
        row.setRate,
        row.ccy,
        config.reportingCurrency,
        row.basis || getAllocationBasis(config, "MRMONTHLY"),
        row.costRCCY,
        [],
        (monthDate, amount) => round2(amount * getExactExampleMonthlyFactor(monthDate, row.asOnDate, row.endDate))
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
      const previousDate = parseDate(sorted[index - 1]?.date);
      const eventDate = parseDate(event.date);
      const startDate = previousDate || eventDate || masterStart;
      const endDate = eventDate || masterEnd;
      const eligibleFlights = flights.filter((flight) => {
        const flightDate = getFlightDate(flight);
        if (!flightDate || !startDate || !endDate) return false;
        if (flightDate < startDate || flightDate > endDate) return false;
        if (!matchesOptional(event.pn, getFlightPartNumber(flight))) return false;
        return matchesOptional(event.msnEsnApun, getFlightMsn(flight)) ||
          matchesOptional(event.msnEsnApun, getFlightRegistration(flight)) ||
          (flight.snList || []).includes(normalize(event.msnEsnApun));
      });
      const qualifyingAmount = toNumber(event.remaining) > 0
        ? toNumber(event.remaining)
        : Math.max(toNumber(event.cost) - toNumber(event.mrDrawdown), 0) || toNumber(event.cost);
      distributePool(
        eligibleFlights,
        "qualifyingSchMxEvents",
        qualifyingAmount,
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
      row.costRCCY,
      [],
      (monthDate, amount) => round2(amount * getExactExampleMonthlyFactor(monthDate, row.fromDate, row.toDate))
    );
  });

  config.otherDoc.forEach((row) => {
    if (row.per !== "MONTH") return;
    const eligibleFlights = flights.filter((flight) => {
      const flightDate = getFlightDate(flight);
      if (!matchesOptional(row.sector, getFlightSector(flight))) return false;
      if (!matchesOptional(row.depStn, getFlightDep(flight))) return false;
      if (!matchesOptional(row.arrStn, getFlightArr(flight))) return false;
      if (!matchesOtherDocAircraft(row, { variant: getFlightVariant(flight), acftRegn: getFlightRegistration(flight) })) return false;
      return isWithinRange(flightDate, row.fromDate, row.toDate);
    });
    distributeMonthlyPoolByBasis(
      eligibleFlights,
      "otherDoc",
      row.cost,
      row.ccy,
      config.reportingCurrency,
      getAllocationBasis(config, "OTHERDOC"),
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
  flight.mrContributionCCY = flight.maintenanceReserveContributionCCY;
  flight.mrContributionRCCY = flight.maintenanceReserveContributionRCCY;
  flight.transitMx = flight.transitMaintenance;
  flight.otherMx = flight.otherMaintenance;
  flight.majorSchMx = flight.qualifyingSchMxEvents;
  flight.apuFuel = flight.apuFuelCost;
  flight.crewPositioning = flight.crewPositioningCost;
};

const applyTotals = (flight, reportingCurrency) => {
  const total = COST_FIELDS
    .filter((field) => field !== "totalCost")
    .reduce((sum, field) => sum + toNumber(flight[field]), 0);
  const totalRccy = COST_FIELDS
    .filter((field) => field !== "totalCost")
    .reduce((sum, field) => sum + toNumber(flight[`${field}RCCY`]), 0);
  flight.totalCost = round2(total);
  flight.totalCostCCY = reportingCurrency || "";
  flight.totalCostRCCY = round2(totalRccy);
};

const computeFlightCostsBatch = (inputFlights = [], rawConfig = {}) => {
  const config = rawConfig?.__normalized ? rawConfig : normalizeCostConfig(rawConfig);

  const debugCosts = Boolean(rawConfig?.debugCosts || rawConfig?.debug);
  const flights = (inputFlights || []).map((flight) => {
    const next = initializeFlight(flight, config.reportingCurrency);
    next.__costFxRates = config.fxRates || [];
    if (debugCosts) {
      next.costDebug = {
        fuel: {},
        apu: {},
        maintenance: {},
        navigation: {},
        airport: {},
        otherDoc: {},
        missingLookups: [],
      };
    }
    return next;
  });

  flights.forEach((flight) => applySnContext(flight, config));
  enrichDirectCosts(flights, config);
  enrichAllocatedCosts(flights, config);
  flights.forEach((flight) => {
    applyCompatibilityAliases(flight);
    applyTotals(flight, config.reportingCurrency);
    delete flight.__costFxRates;
  });

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
  normalizeOtherDoc,
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
