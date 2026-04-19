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

const getFlightDate = (flight) => parseDate(flight?.date);
const getFlightMonthKey = (flight) => normalizeMonthKey(flight?.date);
const getFlightRegistration = (flight) => normalize(flight?.aircraft?.registration || flight?.acftRegn || flight?.registration);
const getFlightMsn = (flight) => normalize(flight?.aircraft?.msn ?? flight?.msn);
const getFlightVariant = (flight) => normalize(flight?.variant);
const getFlightSector = (flight) => normalize(flight?.sector);
const getFlightDep = (flight) => normalize(flight?.depStn);
const getFlightArr = (flight) => normalize(flight?.arrStn);
const getFlightDomIntl = (flight) => normalize(flight?.domIntl);

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

const normalizeFuelConsum = (rows = []) => {
  const normalized = [];
  rows.forEach((row) => {
    const base = {
      sectorOrGcd: normalize(pick(row, ["sectorOrGcd", "sector", "type", "gcd", "fuelBasis"])),
      acftRegn: normalize(pick(row, ["acftRegn", "acftReg", "acft", "regn"])),
      ccy: normalize(pick(row, ["ccy"])),
      reportingAmount: toNumber(pick(row, ["reportingAmount", "costRCCY"])),
    };

    const reg1 = normalize(pick(row, ["acft1"]));
    const reg2 = normalize(pick(row, ["acft2"]));
    const monthRecords = buildLegacyMonthRecords(row, ["fuelConsumptionKg", "fuelConsumption", "consumption", "fuelKg", "value", "rate"]);

    if (monthRecords.length > 0) {
      monthRecords.forEach((record, index) => {
        normalized.push({
          ...base,
          acftRegn: base.acftRegn || (index === 0 ? reg1 : reg2),
          month: record.month,
          fuelConsumptionKg: round2(record.amount),
          fuelPrice: toNumber(pick(record, ["fuelPrice", "price"])),
        });
      });
    } else {
      normalized.push({
        ...base,
        month: normalizeMonthKey(pick(row, ["month", "mmmYy", "mmmYY", "period", "mth", "mmYY"])),
        fuelConsumptionKg: round2(pick(row, ["fuelConsumptionKg", "fuelConsumption", "consumption", "fuelKg", "value", "rate"])),
        fuelPrice: toNumber(pick(row, ["fuelPrice", "price"])),
      });
    }
  });
  return normalized;
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
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.arrStn || row.variant || row.acftRegn);

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

const normalizeLeasedReserve = (rows = []) => rows.map((row) => ({
  mrAccId: pick(row, ["mrAccId"]),
  acftRegn: normalize(pick(row, ["acftRegn", "acftReg"])),
  pn: normalize(pick(row, ["pn"])),
  sn: normalize(pick(row, ["sn", "esn", "apun", "msn"])),
  driver: normalizeMetric(pick(row, ["driver"])),
  setRate: toNumber(pick(row, ["setRate", "rate", "contribution"])),
  setBalance: toNumber(pick(row, ["setBalance"])),
  endDate: pick(row, ["endDate", "toDate"]),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  basis: normalizeMetric(pick(row, ["basis"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.acftRegn || row.pn || row.sn || row.driver);

const normalizeSchMxEvents = (rows = []) => rows.map((row) => ({
  date: pick(row, ["date"]),
  event: normalize(pick(row, ["event", "schMxEvent", "schEvent", "label"])),
  msnEsnApun: normalize(pick(row, ["msnEsnApun", "msn", "msnEsn", "acftRegn"])),
  pn: normalize(pick(row, ["pn"])),
  snBn: normalize(pick(row, ["snBn", "sn", "bn"])),
  cost: toNumber(pick(row, ["cost", "eventTotalCost"])),
  capitalisation: normalize(pick(row, ["capitalisation", "capitalization", "cap"])),
  mrAccId: pick(row, ["mrAccId"]),
  drawdownDate: pick(row, ["drawdownDate", "mrDrawdownDate"]),
  openingBal: toNumber(pick(row, ["openingBal", "openBal"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.date || row.event || row.msnEsnApun);

const normalizeTransitMx = (rows = []) => rows.map((row) => ({
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
  label: pick(row, ["label", "lbl"]),
  date: pick(row, ["date"]),
  month: normalizeMonthKey(pick(row, ["month"] || [])),
  pn: normalize(pick(row, ["pn"])),
  msn: normalize(pick(row, ["msn"])),
  acftRegn: normalize(pick(row, ["acftRegn", "acft"])),
  cost: toNumber(pick(row, ["cost"])),
  ccy: normalize(pick(row, ["ccy", "currency"])),
  costRCCY: toNumber(pick(row, ["costRCCY", "reportingAmount"])),
})).filter((row) => row.acftRegn || row.date || row.month);

const normalizeAllocationTable = (rows = []) => rows.map((row) => ({
  costCode: normalize(pick(row, ["costCode", "cost", "label"])),
  basis: normalizeMetric(pick(row, ["basis", "basisOfAllocation"])),
  scope: normalize(pick(row, ["scope"])),
})).filter((row) => row.costCode || row.basis);

const normalizeCostConfig = (config = {}) => ({
  __normalized: true,
  reportingCurrency: normalize(pick(config, ["reportingCurrency"])) || "USD",
  fxRates: Array.isArray(config.fxRates) ? config.fxRates : [],
  allocationTable: normalizeAllocationTable(config.allocationTable || config.costAllocation || []),
  fuelConsum: normalizeFuelConsum(config.fuelConsum || []),
  apuUsage: normalizeApuUsage(config.apuUsage || []),
  plfEffect: normalizePlfEffect(config.plfEffect || []),
  ccyFuel: normalizeFuelPrice(config.ccyFuel || []),
  leasedReserve: normalizeLeasedReserve(config.leasedReserve || []),
  schMxEvents: normalizeSchMxEvents(config.schMxEvents || []),
  transitMx: normalizeTransitMx(config.transitMx || []),
  otherMx: normalizeOtherMx(config.otherMx || []),
  rotableChanges: normalizeRotableChanges(config.rotableChanges || []),
  navEnr: normalizeStationCost(config.navEnr || [], "sector"),
  navTerm: normalizeStationCost(config.navTerm || [], "arrStn"),
  airportLanding: normalizeStationCost(config.airportLanding || [], "arrStn"),
  airportAvsec: normalizeStationCost(config.airportAvsec || [], "arrStn"),
  airportDom: normalizeStationCost(config.airportDom || [], "arrStn"),
  airportIntl: normalizeStationCost(config.airportIntl || [], "arrStn"),
  otherDoc: normalizeOtherDoc(config.otherDoc || []),
});

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
  const match = (config.allocationTable || []).find((row) => row.costCode === normalize(costCode));
  return match?.basis || getDefaultAllocationBasis(normalize(costCode));
};

const getPricePerKg = (row) => {
  const rate = toNumber(row?.intoPlaneRate);
  const kgPerLtr = toNumber(row?.kgPerLtr);
  if (rate <= 0) return 0;
  if (kgPerLtr > 0) return rate / (kgPerLtr * 1000);
  return rate;
};

const convertToRccy = (amount, currency, reportingCurrency, explicitRccy) => {
  const numeric = round2(amount);
  if (toNumber(explicitRccy) > 0) return round2(explicitRccy);
  if (!currency || normalize(currency) === normalize(reportingCurrency)) return numeric;
  return numeric;
};

const scoreSpecificity = (pairs) => pairs.reduce((sum, entry) => sum + (entry ? 1 : 0), 0);

const matchesOptional = (target, actual) => !target || target === actual;

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

const selectPlfFactor = (rule, paxLf) => {
  if (!rule?.thresholds?.length) return 1;
  const pct = toNumber(paxLf);
  let factor = 1;
  rule.thresholds.forEach((entry) => {
    if (pct >= entry.threshold) factor = entry.factor || factor;
  });
  return factor || 1;
};

const enrichDirectCosts = (flights, config) => {
  flights.forEach((flight) => {
    const flightDate = getFlightDate(flight);
    const flightMonthKey = getFlightMonthKey(flight);
    const acftReg = getFlightRegistration(flight);
    const msn = getFlightMsn(flight);
    const sector = getFlightSector(flight);
    const depStn = getFlightDep(flight);
    const arrStn = getFlightArr(flight);
    const variant = getFlightVariant(flight);
    const domIntl = getFlightDomIntl(flight);
    const bh = toNumber(flight.bh);
    const fh = toNumber(flight.fh);
    const departures = 1;

    const fuelRule = pickBest(config.fuelConsum, (row) => {
      if (!matchesOptional(row.sectorOrGcd, sector) && !matchesOptional(row.sectorOrGcd, normalize(flight.dist))) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      return scoreSpecificity([row.sectorOrGcd, row.acftRegn, row.month]) * 10;
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

    const plfFactor = selectPlfFactor(plfRule, flight.paxLF || flight.plf);
    const baseFuelConsumption = round2((fuelRule?.fuelConsumptionKg || 0) * plfFactor);
    const pricePerKg = getPricePerKg(fuelPriceRule) || toNumber(fuelRule?.fuelPrice);
    const engineFuelCost = round2(pricePerKg > 0 ? baseFuelConsumption * pricePerKg : baseFuelConsumption);

    flight.engineFuelConsumption = baseFuelConsumption;
    flight.engineFuel = baseFuelConsumption;
    applyCostField(
      flight,
      "engineFuelCost",
      engineFuelCost,
      fuelPriceRule?.ccy || fuelRule?.ccy || "",
      config.reportingCurrency,
      fuelPriceRule?.costRCCY || fuelRule?.reportingAmount
    );

    const mrDirectRule = pickBest(config.leasedReserve, (row) => {
      if (!["BH", "FH", "DEPARTURES"].includes(row.driver)) return -1;
      if (row.endDate && !isWithinRange(flightDate, null, row.endDate)) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (!matchesOptional(row.sn, msn) && !matchesOptional(row.sn, acftReg)) return -1;
      return scoreSpecificity([row.acftRegn, row.sn, row.pn, row.endDate]) * 10;
    });
    if (mrDirectRule) {
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

    const transitRule = pickBest(config.transitMx, (row) => {
      if (!matchesOptional(row.depStn, depStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (!matchesOptional(row.sn, msn)) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.depStn, row.variant, row.acftRegn, row.pn, row.sn, row.fromDate || row.toDate]) * 10;
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

    const otherMxRule = pickBest(config.otherMx, (row) => {
      if (!matchesOptional(row.depStn, depStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (!matchesOptional(row.acftRegn, acftReg)) return -1;
      if (!matchesOptional(row.sn, msn)) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      if (!row.costPerBh && !row.costPerDeparture) return -1;
      return scoreSpecificity([row.depStn, row.variant, row.acftRegn, row.pn, row.sn, row.fromDate || row.toDate]) * 10;
    });
    if (otherMxRule) {
      const perBh = otherMxRule.costPerBh * bh;
      const perDep = otherMxRule.costPerDeparture * departures;
      flight.otherMaintenance1 = round2(perBh);
      flight.otherMaintenance2 = round2(perDep);
      applyCostField(
        flight,
        "otherMaintenance",
        perBh + perDep,
        otherMxRule.ccy,
        config.reportingCurrency,
        otherMxRule.costRCCY
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
    flight.navEnr = round2(enrRule?.cost || 0);
    flight.navTrml = round2(termRule?.cost || 0);
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
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const avsecRule = pickBest(config.airportAvsec, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    const handlingSource = domIntl === "INTL" ? config.airportIntl : config.airportDom;
    const handlingRule = pickBest(handlingSource, (row) => {
      if (!matchesOptional(row.arrStn, arrStn)) return -1;
      if (!matchesOptional(row.variant, variant)) return -1;
      if (row.month && row.month !== flightMonthKey) return -1;
      if (!isWithinRange(flightDate, row.fromDate, row.toDate)) return -1;
      return scoreSpecificity([row.arrStn, row.variant, row.month, row.fromDate || row.toDate]) * 10;
    });
    flight.aptLandingCost = round2(landingRule?.cost || 0);
    flight.aptOtherCost = round2(avsecRule?.cost || 0);
    flight.aptHandlingCost = round2(handlingRule?.cost || 0);
    applyCostField(
      flight,
      "airport",
      flight.aptLandingCost + flight.aptHandlingCost + flight.aptOtherCost,
      landingRule?.ccy || handlingRule?.ccy || avsecRule?.ccy || "",
      config.reportingCurrency,
      landingRule?.costRCCY || handlingRule?.costRCCY || avsecRule?.costRCCY
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
    const monthFlights = flights.filter((flight) => {
      const flightDate = getFlightDate(flight);
      return getFlightArr(flight) === row.arrStn &&
        matchesOptional(row.variant, getFlightVariant(flight)) &&
        matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
        isWithinRange(flightDate, row.fromDate, row.toDate);
    });

    if (!monthFlights.length) return;

    const priceRule = pickBest(config.ccyFuel, (price) => {
      if (!matchesOptional(price.station, monthFlights[0] ? getFlightDep(monthFlights[0]) : "")) return -1;
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

  config.leasedReserve.forEach((row) => {
    if (row.driver !== "MONTH") return;
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
      return matchesOptional(row.depStn, getFlightDep(flight)) &&
        matchesOptional(row.variant, getFlightVariant(flight)) &&
        matchesOptional(row.acftRegn, getFlightRegistration(flight)) &&
        matchesOptional(row.sn, getFlightMsn(flight)) &&
        isWithinRange(flightDate, row.fromDate, row.toDate);
    });
    distributePool(
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

  enrichDirectCosts(flights, config);
  enrichAllocatedCosts(flights, config);
  flights.forEach(applyCompatibilityAliases);

  return flights;
};

const computeFlightCosts = (flight, rawConfig = {}) => computeFlightCostsBatch([flight], rawConfig)[0];

module.exports = {
  normalizeCostConfig,
  computeFlightCostsBatch,
  computeFlightCosts,
};
