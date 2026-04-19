const moment = require("moment");

const Connections = require("../model/connectionSchema");
const Data = require("../model/dataSchema");
const Flight = require("../model/flight");
const PooTable = require("../model/pooTable");
const RevenueConfig = require("../model/revenueConfigSchema");
const Sector = require("../model/sectorSchema");
const Station = require("../model/stationSchema");

const TRAFFIC_TYPES = {
    LEG: "leg",
    BEHIND: "behind",
    BEYOND: "beyond",
    TRANSIT_FL: "transit_fl",
    TRANSIT_SL: "transit_sl",
};

const DISPLAY_LABELS = {
    [TRAFFIC_TYPES.LEG]: "Leg",
    [TRAFFIC_TYPES.BEHIND]: "Behind",
    [TRAFFIC_TYPES.BEYOND]: "Beyond",
    [TRAFFIC_TYPES.TRANSIT_FL]: "Transit FL",
    [TRAFFIC_TYPES.TRANSIT_SL]: "Transit SL",
};

const REVENUE_FIELDS = [
    "legFare",
    "legRate",
    "odFare",
    "odRate",
    "prorateRatioL1",
    "fareProrateRatioL1L2",
    "rateProrateRatioL1L2",
    "pooCcyToRccy",
];

const STRING_FIELDS = [
    "pooCcy",
    "interline",
    "codeshare",
];

const BOOLEAN_FIELDS = ["applySSPricing"];
const BLANK_OPTION_VALUE = "__BLANK__";

function normalizeStation(value) {
    return String(value || "").trim().toUpperCase();
}

function normalizeRevenueLabel(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (normalized.includes("intl")) return "Intl";
    if (normalized.includes("dom")) return "Dom";
    return "";
}

function normalizeDomIntl(value) {
    return String(value || "").trim().toLowerCase() === "intl" ? "Intl" : "Dom";
}

function normalizeCurrencyCode(value) {
    return String(value || "")
        .trim()
        .toUpperCase()
        .replace(/[^A-Z]/g, "")
        .slice(0, 3);
}

function parseNumber(value, fallback = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function roundToTwo(value) {
    return Number(parseNumber(value).toFixed(2));
}

function roundToWhole(value) {
    return Math.round(parseNumber(value));
}

function splitPaxValue(total) {
    const normalized = roundToWhole(total);
    const arrival = Math.floor(normalized / 2);
    const departure = normalized - arrival;
    return [departure, arrival];
}

function splitCargoValue(total) {
    const normalized = roundToTwo(total);
    const arrival = roundToTwo(normalized / 2);
    const departure = roundToTwo(normalized - arrival);
    return [departure, arrival];
}

function safeRatio(numerator, denominator) {
    const bottom = parseNumber(denominator);
    if (bottom <= 0) return 0;
    return roundToTwo(parseNumber(numerator) / bottom);
}

function formatDateKey(date) {
    return moment(date).format("YYYY-MM-DD");
}

function timeToMinutes(value) {
    const [hours, minutes] = String(value || "")
        .split(":")
        .map((part) => Number(part));
    if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
    return (hours * 60) + minutes;
}

function formatDuration(minutes) {
    const safeMinutes = Math.max(0, Math.round(parseNumber(minutes)));
    const hours = Math.floor(safeMinutes / 60);
    const mins = safeMinutes % 60;
    return `${String(hours).padStart(2, "0")}:${String(mins).padStart(2, "0")}`;
}

function diffMinutes(start, end) {
    const startMinutes = timeToMinutes(start);
    const endMinutes = timeToMinutes(end);
    if (startMinutes === null || endMinutes === null) return 0;
    let diff = endMinutes - startMinutes;
    if (diff < 0) diff += 24 * 60;
    return diff;
}

function calculateLayoverMinutes(firstSta, secondStd) {
    return diffMinutes(firstSta, secondStd);
}

function calculateTimeInclLayover(firstSnapshot, secondSnapshot = null) {
    if (!secondSnapshot) {
        return formatDuration(diffMinutes(firstSnapshot.std, firstSnapshot.sta));
    }

    const firstLeg = diffMinutes(firstSnapshot.std, firstSnapshot.sta);
    const layover = calculateLayoverMinutes(firstSnapshot.sta, secondSnapshot.std);
    const secondLeg = diffMinutes(secondSnapshot.std, secondSnapshot.sta);
    return formatDuration(firstLeg + layover + secondLeg);
}

function buildValidationError(issues) {
    const error = new Error(
        issues.map((issue) => issue.message).join(" | ")
    );
    error.validationIssues = issues;
    return error;
}

function normalizeGroupByField(field) {
    const normalized = String(field || "").trim();
    if (normalized === "stop") return "stops";
    return normalized;
}

function buildRevenueGroupKeyExpression(fields) {
    if (fields.length === 1) {
        return `$${fields[0]}`;
    }

    const concatParts = [];
    fields.forEach((field, index) => {
        if (index > 0) concatParts.push(" | ");
        if (field === "stops") {
            concatParts.push({
                $ifNull: [
                    { $toString: `$${field}` },
                    "Unknown",
                ],
            });
        } else {
            concatParts.push({
                $ifNull: [`$${field}`, "Unknown"],
            });
        }
    });

    return { $concat: concatParts };
}

function buildRevenueSelectionClauses(values, type) {
    const normalizedValues = values
        .map((value) => String(value || "").trim().toLowerCase())
        .filter(Boolean);

    if (normalizedValues.length === 0) return [];

    if (type === "label") {
        const clauses = [];
        if (normalizedValues.includes("domestic_od")) clauses.push({ odDI: "Dom" });
        if (normalizedValues.includes("international_od")) clauses.push({ odDI: "Intl" });
        if (normalizedValues.includes("domestic_sector")) clauses.push({ legDI: "Dom" });
        if (normalizedValues.includes("international_sector")) clauses.push({ legDI: "Intl" });
        return clauses;
    }

    if (type === "trafficClass") {
        const clauses = [];
        if (normalizedValues.includes("leg")) clauses.push({ trafficType: "leg" });
        if (normalizedValues.includes("beyond")) clauses.push({ trafficType: "beyond" });
        if (normalizedValues.includes("behind")) clauses.push({ trafficType: "behind" });
        if (normalizedValues.includes("transit")) clauses.push({ trafficType: { $in: ["transit_fl", "transit_sl"] } });
        if (normalizedValues.includes("interline")) clauses.push({ interline: { $nin: ["", null] } });
        if (normalizedValues.includes("codeshare")) clauses.push({ codeshare: { $nin: ["", null] } });
        return clauses;
    }

    return [];
}

function buildSectorMap(sectors) {
    const sectorMap = new Map();
    sectors.forEach((sector) => {
        const key = `${normalizeStation(sector.sector1)}-${normalizeStation(sector.sector2)}`;
        sectorMap.set(key, {
            gcd: parseNumber(sector.gcd),
            paxCapacity: parseNumber(sector.paxCapacity),
            cargoCapT: parseNumber(sector.CargoCapT),
            std: String(sector.std || "").trim(),
            sta: String(sector.sta || "").trim(),
        });
    });
    return sectorMap;
}

function buildStationCurrencyMap(stations) {
    const stationMap = new Map();
    stations.forEach((station) => {
        stationMap.set(normalizeStation(station.stationName), normalizeCurrencyCode(station.currencyCode));
    });
    return stationMap;
}

function normalizeRevenueConfig(config = {}) {
    const reportingCurrency = normalizeCurrencyCode(config.reportingCurrency) || "USD";
    const currencyCodes = [
        reportingCurrency,
        ...(Array.isArray(config.currencyCodes) ? config.currencyCodes : []),
    ]
        .map(normalizeCurrencyCode)
        .filter(Boolean);

    return {
        reportingCurrency,
        currencyCodes: [...new Set(currencyCodes)],
        fxRates: Array.isArray(config.fxRates)
            ? config.fxRates.map((row) => ({
                pair: String(row?.pair || "").trim().toUpperCase(),
                dateKey: String(row?.dateKey || "").trim(),
                rate: roundToTwo(row?.rate || 1),
            }))
            : [],
    };
}

function buildFxRateMap(fxRates = []) {
    const map = new Map();
    fxRates.forEach((row) => {
        if (!row.pair || !row.dateKey) return;
        map.set(`${row.pair}::${row.dateKey}`, roundToTwo(row.rate || 1));
    });
    return map;
}

function resolveCurrencyContext({ stationCurrencyMap, revenueConfig, fxRateMap, poo, date }) {
    const normalizedPoo = normalizeStation(poo);
    const reportingCurrency = revenueConfig.reportingCurrency || "USD";
    const pooCcy = stationCurrencyMap.get(normalizedPoo) || reportingCurrency;
    const dateKey = formatDateKey(date);
    const pair = `${pooCcy}/${reportingCurrency}`;
    const rate =
        pooCcy === reportingCurrency
            ? 1
            : (fxRateMap.get(`${pair}::${dateKey}`) || 1);

    return {
        pooCcy,
        pooCcyToRccy: roundToTwo(rate),
        reportingCurrency,
        stationCurrencySource: stationCurrencyMap.has(normalizedPoo) ? "station" : "reporting_default",
        reportingCurrencySource: "financial_config",
    };
}

function buildFlightSnapshot(flight, sectorMap) {
    const sector = `${normalizeStation(flight.depStn)}-${normalizeStation(flight.arrStn)}`;
    const sectorInfo = sectorMap.get(sector) || {};
    const sourceSeats = parseNumber(flight.seats, sectorInfo.paxCapacity || 0);
    const sourceCargoCapT = roundToTwo(parseNumber(flight.CargoCapT, sectorInfo.cargoCapT || 0));
    const sourcePaxTotal = roundToWhole(parseNumber(flight.pax));
    const sourceCargoTotal = roundToTwo(parseNumber(flight.CargoT));

    return {
        flightId: String(flight._id),
        userId: flight.userId,
        al: "Own",
        depStn: normalizeStation(flight.depStn),
        arrStn: normalizeStation(flight.arrStn),
        sector,
        od: sector,
        odOrigin: normalizeStation(flight.depStn),
        odDestination: normalizeStation(flight.arrStn),
        odDI: normalizeDomIntl(flight.domIntl),
        legDI: normalizeDomIntl(flight.domIntl),
        date: flight.date,
        day: flight.day,
        flightNumber: String(flight.flight || "").trim(),
        variant: String(flight.variant || "").trim(),
        std: String(flight.std || sectorInfo.std || "").trim(),
        sta: String(flight.sta || sectorInfo.sta || "").trim(),
        bt: String(flight.bt || "").trim(),
        maxPax: sourceSeats,
        maxCargoT: sourceCargoCapT,
        sourceSeats,
        sourceCargoCapT,
        sourcePaxTotal,
        sourceCargoTotal,
        sourcePaxLF: sourceSeats > 0 ? roundToTwo((sourcePaxTotal / sourceSeats) * 100) : 0,
        sourceCargoLF: sourceCargoCapT > 0 ? roundToTwo((sourceCargoTotal / sourceCargoCapT) * 100) : 0,
        sectorGcd: parseNumber(flight.dist, sectorInfo.gcd || 0),
        odViaGcd: parseNumber(flight.dist, sectorInfo.gcd || 0),
    };
}

function describeRow(row) {
    return `${row.identifier} ${row.od} ${row.flightNumber}${row.poo ? ` [POO ${row.poo}]` : ""}`;
}

function describeBucket(row) {
    return `${row.od} ${row.flightNumber}${row.poo ? ` [POO ${row.poo}]` : ""}`;
}

function getSharedConnectionLimits(firstSnapshot, secondSnapshot) {
    return {
        maxPax: Math.min(parseNumber(firstSnapshot.maxPax), parseNumber(secondSnapshot.maxPax)),
        maxCargoT: roundToTwo(Math.min(parseNumber(firstSnapshot.maxCargoT), parseNumber(secondSnapshot.maxCargoT))),
    };
}

function buildRowKey({
    source,
    trafficType,
    poo,
    flightId,
    connectedFlightId,
    odGroupKey,
}) {
    return [
        source || "system",
        trafficType,
        normalizeStation(poo),
        String(flightId || ""),
        String(connectedFlightId || "none"),
        odGroupKey || "base",
    ].join("|");
}

function buildLegPairKey(flightId) {
    return `leg::${flightId}`;
}

function buildConnectionGroupKey({ firstFlightId, secondFlightId, od, source }) {
    return `${source || "system"}::${od}::${firstFlightId}::${secondFlightId}`;
}

function buildRowMatchKey(row) {
    return [
        row.poo,
        row.od,
        row.trafficType,
        row.identifier,
        row.sector,
        row.flightNumber,
        row.connectedFlightNumber || "",
        row.variant || "",
        row.source || "system",
    ].join("|");
}

function createWorkingState(record) {
    return {
        _id: record._id,
        rowKey: record.rowKey,
        odGroupKey: record.odGroupKey || null,
        trafficType: record.trafficType,
        identifier: record.identifier,
        source: record.source || "system",
        isUserDefined: Boolean(record.isUserDefined),
        poo: normalizeStation(record.poo),
        od: record.od,
        odOrigin: normalizeStation(record.odOrigin),
        odDestination: normalizeStation(record.odDestination),
        sector: record.sector,
        flightId: record.flightId,
        connectedFlightId: record.connectedFlightId || null,
        flightNumber: record.flightNumber,
        connectedFlightNumber: record.connectedFlightNumber || null,
        flightList: Array.isArray(record.flightList) ? record.flightList : [],
        timeInclLayover: String(record.timeInclLayover || "").trim(),
        maxPax: parseNumber(record.maxPax),
        maxCargoT: roundToTwo(record.maxCargoT),
        pax: roundToWhole(record.pax),
        cargoT: roundToTwo(record.cargoT),
        sourcePaxTotal: roundToWhole(record.sourcePaxTotal),
        sourceCargoTotal: roundToTwo(record.sourceCargoTotal),
        sourceSeats: parseNumber(record.sourceSeats),
        sourceCargoCapT: roundToTwo(record.sourceCargoCapT),
        sourcePaxLF: roundToTwo(record.sourcePaxLF),
        sourceCargoLF: roundToTwo(record.sourceCargoLF),
        sectorGcd: parseNumber(record.sectorGcd),
        odViaGcd: parseNumber(record.odViaGcd),
        totalGcd: parseNumber(record.totalGcd),
        stops: parseNumber(record.stops),
        legFare: roundToTwo(record.legFare),
        legRate: roundToTwo(record.legRate),
        odFare: roundToTwo(record.odFare),
        odRate: roundToTwo(record.odRate),
        prorateRatioL1: roundToTwo(record.prorateRatioL1),
        fareProrateRatioL1L2: roundToTwo(record.fareProrateRatioL1L2),
        rateProrateRatioL1L2: roundToTwo(record.rateProrateRatioL1L2),
        legPaxRev: roundToTwo(record.legPaxRev),
        legCargoRev: roundToTwo(record.legCargoRev),
        legTotalRev: roundToTwo(record.legTotalRev),
        odPaxRev: roundToTwo(record.odPaxRev),
        odCargoRev: roundToTwo(record.odCargoRev),
        odTotalRev: roundToTwo(record.odTotalRev),
        pooCcy: String(record.pooCcy || "").trim(),
        pooCcyToRccy: roundToTwo(record.pooCcyToRccy || 1),
        rccyLegPaxRev: roundToTwo(record.rccyLegPaxRev),
        rccyLegCargoRev: roundToTwo(record.rccyLegCargoRev),
        rccyLegTotalRev: roundToTwo(record.rccyLegTotalRev),
        rccyOdPaxRev: roundToTwo(record.rccyOdPaxRev),
        rccyOdCargoRev: roundToTwo(record.rccyOdCargoRev),
        rccyOdTotalRev: roundToTwo(record.rccyOdTotalRev),
        rccyPax: roundToTwo(record.rccyPax),
        rccyCargo: roundToTwo(record.rccyCargo),
        rccyTotalRev: roundToTwo(record.rccyTotalRev),
        fnlRccyPaxRev: roundToTwo(record.fnlRccyPaxRev),
        fnlRccyCargoRev: roundToTwo(record.fnlRccyCargoRev),
        fnlRccyTotalRev: roundToTwo(record.fnlRccyTotalRev),
        reportingCurrency: normalizeCurrencyCode(record.reportingCurrency),
        stationCurrencySource: String(record.stationCurrencySource || "manual").trim(),
        reportingCurrencySource: String(record.reportingCurrencySource || "manual").trim(),
        applySSPricing: Boolean(record.applySSPricing),
        interline: String(record.interline || "").trim(),
        codeshare: String(record.codeshare || "").trim(),
    };
}

function calculateProrateRatio(row, fieldName) {
    const explicit = parseNumber(row[fieldName]);
    if (explicit > 0) {
        return Math.min(explicit, 1);
    }
    const sharedExplicit = parseNumber(row.prorateRatioL1);
    if (sharedExplicit > 0) {
        return Math.min(sharedExplicit, 1);
    }
    if (row.stops !== 1) {
        return 1;
    }
    return Math.min(safeRatio(row.sectorGcd, row.odViaGcd), 1);
}

function calculateLegShare(row, fieldName) {
    if (row.stops !== 1) {
        return 1;
    }
    const firstLegRatio = calculateProrateRatio(row, fieldName);
    if (
        row.trafficType === TRAFFIC_TYPES.BEHIND ||
        row.trafficType === TRAFFIC_TYPES.TRANSIT_FL
    ) {
        return firstLegRatio;
    }
    return roundToTwo(1 - firstLegRatio);
}

function recalculateRevenue(row) {
    const next = { ...row };
    const fareShare = calculateLegShare(row, "fareProrateRatioL1L2");
    const rateShare = calculateLegShare(row, "rateProrateRatioL1L2");

    if (row.stops === 1) {
        next.legFare = roundToTwo(row.odFare * fareShare);
        next.legRate = roundToTwo(row.odRate * rateShare);
    }

    next.legPaxRev = roundToTwo(parseNumber(next.pax) * parseNumber(next.legFare));
    next.legCargoRev = roundToTwo(parseNumber(next.cargoT) * parseNumber(next.legRate));
    next.legTotalRev = roundToTwo(next.legPaxRev + next.legCargoRev);

    const odFareBasis = row.applySSPricing ? next.legFare : row.odFare;
    const odRateBasis = row.applySSPricing ? next.legRate : row.odRate;

    next.odPaxRev = roundToTwo(parseNumber(next.pax) * parseNumber(odFareBasis));
    next.odCargoRev = roundToTwo(parseNumber(next.cargoT) * parseNumber(odRateBasis));
    next.odTotalRev = roundToTwo(next.odPaxRev + next.odCargoRev);

    const rate = parseNumber(row.pooCcyToRccy, 1) || 1;
    next.rccyLegPaxRev = roundToTwo(next.legPaxRev * rate);
    next.rccyLegCargoRev = roundToTwo(next.legCargoRev * rate);
    next.rccyLegTotalRev = roundToTwo(next.legTotalRev * rate);
    next.rccyOdPaxRev = roundToTwo(next.odPaxRev * rate);
    next.rccyOdCargoRev = roundToTwo(next.odCargoRev * rate);
    next.rccyOdTotalRev = roundToTwo(next.odTotalRev * rate);
    next.rccyPax = next.rccyOdPaxRev;
    next.rccyCargo = next.rccyOdCargoRev;
    next.rccyTotalRev = next.rccyOdTotalRev;
    next.fnlRccyPaxRev = next.rccyOdPaxRev;
    next.fnlRccyCargoRev = next.rccyOdCargoRev;
    next.fnlRccyTotalRev = roundToTwo(next.fnlRccyPaxRev + next.fnlRccyCargoRev);

    return next;
}

function buildEditableResponse(records) {
    return [...records]
        .sort((a, b) => {
            if (a.sNo !== b.sNo) return a.sNo - b.sNo;
            return a.identifier.localeCompare(b.identifier);
        })
        .map((record) => {
            const baseRecord = typeof record.toObject === "function" ? record.toObject() : record;
            return {
            ...baseRecord,
            displayType: DISPLAY_LABELS[record.trafficType] || record.identifier || record.trafficType,
            rowMatchKey: buildRowMatchKey(record),
        };
        });
}

async function getSectorInfoMap(userId) {
    const sectors = await Sector.find({ userId }).lean();
    return buildSectorMap(sectors);
}

function getExistingFlightRows(existingRecords, flightId) {
    return existingRecords.filter(
        (record) =>
            String(record.flightId) === String(flightId) ||
            String(record.connectedFlightId) === String(flightId)
    );
}

function getPreviousFlightMetrics(existingRecords, flightId) {
    const rows = getExistingFlightRows(existingRecords, flightId);
    if (!rows.length) {
        return {
            maxPax: 0,
            maxCargoT: 0,
            sourcePaxTotal: 0,
            sourceCargoTotal: 0,
        };
    }

    return rows.reduce(
        (acc, row) => ({
            maxPax: Math.max(acc.maxPax, parseNumber(row.maxPax)),
            maxCargoT: Math.max(acc.maxCargoT, roundToTwo(row.maxCargoT)),
            sourcePaxTotal: Math.max(acc.sourcePaxTotal, roundToWhole(row.sourcePaxTotal)),
            sourceCargoTotal: Math.max(acc.sourceCargoTotal, roundToTwo(row.sourceCargoTotal)),
        }),
        { maxPax: 0, maxCargoT: 0, sourcePaxTotal: 0, sourceCargoTotal: 0 }
    );
}

function buildLegRows({
    snapshot,
    existingRowsByKey,
    existingRecords,
    currencyContextByPoo = {},
}) {
    const odGroupKey = buildLegPairKey(snapshot.flightId);
    const depRowKey = buildRowKey({
        source: "system",
        trafficType: TRAFFIC_TYPES.LEG,
        poo: snapshot.depStn,
        flightId: snapshot.flightId,
        odGroupKey,
    });
    const arrRowKey = buildRowKey({
        source: "system",
        trafficType: TRAFFIC_TYPES.LEG,
        poo: snapshot.arrStn,
        flightId: snapshot.flightId,
        odGroupKey,
    });

    const prevMetrics = getPreviousFlightMetrics(existingRecords, snapshot.flightId);
    const depExisting = existingRowsByKey.get(depRowKey);
    const arrExisting = existingRowsByKey.get(arrRowKey);

    const maxDecreased =
        snapshot.maxPax < prevMetrics.maxPax ||
        snapshot.maxCargoT < prevMetrics.maxCargoT;
    const loadDecreased =
        snapshot.sourcePaxTotal < prevMetrics.sourcePaxTotal ||
        snapshot.sourceCargoTotal < prevMetrics.sourceCargoTotal;
    const forceReset = maxDecreased || loadDecreased;

    let depPax;
    let arrPax;
    let depCargo;
    let arrCargo;

    if (!depExisting || !arrExisting || forceReset) {
        [depPax, arrPax] = splitPaxValue(snapshot.sourcePaxTotal);
        [depCargo, arrCargo] = splitCargoValue(snapshot.sourceCargoTotal);
    } else {
        const paxDelta = snapshot.sourcePaxTotal - prevMetrics.sourcePaxTotal;
        const cargoDelta = roundToTwo(snapshot.sourceCargoTotal - prevMetrics.sourceCargoTotal);
        const [depPaxAdd, arrPaxAdd] = splitPaxValue(Math.max(paxDelta, 0));
        const [depCargoAdd, arrCargoAdd] = splitCargoValue(Math.max(cargoDelta, 0));
        depPax = roundToWhole(depExisting.pax + depPaxAdd);
        arrPax = roundToWhole(arrExisting.pax + arrPaxAdd);
        depCargo = roundToTwo(depExisting.cargoT + depCargoAdd);
        arrCargo = roundToTwo(arrExisting.cargoT + arrCargoAdd);
    }

    const common = {
        userId: snapshot.userId,
        al: snapshot.al,
        od: snapshot.od,
        odOrigin: snapshot.odOrigin,
        odDestination: snapshot.odDestination,
        odDI: snapshot.odDI,
        sector: snapshot.sector,
        legDI: snapshot.legDI,
        date: snapshot.date,
        day: snapshot.day,
        flightNumber: snapshot.flightNumber,
        variant: snapshot.variant,
        std: snapshot.std,
        sta: snapshot.sta,
        maxPax: snapshot.maxPax,
        maxCargoT: snapshot.maxCargoT,
        sourcePaxTotal: snapshot.sourcePaxTotal,
        sourceCargoTotal: snapshot.sourceCargoTotal,
        sourceSeats: snapshot.sourceSeats,
        sourceCargoCapT: snapshot.sourceCargoCapT,
        sourcePaxLF: snapshot.sourcePaxLF,
        sourceCargoLF: snapshot.sourceCargoLF,
        sectorGcd: snapshot.sectorGcd,
        odViaGcd: snapshot.odViaGcd,
        stops: 0,
        trafficType: TRAFFIC_TYPES.LEG,
        identifier: "Leg",
        source: "system",
        isUserDefined: false,
        flightId: snapshot.flightId,
        connectedFlightId: null,
        connectedFlightNumber: null,
        connectedStd: null,
        connectedSta: null,
        flightList: [snapshot.flightNumber],
        timeInclLayover: calculateTimeInclLayover(snapshot),
        ownerFlightId: snapshot.flightId,
        connectionKey: null,
        odGroupKey,
        totalGcd: snapshot.odViaGcd,
        applySSPricing: depExisting ? Boolean(depExisting.applySSPricing) : false,
    };

    return {
        rows: [
            recalculateRevenue({
                ...common,
                sNo: 0,
                rowKey: depRowKey,
                poo: snapshot.depStn,
                legFare: depExisting ? roundToTwo(depExisting.legFare) : 0,
                legRate: depExisting ? roundToTwo(depExisting.legRate) : 0,
                odFare: depExisting ? roundToTwo(depExisting.odFare) : 0,
                odRate: depExisting ? roundToTwo(depExisting.odRate) : 0,
                prorateRatioL1: depExisting ? roundToTwo(depExisting.prorateRatioL1) : 0,
                fareProrateRatioL1L2: depExisting ? roundToTwo(depExisting.fareProrateRatioL1L2) : 0,
                rateProrateRatioL1L2: depExisting ? roundToTwo(depExisting.rateProrateRatioL1L2) : 0,
                pooCcy: depExisting ? String(depExisting.pooCcy || "") : currencyContextByPoo[snapshot.depStn]?.pooCcy || "",
                pooCcyToRccy: depExisting ? roundToTwo(depExisting.pooCcyToRccy || 1) : currencyContextByPoo[snapshot.depStn]?.pooCcyToRccy || 1,
                reportingCurrency: depExisting ? normalizeCurrencyCode(depExisting.reportingCurrency) : currencyContextByPoo[snapshot.depStn]?.reportingCurrency || "",
                stationCurrencySource: depExisting ? String(depExisting.stationCurrencySource || "manual") : currencyContextByPoo[snapshot.depStn]?.stationCurrencySource || "manual",
                reportingCurrencySource: depExisting ? String(depExisting.reportingCurrencySource || "manual") : currencyContextByPoo[snapshot.depStn]?.reportingCurrencySource || "manual",
                applySSPricing: depExisting ? Boolean(depExisting.applySSPricing) : false,
                interline: depExisting ? String(depExisting.interline || "") : "",
                codeshare: depExisting ? String(depExisting.codeshare || "") : "",
                pax: depPax,
                cargoT: depCargo,
            }),
            recalculateRevenue({
                ...common,
                sNo: 0,
                rowKey: arrRowKey,
                poo: snapshot.arrStn,
                legFare: arrExisting ? roundToTwo(arrExisting.legFare) : 0,
                legRate: arrExisting ? roundToTwo(arrExisting.legRate) : 0,
                odFare: arrExisting ? roundToTwo(arrExisting.odFare) : 0,
                odRate: arrExisting ? roundToTwo(arrExisting.odRate) : 0,
                prorateRatioL1: arrExisting ? roundToTwo(arrExisting.prorateRatioL1) : 0,
                fareProrateRatioL1L2: arrExisting ? roundToTwo(arrExisting.fareProrateRatioL1L2) : 0,
                rateProrateRatioL1L2: arrExisting ? roundToTwo(arrExisting.rateProrateRatioL1L2) : 0,
                pooCcy: arrExisting ? String(arrExisting.pooCcy || "") : currencyContextByPoo[snapshot.arrStn]?.pooCcy || "",
                pooCcyToRccy: arrExisting ? roundToTwo(arrExisting.pooCcyToRccy || 1) : currencyContextByPoo[snapshot.arrStn]?.pooCcyToRccy || 1,
                reportingCurrency: arrExisting ? normalizeCurrencyCode(arrExisting.reportingCurrency) : currencyContextByPoo[snapshot.arrStn]?.reportingCurrency || "",
                stationCurrencySource: arrExisting ? String(arrExisting.stationCurrencySource || "manual") : currencyContextByPoo[snapshot.arrStn]?.stationCurrencySource || "manual",
                reportingCurrencySource: arrExisting ? String(arrExisting.reportingCurrencySource || "manual") : currencyContextByPoo[snapshot.arrStn]?.reportingCurrencySource || "manual",
                applySSPricing: arrExisting ? Boolean(arrExisting.applySSPricing) : false,
                interline: arrExisting ? String(arrExisting.interline || "") : "",
                codeshare: arrExisting ? String(arrExisting.codeshare || "") : "",
                pax: arrPax,
                cargoT: arrCargo,
            }),
        ],
        forceReset,
    };
}

function buildSystemConnectionRows({
    pagePoo,
    firstSnapshot,
    secondSnapshot,
    existingRowsByKey,
    shouldReset,
    pageCurrencyContext = {},
}) {
    const od = `${firstSnapshot.depStn}-${secondSnapshot.arrStn}`;
    const odDI = firstSnapshot.odDI === "Intl" || secondSnapshot.odDI === "Intl" ? "Intl" : "Dom";
    const sharedLimits = getSharedConnectionLimits(firstSnapshot, secondSnapshot);
    const odGroupKey = buildConnectionGroupKey({
        firstFlightId: firstSnapshot.flightId,
        secondFlightId: secondSnapshot.flightId,
        od,
        source: "system",
    });

    const behindRowKey = buildRowKey({
        source: "system",
        trafficType: TRAFFIC_TYPES.BEHIND,
        poo: pagePoo,
        flightId: firstSnapshot.flightId,
        connectedFlightId: secondSnapshot.flightId,
        odGroupKey,
    });
    const beyondRowKey = buildRowKey({
        source: "system",
        trafficType: TRAFFIC_TYPES.BEYOND,
        poo: pagePoo,
        flightId: secondSnapshot.flightId,
        connectedFlightId: firstSnapshot.flightId,
        odGroupKey,
    });

    const behindExisting = existingRowsByKey.get(behindRowKey);
    const beyondExisting = existingRowsByKey.get(beyondRowKey);
    const sharedRevenue = behindExisting || beyondExisting;

    return [
        recalculateRevenue({
            userId: firstSnapshot.userId,
            sNo: 0,
            rowKey: behindRowKey,
            flightId: firstSnapshot.flightId,
            connectedFlightId: secondSnapshot.flightId,
            connectedFlightNumber: secondSnapshot.flightNumber,
            ownerFlightId: firstSnapshot.flightId,
            connectionKey: `${firstSnapshot.flightId}::${secondSnapshot.flightId}`,
            odGroupKey,
            trafficType: TRAFFIC_TYPES.BEHIND,
            source: "system",
            isUserDefined: false,
            al: firstSnapshot.al,
            poo: pagePoo,
            od,
            odOrigin: firstSnapshot.depStn,
            odDestination: secondSnapshot.arrStn,
            odDI,
            stops: 1,
            identifier: "Behind",
            sector: firstSnapshot.sector,
            legDI: firstSnapshot.legDI,
            date: firstSnapshot.date,
            day: firstSnapshot.day,
            flightNumber: firstSnapshot.flightNumber,
            variant: firstSnapshot.variant,
            std: firstSnapshot.std,
            sta: firstSnapshot.sta,
            connectedStd: secondSnapshot.std,
            connectedSta: secondSnapshot.sta,
            flightList: [firstSnapshot.flightNumber, secondSnapshot.flightNumber],
            timeInclLayover: calculateTimeInclLayover(firstSnapshot, secondSnapshot),
            maxPax: sharedLimits.maxPax,
            maxCargoT: sharedLimits.maxCargoT,
            pax: shouldReset || !behindExisting ? 0 : roundToWhole(behindExisting.pax),
            cargoT: shouldReset || !behindExisting ? 0 : roundToTwo(behindExisting.cargoT),
            sourcePaxTotal: firstSnapshot.sourcePaxTotal,
            sourceCargoTotal: firstSnapshot.sourceCargoTotal,
            sourceSeats: firstSnapshot.sourceSeats,
            sourceCargoCapT: firstSnapshot.sourceCargoCapT,
            sourcePaxLF: firstSnapshot.sourcePaxLF,
            sourceCargoLF: firstSnapshot.sourceCargoLF,
            sectorGcd: firstSnapshot.sectorGcd,
            odViaGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            totalGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            legFare: 0,
            legRate: 0,
            odFare: sharedRevenue ? roundToTwo(sharedRevenue.odFare) : 0,
            odRate: sharedRevenue ? roundToTwo(sharedRevenue.odRate) : 0,
            prorateRatioL1: sharedRevenue ? roundToTwo(sharedRevenue.prorateRatioL1) : 0,
            fareProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.fareProrateRatioL1L2) : 0,
            rateProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.rateProrateRatioL1L2) : 0,
            pooCcy: sharedRevenue ? String(sharedRevenue.pooCcy || "") : pageCurrencyContext.pooCcy || "",
            pooCcyToRccy: sharedRevenue ? roundToTwo(sharedRevenue.pooCcyToRccy || 1) : pageCurrencyContext.pooCcyToRccy || 1,
            reportingCurrency: sharedRevenue ? normalizeCurrencyCode(sharedRevenue.reportingCurrency) : pageCurrencyContext.reportingCurrency || "",
            stationCurrencySource: sharedRevenue ? String(sharedRevenue.stationCurrencySource || "manual") : pageCurrencyContext.stationCurrencySource || "manual",
            reportingCurrencySource: sharedRevenue ? String(sharedRevenue.reportingCurrencySource || "manual") : pageCurrencyContext.reportingCurrencySource || "manual",
            applySSPricing: sharedRevenue ? Boolean(sharedRevenue.applySSPricing) : false,
            interline: sharedRevenue ? String(sharedRevenue.interline || "") : "",
            codeshare: sharedRevenue ? String(sharedRevenue.codeshare || "") : "",
        }),
        recalculateRevenue({
            userId: firstSnapshot.userId,
            sNo: 0,
            rowKey: beyondRowKey,
            flightId: secondSnapshot.flightId,
            connectedFlightId: firstSnapshot.flightId,
            connectedFlightNumber: firstSnapshot.flightNumber,
            ownerFlightId: secondSnapshot.flightId,
            connectionKey: `${firstSnapshot.flightId}::${secondSnapshot.flightId}`,
            odGroupKey,
            trafficType: TRAFFIC_TYPES.BEYOND,
            source: "system",
            isUserDefined: false,
            al: secondSnapshot.al,
            poo: pagePoo,
            od,
            odOrigin: firstSnapshot.depStn,
            odDestination: secondSnapshot.arrStn,
            odDI,
            stops: 1,
            identifier: "Beyond",
            sector: secondSnapshot.sector,
            legDI: secondSnapshot.legDI,
            date: secondSnapshot.date,
            day: secondSnapshot.day,
            flightNumber: secondSnapshot.flightNumber,
            variant: secondSnapshot.variant,
            std: secondSnapshot.std,
            sta: secondSnapshot.sta,
            connectedStd: firstSnapshot.std,
            connectedSta: firstSnapshot.sta,
            flightList: [firstSnapshot.flightNumber, secondSnapshot.flightNumber],
            timeInclLayover: calculateTimeInclLayover(firstSnapshot, secondSnapshot),
            maxPax: sharedLimits.maxPax,
            maxCargoT: sharedLimits.maxCargoT,
            pax: shouldReset || !beyondExisting ? 0 : roundToWhole(beyondExisting.pax),
            cargoT: shouldReset || !beyondExisting ? 0 : roundToTwo(beyondExisting.cargoT),
            sourcePaxTotal: secondSnapshot.sourcePaxTotal,
            sourceCargoTotal: secondSnapshot.sourceCargoTotal,
            sourceSeats: secondSnapshot.sourceSeats,
            sourceCargoCapT: secondSnapshot.sourceCargoCapT,
            sourcePaxLF: secondSnapshot.sourcePaxLF,
            sourceCargoLF: secondSnapshot.sourceCargoLF,
            sectorGcd: secondSnapshot.sectorGcd,
            odViaGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            totalGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            legFare: 0,
            legRate: 0,
            odFare: sharedRevenue ? roundToTwo(sharedRevenue.odFare) : 0,
            odRate: sharedRevenue ? roundToTwo(sharedRevenue.odRate) : 0,
            prorateRatioL1: sharedRevenue ? roundToTwo(sharedRevenue.prorateRatioL1) : 0,
            fareProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.fareProrateRatioL1L2) : 0,
            rateProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.rateProrateRatioL1L2) : 0,
            pooCcy: sharedRevenue ? String(sharedRevenue.pooCcy || "") : pageCurrencyContext.pooCcy || "",
            pooCcyToRccy: sharedRevenue ? roundToTwo(sharedRevenue.pooCcyToRccy || 1) : pageCurrencyContext.pooCcyToRccy || 1,
            reportingCurrency: sharedRevenue ? normalizeCurrencyCode(sharedRevenue.reportingCurrency) : pageCurrencyContext.reportingCurrency || "",
            stationCurrencySource: sharedRevenue ? String(sharedRevenue.stationCurrencySource || "manual") : pageCurrencyContext.stationCurrencySource || "manual",
            reportingCurrencySource: sharedRevenue ? String(sharedRevenue.reportingCurrencySource || "manual") : pageCurrencyContext.reportingCurrencySource || "manual",
            applySSPricing: sharedRevenue ? Boolean(sharedRevenue.applySSPricing) : false,
            interline: sharedRevenue ? String(sharedRevenue.interline || "") : "",
            codeshare: sharedRevenue ? String(sharedRevenue.codeshare || "") : "",
        }),
    ];
}

function buildUserTransitRows({
    pagePoo,
    firstSnapshot,
    secondSnapshot,
    existingRowsByKey,
    shouldReset,
    pageCurrencyContext = {},
}) {
    const od = `${firstSnapshot.depStn}-${secondSnapshot.arrStn}`;
    const odDI = firstSnapshot.odDI === "Intl" || secondSnapshot.odDI === "Intl" ? "Intl" : "Dom";
    const sharedLimits = getSharedConnectionLimits(firstSnapshot, secondSnapshot);
    const odGroupKey = buildConnectionGroupKey({
        firstFlightId: firstSnapshot.flightId,
        secondFlightId: secondSnapshot.flightId,
        od,
        source: "user",
    });

    const firstRowKey = buildRowKey({
        source: "user",
        trafficType: TRAFFIC_TYPES.TRANSIT_FL,
        poo: pagePoo,
        flightId: firstSnapshot.flightId,
        connectedFlightId: secondSnapshot.flightId,
        odGroupKey,
    });
    const secondRowKey = buildRowKey({
        source: "user",
        trafficType: TRAFFIC_TYPES.TRANSIT_SL,
        poo: pagePoo,
        flightId: secondSnapshot.flightId,
        connectedFlightId: firstSnapshot.flightId,
        odGroupKey,
    });

    const firstExisting = existingRowsByKey.get(firstRowKey);
    const secondExisting = existingRowsByKey.get(secondRowKey);
    const sharedRevenue = firstExisting || secondExisting;

    return [
        recalculateRevenue({
            userId: firstSnapshot.userId,
            sNo: 0,
            rowKey: firstRowKey,
            flightId: firstSnapshot.flightId,
            connectedFlightId: secondSnapshot.flightId,
            connectedFlightNumber: secondSnapshot.flightNumber,
            ownerFlightId: firstSnapshot.flightId,
            connectionKey: `${firstSnapshot.flightId}::${secondSnapshot.flightId}`,
            odGroupKey,
            trafficType: TRAFFIC_TYPES.TRANSIT_FL,
            source: "user",
            isUserDefined: true,
            al: firstSnapshot.al,
            poo: pagePoo,
            od,
            odOrigin: firstSnapshot.depStn,
            odDestination: secondSnapshot.arrStn,
            odDI,
            stops: 1,
            identifier: "Transit FL",
            sector: firstSnapshot.sector,
            legDI: firstSnapshot.legDI,
            date: firstSnapshot.date,
            day: firstSnapshot.day,
            flightNumber: firstSnapshot.flightNumber,
            variant: firstSnapshot.variant,
            std: firstSnapshot.std,
            sta: firstSnapshot.sta,
            connectedStd: secondSnapshot.std,
            connectedSta: secondSnapshot.sta,
            flightList: [firstSnapshot.flightNumber, secondSnapshot.flightNumber],
            timeInclLayover: calculateTimeInclLayover(firstSnapshot, secondSnapshot),
            maxPax: sharedLimits.maxPax,
            maxCargoT: sharedLimits.maxCargoT,
            pax: shouldReset || !firstExisting ? 0 : roundToWhole(firstExisting.pax),
            cargoT: shouldReset || !firstExisting ? 0 : roundToTwo(firstExisting.cargoT),
            sourcePaxTotal: firstSnapshot.sourcePaxTotal,
            sourceCargoTotal: firstSnapshot.sourceCargoTotal,
            sourceSeats: firstSnapshot.sourceSeats,
            sourceCargoCapT: firstSnapshot.sourceCargoCapT,
            sourcePaxLF: firstSnapshot.sourcePaxLF,
            sourceCargoLF: firstSnapshot.sourceCargoLF,
            sectorGcd: firstSnapshot.sectorGcd,
            odViaGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            totalGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            legFare: 0,
            legRate: 0,
            odFare: sharedRevenue ? roundToTwo(sharedRevenue.odFare) : 0,
            odRate: sharedRevenue ? roundToTwo(sharedRevenue.odRate) : 0,
            prorateRatioL1: sharedRevenue ? roundToTwo(sharedRevenue.prorateRatioL1) : 0,
            fareProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.fareProrateRatioL1L2) : 0,
            rateProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.rateProrateRatioL1L2) : 0,
            pooCcy: sharedRevenue ? String(sharedRevenue.pooCcy || "") : pageCurrencyContext.pooCcy || "",
            pooCcyToRccy: sharedRevenue ? roundToTwo(sharedRevenue.pooCcyToRccy || 1) : pageCurrencyContext.pooCcyToRccy || 1,
            reportingCurrency: sharedRevenue ? normalizeCurrencyCode(sharedRevenue.reportingCurrency) : pageCurrencyContext.reportingCurrency || "",
            stationCurrencySource: sharedRevenue ? String(sharedRevenue.stationCurrencySource || "manual") : pageCurrencyContext.stationCurrencySource || "manual",
            reportingCurrencySource: sharedRevenue ? String(sharedRevenue.reportingCurrencySource || "manual") : pageCurrencyContext.reportingCurrencySource || "manual",
            applySSPricing: sharedRevenue ? Boolean(sharedRevenue.applySSPricing) : false,
            interline: sharedRevenue ? String(sharedRevenue.interline || "") : "",
            codeshare: sharedRevenue ? String(sharedRevenue.codeshare || "") : "",
        }),
        recalculateRevenue({
            userId: firstSnapshot.userId,
            sNo: 0,
            rowKey: secondRowKey,
            flightId: secondSnapshot.flightId,
            connectedFlightId: firstSnapshot.flightId,
            connectedFlightNumber: firstSnapshot.flightNumber,
            ownerFlightId: secondSnapshot.flightId,
            connectionKey: `${firstSnapshot.flightId}::${secondSnapshot.flightId}`,
            odGroupKey,
            trafficType: TRAFFIC_TYPES.TRANSIT_SL,
            source: "user",
            isUserDefined: true,
            al: secondSnapshot.al,
            poo: pagePoo,
            od,
            odOrigin: firstSnapshot.depStn,
            odDestination: secondSnapshot.arrStn,
            odDI,
            stops: 1,
            identifier: "Transit SL",
            sector: secondSnapshot.sector,
            legDI: secondSnapshot.legDI,
            date: secondSnapshot.date,
            day: secondSnapshot.day,
            flightNumber: secondSnapshot.flightNumber,
            variant: secondSnapshot.variant,
            std: secondSnapshot.std,
            sta: secondSnapshot.sta,
            connectedStd: firstSnapshot.std,
            connectedSta: firstSnapshot.sta,
            flightList: [firstSnapshot.flightNumber, secondSnapshot.flightNumber],
            timeInclLayover: calculateTimeInclLayover(firstSnapshot, secondSnapshot),
            maxPax: sharedLimits.maxPax,
            maxCargoT: sharedLimits.maxCargoT,
            pax: shouldReset || !secondExisting ? 0 : roundToWhole(secondExisting.pax),
            cargoT: shouldReset || !secondExisting ? 0 : roundToTwo(secondExisting.cargoT),
            sourcePaxTotal: secondSnapshot.sourcePaxTotal,
            sourceCargoTotal: secondSnapshot.sourceCargoTotal,
            sourceSeats: secondSnapshot.sourceSeats,
            sourceCargoCapT: secondSnapshot.sourceCargoCapT,
            sourcePaxLF: secondSnapshot.sourcePaxLF,
            sourceCargoLF: secondSnapshot.sourceCargoLF,
            sectorGcd: secondSnapshot.sectorGcd,
            odViaGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            totalGcd: roundToTwo(firstSnapshot.sectorGcd + secondSnapshot.sectorGcd),
            legFare: 0,
            legRate: 0,
            odFare: sharedRevenue ? roundToTwo(sharedRevenue.odFare) : 0,
            odRate: sharedRevenue ? roundToTwo(sharedRevenue.odRate) : 0,
            prorateRatioL1: sharedRevenue ? roundToTwo(sharedRevenue.prorateRatioL1) : 0,
            fareProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.fareProrateRatioL1L2) : 0,
            rateProrateRatioL1L2: sharedRevenue ? roundToTwo(sharedRevenue.rateProrateRatioL1L2) : 0,
            pooCcy: sharedRevenue ? String(sharedRevenue.pooCcy || "") : pageCurrencyContext.pooCcy || "",
            pooCcyToRccy: sharedRevenue ? roundToTwo(sharedRevenue.pooCcyToRccy || 1) : pageCurrencyContext.pooCcyToRccy || 1,
            reportingCurrency: sharedRevenue ? normalizeCurrencyCode(sharedRevenue.reportingCurrency) : pageCurrencyContext.reportingCurrency || "",
            stationCurrencySource: sharedRevenue ? String(sharedRevenue.stationCurrencySource || "manual") : pageCurrencyContext.stationCurrencySource || "manual",
            reportingCurrencySource: sharedRevenue ? String(sharedRevenue.reportingCurrencySource || "manual") : pageCurrencyContext.reportingCurrencySource || "manual",
            applySSPricing: sharedRevenue ? Boolean(sharedRevenue.applySSPricing) : false,
            interline: sharedRevenue ? String(sharedRevenue.interline || "") : "",
            codeshare: sharedRevenue ? String(sharedRevenue.codeshare || "") : "",
        }),
    ];
}

function assignSerialNumbers(rows) {
    const sorted = [...rows].sort((a, b) => {
        const groupCompare = (a.odGroupKey || "").localeCompare(b.odGroupKey || "");
        if (groupCompare !== 0) return groupCompare;
        const pooCompare = a.poo.localeCompare(b.poo);
        if (pooCompare !== 0) return pooCompare;
        return a.identifier.localeCompare(b.identifier);
    });

    sorted.forEach((row, index) => {
        row.sNo = index + 1;
    });

    return sorted;
}

async function resolveFlightsForTransitDraft({ userId, date, transitDraft, flightsById }) {
    let firstFlight = transitDraft.firstFlightId ? flightsById.get(String(transitDraft.firstFlightId)) : null;
    let secondFlight = transitDraft.secondFlightId ? flightsById.get(String(transitDraft.secondFlightId)) : null;

    if (!firstFlight || !secondFlight) {
        const filters = {
            userId,
            date: {
                $gte: moment(date).startOf("day").toDate(),
                $lte: moment(date).endOf("day").toDate(),
            },
        };

        if (!firstFlight && transitDraft.firstFlightNumber) {
            firstFlight = await Flight.findOne({
                ...filters,
                flight: transitDraft.firstFlightNumber,
            }).lean();
        }

        if (!secondFlight && transitDraft.secondFlightNumber) {
            secondFlight = await Flight.findOne({
                ...filters,
                flight: transitDraft.secondFlightNumber,
            }).lean();
        }
    }

    return { firstFlight, secondFlight };
}

async function buildPooDataset({ userId, poo, date }) {
    const dayStart = moment(date).startOf("day").toDate();
    const dayEnd = moment(date).endOf("day").toDate();
    const normalizedPoo = normalizeStation(poo);

    const [sectorMap, existingRecords, directFlights, stations, rawRevenueConfig] = await Promise.all([
        getSectorInfoMap(userId),
        PooTable.find({
            userId,
            date: { $gte: dayStart, $lte: dayEnd },
        }).lean(),
        Flight.find({
            userId,
            date: { $gte: dayStart, $lte: dayEnd },
            $or: [
                { depStn: normalizedPoo },
                { arrStn: normalizedPoo },
            ],
        }).lean(),
        Station.find({ userId }).lean(),
        RevenueConfig.findOne({ userId }).lean(),
    ]);
    const stationCurrencyMap = buildStationCurrencyMap(stations);
    const revenueConfig = normalizeRevenueConfig(rawRevenueConfig || {});
    const fxRateMap = buildFxRateMap(revenueConfig.fxRates);

    const existingRowsByKey = new Map(existingRecords.map((record) => [record.rowKey, record]));
    const flightsById = new Map(directFlights.map((flight) => [String(flight._id), flight]));

    const directFlightIds = directFlights.map((flight) => String(flight._id));
    const connectionEdges = directFlightIds.length
        ? await Connections.find({
            userId,
            $or: [
                { flightID: { $in: directFlightIds } },
                { beyondOD: { $in: directFlightIds } },
            ],
        }).lean()
        : [];

    const connectionFlightIds = [
        ...new Set(
            connectionEdges.flatMap((edge) => [String(edge.flightID), String(edge.beyondOD)])
        ),
    ];

    const existingTransitRows = existingRecords.filter(
        (row) =>
            row.poo === normalizedPoo &&
            row.source === "user" &&
            (
                row.trafficType === TRAFFIC_TYPES.TRANSIT_FL ||
                row.trafficType === TRAFFIC_TYPES.TRANSIT_SL
            )
    );

    const extraFlightIds = [
        ...new Set(
            existingTransitRows.flatMap((row) => [
                String(row.flightId),
                String(row.connectedFlightId || ""),
            ]).filter(Boolean)
        ),
    ];

    const missingFlightIds = [...new Set([...connectionFlightIds, ...extraFlightIds])].filter(
        (flightId) => !flightsById.has(flightId)
    );

    if (missingFlightIds.length) {
        const extraFlights = await Flight.find({
            userId,
            _id: { $in: missingFlightIds },
            date: { $gte: dayStart, $lte: dayEnd },
        }).lean();
        extraFlights.forEach((flight) => {
            flightsById.set(String(flight._id), flight);
        });
    }

    const snapshots = new Map();
    const resetByFlightId = new Map();
    const rows = [];
    const currencyContextByPoo = {};

    const getCurrencyContextForPoo = (pooCode) => {
        const normalizedCode = normalizeStation(pooCode);
        if (!currencyContextByPoo[normalizedCode]) {
            currencyContextByPoo[normalizedCode] = resolveCurrencyContext({
                stationCurrencyMap,
                revenueConfig,
                fxRateMap,
                poo: normalizedCode,
                date,
            });
        }
        return currencyContextByPoo[normalizedCode];
    };

    for (const flight of flightsById.values()) {
        const snapshot = buildFlightSnapshot(flight, sectorMap);
        snapshots.set(snapshot.flightId, snapshot);
        const { rows: legRows, forceReset } = buildLegRows({
            snapshot,
            existingRowsByKey,
            existingRecords,
            currencyContextByPoo: {
                [snapshot.depStn]: getCurrencyContextForPoo(snapshot.depStn),
                [snapshot.arrStn]: getCurrencyContextForPoo(snapshot.arrStn),
            },
        });
        rows.push(...legRows);
        resetByFlightId.set(snapshot.flightId, forceReset);
    }

    connectionEdges.forEach((edge) => {
        const firstFlight = flightsById.get(String(edge.flightID));
        const secondFlight = flightsById.get(String(edge.beyondOD));
        if (!firstFlight || !secondFlight) return;

        const firstSnapshot = snapshots.get(String(firstFlight._id));
        const secondSnapshot = snapshots.get(String(secondFlight._id));
        if (!firstSnapshot || !secondSnapshot) return;

        const connectionMatchesPage =
            normalizedPoo === firstSnapshot.depStn ||
            normalizedPoo === secondSnapshot.arrStn ||
            normalizedPoo === firstSnapshot.odOrigin ||
            normalizedPoo === secondSnapshot.odDestination;
        if (!connectionMatchesPage) return;

        rows.push(
            ...buildSystemConnectionRows({
                pagePoo: normalizedPoo,
                firstSnapshot,
                secondSnapshot,
                existingRowsByKey,
                pageCurrencyContext: getCurrencyContextForPoo(normalizedPoo),
                shouldReset:
                    Boolean(resetByFlightId.get(firstSnapshot.flightId)) ||
                    Boolean(resetByFlightId.get(secondSnapshot.flightId)),
            })
        );
    });

    const existingTransitGroups = new Map();
    existingTransitRows.forEach((row) => {
        if (!row.odGroupKey) return;
        if (!existingTransitGroups.has(row.odGroupKey)) {
            existingTransitGroups.set(row.odGroupKey, []);
        }
        existingTransitGroups.get(row.odGroupKey).push(row);
    });

    existingTransitGroups.forEach((groupRows) => {
        const lead = groupRows[0];
        const firstSnapshot = snapshots.get(String(lead.flightId));
        const secondSnapshot = snapshots.get(String(lead.connectedFlightId));
        if (!firstSnapshot || !secondSnapshot) return;

        rows.push(
            ...buildUserTransitRows({
                pagePoo: normalizedPoo,
                firstSnapshot,
                secondSnapshot,
                existingRowsByKey,
                pageCurrencyContext: getCurrencyContextForPoo(normalizedPoo),
                shouldReset:
                    Boolean(resetByFlightId.get(firstSnapshot.flightId)) ||
                    Boolean(resetByFlightId.get(secondSnapshot.flightId)),
            })
        );
    });

    const dedupedRows = [];
    const seenRowKeys = new Set();
    rows.forEach((row) => {
        if (seenRowKeys.has(row.rowKey)) return;
        seenRowKeys.add(row.rowKey);
        dedupedRows.push(row);
    });

    const serialisedRows = assignSerialNumbers(dedupedRows);

    return {
        dayStart,
        dayEnd,
        normalizedPoo,
        rows: serialisedRows,
        keepRowKeys: new Set(serialisedRows.map((row) => row.rowKey)),
        touchedFlightIds: [...flightsById.keys()],
    };
}

function applyFieldEdits(row, requested) {
    let next = { ...row };

    if (requested.pax !== undefined) {
        next.pax = roundToWhole(requested.pax);
    }

    if (requested.cargoT !== undefined) {
        next.cargoT = roundToTwo(requested.cargoT);
    }

    REVENUE_FIELDS.forEach((field) => {
        if (requested[field] !== undefined) {
            next[field] = roundToTwo(requested[field]);
        }
    });

    STRING_FIELDS.forEach((field) => {
        if (requested[field] !== undefined) {
            next[field] = field === "pooCcy"
                ? normalizeCurrencyCode(requested[field])
                : String(requested[field] || "").trim();
        }
    });

    BOOLEAN_FIELDS.forEach((field) => {
        if (requested[field] !== undefined) {
            next[field] = Boolean(requested[field]);
        }
    });

    return recalculateRevenue(next);
}

function getLegRowsByFlight(stateRows) {
    const map = new Map();
    stateRows.forEach((row) => {
        if (row.trafficType !== TRAFFIC_TYPES.LEG) return;
        if (!map.has(String(row.flightId))) {
            map.set(String(row.flightId), []);
        }
        map.get(String(row.flightId)).push(row);
    });
    return map;
}

function getGroupRowsByOdKey(stateRows) {
    const map = new Map();
    stateRows.forEach((row) => {
        if (!row.odGroupKey) return;
        if (!map.has(row.odGroupKey)) {
            map.set(row.odGroupKey, []);
        }
        map.get(row.odGroupKey).push(row);
    });
    return map;
}

function findLegRowForPoo(legRowsByFlight, flightId, poo) {
    const rows = legRowsByFlight.get(String(flightId)) || [];
    return rows.find((row) => normalizeStation(row.poo) === normalizeStation(poo)) || null;
}

function validateTraffic(rows) {
    const issues = [];

    rows.forEach((row) => {
        if (row.pax < 0) {
            issues.push({
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "pax",
                code: "LESS_THAN_ZERO",
                message: `${describeRow(row)}: Pax cannot be less than 0`,
            });
        }
        if (row.cargoT < 0) {
            issues.push({
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "cargoT",
                code: "LESS_THAN_ZERO",
                message: `${describeRow(row)}: Cargo T cannot be less than 0`,
            });
        }
        if (row.pax > row.maxPax) {
            issues.push({
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "pax",
                code: "EXCEEDS_MAX",
                message: `${describeRow(row)}: Pax exceeds Max Pax (${row.pax} > ${row.maxPax})`,
            });
        }
        if (row.cargoT > row.maxCargoT) {
            issues.push({
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "cargoT",
                code: "EXCEEDS_MAX",
                message: `${describeRow(row)}: Cargo T exceeds Max Cargo T (${row.cargoT} > ${row.maxCargoT})`,
            });
        }
    });

    if (issues.length) {
        throw buildValidationError(issues);
    }
}

function rebalanceLegRow(row, paxDelta, cargoDelta) {
    row.pax = roundToWhole(row.pax - paxDelta);
    row.cargoT = roundToTwo(row.cargoT - cargoDelta);
    return recalculateRevenue(row);
}

function assertBucketAvailability(row, paxDelta, cargoDelta, context) {
    if (paxDelta > 0 && row.pax < paxDelta) {
        throw buildValidationError([
            {
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "pax",
                code: "BALANCING_BUCKET_OVERDRAWN",
                message: `${context}: balancing Pax bucket ${describeBucket(row)} only has ${row.pax}, requested ${paxDelta}`,
            },
        ]);
    }
    if (cargoDelta > 0 && row.cargoT < cargoDelta) {
        throw buildValidationError([
            {
                rowId: String(row._id || ""),
                rowKey: row.rowKey,
                field: "cargoT",
                code: "BALANCING_BUCKET_OVERDRAWN",
                message: `${context}: balancing Cargo bucket ${describeBucket(row)} only has ${row.cargoT}, requested ${cargoDelta}`,
            },
        ]);
    }
}

function applyTrafficUpdates(stateRows, requestedEdits) {
    const rowsById = new Map(stateRows.map((row) => [String(row._id), row]));
    const legRowsByFlight = getLegRowsByFlight(stateRows);
    const groupRowsByOdKey = getGroupRowsByOdKey(stateRows);

    requestedEdits.forEach((requested) => {
        const current = rowsById.get(String(requested._id));
        if (!current) {
            throw new Error("One or more POO rows were not found");
        }

        const original = { ...current };
        const edited = applyFieldEdits(current, requested);

        if (current.trafficType === TRAFFIC_TYPES.LEG) {
            const pairedLeg = (legRowsByFlight.get(String(current.flightId)) || []).find(
                (row) => String(row._id) !== String(current._id)
            );
            if (!pairedLeg) {
                throw new Error(`Missing balancing leg row for flight ${current.flightNumber}`);
            }

            const paxDelta = roundToWhole(edited.pax - original.pax);
            const cargoDelta = roundToTwo(edited.cargoT - original.cargoT);

            assertBucketAvailability(
                pairedLeg,
                paxDelta,
                cargoDelta,
                `${describeRow(edited)} cannot be updated`
            );

            rowsById.set(String(edited._id), edited);
            Object.assign(current, edited);
            Object.assign(pairedLeg, rebalanceLegRow(pairedLeg, paxDelta, cargoDelta));
            return;
        }

        if (
            current.trafficType === TRAFFIC_TYPES.BEHIND ||
            current.trafficType === TRAFFIC_TYPES.BEYOND ||
            current.trafficType === TRAFFIC_TYPES.TRANSIT_FL ||
            current.trafficType === TRAFFIC_TYPES.TRANSIT_SL
        ) {
            const groupRows = groupRowsByOdKey.get(current.odGroupKey) || [];
            const firstRow = groupRows.find(
                (row) =>
                    row.trafficType === TRAFFIC_TYPES.BEHIND ||
                    row.trafficType === TRAFFIC_TYPES.TRANSIT_FL
            );
            const secondRow = groupRows.find(
                (row) =>
                    row.trafficType === TRAFFIC_TYPES.BEYOND ||
                    row.trafficType === TRAFFIC_TYPES.TRANSIT_SL
            );

            if (!firstRow || !secondRow) {
                throw new Error(`Incomplete OD group ${current.od}`);
            }

            const desiredPax = edited.pax;
            const desiredCargoT = edited.cargoT;
            const paxDelta = roundToWhole(desiredPax - firstRow.pax);
            const cargoDelta = roundToTwo(desiredCargoT - firstRow.cargoT);

            const updatedFirst = applyFieldEdits(firstRow, {
                ...requested,
                pax: desiredPax,
                cargoT: desiredCargoT,
            });
            const updatedSecond = applyFieldEdits(secondRow, {
                ...requested,
                pax: desiredPax,
                cargoT: desiredCargoT,
            });

            Object.assign(firstRow, updatedFirst);
            Object.assign(secondRow, updatedSecond);

            const originLeg = findLegRowForPoo(
                legRowsByFlight,
                firstRow.flightId,
                firstRow.odOrigin
            );
            const destinationLeg = findLegRowForPoo(
                legRowsByFlight,
                secondRow.flightId,
                firstRow.odDestination
            );

            if (!originLeg || !destinationLeg) {
                throw new Error(`Missing balancing leg buckets for ${current.od}`);
            }

            assertBucketAvailability(
                originLeg,
                paxDelta,
                cargoDelta,
                `${describeRow(updatedFirst)} cannot be updated`
            );
            assertBucketAvailability(
                destinationLeg,
                paxDelta,
                cargoDelta,
                `${describeRow(updatedSecond)} cannot be updated`
            );

            Object.assign(originLeg, rebalanceLegRow(originLeg, paxDelta, cargoDelta));
            Object.assign(destinationLeg, rebalanceLegRow(destinationLeg, paxDelta, cargoDelta));
            return;
        }
    });

    const finalRows = [...rowsById.values()].map((row) => recalculateRevenue(row));
    validateTraffic(finalRows);
    return finalRows;
}

async function ensureTransitRows({ userId, date, transitDraft }) {
    if (!transitDraft || !transitDraft.poo) {
        return [];
    }

    const dayStart = moment(date).startOf("day").toDate();
    const dayEnd = moment(date).endOf("day").toDate();
    const [sectorMap, stations, rawRevenueConfig] = await Promise.all([
        getSectorInfoMap(userId),
        Station.find({ userId }).lean(),
        RevenueConfig.findOne({ userId }).lean(),
    ]);
    const stationCurrencyMap = buildStationCurrencyMap(stations);
    const revenueConfig = normalizeRevenueConfig(rawRevenueConfig || {});
    const fxRateMap = buildFxRateMap(revenueConfig.fxRates);
    const flightsById = new Map();

    const { firstFlight, secondFlight } = await resolveFlightsForTransitDraft({
        userId,
        date,
        transitDraft,
        flightsById,
    });

    if (!firstFlight || !secondFlight) {
        throw new Error("Transit flights were not found for the selected date");
    }

    const existingRecords = await PooTable.find({
        userId,
        date: { $gte: dayStart, $lte: dayEnd },
    }).lean();
    const existingRowsByKey = new Map(existingRecords.map((record) => [record.rowKey, record]));

    const firstSnapshot = buildFlightSnapshot(firstFlight, sectorMap);
    const secondSnapshot = buildFlightSnapshot(secondFlight, sectorMap);
    const transitRows = buildUserTransitRows({
        pagePoo: normalizeStation(transitDraft.poo),
        firstSnapshot,
        secondSnapshot,
        existingRowsByKey,
        pageCurrencyContext: resolveCurrencyContext({
            stationCurrencyMap,
            revenueConfig,
            fxRateMap,
            poo: transitDraft.poo,
            date,
        }),
        shouldReset: false,
    });

    const bulkOps = transitRows.map((row) => ({
        updateOne: {
            filter: { userId, rowKey: row.rowKey },
            update: { $set: row },
            upsert: true,
        },
    }));

    if (bulkOps.length) {
        await PooTable.bulkWrite(bulkOps, { ordered: true });
    }

    return PooTable.find({
        userId,
        rowKey: { $in: transitRows.map((row) => row.rowKey) },
    });
}

function buildApplySignature(row) {
    return {
        rowMatchKey: buildRowMatchKey(row),
        sourceSeats: parseNumber(row.sourceSeats),
        sourceCargoCapT: roundToTwo(row.sourceCargoCapT),
        sourcePaxLF: roundToTwo(row.sourcePaxLF),
        sourceCargoLF: roundToTwo(row.sourceCargoLF),
        std: String(row.std || ""),
        sta: String(row.sta || ""),
    };
}

function doesSignatureMatch(source, candidate) {
    return (
        source.rowMatchKey === buildRowMatchKey(candidate) &&
        source.sourceSeats === parseNumber(candidate.sourceSeats) &&
        source.sourceCargoCapT === roundToTwo(candidate.sourceCargoCapT) &&
        source.sourcePaxLF === roundToTwo(candidate.sourcePaxLF) &&
        source.sourceCargoLF === roundToTwo(candidate.sourceCargoLF) &&
        source.std === String(candidate.std || "") &&
        source.sta === String(candidate.sta || "")
    );
}

async function applyUpdatesForDate({ userId, updates }) {
    if (!updates.length) return [];

    const editIds = updates.map((item) => item._id);
    const rows = await PooTable.find({
        userId,
        _id: { $in: editIds },
    });

    if (rows.length !== editIds.length) {
        throw new Error("One or more target POO rows were not found");
    }

    const workingRows = await PooTable.find({
        userId,
        $or: [
            { _id: { $in: editIds } },
            { flightId: { $in: rows.map((row) => row.flightId) } },
            { connectedFlightId: { $in: rows.map((row) => row.flightId) } },
            { odGroupKey: { $in: rows.map((row) => row.odGroupKey).filter(Boolean) } },
        ],
    });

    const finalRows = applyTrafficUpdates(
        workingRows.map(createWorkingState),
        updates
    );

    const rowsToDelete = [];
    const rowsToPersist = [];
    const grouped = getGroupRowsByOdKey(finalRows);

    finalRows.forEach((row) => {
        if (row.trafficType === TRAFFIC_TYPES.TRANSIT_FL || row.trafficType === TRAFFIC_TYPES.TRANSIT_SL) {
            const pair = grouped.get(row.odGroupKey) || [];
            const totalPax = pair.reduce((sum, item) => sum + parseNumber(item.pax), 0);
            const totalCargo = pair.reduce((sum, item) => sum + parseNumber(item.cargoT), 0);
            if (totalPax === 0 && totalCargo === 0) {
                rowsToDelete.push(String(row._id));
                return;
            }
        }
        rowsToPersist.push(row);
    });

    if (rowsToPersist.length) {
        await PooTable.bulkWrite(
            rowsToPersist.map((row) => ({
                updateOne: {
                    filter: { _id: row._id, userId },
                    update: {
                        $set: {
                            pax: row.pax,
                            cargoT: row.cargoT,
                            legFare: row.legFare,
                            legRate: row.legRate,
                            odFare: row.odFare,
                            odRate: row.odRate,
                            prorateRatioL1: row.prorateRatioL1,
                            fareProrateRatioL1L2: row.fareProrateRatioL1L2,
                            rateProrateRatioL1L2: row.rateProrateRatioL1L2,
                            pooCcy: row.pooCcy,
                            pooCcyToRccy: row.pooCcyToRccy,
                            reportingCurrency: row.reportingCurrency,
                            stationCurrencySource: row.stationCurrencySource,
                            reportingCurrencySource: row.reportingCurrencySource,
                            applySSPricing: row.applySSPricing,
                            interline: row.interline,
                            codeshare: row.codeshare,
                            legPaxRev: row.legPaxRev,
                            legCargoRev: row.legCargoRev,
                            legTotalRev: row.legTotalRev,
                            odPaxRev: row.odPaxRev,
                            odCargoRev: row.odCargoRev,
                            odTotalRev: row.odTotalRev,
                            rccyLegPaxRev: row.rccyLegPaxRev,
                            rccyLegCargoRev: row.rccyLegCargoRev,
                            rccyLegTotalRev: row.rccyLegTotalRev,
                            rccyOdPaxRev: row.rccyOdPaxRev,
                            rccyOdCargoRev: row.rccyOdCargoRev,
                            rccyOdTotalRev: row.rccyOdTotalRev,
                            rccyPax: row.rccyPax,
                            rccyCargo: row.rccyCargo,
                            rccyTotalRev: row.rccyTotalRev,
                            fnlRccyPaxRev: row.fnlRccyPaxRev,
                            fnlRccyCargoRev: row.fnlRccyCargoRev,
                            fnlRccyTotalRev: row.fnlRccyTotalRev,
                        },
                    },
                },
            })),
            { ordered: true }
        );
    }

    if (rowsToDelete.length) {
        await PooTable.deleteMany({
            userId,
            _id: { $in: rowsToDelete },
        });
    }

    return PooTable.find({
        userId,
        _id: { $in: rowsToPersist.map((row) => row._id) },
    });
}

exports.getPooData = async (req, res) => {
    try {
        const userId = req.user.id;
        const {
            poo,
            date,
            flightNumber,
            sector,
            variant,
            trafficType,
            identifier,
        } = req.query;

        const filter = { userId };

        if (poo) filter.poo = normalizeStation(poo);
        if (date) {
            filter.date = {
                $gte: moment(date).startOf("day").toDate(),
                $lte: moment(date).endOf("day").toDate(),
            };
        }
        if (flightNumber) filter.flightNumber = flightNumber;
        if (sector) filter.sector = normalizeStation(sector);
        if (variant) filter.variant = variant;
        if (trafficType) filter.trafficType = trafficType;
        if (identifier) filter.identifier = identifier;

        const [records, station, rawRevenueConfig] = await Promise.all([
            PooTable.find(filter),
            poo ? Station.findOne({ userId, stationName: normalizeStation(poo) }).lean() : null,
            RevenueConfig.findOne({ userId }).lean(),
        ]);
        const revenueConfig = normalizeRevenueConfig(rawRevenueConfig || {});

        res.status(200).json({
            data: buildEditableResponse(records),
            meta: {
                selectedPoo: poo ? normalizeStation(poo) : "",
                stationCurrency: normalizeCurrencyCode(station?.currencyCode),
                reportingCurrency: revenueConfig.reportingCurrency,
            },
        });
    } catch (error) {
        console.error("🔥 Error fetching POO data:", error);
        res.status(500).json({ message: "Failed to fetch POO data", error: error.message });
    }
};

exports.populatePoo = async (req, res) => {
    try {
        const userId = req.user.id;
        const { poo, date } = req.body;

        if (!poo || !date) {
            return res.status(400).json({ message: "POO station and date are required" });
        }

        const dataset = await buildPooDataset({ userId, poo, date });

        if (!dataset.rows.length) {
            await PooTable.deleteMany({
                userId,
                date: { $gte: dataset.dayStart, $lte: dataset.dayEnd },
                poo: dataset.normalizedPoo,
            });
            return res.status(200).json({
                data: [],
                message: "No POO rows found for this station and date",
            });
        }

        await PooTable.bulkWrite(
            dataset.rows.map((row) => ({
                updateOne: {
                    filter: { userId, rowKey: row.rowKey },
                    update: { $set: row },
                    upsert: true,
                },
            })),
            { ordered: true }
        );

        await PooTable.deleteMany({
            userId,
            date: { $gte: dataset.dayStart, $lte: dataset.dayEnd },
            rowKey: { $nin: [...dataset.keepRowKeys] },
            $or: [
                { flightId: { $in: dataset.touchedFlightIds } },
                { connectedFlightId: { $in: dataset.touchedFlightIds } },
            ],
        });

        const savedRows = await PooTable.find({
            userId,
            poo: dataset.normalizedPoo,
            date: { $gte: dataset.dayStart, $lte: dataset.dayEnd },
        });

        res.status(200).json({
            data: buildEditableResponse(savedRows),
            message: `POO traffic allocation refreshed (${savedRows.length} rows)`,
        });
    } catch (error) {
        console.error("🔥 Error populating POO:", error);
        res.status(500).json({ message: "Failed to populate POO", error: error.message });
    }
};

exports.updatePooRecords = async (req, res) => {
    try {
        const userId = req.user.id;
        const {
            records = [],
            transitDraft = null,
            applyToDates = [],
        } = req.body;

        let createdTransitRows = [];
        if (transitDraft) {
            createdTransitRows = await ensureTransitRows({
                userId,
                date: transitDraft.date,
                transitDraft,
            });
        }

        const normalizedUpdates = [...records];
        if (transitDraft && createdTransitRows.length) {
            createdTransitRows.forEach((row) => {
                normalizedUpdates.push({
                    _id: row._id,
                    pax: transitDraft.pax ?? 0,
                    cargoT: transitDraft.cargoT ?? 0,
                    odFare: transitDraft.odFare ?? row.odFare,
                    odRate: transitDraft.odRate ?? row.odRate,
                    prorateRatioL1: transitDraft.prorateRatioL1 ?? row.prorateRatioL1,
                    fareProrateRatioL1L2: transitDraft.fareProrateRatioL1L2 ?? row.fareProrateRatioL1L2,
                    rateProrateRatioL1L2: transitDraft.rateProrateRatioL1L2 ?? row.rateProrateRatioL1L2,
                    pooCcy: transitDraft.pooCcy ?? row.pooCcy,
                    pooCcyToRccy: transitDraft.pooCcyToRccy ?? row.pooCcyToRccy,
                    applySSPricing: transitDraft.applySSPricing ?? row.applySSPricing,
                    interline: transitDraft.interline ?? row.interline,
                    codeshare: transitDraft.codeshare ?? row.codeshare,
                });
            });
        }

        if (!normalizedUpdates.length) {
            return res.status(400).json({ message: "No POO record updates provided" });
        }

        const refreshedRows = await applyUpdatesForDate({
            userId,
            updates: normalizedUpdates,
        });

        const appliedDates = [];
        const skippedDates = [];

        if (Array.isArray(applyToDates) && applyToDates.length) {
            const sourceRowsById = new Map(refreshedRows.map((row) => [String(row._id), row]));
            const sourceTargets = normalizedUpdates
                .map((update) => sourceRowsById.get(String(update._id)))
                .filter(Boolean)
                .map((row) => ({
                    signature: buildApplySignature(row),
                    payload: {
                        pax: row.pax,
                        cargoT: row.cargoT,
                        odFare: row.odFare,
                        odRate: row.odRate,
                        pooCcy: row.pooCcy,
                        pooCcyToRccy: row.pooCcyToRccy,
                        prorateRatioL1: row.prorateRatioL1,
                        fareProrateRatioL1L2: row.fareProrateRatioL1L2,
                        rateProrateRatioL1L2: row.rateProrateRatioL1L2,
                        applySSPricing: row.applySSPricing,
                        interline: row.interline,
                        codeshare: row.codeshare,
                    },
                }));

            for (const targetDate of applyToDates) {
                const targetRows = await PooTable.find({
                    userId,
                    date: {
                        $gte: moment(targetDate).startOf("day").toDate(),
                        $lte: moment(targetDate).endOf("day").toDate(),
                    },
                });

                const targetUpdates = [];
                sourceTargets.forEach((sourceRow) => {
                    const candidate = targetRows.find((row) => doesSignatureMatch(sourceRow.signature, row));
                    if (candidate) {
                        targetUpdates.push({
                            _id: candidate._id,
                            ...sourceRow.payload,
                        });
                    }
                });

                if (targetUpdates.length === sourceTargets.length && sourceTargets.length > 0) {
                    await applyUpdatesForDate({ userId, updates: targetUpdates });
                    appliedDates.push(targetDate);
                } else {
                    skippedDates.push(targetDate);
                }
            }
        }

        res.status(200).json({
            message: "POO traffic allocation updated successfully",
            data: buildEditableResponse(refreshedRows),
            appliedDates,
            skippedDates,
        });
    } catch (error) {
        console.error("🔥 Error updating POO records:", error);
        res.status(400).json({
            message: error.message || "Failed to update POO records",
            errors: Array.isArray(error.validationIssues) ? error.validationIssues : [],
        });
    }
};

exports.getRevenueConfig = async (req, res) => {
    try {
        const userId = req.user.id;
        const config = normalizeRevenueConfig(await RevenueConfig.findOne({ userId }).lean() || {});
        res.status(200).json({ success: true, data: config });
    } catch (error) {
        console.error("🔥 Error fetching revenue config:", error);
        res.status(500).json({ success: false, message: "Failed to fetch revenue config" });
    }
};

exports.saveRevenueConfig = async (req, res) => {
    try {
        const userId = req.user.id;
        const payload = normalizeRevenueConfig(req.body || {});
        const config = await RevenueConfig.findOneAndUpdate(
            { userId },
            { $set: payload },
            { upsert: true, new: true }
        );
        res.status(200).json({ success: true, data: normalizeRevenueConfig(config.toObject()) });
    } catch (error) {
        console.error("🔥 Error saving revenue config:", error);
        res.status(500).json({ success: false, message: "Failed to save revenue config" });
    }
};

exports.getRevenueData = async (req, res) => {
    try {
        const userId = req.user.id;
        const {
            mode = "aggregate",
            label,
            trafficClass,
            fromDate,
            toDate,
            from,
            to,
            flight,
            flightNumber,
            poo,
            od,
            odDI,
            legDI,
            sector,
            variant,
            userTag1,
            userTag2,
            trafficType,
            identifier,
            al,
            stop,
            stops,
            minTotalGcd,
            maxTotalGcd,
            minMaxPax,
            maxMaxPax,
            minMaxCargoT,
            maxMaxCargoT,
            minPax,
            maxPax,
            minCargoT,
            maxCargoT,
            minFare,
            maxFare,
            minRate,
            maxRate,
            timeInclLayover,
            groupBy = "poo",
            periodicity = "monthly",
        } = req.query;

        const match = { userId };
        const andClauses = [];

        if (fromDate && toDate) {
            andClauses.push({
                date: {
                    $gte: moment(fromDate).startOf("day").toDate(),
                    $lte: moment(toDate).endOf("day").toDate(),
                },
            });
        } else if (fromDate) {
            andClauses.push({
                date: { $gte: moment(fromDate).startOf("day").toDate() },
            });
        } else if (toDate) {
            andClauses.push({
                date: { $lte: moment(toDate).endOf("day").toDate() },
            });
        }

        if (poo) andClauses.push({ poo: { $in: poo.split(",").map(normalizeStation) } });
        if (od) andClauses.push({ od: { $in: od.split(",").map((item) => item.trim().toUpperCase()) } });
        if (odDI) andClauses.push({ odDI: { $in: odDI.split(",").map(normalizeDomIntl) } });
        if (legDI) andClauses.push({ legDI: { $in: legDI.split(",").map(normalizeDomIntl) } });
        if (sector) andClauses.push({ sector: { $in: sector.split(",").map((item) => item.trim().toUpperCase()) } });
        const normalizedDirectFlights = String(flightNumber || flight || "").split(",").map((item) => item.trim()).filter(Boolean);
        if (normalizedDirectFlights.length > 0) andClauses.push({ flightNumber: { $in: normalizedDirectFlights } });
        if (variant) andClauses.push({ variant: { $in: variant.split(",").map((item) => item.trim()) } });
        if (trafficType) andClauses.push({ trafficType: { $in: trafficType.split(",").map((item) => item.trim()) } });
        if (identifier) andClauses.push({ identifier: { $in: identifier.split(",").map((item) => item.trim()) } });
        if (al) andClauses.push({ al: { $in: al.split(",").map((item) => item.trim().toUpperCase()) } });

        const requestedStops = String(stop || stops || "")
            .split(",")
            .map((item) => String(item || "").trim())
            .filter(Boolean);
        const normalizedStops = requestedStops
            .map((item) => Number(item))
            .filter(Number.isFinite);
        const requestedBlankStop = requestedStops.includes(BLANK_OPTION_VALUE) || requestedStops.includes("(blank)");

        if (requestedBlankStop && normalizedStops.length > 0) {
            andClauses.push({
                $or: [
                    { stops: { $in: normalizedStops } },
                    { stops: null },
                    { stops: { $exists: false } },
                ],
            });
        } else if (requestedBlankStop) {
            andClauses.push({
                $or: [
                    { stops: null },
                    { stops: { $exists: false } },
                ],
            });
        } else if (normalizedStops.length > 0) {
            andClauses.push({ stops: { $in: normalizedStops } });
        }
        if (timeInclLayover) andClauses.push({ timeInclLayover: String(timeInclLayover).trim() });

        const numericRangeFilters = [
            ["totalGcd", minTotalGcd, maxTotalGcd],
            ["maxPax", minMaxPax, maxMaxPax],
            ["maxCargoT", minMaxCargoT, maxMaxCargoT],
            ["pax", minPax, maxPax],
            ["cargoT", minCargoT, maxCargoT],
        ];
        numericRangeFilters.forEach(([field, minValue, maxValue]) => {
            const range = {};
            if (minValue !== undefined && minValue !== "") range.$gte = Number(minValue);
            if (maxValue !== undefined && maxValue !== "") range.$lte = Number(maxValue);
            if (Object.keys(range).length > 0) andClauses.push({ [field]: range });
        });

        const fareRateRangeFilters = [
            ["odFare", minFare, maxFare],
            ["odRate", minRate, maxRate],
        ];
        fareRateRangeFilters.forEach(([field, minValue, maxValue]) => {
            const range = {};
            if (minValue !== undefined && minValue !== "") range.$gte = Number(minValue);
            if (maxValue !== undefined && maxValue !== "") range.$lte = Number(maxValue);
            if (Object.keys(range).length > 0) andClauses.push({ [field]: range });
        });

        const labelClauses = buildRevenueSelectionClauses(String(label || "").split(","), "label");
        if (labelClauses.length > 0) andClauses.push({ $or: labelClauses });

        const trafficClassClauses = buildRevenueSelectionClauses(String(trafficClass || "").split(","), "trafficClass");
        if (trafficClassClauses.length > 0) andClauses.push({ $or: trafficClassClauses });

        const dataFilterClauses = [];
        const normalizedFrom = String(from || "").split(",").map((item) => normalizeStation(item)).filter(Boolean);
        const normalizedTo = String(to || "").split(",").map((item) => normalizeStation(item)).filter(Boolean);
        const normalizedFlights = String(flight || flightNumber || "").split(",").map((item) => String(item || "").trim()).filter(Boolean);
        const normalizedVariants = String(variant || "").split(",").map((item) => String(item || "").trim()).filter(Boolean);
        const normalizedUserTag1 = String(userTag1 || "").split(",").map((item) => String(item || "").trim()).filter(Boolean);
        const normalizedUserTag2 = String(userTag2 || "").split(",").map((item) => String(item || "").trim()).filter(Boolean);

        if (normalizedFrom.length > 0) andClauses.push({ odOrigin: { $in: normalizedFrom } });
        if (normalizedTo.length > 0) andClauses.push({ odDestination: { $in: normalizedTo } });
        if (normalizedFlights.length > 0) dataFilterClauses.push({ flight: { $in: normalizedFlights } });
        if (normalizedVariants.length > 0) dataFilterClauses.push({ variant: { $in: normalizedVariants } });
        if (normalizedUserTag1.length > 0) dataFilterClauses.push({ userTag1: { $in: normalizedUserTag1 } });
        if (normalizedUserTag2.length > 0) dataFilterClauses.push({ userTag2: { $in: normalizedUserTag2 } });

        if (dataFilterClauses.length > 0) {
            const dataMatch = { userId };
            const normalizedRevenueLabel = normalizeRevenueLabel(label);

            if (String(label || "").trim().toLowerCase() !== "both" && normalizedRevenueLabel) {
                dataMatch.domINTL = normalizedRevenueLabel;
            }

            dataMatch.$and = dataFilterClauses;

            const matchingData = await Data.find(dataMatch).select("flight depStn arrStn sector");

            const allowedFlights = [...new Set(matchingData.map((row) => String(row.flight || "").trim()).filter(Boolean))];
            const allowedSectors = [...new Set(matchingData.map((row) => String(row.sector || "").trim().toUpperCase()).filter(Boolean))];

            if (matchingData.length === 0) {
                andClauses.push({ _id: null });
            }

            if (allowedFlights.length > 0) {
                andClauses.push({ flightNumber: { $in: allowedFlights } });
            }

            if (allowedSectors.length > 0) {
                andClauses.push({ sector: { $in: allowedSectors } });
            }
        }

        if (andClauses.length > 0) {
            match.$and = andClauses;
        }

        if (String(mode).toLowerCase() === "detail") {
            const rows = await PooTable.find(match)
                .sort({ date: 1, od: 1, sNo: 1 })
                .lean();

            const summary = rows.reduce((acc, row) => ({
                pax: roundToWhole(acc.pax + parseNumber(row.pax)),
                cargoT: roundToTwo(acc.cargoT + parseNumber(row.cargoT)),
                odTotalRev: roundToTwo(acc.odTotalRev + parseNumber(row.odTotalRev)),
                fnlRccyTotalRev: roundToTwo(acc.fnlRccyTotalRev + parseNumber(row.fnlRccyTotalRev)),
            }), { pax: 0, cargoT: 0, odTotalRev: 0, fnlRccyTotalRev: 0 });

            return res.status(200).json({
                mode: "detail",
                rows,
                summary,
            });
        }

        const groupByFields = String(groupBy || "poo")
            .split(",")
            .map(normalizeGroupByField)
            .filter(Boolean);
        const safeGroupByFields = groupByFields.length > 0 ? [...new Set(groupByFields)] : ["poo"];
        const groupKeyExpr = buildRevenueGroupKeyExpression(safeGroupByFields);

        let periodExpr;
        if (periodicity.toLowerCase() === "annually") {
            periodExpr = { $dateToString: { format: "%Y", date: "$date" } };
        } else if (periodicity.toLowerCase() === "quarterly") {
            periodExpr = {
                $concat: [
                    { $toString: { $year: "$date" } },
                    "-Q",
                    { $toString: { $ceil: { $divide: [{ $month: "$date" }, 3] } } },
                ],
            };
        } else if (periodicity.toLowerCase() === "weekly") {
            periodExpr = { $dateToString: { format: "%Y-W%V", date: "$date" } };
        } else if (periodicity.toLowerCase() === "daily") {
            periodExpr = { $dateToString: { format: "%Y-%m-%d", date: "$date" } };
        } else {
            periodExpr = { $dateToString: { format: "%Y-%m", date: "$date" } };
        }

        const results = await PooTable.aggregate([
            { $match: match },
            {
                $group: {
                    _id: {
                        groupKey: groupKeyExpr,
                        period: periodExpr,
                    },
                    pax: { $sum: "$pax" },
                    cargoT: { $sum: "$cargoT" },
                    legRev: { $sum: "$legTotalRev" },
                    odRev: { $sum: "$odTotalRev" },
                    paxRev: { $sum: "$odPaxRev" },
                    cargoRev: { $sum: "$odCargoRev" },
                    totalRev: { $sum: "$fnlRccyTotalRev" },
                    fnlRccyPaxRev: { $sum: "$fnlRccyPaxRev" },
                    fnlRccyCargoRev: { $sum: "$fnlRccyCargoRev" },
                    count: { $sum: 1 },
                },
            },
            { $sort: { "_id.groupKey": 1, "_id.period": 1 } },
        ]);

        const pivoted = {};
        const periods = new Set();

        results.forEach((result) => {
            const key = result._id.groupKey || "Unknown";
            const period = result._id.period;
            periods.add(period);
            if (!pivoted[key]) pivoted[key] = {};
            pivoted[key][period] = {
                pax: roundToTwo(result.pax),
                cargoT: roundToTwo(result.cargoT),
                legRev: roundToTwo(result.legRev),
                odRev: roundToTwo(result.odRev),
                paxRev: roundToTwo(result.paxRev),
                cargoRev: roundToTwo(result.cargoRev),
                totalRev: roundToTwo(result.totalRev),
                fnlRccyPaxRev: roundToTwo(result.fnlRccyPaxRev),
                fnlRccyCargoRev: roundToTwo(result.fnlRccyCargoRev),
                count: result.count,
            };
        });

        res.status(200).json({
            mode: "aggregate",
            data: pivoted,
            periods: [...periods].sort(),
            groupBy: safeGroupByFields.join(","),
            periodicity,
        });
    } catch (error) {
        console.error("🔥 Error fetching revenue data:", error);
        res.status(500).json({ message: "Failed to fetch revenue data", error: error.message });
    }
};

exports.deletePooRecords = async (req, res) => {
    try {
        const userId = req.user.id;
        const { ids } = req.body;

        if (!Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ message: "No record IDs provided" });
        }

        const result = await PooTable.deleteMany({
            userId,
            _id: { $in: ids },
        });

        res.status(200).json({
            message: `${result.deletedCount} POO rows deleted successfully`,
        });
    } catch (error) {
        console.error("🔥 Error deleting POO records:", error);
        res.status(500).json({ message: "Failed to delete records", error: error.message });
    }
};
