const mongoose = require("mongoose");
const moment = require("moment");
const ApuFuelConsumptionCost = require("../model/apuFuelConsumptionCostSchema");
const Flights = require("../model/flight");
const CostConfig = require("../model/costConfigSchema");
const {
  normalizeCostConfig,
} = require("../utils/costLogic");

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const trimString = (value) => (value === null || value === undefined ? "" : String(value).trim());

const normalizeValue = (value) => trimString(value).toUpperCase();

const getMonthKey = (date) => {
  const parsed = new Date(date);
  if (Number.isNaN(parsed.getTime())) return "";
  return `${String(parsed.getMonth() + 1).padStart(2, "0")}/${String(parsed.getFullYear()).slice(-2)}`;
};

const getMonthStart = (date) => new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
const getMonthEnd = (date) => new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));

const getApuUsageDateRange = (row = {}, flights = []) => {
  const from = row.fromDate ? new Date(row.fromDate) : null;
  const to = row.toDate ? new Date(row.toDate) : null;
  const validFrom = from && !Number.isNaN(from.getTime()) ? from : null;
  const validTo = to && !Number.isNaN(to.getTime()) ? to : null;
  const fallback = validFrom || validTo || flights.map((flight) => flight?.date ? new Date(flight.date) : null).find((date) => date && !Number.isNaN(date.getTime()));
  if (!fallback) return null;
  if (validFrom && validTo) return validFrom <= validTo ? { from: validFrom, to: validTo } : { from: validTo, to: validFrom };
  if (validFrom) return { from: validFrom, to: getMonthEnd(validFrom) };
  if (validTo) return { from: getMonthStart(validTo), to: validTo };
  return { from: getMonthStart(fallback), to: getMonthEnd(fallback) };
};

const getApuUsageMonths = (row = {}, flights = []) => {
  const range = getApuUsageDateRange(row, flights);
  if (!range) return [];
  const start = getMonthStart(range.from);
  const end = getMonthStart(range.to);
  const months = [];
  for (
    let cursor = new Date(start);
    cursor <= end;
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1))
  ) {
    const monthStart = getMonthStart(cursor);
    const monthEnd = getMonthEnd(cursor);
    const overlapStart = range.from > monthStart ? range.from : monthStart;
    const overlapEnd = range.to < monthEnd ? range.to : monthEnd;
    if (overlapStart <= overlapEnd) {
      months.push({
        monthStart,
        monthKey: getMonthKey(monthStart),
        days: Math.floor((overlapEnd - overlapStart) / 86400000) + 1,
      });
    }
  }
  return months;
};

const isWithinRange = (targetDate, fromValue, toValue) => {
  const target = targetDate ? new Date(targetDate) : null;
  if (!target || Number.isNaN(target.getTime())) return true;

  const from = fromValue ? new Date(fromValue) : null;
  const to = toValue ? new Date(toValue) : null;

  if (from && !Number.isNaN(from.getTime()) && target < from) return false;
  if (to && !Number.isNaN(to.getTime()) && target > to) return false;
  return true;
};

const getFlightRegistration = (flight) => trimString(flight?.aircraft?.registration || flight?.acftRegn || flight?.registration);
const getFlightArrStn = (flight) => normalizeValue(flight?.arrStn);
const getFlightVariant = (flight) => normalizeValue(flight?.variant || flight?.acftType);
const isAdditionalApuUseRow = (row) => normalizeValue(row?.addlnUse) === "Y";
const getApuUsageStation = (row) => trimString(row?.stn ?? row?.arrStn);
const getApuUsageHours = (row) => Number(row?.apuHrPerDay ?? row?.apuHours ?? row?.apuHr ?? 0);
const getApuUsageConsumptionRate = (row) => Number(
  row?.kgPerApuHr ?? row?.consumptionKgPerApuHr ?? row?.consumptionPerApuHour ?? 0
);

const scoreApuUsageRow = (row, flight) => {
  if (!isWithinRange(flight.date, row.fromDate, row.toDate)) return -1;

  const rowArr = normalizeValue(row.stn ?? row.arrStn);
  const rowVariant = normalizeValue(row.variant);
  const rowRegn = normalizeValue(row.acftRegn);
  const flightArr = getFlightArrStn(flight);
  const flightVariant = getFlightVariant(flight);
  const flightRegn = getFlightRegistration(flight);
  const additionalUse = isAdditionalApuUseRow(row);

  if (!additionalUse && rowArr && rowArr !== flightArr) return -1;
  if (rowVariant && rowVariant !== flightVariant) return -1;
  if (rowRegn && rowRegn !== flightRegn) return -1;

  return (rowArr ? 1000 : 0) + (rowRegn ? 200 : 0) + (rowVariant ? 100 : 0) + (row.fromDate || row.toDate ? 10 : 0);
};

const scoreFuelPriceRow = (row, flightMonth, depStn) => {
  const station = normalizeValue(row.station);
  const month = normalizeValue(row.month);
  if (station && station !== depStn) return -1;
  if (month && month !== normalizeValue(flightMonth)) return -1;
  return (station ? 1000 : 0) + (month ? 100 : 0);
};

const pickBest = (rows = [], scorer) => {
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

const roundToTwo = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? Math.round(num * 100) / 100 : 0;
};

const buildGeneratedApuFuelRow = (flight, costConfig, flights = []) => {
  const flightDate = flight?.date ? new Date(flight.date) : null;
  if (!flightDate || Number.isNaN(flightDate.getTime())) return null;

  const apuUsage = pickBest(costConfig.apuUsage || [], (row) => scoreApuUsageRow(row, flight));
  const additionalUse = isAdditionalApuUseRow(apuUsage);
  const apuStation = apuUsage ? getApuUsageStation(apuUsage) : trimString(flight?.arrStn || "");
  const priceDate = additionalUse ? (apuUsage?.fromDate || apuUsage?.toDate || flightDate) : flightDate;
  const fuelPrice = pickBest(costConfig.ccyFuel || [], (row) => scoreFuelPriceRow(
    row,
    getMonthKey(priceDate),
    normalizeValue(apuStation)
  ));

  const apuHr = apuUsage ? getApuUsageHours(apuUsage) : 0;
  const consumptionKgPerApuHr = apuUsage ? getApuUsageConsumptionRate(apuUsage) : 0;
  const consumptionKg = apuHr * consumptionKgPerApuHr;
  const kgPerLtr = fuelPrice ? Number(fuelPrice.kgPerLtr || 0) : 0;
  const intoPlaneRate = fuelPrice ? Number(fuelPrice.intoPlaneRate || 0) : 0;
  const consumptionLitres = kgPerLtr > 0 ? consumptionKg / kgPerLtr : 0;
  const costPerLtr = intoPlaneRate > 0 ? intoPlaneRate / 1000 : 0;
  const totalFuelCost = consumptionLitres * costPerLtr;
  const hasMatch = Boolean(apuUsage && fuelPrice);

  return {
    rowKey: String(flight._id || `${flight.flight || ""}-${flightDate.toISOString()}`),
    date: flightDate,
    stn: apuStation,
    arrStn: apuStation,
    acftRegn: getFlightRegistration(flight),
    apun: trimString(flight?.apun || flight?.aircraft?.apun || ""),
    apuHr: roundToTwo(apuHr),
    apuHrPerDay: roundToTwo(apuHr),
    consumptionKgPerApuHr: roundToTwo(consumptionKgPerApuHr),
    kgPerApuHr: roundToTwo(consumptionKgPerApuHr),
    consumptionKg: roundToTwo(consumptionKg),
    consumptionLitres,
    costPerLtr,
    totalFuelCost,
    currency: trimString(fuelPrice?.ccy || apuUsage?.ccy || ""),
    costSourceType: fuelPrice ? "STN_MONTH" : "UNMATCHED",
    costSourceStation: apuStation,
    sourceFlightId: String(flight?._id || ""),
    remarks: hasMatch ? "" : "No matching APU usage or fuel price row found",
    monthKey: getMonthKey(flightDate),
  };
};

const buildGeneratedApuFuelRows = (flights = [], costConfig = {}) => {
  const rows = [];
  (costConfig.apuUsage || []).forEach((apuUsage, usageIndex) => {
    const apuStation = getApuUsageStation(apuUsage);
    const apuHr = getApuUsageHours(apuUsage);
    const consumptionKgPerApuHr = getApuUsageConsumptionRate(apuUsage);
    if (!apuStation || apuHr <= 0 || consumptionKgPerApuHr <= 0) return;

    getApuUsageMonths(apuUsage, flights).forEach(({ monthStart, monthKey, days }) => {
      const fuelPrice = pickBest(costConfig.ccyFuel || [], (row) => scoreFuelPriceRow(
        row,
        monthKey,
        normalizeValue(apuStation)
      ));
      const matchedFlights = flights.filter((flight) => {
        if (getMonthKey(flight?.date) !== monthKey) return false;
        if (apuUsage.variant && normalizeValue(apuUsage.variant) !== getFlightVariant(flight)) return false;
        if (apuUsage.acftRegn && normalizeValue(apuUsage.acftRegn) !== normalizeValue(getFlightRegistration(flight))) return false;
        if (!isAdditionalApuUseRow(apuUsage) && normalizeValue(apuStation) !== getFlightArrStn(flight)) return false;
        return true;
      });
      const aircraftKeys = [...new Set(matchedFlights.map(getFlightRegistration).map(trimString).filter(Boolean))];
      const targetAircraft = apuUsage.acftRegn ? [trimString(apuUsage.acftRegn)] : aircraftKeys;
      targetAircraft.forEach((acftRegn) => {
        const consumptionKg = apuHr * days * consumptionKgPerApuHr;
        const kgPerLtr = fuelPrice ? Number(fuelPrice.kgPerLtr || 0) : 0;
        const intoPlaneRate = fuelPrice ? Number(fuelPrice.intoPlaneRate || 0) : 0;
        const consumptionLitres = kgPerLtr > 0 ? consumptionKg / kgPerLtr : 0;
        const costPerLtr = intoPlaneRate > 0 ? intoPlaneRate / 1000 : 0;
        const totalFuelCost = fuelPrice ? consumptionLitres * costPerLtr : 0;
        rows.push({
          rowKey: `apu-${usageIndex}-${acftRegn || "NA"}-${monthKey}-${normalizeValue(apuStation)}`,
          date: monthStart,
          stn: apuStation,
          arrStn: apuStation,
          acftRegn,
          apun: "",
          apuHr: roundToTwo(apuHr),
          apuHrPerDay: roundToTwo(apuHr),
          consumptionKgPerApuHr: roundToTwo(consumptionKgPerApuHr),
          kgPerApuHr: roundToTwo(consumptionKgPerApuHr),
          consumptionKg: roundToTwo(consumptionKg),
          consumptionLitres,
          costPerLtr,
          totalFuelCost,
          currency: trimString(fuelPrice?.ccy || apuUsage?.ccy || ""),
          costSourceType: fuelPrice ? "STN_MONTH" : "UNMATCHED",
          costSourceStation: apuStation,
          sourceFlightId: "",
          remarks: fuelPrice ? "" : "No matching station-month fuel price row found",
          monthKey,
        });
      });
    });
  });

  if (rows.length > 0) return rows;
  return flights.map((flight) => buildGeneratedApuFuelRow(flight, costConfig, flights)).filter(Boolean);
};

const normalizeApuFuelRow = (record, userId) => {
  const date = record?.date ? new Date(record.date) : null;
  const apuHr = toNumber(record?.apuHr ?? record?.apuHrPerDay ?? record?.apuHours);
  const consumptionKgPerApuHr = toNumber(
    record?.consumptionKgPerApuHr ?? record?.kgPerApuHr ?? record?.consumptionPerApuHr ?? record?.consumptionPerApuHour
  );
  const consumptionKg = toNumber(record?.consumptionKg) || (apuHr > 0 ? apuHr * consumptionKgPerApuHr : 0);
  const consumptionLitres = toNumber(record?.consumptionLitres);
  const costPerLtr = toNumber(record?.costPerLtr ?? record?.costLtr);
  const totalFuelCost = toNumber(record?.totalFuelCost) || (consumptionLitres > 0 ? consumptionLitres * costPerLtr : 0);
  const rowKey = trimString(record?.rowKey || record?._id || record?.id || new mongoose.Types.ObjectId().toHexString());
  const stn = trimString(record?.stn ?? record?.arrStn);
  const acftRegn = trimString(record?.acftRegn);
  const apun = trimString(record?.apun);
  const monthKey = date && !Number.isNaN(date.getTime()) ? moment.utc(date).format("YYYY-MM") : "";

  return {
    rowKey,
    userId: String(userId),
    date: date && !Number.isNaN(date.getTime()) ? date : new Date(),
    stn,
    arrStn: stn,
    acftRegn,
    apun,
    apuHr,
    apuHrPerDay: apuHr,
    consumptionKgPerApuHr,
    kgPerApuHr: consumptionKgPerApuHr,
    consumptionKg,
    consumptionLitres,
    costPerLtr,
    totalFuelCost,
    currency: trimString(record?.currency || record?.ccy || "INR") || "INR",
    costSourceType: trimString(record?.costSourceType || (stn ? "STN_MONTH" : "UNMATCHED")) || "STN_MONTH",
    costSourceStation: trimString(record?.costSourceStation || record?.depStnOfLastFlight || ""),
    sourceFlightId: trimString(record?.sourceFlightId || record?.flightId || ""),
    remarks: trimString(record?.remarks || record?.note || ""),
    monthKey,
  };
};

const withApuFuelAliases = (record) => {
  if (!record) return record;
  const stn = trimString(record.stn ?? record.arrStn);
  const apuHr = toNumber(record.apuHr ?? record.apuHrPerDay);
  const consumptionKgPerApuHr = toNumber(record.consumptionKgPerApuHr ?? record.kgPerApuHr);
  return {
    ...record,
    stn,
    arrStn: trimString(record.arrStn || stn),
    apuHr,
    apuHrPerDay: toNumber(record.apuHrPerDay || apuHr),
    consumptionKgPerApuHr,
    kgPerApuHr: toNumber(record.kgPerApuHr || consumptionKgPerApuHr),
  };
};

exports.getApuFuelCosts = async (req, res) => {
  try {
    const userId = req.user.id;
    const records = await ApuFuelConsumptionCost.find({ userId: String(userId) })
      .sort({ date: 1, createdAt: 1 })
      .lean();

    res.status(200).json({ success: true, data: records.map(withApuFuelAliases) });
  } catch (error) {
    console.error("Error fetching APU fuel costs:", error);
    res.status(500).json({ success: false, message: "Failed to fetch APU fuel costs.", error: error.message });
  }
};

exports.bulkSaveApuFuelCosts = async (req, res) => {
  try {
    const userId = req.user.id;
    const rows = req.body.apuFuelData || req.body.apuFuelRows || req.body.rows;

    if (!Array.isArray(rows)) {
      return res.status(400).json({ success: false, message: "Invalid payload. Expected an array of records." });
    }

    const bulkOperations = rows.map((record) => {
      const normalized = normalizeApuFuelRow(record, userId);
      return {
        updateOne: {
          filter: { userId: String(userId), rowKey: normalized.rowKey },
          update: { $set: normalized },
          upsert: true,
        },
      };
    });

    if (bulkOperations.length > 0) {
      await ApuFuelConsumptionCost.bulkWrite(bulkOperations, { ordered: false });
    }

    const savedRows = await ApuFuelConsumptionCost.find({ userId: String(userId) })
      .sort({ date: 1, createdAt: 1 })
      .lean();

    res.status(200).json({
      success: true,
      message: "APU fuel consumption & cost table saved successfully.",
      data: savedRows.map(withApuFuelAliases),
    });
  } catch (error) {
    console.error("Error saving APU fuel costs:", error);
    res.status(500).json({ success: false, message: "Failed to save APU fuel costs.", error: error.message });
  }
};

exports.rebuildApuFuelCosts = async (req, res) => {
  try {
    const userId = String(req.user.id);
    const [configDoc, flights] = await Promise.all([
      CostConfig.findOne({ userId }).lean(),
      Flights.find({ userId }).lean(),
    ]);

    const costConfig = normalizeCostConfig(configDoc || {});
    const generatedRows = buildGeneratedApuFuelRows(flights, costConfig);

    if (generatedRows.length > 0) {
      const bulkOperations = generatedRows.map((row) => ({
        updateOne: {
          filter: { userId, rowKey: row.rowKey },
          update: { $set: { ...row, userId } },
          upsert: true,
        },
      }));
      await ApuFuelConsumptionCost.bulkWrite(bulkOperations, { ordered: false });
    }

    const existingRowKeys = new Set(generatedRows.map((row) => row.rowKey));
    await ApuFuelConsumptionCost.deleteMany({
      userId,
      rowKey: { $nin: Array.from(existingRowKeys) },
    });

    const savedRows = await ApuFuelConsumptionCost.find({ userId }).sort({ date: 1, createdAt: 1 }).lean();

    res.status(200).json({
      success: true,
      message: "APU fuel consumption & cost table rebuilt successfully.",
      count: savedRows.length,
      data: savedRows.map(withApuFuelAliases),
    });
  } catch (error) {
    console.error("Error rebuilding APU fuel costs:", error);
    res.status(500).json({ success: false, message: "Failed to rebuild APU fuel costs.", error: error.message });
  }
};

exports.__private__ = {
  buildGeneratedApuFuelRow,
  buildGeneratedApuFuelRows,
  scoreApuUsageRow,
  scoreFuelPriceRow,
  getMonthKey,
};
