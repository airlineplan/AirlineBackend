const mongoose = require("mongoose");
const moment = require("moment");
const ApuFuelConsumptionCost = require("../model/apuFuelConsumptionCostSchema");
const Flights = require("../model/flight");
const CostConfig = require("../model/costConfigSchema");
const {
  normalizeCostConfig,
  getApuFuelPriceSourceFlight,
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
const getFlightDepStn = (flight) => normalizeValue(flight?.depStn);
const getFlightVariant = (flight) => normalizeValue(flight?.variant || flight?.acftType);
const isAdditionalApuUseRow = (row) => normalizeValue(row?.addlnUse) === "Y";

const scoreApuUsageRow = (row, flight) => {
  if (!isWithinRange(flight.date, row.fromDate, row.toDate)) return -1;

  const rowArr = normalizeValue(row.stn ?? row.arrStn);
  const rowVariant = normalizeValue(row.variant);
  const rowRegn = normalizeValue(row.acftRegn);
  const flightArr = getFlightArrStn(flight);
  const flightVariant = getFlightVariant(flight);
  const flightRegn = getFlightRegistration(flight);

  if (rowArr && rowArr !== flightArr) return -1;
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
  const priceSourceFlight = getApuFuelPriceSourceFlight(flight, flights, apuUsage || {});
  const fuelPrice = pickBest(costConfig.ccyFuel || [], (row) => scoreFuelPriceRow(
    row,
    getMonthKey(priceSourceFlight?.date || flightDate),
    getFlightDepStn(priceSourceFlight || flight)
  ));
  const additionalUse = isAdditionalApuUseRow(apuUsage);

  const apuHr = apuUsage ? Number(apuUsage.apuHrPerDay || 0) : 0;
  const consumptionKgPerApuHr = apuUsage ? Number(apuUsage.kgPerApuHr || 0) : 0;
  const consumptionKg = apuHr * consumptionKgPerApuHr;
  const kgPerLtr = fuelPrice ? Number(fuelPrice.kgPerLtr || 0) : 0;
  const intoPlaneRate = fuelPrice ? Number(fuelPrice.intoPlaneRate || 0) : 0;
  const consumptionLitres = kgPerLtr > 0 ? consumptionKg / kgPerLtr : 0;
  const costPerLtr = intoPlaneRate > 0 ? intoPlaneRate / 1000 : 0;
  const totalFuelCost = consumptionLitres * costPerLtr;
  const hasMatch = Boolean(apuUsage || fuelPrice);

  return {
    rowKey: String(flight._id || `${flight.flight || ""}-${flightDate.toISOString()}`),
    date: flightDate,
    stn: additionalUse ? "" : trimString(flight?.arrStn || ""),
    acftRegn: getFlightRegistration(flight),
    apun: trimString(flight?.apun || flight?.aircraft?.apun || ""),
    apuHr: roundToTwo(apuHr),
    consumptionKgPerApuHr: roundToTwo(consumptionKgPerApuHr),
    consumptionKg: roundToTwo(consumptionKg),
    consumptionLitres,
    costPerLtr,
    totalFuelCost,
    currency: trimString(fuelPrice?.ccy || apuUsage?.ccy || ""),
    costSourceType: additionalUse
      ? "LAST_DEP_STN_RCCY"
      : (fuelPrice ? "DEP_STN_MONTH" : "UNMATCHED"),
    costSourceStation: trimString(getFlightDepStn(priceSourceFlight || flight) || flight?.depStn || ""),
    sourceFlightId: String((priceSourceFlight || flight)?._id || flight._id || ""),
    remarks: hasMatch ? "" : "No matching APU usage or fuel price row found",
    monthKey: getMonthKey(flightDate),
  };
};

const buildGeneratedApuFuelRows = (flights = [], costConfig = {}) => {
  return flights
    .map((flight) => buildGeneratedApuFuelRow(flight, costConfig, flights))
    .filter(Boolean);
};

const normalizeApuFuelRow = (record, userId) => {
  const date = record?.date ? new Date(record.date) : null;
  const apuHr = toNumber(record?.apuHr ?? record?.apuHours);
  const consumptionKgPerApuHr = toNumber(
    record?.consumptionKgPerApuHr ?? record?.consumptionPerApuHr ?? record?.consumptionPerApuHour
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
    acftRegn,
    apun,
    apuHr,
    consumptionKgPerApuHr,
    consumptionKg,
    consumptionLitres,
    costPerLtr,
    totalFuelCost,
    currency: trimString(record?.currency || record?.ccy || "INR") || "INR",
    costSourceType: trimString(record?.costSourceType || (stn ? "ARR_STN" : "LAST_DEP_STN_RCCY")) || "ARR_STN",
    costSourceStation: trimString(record?.costSourceStation || record?.depStnOfLastFlight || ""),
    sourceFlightId: trimString(record?.sourceFlightId || record?.flightId || ""),
    remarks: trimString(record?.remarks || record?.note || ""),
    monthKey,
  };
};

exports.getApuFuelCosts = async (req, res) => {
  try {
    const userId = req.user.id;
    const records = await ApuFuelConsumptionCost.find({ userId: String(userId) })
      .sort({ date: 1, createdAt: 1 })
      .lean();

    res.status(200).json({ success: true, data: records });
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
      data: savedRows,
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
      data: savedRows,
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
