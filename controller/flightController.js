const User = require("../model/userSchema");
const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const DataHistory = require("../model/dataHistorySchema");
const SectorHistory = require("../model/sectorHistorySchema");
const Flights = require("../model/flight");
const FlightHistory = require("../model/flightHistory")
const RotationSummary = require("../model/rotationSummary");
const RotationDetails = require("../model/rotationDetails");
const Stations = require("../model/stationSchema");
const StationsHistory = require("../model/stationHistorySchema");
const xlsx = require("xlsx");
const exceljs = require("exceljs");
const { getJsDateFromExcel } = require("excel-date-to-js");
const jwt = require("jsonwebtoken");
const secretKey = "HelloCableBuddy";
const fs = require("fs");
const mongoose = require('mongoose');
const moment = require("moment-timezone");
require("dotenv").config();
const { isValidObjectId, Types } = require("mongoose");
const Connections = require("../model/connectionSchema");
const CostConfig = require("../model/costConfigSchema");
const { normalizeCostConfig, computeCategoryFlightCostsBatch: computeFlightCostsBatch } = require("../utils/costLogic");
const { categoryFilter } = require("../utils/flightCategory");
const { buildMaintenanceReserveContext } = require("../utils/maintenanceReserveContext");
const { scopedUserQuery } = require("./accessScope");
const { dayNamesForDow, utcDateOnly } = require("../utils/rotationDates");

const createConnections = require('../helper/createConnections');


moment.tz.setDefault("America/New_York");



const {
  timeToMinutes,
  parseTimeString,
  isValidDow,
  compareTimes,
  isValidArrStn,
  sub24Hours,
  isValidVariant,
  generateLastDayOfMonths,
  generateAnnualDates,
  binarySearchByStd,
  calculateTimeDifference,
  generateWeeklyDates,
  isTimeInRange,
  generateDailyDates,
  processExcelRow,
  filterFlightsByTimeRange,
  normalizeDate,
  convertTimeToTZ,
  isValidFlightNumber,
  addDays,
  timeZoneCorrectedDates,
  calculateTime,
  addTimeStrings,
  deleteConnections,
  convertTimeStringToMinutes,
  parseUTCOffsetToMinutes,
  generateQuarterlyDates,
  roundToLastDateOfPresentYear,
  roundToLastDateOfNextQuarter,
  isValidDepStn
} = require('./controllerUtils');

const FLIGHT_PAGE_SORT = { date: 1, flight: 1 };

const escapeRegExp = (value) => String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Request key -> flight field for text "contains" filters.
const SEARCH_TEXT_FIELDS = {
  flight: 'flight', depStn: 'depStn', std: 'std', bt: 'bt', ft: 'ft', sta: 'sta', arrStn: 'arrStn',
  sector: 'sector', variant: 'variant', day: 'day', rotations: 'rotationNumber', domIntl: 'domIntl',
  userTag1: 'userTag1', userTag2: 'userTag2', remarks1: 'remarks1', remarks2: 'remarks2',
};

// Request key -> flight field for numeric "contains" filters.
const SEARCH_NUMERIC_FIELDS = {
  bh: 'bh', fh: 'fh', seats: 'seats', CargoCapT: 'CargoCapT', dist: 'dist', pax: 'pax',
  CargoT: 'CargoT', cargoT: 'CargoT', ask: 'ask', rsk: 'rsk', cargoAtk: 'cargoAtk', cargoRtk: 'cargoRtk',
};

const getFlights = async (req, res) => {
  try {
    // Get pagination parameters from the query (default to page 1, 10 rows per page)
    const { page = 1, limit = 10 } = req.query;
    const query = scopedUserQuery(req, { isComplete: true });

    const data = await Flights.find(query)
      .sort(FLIGHT_PAGE_SORT) // Stable order so rows don't repeat or vanish between pages
      .allowDiskUse(true)
      .skip((page - 1) * limit) // Skip documents for previous pages
      .limit(Number(limit)); // Limit results to `limit`

    // Get the total count for pagination
    const total = await Flights.countDocuments(query);

    console.log("Query finished, page : " + page + " data length : " + data.length);

    res.json({ data, total }); // Send both data and total count
  } catch (error) {
    console.error("Error occurred in getFlights:", error); // Detailed error log
    res.status(500).json({ error: "Internal server error" });
  }
};
const searchFlights = async (req, res) => {
  try {
    const { date, acftType, page, limit } = req.body;

    // Build a query object
    let query = categoryFilter(req.body.category);

    // "Contains" filters. User input is escaped so characters like "(" or "+"
    // are matched literally instead of failing the query.
    const addRegexFilter = (field, value) => {
      if (value) query[field] = { $regex: escapeRegExp(value), $options: 'i' };
    };
    // $regex never matches numbers, so numeric columns are matched on their text form.
    const numericClauses = [];
    const addNumericContainsFilter = (field, value) => {
      if (value === undefined || value === null || value === '') return;
      numericClauses.push({
        $expr: {
          $regexMatch: {
            input: { $toString: `$${field}` },
            regex: escapeRegExp(value),
            options: 'i',
          },
        },
      });
    };


    // 🚀 THE BULLETPROOF ACFT FILTER
    if (acftType) {
      // We ONLY search the fields that actually exist in your new schema
      const orConditions = [
        { 'aircraft.registration': { $regex: escapeRegExp(acftType), $options: 'i' } },
        { acftType: { $regex: escapeRegExp(acftType), $options: 'i' } }
      ];

      // If the user typed a number (like '1' or '1021'), also search the exact MSN
      if (!isNaN(acftType) && acftType.trim() !== '') {
        orConditions.push({ 'aircraft.msn': Number(acftType) });
      }

      query.$or = orConditions;
    }

    // Request keys match the FLGTs column keys; a few legacy aliases map to schema fields.
    Object.entries(SEARCH_TEXT_FIELDS).forEach(([bodyKey, field]) => addRegexFilter(field, req.body[bodyKey]));
    Object.entries(SEARCH_NUMERIC_FIELDS).forEach(([bodyKey, field]) => addNumericContainsFilter(field, req.body[bodyKey]));
    if (numericClauses.length > 0) query.$and = numericClauses;

    // Date filter
    if (date) {
      const formattedDate = moment(date, ['DD-MMM-YY', 'DD/MM/YYYY']).format('YYYY-MM-DD');
      query.date = formattedDate;
    }

    // Add user ID and isComplete filters
    query = scopedUserQuery(req, query);
    query.isComplete = true;

    // Pagination
    const skip = (page - 1) * limit;
    const limitNum = Number(limit);

    // Fetch flights
    const data = await Flights.find(query).sort(FLIGHT_PAGE_SORT).allowDiskUse(true).skip(skip).limit(limitNum);
    const costConfig = normalizeCostConfig(await CostConfig.findOne({ userId: req.user.id }).lean() || {});
    const mrContext = await buildMaintenanceReserveContext(req.user.id, data);
    const enrichedData = computeFlightCostsBatch(
      data.map((flgt) => flgt.toObject()),
      { ...costConfig, ...mrContext }
    );

    // Get total count
    const total = await Flights.countDocuments(query);

    res.status(200).json({ data: enrichedData, total });
  } catch (error) {
    console.error('Error searching flights:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};
const getFlightsWoRotations = async (req, res) => {
  try {
    const id = req.user.id;

    const {
      allowedDeptStn,
      allowedStdLt,
      selectedVariant,
      effToDate,
      effFromDate,
      dow,
      page = 1,
      limit = 8,
      filters = {},
      sort = {},
    } = req.body;
    // Flights are already expanded into dated occurrences. Query those occurrence
    // dates directly instead of also requiring copied network effective-date/DOW
    // fields, which are absent on some legacy occurrences.
    const fromDate = utcDateOnly(effFromDate);
    const toDate = utcDateOnly(effToDate);
    if (!fromDate || !toDate || fromDate > toDate) {
      return res.status(400).json({ error: "Invalid rotation effective date range" });
    }
    toDate.setUTCHours(23, 59, 59, 999);
    let filter = {
      userId: id,
      isComplete: true,
      $or: [
        { rotationNumber: { $exists: false } },
        { rotationNumber: null },
        { rotationNumber: "" },
      ],
      variant: selectedVariant,
    };

    const daysOfWeekStrings = dayNamesForDow(dow);
    if (daysOfWeekStrings.length === 0) {
      return res.status(400).json({ error: "Invalid rotation DOW" });
    }

    filter.date = { $gte: fromDate, $lte: toDate };
    filter.day = { $in: daysOfWeekStrings };

    // Add optional filters if available
    if (allowedDeptStn) {
      filter.depStn = allowedDeptStn;
    }
    if (allowedStdLt) {
      filter.std = { $gte: allowedStdLt };
    }

    const escapeRegex = (value) => String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const filterFieldMap = {
      day: "day",
      flight: "flight",
      depStn: "depStn",
      std: "std",
      bt: "bt",
      sta: "sta",
      arrStn: "arrStn",
      variant: "variant",
    };
    const additionalFilters = [];

    Object.entries(filters || {}).forEach(([key, value]) => {
      if (!value) return;
      const trimmedValue = String(value).trim();
      if (!trimmedValue) return;

      if (key === "date") {
        const parsedDate = new Date(trimmedValue);
        if (!Number.isNaN(parsedDate.getTime())) {
          parsedDate.setUTCHours(0, 0, 0, 0);
          const nextDate = new Date(parsedDate);
          nextDate.setUTCDate(nextDate.getUTCDate() + 1);
          additionalFilters.push({ date: { $gte: parsedDate, $lt: nextDate } });
        }
        return;
      }

      const field = filterFieldMap[key];
      if (field) {
        additionalFilters.push({ [field]: { $regex: escapeRegex(trimmedValue), $options: "i" } });
      }
    });
    if (additionalFilters.length > 0) {
      filter.$and = additionalFilters;
    }

    const sortFieldMap = {
      date: "date",
      day: "day",
      flight: "flight",
      depStn: "depStn",
      std: "std",
      bt: "bt",
      sta: "sta",
      arrStn: "arrStn",
      variant: "variant",
    };
    const sortField = sortFieldMap[sort.column] || "flight";
    const sortDirection = sort.direction === "Down" ? -1 : 1;
    const sortQuery = sortField === "flight"
      ? { flight: sortDirection, date: 1 }
      : { [sortField]: sortDirection, flight: 1, date: 1 };

    const pageNumber = Math.max(parseInt(page, 10) || 1, 1);
    const pageSize = Math.min(Math.max(parseInt(limit, 10) || 8, 1), 100);
    const skip = (pageNumber - 1) * pageSize;

    const [data, total] = await Promise.all([
      Flights.find(filter)
        .select("date day flight depStn std bt sta arrStn variant timeZone")
        .sort(sortQuery)
        .skip(skip)
        .limit(pageSize)
        .lean(),
      Flights.countDocuments(filter),
    ]);


    let timeZone;
    if (Array.isArray(data) && data.length > 0) {
      timeZone = data[0].timeZone;
    }

    res.status(200).json({
      data,
      timeZone,
      pagination: {
        page: pageNumber,
        limit: pageSize,
        total,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      }
    });
  } catch (error) {
    console.error(error);

    res.status(500).json({ error: "Internal server error" });
  }
};

module.exports = {
  getFlights,
  searchFlights,
  getFlightsWoRotations
};
