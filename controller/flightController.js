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
const csv = require("csvtojson");
const xlsx = require("xlsx");
const exceljs = require("exceljs");
const { getJsDateFromExcel } = require("excel-date-to-js");
const jwt = require("jsonwebtoken");
const secretKey = "HelloCableBuddy";
const CSV = require("csv-parser");
const fs = require("fs");
const mongoose = require('mongoose');
const moment = require("moment-timezone");
require("dotenv").config();
const { DateTime } = require('luxon');
const { isValidObjectId, Types } = require("mongoose");
const Connections = require("../model/connectionSchema");
const CostConfig = require("../model/costConfigSchema");
const { normalizeCostConfig, computeFlightCostsBatch } = require("../utils/costLogic");
const { buildMaintenanceReserveContext } = require("../utils/maintenanceReserveContext");
const { scopedUserQuery } = require("./accessScope");

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
  regexForFindingSuperset,
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

const getFlights = async (req, res) => {
  try {
    // Get pagination parameters from the query (default to page 1, 10 rows per page)
    const { page = 1, limit = 10 } = req.query;
    const query = scopedUserQuery(req, { isComplete: true });

    const data = await Flights.find(query)
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
    const {
      flight, depStn, std, bt, sta, arrStn, variant, date, day, rotations,
      seats, cargoT, dist, pax, ask, rsk, cargoAtk, cargoRtk, domIntl,
      userTag1, userTag2, remarks1, remarks2, acftType, page, limit,
    } = req.body;

    // Build a query object
    let query = {};

    // Add filters based on request parameters
    const addRegexFilter = (field, value) => {
      if (value) query[field] = { $regex: value, $options: 'i' };
    };


    // 🚀 THE BULLETPROOF ACFT FILTER
    if (acftType) {
      // We ONLY search the fields that actually exist in your new schema
      const orConditions = [
        { 'aircraft.registration': { $regex: acftType, $options: 'i' } },
        { acftType: { $regex: acftType, $options: 'i' } }
      ];

      // If the user typed a number (like '1' or '1021'), also search the exact MSN
      if (!isNaN(acftType) && acftType.trim() !== '') {
        orConditions.push({ 'aircraft.msn': Number(acftType) });
      }

      query.$or = orConditions;
    }

    addRegexFilter('flight', flight);
    addRegexFilter('depStn', depStn);
    addRegexFilter('std', std);
    addRegexFilter('bt', bt);
    addRegexFilter('sta', sta);
    addRegexFilter('arrStn', arrStn);
    addRegexFilter('variant', variant);
    addRegexFilter('day', day);
    addRegexFilter('rotations', rotations);
    addRegexFilter('seats', seats);
    addRegexFilter('cargoT', cargoT);
    addRegexFilter('dist', dist);
    addRegexFilter('pax', pax);
    addRegexFilter('ask', ask);
    addRegexFilter('rsk', rsk);
    addRegexFilter('cargoAtk', cargoAtk);
    addRegexFilter('cargoRtk', cargoRtk);
    addRegexFilter('domIntl', domIntl);
    addRegexFilter('userTag1', userTag1);
    addRegexFilter('userTag2', userTag2);
    addRegexFilter('remarks1', remarks1);
    addRegexFilter('remarks2', remarks2);

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
    const data = await Flights.find(query).skip(skip).limit(limitNum);
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
    const dowRegex = regexForFindingSuperset(dow);
    // Parse dates — no UTC offset correction needed (dates are already calendar strings)
    const fromDate = new Date(effFromDate);
    fromDate.setUTCHours(0, 0, 0, 0);
    const toDate = new Date(effToDate);
    toDate.setUTCHours(23, 59, 59, 999);
    // For the effFromDt / effToDt overlap filter, use the raw ISO dates
    const formattedFromDate = fromDate.toISOString();
    const formattedToDate = toDate.toISOString();
    let filter = {
      userId: id,
      isComplete: true,
      $or: [{ rotationNumber: { $exists: false } }, { rotationNumber: null }],
      variant: selectedVariant,
      effFromDt: { $lte: new Date(formattedToDate) },
      effToDt: { $gte: new Date(formattedFromDate) },
      dow: { $regex: dowRegex, $options: 'i' }
    };

    const dowNums = dow.split('').map(Number); // e.g. [1,2,3,4,5]
    const daysOfWeekStrings = dowNums.map(day => {
      switch (day) {
        case 1:
          return "Mon";
        case 2:
          return "Tue";
        case 3:
          return "Wed";
        case 4:
          return "Thu";
        case 5:
          return "Fri";
        case 6:
          return "Sat";
        case 7:
          return "Sun";
        default:
          return null;
      }
    }).filter(Boolean);

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
