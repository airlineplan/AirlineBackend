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
    const id = req.user.id;

    // Get pagination parameters from the query (default to page 1, 10 rows per page)
    const { page = 1, limit = 10 } = req.query;

    const data = await Flights.find({ userId: id, isComplete: true })
      .skip((page - 1) * limit) // Skip documents for previous pages
      .limit(Number(limit)); // Limit results to `limit`

    // Get the total count for pagination
    const total = await Flights.countDocuments({ userId: id, isComplete: true });

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
      userTag1, userTag2, remarks1, remarks2, page, limit,
    } = req.body;

    // Build a query object
    let query = {};

    // Add filters based on request parameters
    const addRegexFilter = (field, value) => {
      if (value) query[field] = { $regex: value, $options: 'i' };
    };

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
    query.userId = req.user.id;
    query.isComplete = true;

    // Pagination
    const skip = (page - 1) * limit;
    const limitNum = Number(limit);

    // Fetch flights
    const data = await Flights.find(query).skip(skip).limit(limitNum);

    // Get total count
    const total = await Flights.countDocuments(query);

    res.status(200).json({ data, total });
  } catch (error) {
    console.error('Error searching flights:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};
const getFlightsWoRotations = async (req, res) => {
  try {
    const id = req.user.id;

    const { allowedDeptStn, allowedStdLt, selectedVariant, effToDate, effFromDate, dow } = req.body;

    const dowRegex = regexForFindingSuperset(dow);

    // Correction for from Date
    const fromDate = new Date(effFromDate);
    fromDate.setUTCDate(fromDate.getUTCDate() + 1);
    fromDate.setUTCHours(0, 0, 0, 0);
    const formattedFromDate = fromDate.toISOString().replace(/\.\d{3}Z$/, "+00:00");

    // Correction for To Date
    const toDate = new Date(effToDate);
    toDate.setUTCDate(toDate.getUTCDate() + 1);
    toDate.setUTCHours(0, 0, 0, 0);
    const formattedToDate = toDate.toISOString().replace(/\.\d{3}Z$/, "+00:00");

    let filter = {
      userId: id,
      isComplete: true,
      $or: [{ rotationNumber: { $exists: false } }, { rotationNumber: null }],
      variant: selectedVariant,
      effFromDt: { $lte: formattedFromDate },
      effToDt: { $gte: formattedToDate },
      dow: { $regex: dowRegex, $options: 'i' }
    };

    const datesArray = [];

    // Iterate through each date between fromDate and toDate
    for (let date = fromDate; date <= toDate; date.setDate(date.getDate() + 1)) {
      const dayOfWeek = date.getDay(); // 0 for Sunday, 1 for Monday, ..., 6 for Saturday

      // Check if the dayOfWeek matches any selectedDow
      if (dow.includes(String(dayOfWeek))) {
        datesArray.push(new Date(date)); // Add the date to the array
      }
    }

    // Add filter for flight dates based on dateArray
    filter.date = { $in: datesArray };

    // Add optional filters if available
    if (allowedDeptStn) {
      filter.depStn = allowedDeptStn;
    }
    if (allowedStdLt) {
      filter.std = { $gte: allowedStdLt };
    }


    const data = await Flights.find(filter).sort({ flight: 1, date: 1 });


    let timeZone;
    if (Array.isArray(data) && data.length > 0) {
      timeZone = data[0].timeZone;
    }

    if (timeZone) {
      startDate = timeZoneCorrectedDates(startDate, timeZone);
      endDate = timeZoneCorrectedDates(endDate, timeZone);
    }

    res.status(200).json({ data, timeZone });
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
