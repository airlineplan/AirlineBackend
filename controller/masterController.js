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

const getVariants = async (req, res) => {
  try {
    const userId = req.user.id;

    // Fetch distinct values for the "variant" field from the Data model
    const distinctVariants = await Data.aggregate([
      { $match: { userId: userId } }, // Filter by user ID
      { $group: { _id: null, variant: { $addToSet: '$variant' } } },
      { $project: { _id: 0, variant: 1 } },
    ]);

    // Format the options
    const formatOptions = (values) =>
      values.map((value) => ({ value: value, label: value }));

    // Get the distinct variants and format them
    const formattedVariants = formatOptions(distinctVariants[0]?.variant || []);

    res.json(formattedVariants);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};
const getMasterWeeks = async (req, res) => {
  try {
    const userId = req.user.id;

    if (!userId) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const result = await Flights.aggregate([
      {
        $match: { userId: userId }
      },
      {
        $group: {
          _id: null,
          minDate: { $min: "$date" },
          maxDate: { $max: "$date" }
        }
      }
    ]);

    if (!result.length || !result[0].minDate || !result[0].maxDate) {
      return res.json({ weeks: [] });
    }

    const { minDate, maxDate } = result[0];

    // 2️⃣ Convert to Date objects
    const startDate = new Date(minDate);
    const endDate = new Date(maxDate);

    // Normalize time
    startDate.setHours(0, 0, 0, 0);
    endDate.setHours(0, 0, 0, 0);

    // 3️⃣ Find first Sunday >= startDate
    const firstSunday = new Date(startDate);
    const day = firstSunday.getDay(); // 0 = Sunday

    if (day !== 0) {
      firstSunday.setDate(firstSunday.getDate() + (7 - day));
    }

    // 4️⃣ Generate all Sundays until endDate
    const weeks = [];
    let current = new Date(firstSunday);

    while (current <= endDate) {
      weeks.push(current.toISOString().split("T")[0]);
      current.setDate(current.getDate() + 7);
    }

    return res.json({ weeks });

  } catch (error) {
    console.error("Error fetching master weeks:", error);
    return res.status(500).json({ message: "Failed to fetch master weeks" });
  }
};

module.exports = {
  getVariants,
  getMasterWeeks
};
