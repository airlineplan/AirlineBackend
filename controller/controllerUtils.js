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



const timeZoneCorrectedDates = (date, tzString) => {
  if (date) {
    return new Date((typeof date === "string" ? new Date(date) : date).toLocaleString("en-US", { timeZone: tzString }));
  } else {
    // Handle the case where date is undefined
    return null; // or whatever you want to return in this case
  }
}
function isValidFlightNumber(flightNumber) {
  console.log(flightNumber);
  const maxFlightNumberLength = 8;
  const isValid = flightNumber.trim().length <= maxFlightNumberLength;
  return isValid;
}
function isValidDepStn(depStn) {
  const alphanumericRegex = /^[a-zA-Z0-9]{1,4}$/;
  return alphanumericRegex.test(depStn);
}

function isValidArrStn(arrStn) {
  const alphanumericRegex = /^[a-zA-Z0-9]{1,4}$/;
  return alphanumericRegex.test(arrStn);
}

function isValidVariant(variant) {
  const alphanumericWithSpecialCharsRegex = /^[a-zA-Z0-9 -]{1,8}$/;
  return alphanumericWithSpecialCharsRegex.test(variant);
}
function isValidDow(dow) {
  const numericRegex = /^[1-7]{1,7}$/;
  return numericRegex.test(dow);
}

function processExcelRow(row) {
  function convertDecimalTimeToHours(decimalTime) {
    if (typeof decimalTime === "string") {
      decimalTime = decimalTime.replace(/( AM| PM)/g, "");
      const formattedTime = decimalTime.replace(/:\d{2}(?=\D|$)/, "");
      return formattedTime;
    }

    const hoursInDay = 24;
    const hours = Math.floor(decimalTime * hoursInDay);
    const minutes = Math.round((decimalTime * hoursInDay - hours) * 60);

    const formattedHours = hours < 10 ? `0${hours}` : `${hours}`;
    const formattedMinutes = minutes < 10 ? `0${minutes}` : `${minutes}`;

    return `${formattedHours}:${formattedMinutes}`;
  }
  return {
    flight: row["Flight #"],
    depStn: row["Dep Stn"],
    std: convertDecimalTimeToHours(row["STD (LT)"]),
    bt: convertDecimalTimeToHours(row["BT"]),
    sta: convertDecimalTimeToHours(row["STA(LT)"]),
    arrStn: row["Arr Stn"],
    variant: row["Variant"],
    effFromDt: getJsDateFromExcel(row["Eff from Dt"]),
    effToDt: getJsDateFromExcel(row["Eff to Dt"]),
    dow: row["DoW"],
    domINTL: row["Dom / INTL"],
    userTag1: row["User Tag 1"],
    userTag2: row["User Tag 2"],
    remarks1: row["Remarks 1"],
    remarks2: row["Remarks 2"],
    gcd: row["GCD"],
    paxCapacity: row["Pax Capacity"],
    cargoCapT: row["Cargo Cap T"],
    paxSF: row["Pax SF%"],
    cargoLF: row["Cargo LF%"],
  };
}
const deleteConnections = async (ids) => {
  //code for connection deletions
  const flightsToBeDeleted = await Flights.find({
    networkId: { $in: ids },
  });

  const deletedFlightData = await Flights.deleteMany({
    networkId: { $in: ids },
  });

  // Extract the IDs of the flights to be deleted
  const deletedFlightIds = flightsToBeDeleted.map(flight => flight._id);

  // Update beyondODs arrays in other flights
  await Flights.updateMany(
    { beyondODs: { $in: deletedFlightIds } },
    { $pullAll: { beyondODs: deletedFlightIds } }
  );

  // Update behindODs arrays in other flights
  await Flights.updateMany(
    { behindODs: { $in: deletedFlightIds } },
    { $pullAll: { behindODs: deletedFlightIds } }
  );


}
function regexForFindingSuperset(inputString) {
  // Create an array of positive lookahead assertions for each letter in the input string
  const lookaheads = inputString.split('').map(letter => `(?=.*${letter})`).join('');

  // Combine the lookaheads with the start and end of string anchors
  const regexPattern = `^${lookaheads}.*$`;

  // Return the regex pattern as a string
  return regexPattern;
}
function calculateTimeDifference(time1, time2) {
  const [hour1, minute1] = time1.split(":").map(Number);
  const [hour2, minute2] = time2.split(":").map(Number);

  let differenceInMinutes = (hour2 * 60 + minute2) - (hour1 * 60 + minute1);

  // Handling negative difference (i.e., crossing over to the previous day)
  if (differenceInMinutes < 0) {
    differenceInMinutes += 24 * 60; // Add a day's worth of minutes
  }

  const differenceHours = Math.floor(differenceInMinutes / 60);
  const paddedHours = differenceHours.toString().padStart(2, '0'); // Ensure hours are 2 digits with leading zero if needed
  const differenceMinutes = differenceInMinutes % 60;
  const paddedMinutes = differenceMinutes.toString().padStart(2, '0'); // Ensure minutes are 2 digits with leading zero if needed

  return `${paddedHours}:${paddedMinutes}`;
}

function convertTimeToTZ(originalTime, originalUTCOffset, targetUTCOffset) {
  // Extract hours and minutes from original time
  const [originalHours, originalMinutes] = originalTime.split(':').map(Number);

  // Extract hours and minutes from UTC offsets
  const originalOffsetSign = originalUTCOffset.startsWith('UTC-') ? -1 : 1;
  const targetOffsetSign = targetUTCOffset.startsWith('UTC-') ? -1 : 1;
  const originalOffsetHours = Number(originalUTCOffset.split(':')[0].slice(4)) * originalOffsetSign;
  const originalOffsetMinutes = Number(originalUTCOffset.split(':')[1]) * originalOffsetSign;
  const targetOffsetHours = Number(targetUTCOffset.split(':')[0].slice(4)) * targetOffsetSign;
  const targetOffsetMinutes = Number(targetUTCOffset.split(':')[1]) * targetOffsetSign;

  // Convert time from original timezone to UTC
  let utcHours = originalHours - originalOffsetHours;
  let utcMinutes = originalMinutes - originalOffsetMinutes;

  // Convert time from UTC to target timezone
  let targetHours = utcHours + targetOffsetHours;
  let targetMinutes = utcMinutes + targetOffsetMinutes;

  // Handle overflow and underflow of minutes
  if (targetMinutes >= 60) {
    targetHours += 1;
    targetMinutes -= 60;
  } else if (targetMinutes < 0) {
    targetHours -= 1;
    targetMinutes += 60;
  }

  // Handle overflow and underflow of hours
  targetHours = (targetHours + 24) % 24;

  // Format the result
  const convertedTime = `${targetHours < 10 ? '0' : ''}${targetHours}:${targetMinutes < 10 ? '0' : ''}${targetMinutes}`;

  return convertedTime;
}

function parseTimeString(timeString) {
  const [hours, minutes] = timeString.split(':').map(Number);
  return new Date(0, 0, 0, hours, minutes); // Month and year are set to 0, day to 0 is equivalent to the previous day
}

function compareTimes(time1, time2) {
  const date1 = parseTimeString(time1);
  const date2 = parseTimeString(time2);
  return date1.getTime() - date2.getTime();
}

function timeToMinutes(timeString) {
  const [hours, minutes] = timeString.split(':').map(Number);
  return hours * 60 + minutes;
}


function addTimeStrings(time1, time2, time3 = '00:00') {
  // Function to convert time string to minutes
  function timeToMinutes(timeString) {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
  }

  // Function to convert minutes to time string
  function minutesToTime(totalMinutes) {
    const hours = Math.floor(totalMinutes / 60);
    const paddedHours = hours.toString().padStart(2, '0'); // Ensure hours are 2 digits with leading zero if needed
    const minutes = totalMinutes % 60;
    const paddedMinutes = minutes.toString().padStart(2, '0'); // Ensure minutes are 2 digits with leading zero if needed
    return `${paddedHours}:${paddedMinutes}`;
  }

  // Convert time strings to total minutes
  const totalMinutes = timeToMinutes(time1) + timeToMinutes(time2) + timeToMinutes(time3);

  // Convert total minutes back to time string
  const resultTime = minutesToTime(totalMinutes);

  return resultTime;
}

function addDays(date, days) {
  const result = new Date(date); // Create a new Date object to avoid modifying the original date
  result.setUTCHours(0, 0, 0, 0);
  result.setUTCDate(result.getUTCDate() + days); // Keep date math stable across server timezones
  return result; // Return the new date object
}
// Helper function to normalize date to UTC midnight
const normalizeDate = (date) => {
  const d = new Date(date);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime();
};

// Binary Search Helper
const binarySearchByStd = (arr, targetTime, findStart) => {
  let low = 0;
  let high = arr.length - 1;
  let result = -1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const cmp = compareTimes(arr[mid].std, targetTime);
    if (cmp === 0) {
      result = mid;
      if (findStart) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    } else if (cmp < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  if (result === -1) {
    if (findStart) {
      if (low < arr.length && compareTimes(arr[low].std, targetTime) >= 0) {
        return low;
      }
      return -1;
    } else {
      if (high >= 0 && compareTimes(arr[high].std, targetTime) <= 0) {
        return high;
      }
      return -1;
    }
  }

  return result;
};
function roundToLastDateOfNextQuarter(date) {
  const workingDate = new Date(date);
  workingDate.setUTCHours(0, 0, 0, 0);

  // Determine the current quarter
  const currentMonth = workingDate.getUTCMonth();
  const currentQuarter = Math.floor(currentMonth / 3); // Quarters are 0-based

  // Calculate the first month of the next quarter
  const firstMonthOfNextQuarter = (currentQuarter + 1) * 3;

  // Set the date to the first day of the next quarter and subtract one day to get the last day of the current quarter
  workingDate.setUTCMonth(firstMonthOfNextQuarter, 1);
  workingDate.setUTCDate(workingDate.getUTCDate() - 1);

  return workingDate;
}

function generateQuarterlyDates(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);
  currentDate.setUTCHours(0, 0, 0, 0);
  const boundaryEndDate = roundToLastDateOfNextQuarter(endDate);

  while (currentDate <= boundaryEndDate) {
    const currentMonth = currentDate.getUTCMonth();

    // Check the current quarter and add the last day accordingly
    if (currentMonth >= 0 && currentMonth < 3) {
      // First quarter, end date is March 31
      currentDate = new Date(Date.UTC(currentDate.getUTCFullYear(), 2, 31));
    } else if (currentMonth >= 3 && currentMonth < 6) {
      // Second quarter, end date is June 30
      currentDate = new Date(Date.UTC(currentDate.getUTCFullYear(), 5, 30));
    } else if (currentMonth >= 6 && currentMonth < 9) {
      // Third quarter, end date is September 30
      currentDate = new Date(Date.UTC(currentDate.getUTCFullYear(), 8, 30));
    } else {
      // Fourth quarter, end date is December 31
      currentDate = new Date(Date.UTC(currentDate.getUTCFullYear(), 11, 31));
    }



    if (currentDate <= boundaryEndDate) {
      periods.push(new Date(currentDate));

    }

    // Move to the next quarter's start date
    currentDate.setUTCMonth(currentDate.getUTCMonth() + 1);
  }

  return periods;
}

function roundToLastDateOfPresentYear(date) {
  const workingDate = new Date(date);
  workingDate.setUTCHours(0, 0, 0, 0);

  // Set the date to December 31st of the current year
  workingDate.setUTCMonth(11, 31);

  return workingDate;
}

function generateAnnualDates(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);
  currentDate.setUTCHours(0, 0, 0, 0);
  const boundaryEndDate = roundToLastDateOfPresentYear(endDate);
  while (currentDate <= boundaryEndDate) {
    // Calculate the last day of the current year (December 31st)
    const lastDayOfYear = new Date(Date.UTC(currentDate.getUTCFullYear(), 11, 31));

    // Push the last day of the year to the periods array
    periods.push(new Date(lastDayOfYear));

    // Move to the next year's start date
    currentDate.setUTCFullYear(currentDate.getUTCFullYear() + 1);
  }

  return periods;
}

function generateLastDayOfMonths(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);
  currentDate.setUTCHours(0, 0, 0, 0);
  const boundaryEndDate = new Date(endDate);
  boundaryEndDate.setUTCHours(0, 0, 0, 0);

  while (currentDate <= boundaryEndDate) {
    const year = currentDate.getUTCFullYear();
    const month = currentDate.getUTCMonth();
    const lastDayOfMonth = new Date(Date.UTC(year, month + 1, 0)); // Set to the last day of the current month

    // Push the last day of the month to the periods array
    periods.push(lastDayOfMonth);

    // Move to the next month's start date
    currentDate.setUTCMonth(month + 1);
    currentDate.setUTCDate(1);
  }

  return periods;
}

function generateWeeklyDates(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  start.setUTCHours(0, 0, 0, 0);
  end.setUTCHours(0, 0, 0, 0);
  const dates = [];

  // Loop through the dates from start to end
  for (let current = new Date(start); current <= end; current.setUTCDate(current.getUTCDate() + 1)) {
    // Check if the current day is a Sunday (day 0)
    if (current.getUTCDay() === 0) {
      // Push the current date to the array
      dates.push(new Date(current));
    }
  }

  // Check if the endDate is not a Sunday
  if (end.getUTCDay() !== 0) {
    // Find the next Sunday after endDate
    const nextSunday = new Date(end);
    nextSunday.setUTCDate(end.getUTCDate() + (7 - end.getUTCDay()));
    dates.push(nextSunday);
  }

  return dates;
}


function generateDailyDates(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  start.setUTCHours(0, 0, 0, 0);
  end.setUTCHours(0, 0, 0, 0);
  const dates = [];

  // Loop through the dates from start to end
  for (let current = new Date(start); current <= end; current.setUTCDate(current.getUTCDate() + 1)) {
    // Push the current date to the array
    dates.push(new Date(current));
  }

  return dates;
}

function isTimeInRange(time, minTime, maxTime) {
  const timeAsMinutes = convertTimeStringToMinutes(time);
  const minTimeAsMinutes = convertTimeStringToMinutes(minTime);
  const maxTimeAsMinutes = convertTimeStringToMinutes(maxTime);

  return timeAsMinutes >= minTimeAsMinutes && timeAsMinutes <= maxTimeAsMinutes;
}

// Function to convert time string to minutes
function convertTimeStringToMinutes(time) {
  if (time) {
    const [hours, minutes] = time.split(":").map(Number);
    return hours * 60 + minutes;
  } else {
    // Handle the case where time is undefined
    console.error('Time is undefined');
    return null; // Or another suitable value
  }
}

// Function to filter flights based on string time comparison
function filterFlightsByTimeRange(flights, minTime, maxTime) {
  return flights.filter((flight) =>
    isTimeInRange(flight.std, minTime, maxTime)
  );
}

function calculateTime(baseTime, offset) {
  // Split hours and minutes from the time strings
  const [baseHours, baseMinutes] = baseTime.split(':').map(Number);
  const [offsetHours, offsetMinutes] = offset.split(':').map(Number);

  // Calculate the total minutes
  let totalMinutes = baseHours * 60 + baseMinutes + offsetHours * 60 + offsetMinutes;

  // Calculate the new hours and minutes
  const newHours = Math.floor(totalMinutes / 60);
  const newMinutes = totalMinutes % 60;

  // Format the result
  const result = `${String(newHours).padStart(2, '0')}:${String(newMinutes).padStart(2, '0')}`;

  return result;
}

function addTimeStrings(time1, time2, time3 = '00:00') {
  // Function to convert time string to minutes
  function timeToMinutes(timeString) {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
  }

  // Function to convert minutes to time string
  function minutesToTime(totalMinutes) {
    const hours = Math.floor(totalMinutes / 60);
    const paddedHours = hours.toString().padStart(2, '0'); // Ensure hours are 2 digits with leading zero if needed
    const minutes = totalMinutes % 60;
    const paddedMinutes = minutes.toString().padStart(2, '0'); // Ensure minutes are 2 digits with leading zero if needed
    return `${paddedHours}:${paddedMinutes}`;
  }

  // Convert time strings to total minutes
  const totalMinutes = timeToMinutes(time1) + timeToMinutes(time2) + timeToMinutes(time3);

  // Convert total minutes back to time string
  const resultTime = minutesToTime(totalMinutes);

  return resultTime;
}

const sub24Hours = (timeString) => {
  const [hours, minutes] = timeString.split(':').map(Number);
  let newHours = hours - 24;
  if (newHours < 0) newHours += 24;
  return `${newHours < 10 ? '0' : ''}${newHours}:${minutes < 10 ? '0' : ''}${minutes}`;
};

function timeToMinutes(timeString) {
  const [hours, minutes] = timeString.split(':').map(Number);
  return hours * 60 + minutes;
}
const parseUTCOffsetToMinutes = (tz) => {
  const sign = tz.includes("+") ? 1 : -1;
  const [h, m] = tz.replace("UTC", "").replace("+", "").replace("-", "").split(":").map(Number);
  return sign * ((h || 0) * 60 + (m || 0));
};

module.exports = {
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
  startOfUtcDay: (date) => {
    const d = new Date(date);
    d.setUTCHours(0, 0, 0, 0);
    return d;
  },
  endOfUtcDay: (date) => {
    const d = new Date(date);
    d.setUTCHours(23, 59, 59, 999);
    return d;
  },
  addUtcDays: (date, days) => {
    const d = new Date(date);
    d.setUTCHours(0, 0, 0, 0);
    d.setUTCDate(d.getUTCDate() + days);
    return d;
  },
  getUtcDayOfWeek: (date) => new Date(date).getUTCDay(),
  roundToLastDateOfPresentYear,
  roundToLastDateOfNextQuarter,
  isValidDepStn
};
