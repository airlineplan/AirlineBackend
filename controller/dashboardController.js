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

const populateDashboardDropDowns = async (req, res) => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(400).json({ message: "User ID missing" });
    }

    const distinctSectors = await Flights.aggregate([
      { $match: { userId: userId } },
      { $group: { _id: null, sector: { $addToSet: "$sector" } } },
      { $project: { _id: 0, sector: 1 } },
    ]);

    const distinctValues = await Data.aggregate([
      { $match: { userId: userId } },
      {
        $group: {
          _id: null,
          from: { $addToSet: "$depStn" },
          to: { $addToSet: "$arrStn" },
          variant: { $addToSet: "$variant" },
          userTag1: { $addToSet: "$userTag1" },
          userTag2: { $addToSet: "$userTag2" },
        },
      },
      {
        $project: {
          _id: 0,
          from: 1,
          to: 1,
          variant: 1,
          userTag1: 1,
          userTag2: 1,
        },
      },
    ]);

    const formatOptions = (values = []) =>
      values
        .filter((v) => v !== null && v !== undefined && v !== "")
        .map((value) => ({
          value,
          label: value,
        }));

    // Safe extraction
    const sectorList = distinctSectors?.[0]?.sector ?? [];
    const dataValues = distinctValues?.[0] ?? {};

    const filteredSectors = sectorList.filter(
      (sector) => sector !== "undefined-undefined"
    );

    const data = {
      from: formatOptions(dataValues.from ?? []),
      to: formatOptions(dataValues.to ?? []),
      variant: formatOptions(dataValues.variant ?? []),
      sector: formatOptions(filteredSectors),
      userTag1: formatOptions(dataValues.userTag1 ?? []),
      userTag2: formatOptions(dataValues.userTag2 ?? []),
    };

    return res.json(data);
  } catch (error) {
    console.error("populateDashboardDropDowns error:", error);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};
const getDashboardData = async (req, res) => {

  let { from, to, variant, sector, userTag1, userTag2, label, periodicity } = req.query;

  console.log("from" + from + " to" + to + " variant" + variant + " sector" + sector + " periodicity" + periodicity + " label" + label);

  if (periodicity && label) {
    periodicity = periodicity.value.toLowerCase();
    label = label.value.toLowerCase()
    id = req.user.id;

    //building mongo query
    let datequery = {
      userId: id
    };

    let flightsQuery = {
      userId: id
    };

    if (label === "both") {
      flightsQuery.domIntl = { $in: ["dom", "intl"] };
    } else {
      datequery.domINTL = label
      flightsQuery.domIntl = label
    }

    if (variant && Array.isArray(variant) && variant.length > 0) {
      flightsQuery.variant = { $in: variant.map(item => item.value) };
    }

    if (sector && Array.isArray(sector) && sector.length > 0) {
      flightsQuery.sector = { $in: sector.map(item => item.value) };
    }

    if (userTag1 && Array.isArray(userTag1) && userTag1.length > 0) {
      flightsQuery.userTag1 = { $in: userTag1.map(item => item.value) };
    }

    if (userTag2 && Array.isArray(userTag2) && userTag2.length > 0) {
      flightsQuery.userTag2 = { $in: userTag2.map(item => item.value) };
    }

    if (from && Array.isArray(from) && from.length > 0) {
      flightsQuery.depStn = { $in: from.map(item => item.value) };
    }

    if (to && Array.isArray(to) && to.length > 0) {
      flightsQuery.arrStn = { $in: to.map(item => item.value) };
    }

    try {

      const datas = await Data.find(datequery);
      // Calculate the start and end dates based on the periodicity
      let startDate = new Date(Math.min(...datas.map((data) => data.effFromDt)));
      let endDate = new Date(Math.max(...datas.map((data) => data.effToDt)));

      let timeZone;
      if (Array.isArray(datas) && datas.length > 0) {
        timeZone = datas[0].timeZone;
      }

      // if (timeZone) {
      //   startDate = timeZoneCorrectedDates(startDate, timeZone);
      //   endDate = timeZoneCorrectedDates(endDate, timeZone);
      // }

      startDate.setUTCHours(0, 0, 0, 0)
      endDate.setUTCHours(0, 0, 0, 0)


      // Calculate the periods based on the periodicity
      let periods = [];
      let currentDate = new Date(startDate);

      if (periodicity === 'monthly') {
        periods = generateLastDayOfMonths(startDate, endDate);

      } else if (periodicity === 'quarterly') {
        periods = generateQuarterlyDates(startDate, endDate);

      } else if (periodicity === 'annually') {
        periods = generateAnnualDates(startDate, endDate);
      } else if (periodicity === 'weekly') {
        periods = generateWeeklyDates(startDate, endDate);
      } else if (periodicity === 'daily') {
        periods = generateDailyDates(startDate, endDate);
      }

      try {
        // Initialize an array to store the result data
        const resultData = [];

        for (const periodEndDate of periods) {
          let periodStartDate;
          if (periodicity === 'monthly') {

            periodStartDate = new Date(periodEndDate.getFullYear(), periodEndDate.getMonth(), 1);

          } else if (periodicity === 'quarterly') {

            const quarterStartMonth = Math.floor(periodEndDate.getMonth() / 3) * 3;
            periodStartDate = new Date(periodEndDate.getFullYear(), quarterStartMonth, 1);

          } else if (periodicity === 'annually') {

            periodStartDate = new Date(periodEndDate.getFullYear(), 0, 1);
          } else if (periodicity === 'weekly') {

            const dayOfWeek = periodEndDate.getDay();

            // Calculate the difference in days to get to the previous Monday
            const daysUntilMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;

            // Clone the periodEndDate to avoid modifying the original date
            const startDateis = new Date(periodEndDate);

            // Subtract the days to get to the previous Monday
            periodStartDate = startDateis.setDate(periodEndDate.getDate() - daysUntilMonday);
          } else if (periodicity === 'daily') {

            periodStartDate = new Date(periodEndDate);
          }

          flightsQuery.date = {
            $gte: periodStartDate,
            $lte: periodEndDate
          }



          // const flightsInPeriod = await Flights.find({
          //   userId: id,
          //   date: {
          //     $gte: periodStartDate,
          //     $lte: periodEndDate
          //   },
          //   $or: [
          //     { depStn: { $in: depStnArray } },
          //     { arrStn: { $in: arrStnArray } },
          //     { variant: { $in: variantArray } }
          //     // Add more fields as needed
          //   ]
          // });

          const flightsInPeriod = await Flights.find(flightsQuery);

          const uniqueStations = new Set();
          flightsInPeriod.forEach((flight) => {
            uniqueStations.add(flight.arrStn);
            uniqueStations.add(flight.depStn);
          });

          const sumOfSeats = flightsInPeriod.reduce((totalSeats, flight) => {
            if (typeof flight.pax === 'number') {
              return totalSeats + flight.seats;
            } else if (typeof flight.seats === 'string' && !isNaN(flight.seats)) {
              return totalSeats + Number(flight.seats);
            } else {
              return totalSeats;
            }
          }, 0);

          const sumOfPax = flightsInPeriod.reduce((totalPax, flight) => {
            if (typeof flight.pax === 'number') {
              return totalPax + flight.pax;
            } else if (typeof flight.pax === 'string' && !isNaN(flight.pax)) {
              return totalPax + Number(flight.pax);
            } else {
              return totalPax;
            }
          }, 0);

          const sumOfCargoCapT = flightsInPeriod.reduce((totalCargoCapT, flight) => {
            if (typeof flight.pax === 'number') {
              return totalCargoCapT + flight.CargoCapT;
            } else if (typeof flight.CargoCapT === 'string' && !isNaN(flight.CargoCapT)) {
              return totalCargoCapT + Number(flight.CargoCapT);
            } else {
              return totalCargoCapT;
            }
          }, 0);

          const sumOfCargoT = flightsInPeriod.reduce((totalCargoT, flight) => {
            if (typeof flight.CargoT === 'number') {
              return totalCargoT + flight.CargoT;
            } else if (typeof flight.CargoT === 'string' && !isNaN(flight.CargoT)) {
              return totalCargoT + Number(flight.CargoT);
            } else {
              return totalCargoT;
            }
          }, 0);

          const sumOfask = flightsInPeriod.reduce((totalask, flight) => {
            if (typeof flight.ask === 'number') {
              return totalask + flight.ask;
            } else if (typeof flight.ask === 'string' && !isNaN(flight.ask)) {
              return totalask + Number(flight.ask);
            } else {
              return totalask;
            }
          }, 0);

          const sumOfrsk = flightsInPeriod.reduce((totalrsk, flight) => {
            if (typeof flight.rsk === 'number') {
              return totalrsk + flight.rsk;
            } else if (typeof flight.rsk === 'string' && !isNaN(flight.rsk)) {
              return totalrsk + Number(flight.rsk);
            } else {
              return totalrsk;
            }
          }, 0);

          const sumOfcargoAtk = flightsInPeriod.reduce((totalcargoAtk, flight) => {
            if (typeof flight.cargoAtk === 'number') {
              return totalcargoAtk + flight.cargoAtk;
            } else if (typeof flight.cargoAtk === 'string' && !isNaN(flight.cargoAtk)) {
              return totalcargoAtk + Number(flight.cargoAtk);
            } else {
              return totalcargoAtk;
            }
          }, 0);

          const sumOfcargoRtk = flightsInPeriod.reduce((totalcargoRtk, flight) => {
            if (typeof flight.cargoRtk === 'number') {
              return totalcargoRtk + flight.cargoRtk;
            } else if (typeof flight.cargoRtk === 'string' && !isNaN(flight.cargoRtk)) {
              return totalcargoRtk + Number(flight.cargoRtk);
            } else {
              return totalcargoRtk;
            }
          }, 0);

          const sumOfGcd = flightsInPeriod.reduce((totalGcd, flight) => {
            if (typeof flight.dist === 'number') {
              return totalGcd + flight.dist;
            } else if (typeof flight.dist === 'string' && !isNaN(flight.dist)) {
              return totalGcd + Number(flight.dist);
            } else {
              return totalGcd;
            }
          }, 0);

          const validRotationFlights = flightsInPeriod.filter(flight => typeof flight.rotationNumber === 'string' && flight.rotationNumber.trim() !== '');

          function getFlightsWithBehindODs(flightsInPeriod) {
            let flightsWithBehindODs = [];

            flightsInPeriod.forEach(flight => {
              // Check if behindODs is true
              if (flight.behindODs) {
                flightsWithBehindODs.push(flight);
              }
            });

            return flightsWithBehindODs;
          }

          function getFlightsWithBeyondODs(flightsInPeriod) {
            let flightsWithBeyondODs = [];

            flightsInPeriod.forEach(flight => {
              // Check if beyondODs is true
              if (flight.beyondODs) {
                flightsWithBeyondODs.push(flight);
              }
            });

            return flightsWithBeyondODs;
          }

          //This is for behindOD & beyond as array 
          // function getFlightsWithBehindODs(flightsInPeriod) {
          //   let flightsWithBehindODs = [];

          //   flightsInPeriod.forEach(flight => {
          //     // Check if the behindODs array exists and has at least one entry
          //     if (flight.behindODs && flight.behindODs.length > 0) {
          //       flightsWithBehindODs.push(flight);
          //     }
          //   });

          //   return flightsWithBehindODs;
          // }

          // function getFlightsWithBeyondODs(flightsInPeriod) {
          //   let flightsWithBeyondODs = [];

          //   flightsInPeriod.forEach(flight => {
          //     // Check if the behindODs array exists and has at least one entry
          //     if (flight.beyondODs && flight.beyondODs.length > 0) {
          //       flightsWithBeyondODs.push(flight);
          //     }
          //   });

          //   return flightsWithBeyondODs;
          // }

          const bhdODFlgts = getFlightsWithBehindODs(flightsInPeriod)
          const beyODFlgts = getFlightsWithBeyondODs(flightsInPeriod)


          const connectingFlgts = bhdODFlgts.length;

          const seatCapBehindFlgts = beyODFlgts.reduce((totalSeats, flight) => {
            if (typeof flight.pax === 'number') {
              return totalSeats + flight.seats;
            } else if (typeof flight.seats === 'string' && !isNaN(flight.seats)) {
              return totalSeats + Number(flight.seats);
            } else {
              return totalSeats;
            }
          }, 0);


          const seatCapBeyondFlgts = bhdODFlgts.reduce((totalSeats, flight) => {
            if (typeof flight.pax === 'number') {
              return totalSeats + flight.seats;
            } else if (typeof flight.seats === 'string' && !isNaN(flight.seats)) {
              return totalSeats + Number(flight.seats);
            } else {
              return totalSeats;
            }
          }, 0);

          const cargoCapBehindFlgts = beyODFlgts.reduce((totalCargoCapT, flight) => {
            if (typeof flight.pax === 'number') {
              return totalCargoCapT + flight.CargoCapT;
            } else if (typeof flight.CargoCapT === 'string' && !isNaN(flight.CargoCapT)) {
              return totalCargoCapT + Number(flight.CargoCapT);
            } else {
              return totalCargoCapT;
            }
          }, 0);

          const cargoCapBeyondFlgts = bhdODFlgts.reduce((totalCargoCapT, flight) => {
            if (typeof flight.pax === 'number') {
              return totalCargoCapT + flight.CargoCapT;
            } else if (typeof flight.CargoCapT === 'string' && !isNaN(flight.CargoCapT)) {
              return totalCargoCapT + Number(flight.CargoCapT);
            } else {
              return totalCargoCapT;
            }
          }, 0);


          //we have to deliver bh, computed using hh+(mm/60) from bt in format 

          function convertTimeStringToDecimal(timeString) {
            const [hours, minutes] = timeString.split(':').map(Number);
            const decimalTime = hours + minutes / 60;
            return decimalTime;
          }

          const bh = flightsInPeriod.reduce((totalbh, flight) => totalbh + convertTimeStringToDecimal(flight.bt), 0);

          const fh = flightsInPeriod.reduce((totalfh, flight) => {
            return totalfh + (Number(flight.fh) || 0);
          }, 0);

          resultData.push({
            endDate: periodEndDate.toString(),
            destinations: parseInt(uniqueStations.size).toLocaleString(),
            departures: parseInt(flightsInPeriod.length).toLocaleString(),
            seats: sumOfSeats.toLocaleString(),
            pax: Math.round(sumOfPax).toLocaleString(),
            paxSF: Math.round((sumOfPax / sumOfSeats) * 100),
            paxLF: Math.round((sumOfrsk / sumOfask) * 100),
            cargoCapT: parseFloat(sumOfCargoCapT).toLocaleString('en-US', {
              minimumFractionDigits: 1,
              maximumFractionDigits: 1,
            }),
            cargoT: parseFloat(sumOfCargoT).toLocaleString('en-US', {
              minimumFractionDigits: 1,
              maximumFractionDigits: 1,
            }),
            ct2ctc: Math.round((sumOfCargoT / sumOfCargoCapT) * 100),
            cftk2atk: Math.round((sumOfcargoRtk / sumOfcargoAtk) * 100),
            bh: Math.round(bh).toLocaleString(),
            fh: fh,
            sumOfGcd: Math.round(sumOfGcd),
            adu: validRotationFlights.length > 0 ? (Math.round(bh / validRotationFlights.length * 100) / 100).toFixed(2) : '0',
            connectingFlights: connectingFlgts.toLocaleString(),
            seatCapBeyondFlgts: seatCapBeyondFlgts.toLocaleString(),
            seatCapBehindFlgts: seatCapBehindFlgts.toLocaleString(),
            cargoCapBehindFlgts: cargoCapBehindFlgts.toLocaleString(),
            cargoCapBeyondFlgts: cargoCapBeyondFlgts.toLocaleString(),
            sumOfask: sumOfask,
            sumOfrsk: sumOfrsk,
            sumOfcargoAtk: sumOfcargoAtk,
            sumOfcargoRtk: sumOfcargoRtk
          });
        }

        res.status(200).json(resultData);
      }
      catch (error) {
        console.log(error);
        res.send({ status: 500, success: false, msg: error.message });
      }

    }
    catch (error) {
      console.error(error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  }
};

module.exports = {
  populateDashboardDropDowns,
  getDashboardData
};
