const User = require("../model/userSchema");
const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const DataHistory = require("../model/dataHistorySchema");
const SectorHistory = require("../model/sectorHistorySchema");
const Flights = require("../model/flight");
const FlightHistory = require("../model/flightHistory")
const Fleet = require("../model/fleet");
const PooTable = require("../model/pooTable");
const RotationSummary = require("../model/rotationSummary");
const RotationDetails = require("../model/rotationDetails");
const Stations = require("../model/stationSchema");
const StationsHistory = require("../model/stationHistorySchema");
const CostConfig = require("../model/costConfigSchema");
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
const { normalizeCostConfig } = require("../utils/costLogic");

const createConnections = require('../helper/createConnections');


moment.tz.setDefault("America/New_York");

const normalizeQueryValues = (value) => {
  if (value === undefined || value === null || value === "") {
    return [];
  }

  const values = Array.isArray(value) ? value : [value];
  return values
    .map((item) => {
      if (item && typeof item === "object") {
        return String(item.value ?? item.label ?? "").trim();
      }

      return String(item).trim();
    })
    .filter(Boolean);
};

const normalizeSingleQueryValue = (value) => {
  if (Array.isArray(value)) {
    return normalizeSingleQueryValue(value[0]);
  }

  if (value && typeof value === "object") {
    return String(value.value ?? value.label ?? "").trim();
  }

  return String(value ?? "").trim();
};

const normalizeRevenueLabel = (value) => {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized.includes("intl")) return "Intl";
  if (normalized.includes("dom")) return "Dom";
  return "";
};

const toNumericValue = (value) => {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
};

const getCostConfigRowDate = (row = {}) => {
  if (row?.date) {
    const parsed = new Date(row.date);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }

  const month = String(row?.month ?? "").trim();
  const monthMatch = month.match(/^(\d{1,2})[/-](\d{2,4})$/);
  if (monthMatch) {
    const monthIndex = Number(monthMatch[1]) - 1;
    const year = Number(monthMatch[2].length === 2 ? `20${monthMatch[2]}` : monthMatch[2]);
    const parsed = new Date(Date.UTC(year, monthIndex, 1));
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }

  const fallbackDate = new Date(month);
  return Number.isNaN(fallbackDate.getTime()) ? null : fallbackDate;
};

const getUserIdFromReq = (req) => {
  const rawUserId =
    req.user?.id ??
    req.userId ??
    req.user?.userId ??
    req.user?._id;

  const normalizedUserId = String(rawUserId ?? "").trim();
  return normalizedUserId || "";
};

const normalizeDropdownValueList = (values = []) =>
  Array.from(
    new Set(
      values
        .map((value) => String(value ?? "").trim())
        .filter(Boolean)
    )
  ).sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" })
  );

const BLANK_OPTION_VALUE = "__BLANK__";



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
  startOfUtcDay,
  endOfUtcDay,
  addUtcDays,
  getUtcDayOfWeek,
  roundToLastDateOfPresentYear,
  roundToLastDateOfNextQuarter,
  isValidDepStn
} = require('./controllerUtils');

const populateDashboardDropDowns = async (req, res) => {
  try {
    const userId = getUserIdFromReq(req);

    if (!userId) {
      return res.status(400).json({ message: "User ID missing" });
    }

    const distinctSectors = await Flights.aggregate([
      { $match: { userId: userId } },
      { $group: { _id: null, sector: { $addToSet: "$sector" } } },
      { $project: { _id: 0, sector: 1 } },
    ]);

    const distinctFlights = await Flights.aggregate([
      { $match: { userId: userId } },
      { $group: { _id: null, flight: { $addToSet: "$flight" } } },
      { $project: { _id: 0, flight: 1 } },
    ]);

    const distinctValues = await Data.aggregate([
      { $match: { userId: userId } },
      {
        $group: {
          _id: null,
          flight: { $addToSet: "$flight" },
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
          flight: 1,
          from: 1,
          to: 1,
          variant: 1,
          userTag1: 1,
          userTag2: 1,
        },
      },
    ]);

    const formatOptions = (values = []) =>
      normalizeDropdownValueList(values).map((value) => ({
        value,
        label: value,
      }));

    // Safe extraction
    const sectorList = distinctSectors?.[0]?.sector ?? [];
    const flightList = distinctFlights?.[0]?.flight ?? [];
    const dataValues = distinctValues?.[0] ?? {};

    const filteredSectors = sectorList.filter(
      (sector) => String(sector ?? "").trim() !== "undefined-undefined"
    );

    const distinctPooValues = await PooTable.aggregate([
      { $match: { userId: userId } },
      {
        $group: {
          _id: null,
          poo: { $addToSet: "$poo" },
          od: { $addToSet: "$od" },
          odDI: { $addToSet: "$odDI" },
          legDI: { $addToSet: "$legDI" },
          identifier: { $addToSet: "$identifier" },
          stops: { $addToSet: "$stops" },
          al: { $addToSet: "$al" },
        },
      },
      { $project: { _id: 0, poo: 1, od: 1, odDI: 1, legDI: 1, identifier: 1, stops: 1, al: 1 } },
    ]);

    const distinctSnValues = await Fleet.aggregate([
      { $match: { userId: userId, category: "Aircraft" } },
      { $group: { _id: null, sn: { $addToSet: "$sn" } } },
      { $project: { _id: 0, sn: 1 } },
    ]);

    const rawStopValues = distinctPooValues?.[0]?.stops ?? [];
    const stopOptions = normalizeDropdownValueList(
      rawStopValues
        .filter((value) => value !== null && value !== undefined && String(value).trim() !== "")
        .map((value) => String(value))
    ).map((value) => ({
      value,
      label: value,
    }));

    stopOptions.unshift({
      value: BLANK_OPTION_VALUE,
      label: "(blank)",
    });

    const data = {
      flight: formatOptions(
        Array.from(new Set([...(flightList || []), ...(dataValues.flight ?? [])]))
      ),
      from: formatOptions(dataValues.from ?? []),
      to: formatOptions(dataValues.to ?? []),
      variant: formatOptions(dataValues.variant ?? []),
      sector: formatOptions(filteredSectors),
      sn: formatOptions(distinctSnValues?.[0]?.sn ?? []),
      poo: formatOptions(distinctPooValues?.[0]?.poo ?? []),
      od: formatOptions(distinctPooValues?.[0]?.od ?? []),
      odDI: formatOptions(distinctPooValues?.[0]?.odDI ?? []),
      legDI: formatOptions(distinctPooValues?.[0]?.legDI ?? []),
      identifier: formatOptions(distinctPooValues?.[0]?.identifier ?? []),
      stop: stopOptions,
      al: formatOptions(distinctPooValues?.[0]?.al ?? []),
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

  let { from, to, variant, sector, flight, userTag1, userTag2, label, periodicity } = req.query;

  console.log("from" + from + " to" + to + " variant" + variant + " sector" + sector + " flight" + flight + " periodicity" + periodicity + " label" + label);

  if (!periodicity || !label) {
    return res.status(400).json({ error: "Missing label or periodicity" });
  }

  periodicity = normalizeSingleQueryValue(periodicity).toLowerCase();
  label = normalizeSingleQueryValue(label).toLowerCase();
  const id = req.user.id;

  //building mongo query
  let datequery = {
    userId: id
  };

  let flightsQuery = {
    userId: id
  };
  let revenueQuery = {
    userId: id
  };

  if (label === "both") {
    flightsQuery.domIntl = { $in: ["dom", "intl"] };
  } else {
    datequery.domINTL = label
    flightsQuery.domIntl = label
    const normalizedRevenueLabel = normalizeRevenueLabel(label);
    if (normalizedRevenueLabel) {
      revenueQuery.$or = [
        { odDI: normalizedRevenueLabel },
        { legDI: normalizedRevenueLabel },
      ];
    }
  }

  const normalizedVariant = normalizeQueryValues(variant);
  const normalizedSector = normalizeQueryValues(sector);
  const normalizedFlight = normalizeQueryValues(flight);
  const normalizedUserTag1 = normalizeQueryValues(userTag1);
  const normalizedUserTag2 = normalizeQueryValues(userTag2);
  const normalizedFrom = normalizeQueryValues(from);
  const normalizedTo = normalizeQueryValues(to);

  if (normalizedVariant.length > 0) {
    flightsQuery.variant = { $in: normalizedVariant };
  }

  if (normalizedSector.length > 0) {
    flightsQuery.sector = { $in: normalizedSector };
  }

  if (normalizedFlight.length > 0) {
    flightsQuery.flight = { $in: normalizedFlight };
  }

  if (normalizedUserTag1.length > 0) {
    flightsQuery.userTag1 = { $in: normalizedUserTag1 };
  }

  if (normalizedUserTag2.length > 0) {
    flightsQuery.userTag2 = { $in: normalizedUserTag2 };
  }

  if (normalizedFrom.length > 0) {
    flightsQuery.depStn = { $in: normalizedFrom };
  }

  if (normalizedTo.length > 0) {
    flightsQuery.arrStn = { $in: normalizedTo };
  }

  const dataFilterClauses = [];
  if (normalizedFrom.length > 0) dataFilterClauses.push({ depStn: { $in: normalizedFrom } });
  if (normalizedTo.length > 0) dataFilterClauses.push({ arrStn: { $in: normalizedTo } });
  if (normalizedSector.length > 0) dataFilterClauses.push({ sector: { $in: normalizedSector } });
  if (normalizedVariant.length > 0) dataFilterClauses.push({ variant: { $in: normalizedVariant } });
  if (normalizedFlight.length > 0) dataFilterClauses.push({ flight: { $in: normalizedFlight } });
  if (normalizedUserTag1.length > 0) dataFilterClauses.push({ userTag1: { $in: normalizedUserTag1 } });
  if (normalizedUserTag2.length > 0) dataFilterClauses.push({ userTag2: { $in: normalizedUserTag2 } });

  if (dataFilterClauses.length > 0 || label !== "both") {
    const dataMatch = { userId: id };
    const normalizedRevenueLabel = normalizeRevenueLabel(label);
    if (label !== "both" && normalizedRevenueLabel) {
      dataMatch.domINTL = normalizedRevenueLabel;
    }
    if (dataFilterClauses.length > 0) {
      dataMatch.$and = dataFilterClauses;
    }

    const matchingData = await Data.find(dataMatch).select("flight depStn arrStn sector");
    const allowedFlights = [...new Set(matchingData.map((row) => String(row.flight || "").trim()).filter(Boolean))];
    const allowedSectors = [...new Set(matchingData.map((row) => String(row.sector || "").trim().toUpperCase()).filter(Boolean))];

    if (matchingData.length === 0) {
      revenueQuery.$and = [{ _id: null }];
    } else {
      const revenueClauses = [];
      if (allowedFlights.length > 0) revenueClauses.push({ flightNumber: { $in: allowedFlights } });
      if (allowedSectors.length > 0) revenueClauses.push({ sector: { $in: allowedSectors } });
      if (revenueClauses.length > 0) {
        revenueQuery.$and = revenueClauses;
      }
    }
  }

  try {

      const datas = await Data.find(datequery);
      // Calculate the start and end dates based on the periodicity
      let startDate = startOfUtcDay(new Date(Math.min(...datas.map((data) => data.effFromDt))));
      let endDate = startOfUtcDay(new Date(Math.max(...datas.map((data) => data.effToDt))));

      let timeZone;
      if (Array.isArray(datas) && datas.length > 0) {
        timeZone = datas[0].timeZone;
      }

      // if (timeZone) {
      //   startDate = timeZoneCorrectedDates(startDate, timeZone);
      //   endDate = timeZoneCorrectedDates(endDate, timeZone);
      // }

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

      const [revenueRows, costConfigDoc] = await Promise.all([
        PooTable.find(revenueQuery).lean(),
        CostConfig.findOne({ userId: id }).lean(),
      ]);
      const costConfig = normalizeCostConfig(costConfigDoc || {});
      const schMxEvents = Array.isArray(costConfig.schMxEvents) ? costConfig.schMxEvents : [];

      try {
        // Initialize an array to store the result data
        const resultData = [];

        for (const periodEndDate of periods) {
          let periodStartDate;
          if (periodicity === 'monthly') {
            periodStartDate = new Date(Date.UTC(periodEndDate.getUTCFullYear(), periodEndDate.getUTCMonth(), 1));

          } else if (periodicity === 'quarterly') {
            const quarterStartMonth = Math.floor(periodEndDate.getUTCMonth() / 3) * 3;
            periodStartDate = new Date(Date.UTC(periodEndDate.getUTCFullYear(), quarterStartMonth, 1));

          } else if (periodicity === 'annually') {
            periodStartDate = new Date(Date.UTC(periodEndDate.getUTCFullYear(), 0, 1));
          } else if (periodicity === 'weekly') {
            const dayOfWeek = getUtcDayOfWeek(periodEndDate);
            const daysUntilMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
            periodStartDate = addUtcDays(periodEndDate, -daysUntilMonday);
          } else if (periodicity === 'daily') {
            periodStartDate = startOfUtcDay(periodEndDate);
          }

          flightsQuery.date = {
            $gte: startOfUtcDay(periodStartDate),
            $lte: endOfUtcDay(periodEndDate)
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

          const periodStartDay = startOfUtcDay(periodStartDate);
          const periodEndDay = endOfUtcDay(periodEndDate);
          const revenueInPeriod = revenueRows.filter((row) => {
            const rowDate = row?.date ? startOfUtcDay(new Date(row.date)) : null;
            return rowDate && rowDate >= periodStartDay && rowDate <= periodEndDay;
          });
          const fnlRccyPaxRev = revenueInPeriod.reduce((total, row) => total + (Number(row.fnlRccyPaxRev) || 0), 0);
          const fnlRccyCargoRev = revenueInPeriod.reduce((total, row) => total + (Number(row.fnlRccyCargoRev) || 0), 0);
          const fnlRccyTotalRev = fnlRccyPaxRev + fnlRccyCargoRev;

          const schMxInPeriod = schMxEvents.filter((row) => {
            const rowDate = row?.date ? startOfUtcDay(new Date(row.date)) : null;
            return rowDate && rowDate >= periodStartDay && rowDate <= periodEndDay;
          });
          const rotableChangesInPeriod = Array.isArray(costConfig.rotableChanges)
            ? costConfig.rotableChanges.filter((row) => {
                const rowDate = getCostConfigRowDate(row);
                return rowDate && startOfUtcDay(rowDate) >= periodStartDay && startOfUtcDay(rowDate) <= periodEndDay;
              })
            : [];

          const groupedSchMxEvents = new Map();
          schMxInPeriod.forEach((row) => {
            const eventKey = String(row?.event || "").trim() || "Sch.Mx.Event";
            if (!groupedSchMxEvents.has(eventKey)) {
              groupedSchMxEvents.set(eventKey, []);
            }
            groupedSchMxEvents.get(eventKey).push(row);
          });

          const orderedSchMxEvents = Array.from(groupedSchMxEvents.entries())
            .map(([event, rows]) => ({
              event,
              rows: [...rows].sort((a, b) => {
                const aDate = startOfUtcDay(new Date(a.date)).getTime();
                const bDate = startOfUtcDay(new Date(b.date)).getTime();
                return aDate - bDate;
              }),
            }))
            .sort((a, b) => {
              const aDate = a.rows[0] ? startOfUtcDay(new Date(a.rows[0].date)).getTime() : 0;
              const bDate = b.rows[0] ? startOfUtcDay(new Date(b.rows[0].date)).getTime() : 0;
              return aDate - bDate;
            });

          const firstSchMxRows = orderedSchMxEvents[0]?.rows || [];
          const secondSchMxRows = orderedSchMxEvents[1]?.rows || [];
          const sumSchMxRows = (rows = []) => rows.reduce((total, row) => total + toNumericValue(row?.costRCCY ?? row?.cost), 0);

          const schMxEvent1Detail1RCCY = toNumericValue(firstSchMxRows[0]?.costRCCY ?? firstSchMxRows[0]?.cost);
          const schMxEvent1Detail2RCCY = toNumericValue(firstSchMxRows[1]?.costRCCY ?? firstSchMxRows[1]?.cost);
          const schMxEvent1RCCY = sumSchMxRows(firstSchMxRows);
          const schMxEvent2Detail1RCCY = toNumericValue(secondSchMxRows[0]?.costRCCY ?? secondSchMxRows[0]?.cost);
          const schMxEvent2RCCY = sumSchMxRows(secondSchMxRows);
          const qualifyingSchMxEventsRCCY = schMxEvent1RCCY + schMxEvent2RCCY;

          const sumNumericField = (rows = [], field, fallback = 0) => rows.reduce((total, row) => {
            return total + toNumericValue(row?.[field]);
          }, fallback);

          const sumRowFields = (rows = [], fields = []) => rows.reduce((total, row) => {
            const rowTotal = fields.reduce((rowSum, field) => {
              return rowSum + toNumericValue(row?.[field]);
            }, 0);
            return total + rowTotal;
          }, 0);

          const engineFuelConsumption = sumNumericField(flightsInPeriod, "engineFuelConsumption");
          const engineFuelCostRCCY = sumNumericField(flightsInPeriod, "engineFuelCostRCCY");
          const apuFuelCostRCCY = sumNumericField(flightsInPeriod, "apuFuelCostRCCY");
          const totalFuelCostRCCY = engineFuelCostRCCY + apuFuelCostRCCY;

          const maintenanceReserveContributionRCCY = sumNumericField(flightsInPeriod, "maintenanceReserveContributionRCCY");
          const mrMonthlyRCCY = sumNumericField(flightsInPeriod, "mrMonthlyRCCY");
          const totalMrContributionRCCY = maintenanceReserveContributionRCCY + mrMonthlyRCCY;

          const transitMaintenanceRCCY = sumNumericField(flightsInPeriod, "transitMaintenanceRCCY");
          const otherMaintenanceRCCY = sumNumericField(flightsInPeriod, "otherMaintenanceRCCY");
          const otherMaintenanceUtilisationRCCY = sumRowFields(flightsInPeriod, ["otherMaintenance1", "otherMaintenance2"]);
          const otherMaintenanceCalendarRCCY = sumNumericField(flightsInPeriod, "otherMaintenance3");
          const rotableChangesRCCY = rotableChangesInPeriod.reduce((total, row) => total + toNumericValue(row?.costRCCY ?? row?.cost), 0);
          const totalMaintenanceCostRCCY =
            totalMrContributionRCCY +
            qualifyingSchMxEventsRCCY +
            transitMaintenanceRCCY +
            otherMaintenanceRCCY +
            rotableChangesRCCY;

          const crewAllowancesRCCY = sumNumericField(flightsInPeriod, "crewAllowancesRCCY");
          const layoverCostRCCY = sumNumericField(flightsInPeriod, "layoverCostRCCY");
          const crewPositioningCostRCCY = sumNumericField(flightsInPeriod, "crewPositioningCostRCCY");
          const crewTotalDirectCostRCCY = crewAllowancesRCCY + layoverCostRCCY + crewPositioningCostRCCY;

          const airportRCCY = sumNumericField(flightsInPeriod, "airportRCCY");
          const navigationRCCY = sumNumericField(flightsInPeriod, "navigationRCCY");
          const otherDocRCCY = sumNumericField(flightsInPeriod, "otherDocRCCY");
          const totalDocRCCY =
            totalFuelCostRCCY +
            totalMaintenanceCostRCCY +
            crewTotalDirectCostRCCY +
            airportRCCY +
            navigationRCCY +
            otherDocRCCY;

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
            sumOfcargoRtk: sumOfcargoRtk,
            fnlRccyPaxRev,
            fnlRccyCargoRev,
            fnlRccyTotalRev,
            engineFuelConsumption,
            engineFuelCostRCCY,
            apuFuelCostRCCY,
            totalFuelCostRCCY,
            maintenanceReserveContributionRCCY,
            mrMonthlyRCCY,
            totalMrContributionRCCY,
            qualifyingSchMxEventsRCCY,
            schMxEvent1RCCY,
            schMxEvent1Detail1RCCY,
            schMxEvent1Detail2RCCY,
            schMxEvent2RCCY,
            schMxEvent2Detail1RCCY,
            transitMaintenanceRCCY,
            otherMaintenanceRCCY,
            otherMaintenanceUtilisationRCCY,
            otherMaintenanceCalendarRCCY,
            rotableChangesRCCY,
            totalMaintenanceCostRCCY,
            crewAllowancesRCCY,
            layoverCostRCCY,
            crewPositioningCostRCCY,
            crewTotalDirectCostRCCY,
            airportRCCY,
            navigationRCCY,
            otherDocRCCY,
            totalDocRCCY
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
};

module.exports = {
  populateDashboardDropDowns,
  getDashboardData
};
