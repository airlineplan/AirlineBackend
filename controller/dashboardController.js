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
const RevenueConfig = require("../model/revenueConfigSchema");
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
const { normalizeCostConfig, computeFlightCostsBatch } = require("../utils/costLogic");
const { buildMaintenanceReserveContext } = require("../utils/maintenanceReserveContext");
const { normalizeCurrencyCode, normalizeDateKey } = require("../utils/fx");

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

const buildBlankAwareDashboardClause = (field, values = []) => {
  if (!Array.isArray(values) || values.length === 0) return null;
  const wantsBlank = values.includes(BLANK_OPTION_VALUE) || values.includes("(blank)");
  const concrete = values.filter((value) => value !== BLANK_OPTION_VALUE && value !== "(blank)");

  if (wantsBlank && concrete.length) {
    return {
      $or: [
        { [field]: { $in: concrete } },
        { [field]: { $exists: false } },
        { [field]: null },
        { [field]: "" },
      ],
    };
  }

  if (wantsBlank) {
    return { $or: [{ [field]: { $exists: false } }, { [field]: null }, { [field]: "" }] };
  }

  return { [field]: { $in: concrete } };
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
          depStn: { $addToSet: "$depStn" },
          arrStn: { $addToSet: "$arrStn" },
          sector: { $addToSet: "$sector" },
          flightNumber: { $addToSet: "$flightNumber" },
          variant: { $addToSet: "$variant" },
          userTag1: { $addToSet: "$userTag1" },
          userTag2: { $addToSet: "$userTag2" },
          trafficType: { $addToSet: "$trafficType" },
        },
      },
      { $project: { _id: 0, poo: 1, od: 1, odDI: 1, legDI: 1, identifier: 1, stops: 1, al: 1, depStn: 1, arrStn: 1, sector: 1, flightNumber: 1, variant: 1, userTag1: 1, userTag2: 1, trafficType: 1 } },
    ]);

    const distinctSnValues = await Fleet.aggregate([
      { $match: { userId: userId, category: { $in: ["Aircraft", "Engine", "APU"] } } },
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
        Array.from(new Set([...(flightList || []), ...(dataValues.flight ?? []), ...(distinctPooValues?.[0]?.flightNumber ?? [])]))
      ),
      from: formatOptions(Array.from(new Set([...(dataValues.from ?? []), ...(distinctPooValues?.[0]?.depStn ?? [])]))),
      to: formatOptions(Array.from(new Set([...(dataValues.to ?? []), ...(distinctPooValues?.[0]?.arrStn ?? [])]))),
      variant: formatOptions(Array.from(new Set([...(dataValues.variant ?? []), ...(distinctPooValues?.[0]?.variant ?? [])]))),
      sector: formatOptions(Array.from(new Set([...(filteredSectors ?? []), ...(distinctPooValues?.[0]?.sector ?? [])]))),
      sn: formatOptions(distinctSnValues?.[0]?.sn ?? []),
      poo: formatOptions(distinctPooValues?.[0]?.poo ?? []),
      od: formatOptions(distinctPooValues?.[0]?.od ?? []),
      odDI: formatOptions(distinctPooValues?.[0]?.odDI ?? []),
      legDI: formatOptions(distinctPooValues?.[0]?.legDI ?? []),
      identifier: formatOptions(distinctPooValues?.[0]?.identifier ?? []),
      stop: stopOptions,
      al: formatOptions(distinctPooValues?.[0]?.al ?? []),
      userTag1: formatOptions(Array.from(new Set([...(dataValues.userTag1 ?? []), ...(distinctPooValues?.[0]?.userTag1 ?? [])]))),
      userTag2: formatOptions(Array.from(new Set([...(dataValues.userTag2 ?? []), ...(distinctPooValues?.[0]?.userTag2 ?? [])]))),
      trafficType: formatOptions(distinctPooValues?.[0]?.trafficType ?? []),
    };

    return res.json(data);
  } catch (error) {
    console.error("populateDashboardDropDowns error:", error);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};
const getDashboardDataLegacy = async (req, res) => {

  let { from, to, variant, sector, flight, userTag1, userTag2, label, periodicity } = req.query;

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
  const revenueAndClauses = [];

  if (label === "both") {
    flightsQuery.domIntl = { $in: ["dom", "intl"] };
  } else {
    datequery.domINTL = label
    flightsQuery.domIntl = label
    const normalizedRevenueLabel = normalizeRevenueLabel(label);
    if (normalizedRevenueLabel) {
      revenueAndClauses.push({ $or: [
        { odDI: normalizedRevenueLabel },
        { legDI: normalizedRevenueLabel },
      ] });
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
  [
    ["depStn", normalizedFrom],
    ["arrStn", normalizedTo],
    ["sector", normalizedSector],
    ["variant", normalizedVariant],
    ["flightNumber", normalizedFlight],
    ["userTag1", normalizedUserTag1],
    ["userTag2", normalizedUserTag2],
  ].forEach(([field, values]) => {
    const clause = buildBlankAwareDashboardClause(field, values);
    if (clause) revenueAndClauses.push(clause);
  });

  if (revenueAndClauses.length > 0) {
    revenueQuery.$and = revenueAndClauses;
  }

  try {

      const [datas, flightRange] = await Promise.all([
        Data.find(datequery),
        Flights.aggregate([
          { $match: { userId: id } },
          { $group: { _id: null, minDate: { $min: "$date" }, maxDate: { $max: "$date" } } },
        ]),
      ]);
      // Calculate the start and end dates based on the periodicity
      let startDate = flightRange?.[0]?.minDate
        ? startOfUtcDay(new Date(flightRange[0].minDate))
        : startOfUtcDay(new Date(Math.min(...datas.map((data) => data.effFromDt))));
      let endDate = flightRange?.[0]?.maxDate
        ? startOfUtcDay(new Date(flightRange[0].maxDate))
        : startOfUtcDay(new Date(Math.max(...datas.map((data) => data.effToDt))));

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

      const [revenueRows, costConfigDoc, revenueConfigDoc, fleetRows] = await Promise.all([
        PooTable.find(revenueQuery).lean(),
        CostConfig.findOne({ userId: id }).lean(),
        RevenueConfig.findOne({ userId: id }).lean(),
        Fleet.find({ userId: id }).lean(),
      ]);
      const revenueConfig = revenueConfigDoc || {};
      const costConfig = normalizeCostConfig({
        ...(costConfigDoc || {}),
        reportingCurrency: revenueConfig.reportingCurrency || costConfigDoc?.reportingCurrency,
        fxRates: revenueConfig.fxRates || costConfigDoc?.fxRates,
        fleet: fleetRows,
      });
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

          let flightsInPeriod = await Flights.find(flightsQuery).lean();
          const mrContext = await buildMaintenanceReserveContext(id, flightsInPeriod);
          flightsInPeriod = computeFlightCostsBatch(flightsInPeriod, {
            ...costConfig,
            ...mrContext,
            fleet: fleetRows,
          });

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

          const sumFlightCostField = (rows = [], field) => sumNumericField(rows, field);

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
          const otherMxExpensesRCCY = sumNumericField(flightsInPeriod, "otherMxExpensesRCCY");
          const rotableChangesRCCY = rotableChangesInPeriod.reduce((total, row) => total + toNumericValue(row?.costRCCY ?? row?.cost), 0);
          const totalMaintenanceCostRCCY =
            totalMrContributionRCCY +
            qualifyingSchMxEventsRCCY +
            transitMaintenanceRCCY +
            otherMaintenanceRCCY +
            otherMxExpensesRCCY +
            rotableChangesRCCY;

          const crewAllowancesRCCY = sumNumericField(flightsInPeriod, "crewAllowancesRCCY");
          const layoverCostRCCY = sumNumericField(flightsInPeriod, "layoverCostRCCY");
          const crewPositioningCostRCCY = sumNumericField(flightsInPeriod, "crewPositioningCostRCCY");
          const crewTotalDirectCostRCCY = crewAllowancesRCCY + layoverCostRCCY + crewPositioningCostRCCY;

          // Pull the DOC line items straight from the flight rows so the dashboard
          // stays aligned with what is stored on the flight table.
          const airportRCCY = sumFlightCostField(flightsInPeriod, "airportRCCY");
          const navigationRCCY = sumFlightCostField(flightsInPeriod, "navigationRCCY");
          const otherDocRCCY = sumFlightCostField(flightsInPeriod, "otherDocRCCY");
          const totalDocRCCY =
            totalFuelCostRCCY +
            totalMaintenanceCostRCCY +
            crewTotalDirectCostRCCY +
            airportRCCY +
            navigationRCCY +
            otherDocRCCY;
          const grossProfitLossRCCY = fnlRccyTotalRev - totalDocRCCY;

          resultData.push({
            startDate: periodStartDate.toString(),
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
            engineFuelConsumptionKg: sumNumericField(flightsInPeriod, "engineFuelConsumptionKg"),
            apuFuelConsumptionKg: sumNumericField(flightsInPeriod, "apuFuelConsumptionKg"),
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
            otherMxExpensesRCCY,
            rotableChangesRCCY,
            totalMaintenanceCostRCCY,
            crewAllowancesRCCY,
            layoverCostRCCY,
            crewPositioningCostRCCY,
            crewTotalDirectCostRCCY,
            airportRCCY,
            navigationRCCY,
            otherDocRCCY,
            totalDocRCCY,
            grossProfitLossRCCY
          });
        }

        const flightsForFxDates = await Flights.find({ userId: id }).select("date").lean();
        const riskExposureData = {
          currencies: {},
          fuel: resultData.map((period) => ({
            dateKey: moment.utc(period.endDate).format("YYYY-MM-DD"),
            engineFuelKg: toNumericValue(period.engineFuelConsumptionKg),
            apuFuelKg: toNumericValue(period.apuFuelConsumptionKg),
            totalFuelKg: toNumericValue(period.engineFuelConsumptionKg) + toNumericValue(period.apuFuelConsumptionKg),
          })),
        };

        revenueRows.forEach((row) => {
          const ccy = String(row.pooCcy || revenueConfig.reportingCurrency || "").trim().toUpperCase();
          if (!ccy || ccy === String(revenueConfig.reportingCurrency || "").trim().toUpperCase()) return;
          if (!riskExposureData.currencies[ccy]) riskExposureData.currencies[ccy] = { periods: [] };
          riskExposureData.currencies[ccy].periods.push({
            dateKey: moment.utc(row.date).format("YYYY-MM-DD"),
            revenue: toNumericValue(row.odTotalRev || row.legTotalRev),
            cost: 0,
          });
        });

        res.status(200).json({
          data: resultData,
          periods: resultData.map((period) => ({
            key: moment.utc(period.endDate).format("YYYY-MM-DD"),
            startDate: period.startDate,
            endDate: period.endDate,
            dateLabel: moment.utc(period.endDate).format("DD MMM YY"),
            data: period,
          })),
          revenueConfig,
          currencyCodes: revenueConfig.currencyCodes || [],
          fxRates: revenueConfig.fxRates || [],
          flightsForFxDates,
          riskExposureData,
        });
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

const pickNumeric = (row = {}, keys = []) => {
  for (const key of keys) {
    const value = row?.[key];
    if (value !== undefined && value !== null && value !== "") return toNumericValue(value);
  }
  return 0;
};

const safePercent = (numerator, denominator) => {
  const bottom = toNumericValue(denominator);
  if (bottom <= 0) return 0;
  return Number(((toNumericValue(numerator) / bottom) * 100).toFixed(2));
};

const buildDashboardQueries = ({ userId, label, filters }) => {
  const flightsQuery = { userId };
  const revenueQuery = { userId };
  const revenueAndClauses = [];

  if (label && label !== "both") {
    flightsQuery.domIntl = label;
    const revenueLabel = normalizeRevenueLabel(label);
    if (revenueLabel) {
      revenueAndClauses.push({ $or: [{ odDI: revenueLabel }, { legDI: revenueLabel }] });
    }
  }

  const flightMappings = [
    ["depStn", filters.from],
    ["arrStn", filters.to],
    ["sector", filters.sector],
    ["variant", filters.variant],
    ["flight", filters.flight],
    ["userTag1", filters.userTag1],
    ["userTag2", filters.userTag2],
  ];

  flightMappings.forEach(([field, values]) => {
    const clause = buildBlankAwareDashboardClause(field, values);
    if (clause) Object.assign(flightsQuery, clause.$or ? { $and: [...(flightsQuery.$and || []), clause] } : clause);
  });

  [
    ["depStn", filters.from],
    ["arrStn", filters.to],
    ["sector", filters.sector],
    ["variant", filters.variant],
    ["flightNumber", filters.flight],
    ["userTag1", filters.userTag1],
    ["userTag2", filters.userTag2],
  ].forEach(([field, values]) => {
    const clause = buildBlankAwareDashboardClause(field, values);
    if (clause) revenueAndClauses.push(clause);
  });

  if (revenueAndClauses.length) revenueQuery.$and = revenueAndClauses;
  return { flightsQuery, revenueQuery };
};

const buildDashboardPeriods = (startDate, endDate, periodicity) => {
  if (!startDate || !endDate) return [];
  if (periodicity === "monthly") return generateLastDayOfMonths(startDate, endDate);
  if (periodicity === "quarterly") return generateQuarterlyDates(startDate, endDate);
  if (periodicity === "annually") return generateAnnualDates(startDate, endDate);
  if (periodicity === "weekly") return generateWeeklyDates(startDate, endDate);
  if (periodicity === "daily") return generateDailyDates(startDate, endDate);
  return generateWeeklyDates(startDate, endDate);
};

const getPeriodStartForDashboard = (periodEndDate, periodicity) => {
  if (periodicity === "monthly") return new Date(Date.UTC(periodEndDate.getUTCFullYear(), periodEndDate.getUTCMonth(), 1));
  if (periodicity === "quarterly") {
    const quarterStartMonth = Math.floor(periodEndDate.getUTCMonth() / 3) * 3;
    return new Date(Date.UTC(periodEndDate.getUTCFullYear(), quarterStartMonth, 1));
  }
  if (periodicity === "annually") return new Date(Date.UTC(periodEndDate.getUTCFullYear(), 0, 1));
  if (periodicity === "weekly") {
    const dayOfWeek = getUtcDayOfWeek(periodEndDate);
    const daysUntilMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    return startOfUtcDay(addUtcDays(periodEndDate, -daysUntilMonday));
  }
  return startOfUtcDay(periodEndDate);
};

const sumFlightFields = (rows = [], fields = []) =>
  rows.reduce((total, row) => total + pickNumeric(row, fields), 0);

const sumNumericField = (rows = [], field) =>
  rows.reduce((total, row) => total + toNumericValue(row?.[field]), 0);

const monthLabelKey = (date) => moment.utc(date).endOf("month").format("YYYY-MM-DD");

const addCurrencyExposure = (container, ccy, date, revenue = 0, cost = 0) => {
  const code = normalizeCurrencyCode(ccy);
  const dateKey = monthLabelKey(date);
  if (!code || !dateKey) return;
  if (!container[code]) container[code] = {};
  if (!container[code][dateKey]) container[code][dateKey] = { dateKey, revenue: 0, cost: 0, net: 0 };
  container[code][dateKey].revenue += toNumericValue(revenue);
  container[code][dateKey].cost -= Math.abs(toNumericValue(cost));
  container[code][dateKey].net = container[code][dateKey].revenue + container[code][dateKey].cost;
};

const addLocalCostExposures = (currencyBuckets, flight, reportingCurrency) => {
  const fields = [
    "engineFuelCost",
    "apuFuelCost",
    "maintenanceReserveContribution",
    "mrMonthly",
    "qualifyingSchMxEvents",
    "transitMaintenance",
    "otherMaintenance",
    "otherMxExpenses",
    "rotableChanges",
    "crewAllowances",
    "layoverCost",
    "crewPositioningCost",
    "airport",
    "navigation",
    "otherDoc",
  ];
  fields.forEach((field) => {
    const ccy = normalizeCurrencyCode(flight?.[`${field}CCY`]);
    if (!ccy || ccy === normalizeCurrencyCode(reportingCurrency)) return;
    addCurrencyExposure(currencyBuckets, ccy, flight.date, 0, flight[field]);
  });
};

const serializeCurrencyExposure = (buckets = {}) => Object.fromEntries(
  Object.entries(buckets).map(([ccy, byDate]) => [
    ccy,
    Object.values(byDate)
      .map((row) => ({
        dateKey: row.dateKey,
        revenue: Number(row.revenue.toFixed(2)),
        cost: Number(row.cost.toFixed(2)),
        net: Number(row.net.toFixed(2)),
      }))
      .sort((a, b) => a.dateKey.localeCompare(b.dateKey)),
  ])
);

const backfillDashboardPooMasterFields = async (userId) => {
  const pooRows = await PooTable.find({ userId }).select("_id flightId depStn arrStn variant userTag1 userTag2").lean();
  if (!pooRows.length) return;
  const flightIds = [...new Set(pooRows.map((row) => String(row.flightId || "")).filter(Boolean))];
  if (!flightIds.length) return;
  const flights = await Flights.find({ userId, _id: { $in: flightIds } }).select("depStn arrStn variant userTag1 userTag2").lean();
  const flightsById = new Map(flights.map((flight) => [String(flight._id), flight]));
  const ops = [];
  pooRows.forEach((row) => {
    const flight = flightsById.get(String(row.flightId));
    if (!flight) return;
    const $set = {};
    ["depStn", "arrStn", "variant", "userTag1", "userTag2"].forEach((field) => {
      const nextValue = String(flight[field] || "").trim();
      if (nextValue && String(row[field] || "").trim() !== nextValue) $set[field] = nextValue;
    });
    if (Object.keys($set).length) ops.push({ updateOne: { filter: { _id: row._id, userId }, update: { $set } } });
  });
  if (ops.length) await PooTable.bulkWrite(ops, { ordered: false });
};

const getDashboardData = async (req, res) => {
  try {
    const userId = getUserIdFromReq(req);
    if (!userId) return res.status(400).json({ error: "User ID missing" });

    const periodicity = normalizeSingleQueryValue(req.query.periodicity || "weekly").toLowerCase();
    const label = normalizeSingleQueryValue(req.query.label || "both").toLowerCase();
    const filters = {
      from: normalizeQueryValues(req.query.from),
      to: normalizeQueryValues(req.query.to),
      sector: normalizeQueryValues(req.query.sector),
      variant: normalizeQueryValues(req.query.variant),
      flight: normalizeQueryValues(req.query.flight),
      userTag1: normalizeQueryValues(req.query.userTag1),
      userTag2: normalizeQueryValues(req.query.userTag2),
    };
    const { flightsQuery, revenueQuery } = buildDashboardQueries({ userId, label, filters });

    const flightRange = await Flights.aggregate([
      { $match: { userId } },
      { $group: { _id: null, minDate: { $min: "$date" }, maxDate: { $max: "$date" } } },
    ]);

    if (!flightRange?.[0]?.minDate || !flightRange?.[0]?.maxDate) {
      return res.status(200).json({
        data: [],
        periods: [],
        revenueConfig: {},
        currencyCodes: [],
        fxRates: [],
        flightsForFxDates: [],
        riskExposure: { fuel: [], currencies: {} },
        riskExposureData: { fuel: [], currencies: {} },
      });
    }

    const startDate = startOfUtcDay(new Date(flightRange[0].minDate));
    const endDate = startOfUtcDay(new Date(flightRange[0].maxDate));
    const periodEnds = buildDashboardPeriods(startDate, endDate, periodicity);

    await backfillDashboardPooMasterFields(userId);

    const [allRevenueRows, rawCostConfig, rawRevenueConfig, fleetRows, flightsForFxDates] = await Promise.all([
      PooTable.find(revenueQuery).lean(),
      CostConfig.findOne({ userId }).lean(),
      RevenueConfig.findOne({ userId }).lean(),
      Fleet.find({ userId }).lean(),
      Flights.find({ userId }).select("date").lean(),
    ]);

    const revenueConfig = rawRevenueConfig || {};
    const baseCostConfig = normalizeCostConfig({
      ...(rawCostConfig || {}),
      reportingCurrency: revenueConfig.reportingCurrency || rawCostConfig?.reportingCurrency,
      fxRates: revenueConfig.fxRates || rawCostConfig?.fxRates,
      fleet: fleetRows,
    });
    const reportingCurrency = normalizeCurrencyCode(baseCostConfig.reportingCurrency || revenueConfig.reportingCurrency || "USD");
    const resultData = [];
    const periodPayloads = [];
    const currencyExposureBuckets = {};

    allRevenueRows.forEach((row) => {
      const localCcy = normalizeCurrencyCode(row.pooCcy || reportingCurrency);
      if (localCcy && localCcy !== reportingCurrency) {
        const localRevenue = row.applySSPricing ? toNumericValue(row.legTotalRev) : toNumericValue(row.odTotalRev);
        addCurrencyExposure(currencyExposureBuckets, localCcy, row.date, localRevenue, 0);
      }
    });

    for (const periodEnd of periodEnds) {
      const periodStart = getPeriodStartForDashboard(periodEnd, periodicity);
      const periodStartDay = startOfUtcDay(periodStart);
      const periodEndDay = endOfUtcDay(periodEnd);
      const periodFlightsQuery = {
        ...flightsQuery,
        date: { $gte: periodStartDay, $lte: periodEndDay },
      };
      const flightsRaw = await Flights.find(periodFlightsQuery).lean();
      const mrContext = await buildMaintenanceReserveContext(userId, flightsRaw);
      const flightsInPeriod = computeFlightCostsBatch(flightsRaw, {
        ...baseCostConfig,
        ...mrContext,
        fleet: fleetRows,
      });

      const revenueInPeriod = allRevenueRows.filter((row) => {
        const rowDate = row?.date ? startOfUtcDay(new Date(row.date)) : null;
        return rowDate && rowDate >= periodStartDay && rowDate <= periodEndDay;
      });

      flightsInPeriod.forEach((flight) => addLocalCostExposures(currencyExposureBuckets, flight, reportingCurrency));

      const departures = flightsInPeriod.length;
      const destinations = new Set(flightsInPeriod.map((flight) => String(flight.arrStn || "").trim()).filter(Boolean)).size;
      const seats = sumFlightFields(flightsInPeriod, ["seats"]);
      const pax = sumFlightFields(flightsInPeriod, ["pax"]);
      const cargoCapT = sumFlightFields(flightsInPeriod, ["cargoCapT", "CargoCapT", "cargoCap"]);
      const cargoT = sumFlightFields(flightsInPeriod, ["cargoT", "CargoT"]);
      const bh = sumFlightFields(flightsInPeriod, ["bh"]);
      const fh = sumFlightFields(flightsInPeriod, ["fh"]);
      const sumOfGcd = sumFlightFields(flightsInPeriod, ["gcd", "sectorGcd", "dist"]);
      const sumOfask = flightsInPeriod.reduce((total, flight) => total + (pickNumeric(flight, ["ask"]) || (pickNumeric(flight, ["seats"]) * pickNumeric(flight, ["gcd", "sectorGcd", "dist"]))), 0);
      const sumOfrsk = flightsInPeriod.reduce((total, flight) => total + (pickNumeric(flight, ["rsk", "rpk"]) || (pickNumeric(flight, ["pax"]) * pickNumeric(flight, ["gcd", "sectorGcd", "dist"]))), 0);
      const sumOfcargoAtk = flightsInPeriod.reduce((total, flight) => total + (pickNumeric(flight, ["cargoAtk"]) || (pickNumeric(flight, ["cargoCapT", "CargoCapT", "cargoCap"]) * pickNumeric(flight, ["gcd", "sectorGcd", "dist"]))), 0);
      const sumOfcargoRtk = flightsInPeriod.reduce((total, flight) => total + (pickNumeric(flight, ["cargoRtk"]) || (pickNumeric(flight, ["cargoT", "CargoT"]) * pickNumeric(flight, ["gcd", "sectorGcd", "dist"]))), 0);
      const uniqueAircraftDays = new Set(flightsInPeriod.map((flight) => `${flight.msn || flight.acftRegn || flight.acftType || flight.variant || "aircraft"}::${normalizeDateKey(flight.date)}`)).size;

      const fnlRccyPaxRev = sumNumericField(revenueInPeriod, "fnlRccyPaxRev");
      const fnlRccyCargoRev = sumNumericField(revenueInPeriod, "fnlRccyCargoRev");
      const explicitTotalRev = sumNumericField(revenueInPeriod, "fnlRccyTotalRev");
      const fnlRccyTotalRev = explicitTotalRev || (fnlRccyPaxRev + fnlRccyCargoRev);

      const engineFuelCostRCCY = sumNumericField(flightsInPeriod, "engineFuelCostRCCY");
      const apuFuelCostRCCY = sumNumericField(flightsInPeriod, "apuFuelCostRCCY");
      const totalFuelCostRCCY = engineFuelCostRCCY + apuFuelCostRCCY;
      const maintenanceReserveContributionRCCY = sumNumericField(flightsInPeriod, "maintenanceReserveContributionRCCY");
      const mrMonthlyRCCY = sumNumericField(flightsInPeriod, "mrMonthlyRCCY");
      const totalMrContributionRCCY = maintenanceReserveContributionRCCY + mrMonthlyRCCY;
      const qualifyingSchMxEventsRCCY = sumNumericField(flightsInPeriod, "qualifyingSchMxEventsRCCY");
      const transitMaintenanceRCCY = sumNumericField(flightsInPeriod, "transitMaintenanceRCCY");
      const otherMaintenanceRCCY = sumNumericField(flightsInPeriod, "otherMaintenanceRCCY");
      const otherMaintenanceUtilisationRCCY = sumFlightFields(flightsInPeriod, ["otherMaintenance1", "otherMaintenance2"]);
      const otherMaintenanceCalendarRCCY = sumFlightFields(flightsInPeriod, ["otherMaintenance3"]);
      const otherMxExpensesRCCY = sumNumericField(flightsInPeriod, "otherMxExpensesRCCY");
      const rotableChangesRCCY = sumNumericField(flightsInPeriod, "rotableChangesRCCY");
      const totalMaintenanceCostRCCY = totalMrContributionRCCY + qualifyingSchMxEventsRCCY + transitMaintenanceRCCY + otherMaintenanceRCCY + otherMxExpensesRCCY + rotableChangesRCCY;
      const crewAllowancesRCCY = sumNumericField(flightsInPeriod, "crewAllowancesRCCY");
      const layoverCostRCCY = sumNumericField(flightsInPeriod, "layoverCostRCCY");
      const crewPositioningCostRCCY = sumNumericField(flightsInPeriod, "crewPositioningCostRCCY");
      const crewTotalDirectCostRCCY = crewAllowancesRCCY + layoverCostRCCY + crewPositioningCostRCCY;
      const airportRCCY = sumNumericField(flightsInPeriod, "airportRCCY");
      const navigationRCCY = sumNumericField(flightsInPeriod, "navigationRCCY");
      const otherDocRCCY = sumNumericField(flightsInPeriod, "otherDocRCCY");
      const totalDocRCCY = totalFuelCostRCCY + totalMaintenanceCostRCCY + crewTotalDirectCostRCCY + airportRCCY + navigationRCCY + otherDocRCCY;
      const grossProfitLossRCCY = fnlRccyTotalRev - totalDocRCCY;

      const data = {
        destinations,
        departures,
        seats,
        pax,
        paxSF: safePercent(pax, seats),
        paxLF: safePercent(pax, seats),
        cargoCapT,
        cargoT,
        ct2ctc: safePercent(cargoT, cargoCapT),
        cftk2atk: safePercent(sumOfcargoRtk, sumOfcargoAtk),
        bh,
        fh,
        sumOfGcd,
        averageDailyUtilisation: uniqueAircraftDays > 0 ? Number((bh / uniqueAircraftDays).toFixed(2)) : 0,
        adu: uniqueAircraftDays > 0 ? Number((bh / uniqueAircraftDays).toFixed(2)) : 0,
        connectingFlights: 0,
        seatCapBeyondFlgts: 0,
        seatCapBehindFlgts: 0,
        cargoCapBehindFlgts: 0,
        cargoCapBeyondFlgts: 0,
        sumOfask,
        sumOfrsk,
        sumOfcargoAtk,
        sumOfcargoRtk,
        fnlRccyPaxRev,
        fnlRccyCargoRev,
        fnlRccyTotalRev,
        engineFuelConsumption: sumNumericField(flightsInPeriod, "engineFuelConsumption"),
        engineFuelConsumptionKg: sumNumericField(flightsInPeriod, "engineFuelConsumptionKg"),
        apuFuelConsumptionKg: sumNumericField(flightsInPeriod, "apuFuelConsumptionKg"),
        engineFuelCostRCCY,
        apuFuelCostRCCY,
        totalFuelCostRCCY,
        maintenanceReserveContributionRCCY,
        mrMonthlyRCCY,
        totalMrContributionRCCY,
        qualifyingSchMxEventsRCCY,
        transitMaintenanceRCCY,
        otherMaintenanceRCCY,
        otherMaintenanceUtilisationRCCY,
        otherMaintenanceCalendarRCCY,
        otherMxExpensesRCCY,
        rotableChangesRCCY,
        totalMaintenanceCostRCCY,
        crewAllowancesRCCY,
        layoverCostRCCY,
        crewPositioningCostRCCY,
        crewTotalDirectCostRCCY,
        airportRCCY,
        navigationRCCY,
        otherDocRCCY,
        totalDocRCCY,
        grossProfitLossRCCY,
      };

      const period = {
        key: normalizeDateKey(periodEnd),
        startDate: periodStartDay.toISOString(),
        endDate: periodEndDay.toISOString(),
        dateKey: normalizeDateKey(periodEnd),
        dateLabel: moment.utc(periodEnd).format("DD MMM YY"),
        data,
      };
      resultData.push({ ...data, startDate: period.startDate, endDate: period.endDate, dateKey: period.dateKey, dateLabel: period.dateLabel });
      periodPayloads.push(period);
    }

    const riskExposure = {
      fuel: periodPayloads.map((period) => ({
        dateKey: period.dateKey,
        engineFuelKg: toNumericValue(period.data.engineFuelConsumptionKg),
        apuFuelKg: toNumericValue(period.data.apuFuelConsumptionKg),
        totalFuelKg: toNumericValue(period.data.engineFuelConsumptionKg) + toNumericValue(period.data.apuFuelConsumptionKg),
      })),
      currencies: serializeCurrencyExposure(currencyExposureBuckets),
    };

    res.status(200).json({
      data: resultData,
      periods: periodPayloads,
      revenueConfig,
      currencyCodes: revenueConfig.currencyCodes || [],
      fxRates: revenueConfig.fxRates || [],
      flightsForFxDates,
      riskExposure,
      riskExposureData: riskExposure,
    });
  } catch (error) {
    console.error("getDashboardData error:", error);
    res.status(500).json({ error: "Internal Server Error", message: error.message });
  }
};

module.exports = {
  populateDashboardDropDowns,
  getDashboardData
};
