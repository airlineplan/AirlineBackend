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

const { deleteRotation } = require('./rotationController');
const AddData = async (req, res) => {
  try {
    let {
      flightNumber, // Optional property
      flight, // Optional property
      depStn,
      std,
      bt,
      sta,
      arrStn,
      variant,
      effFromDt,
      effToDt,
      effFromDate,
      effToDate,
      dow,
      userTag1,
      userTag2,
      remarks1,
      remarks2,
      timeZone,
      domINTL = ''
    } = req.body;
    const userId = req.user.id;
    domINTL = domINTL.toLowerCase();
    flight = flight || flightNumber;
    effFromDt = effFromDt || effFromDate;
    effToDt = effToDate || effToDt

    if (timeZone) {
      effFromDt = timeZoneCorrectedDates(effFromDt, timeZone);
      effToDt = timeZoneCorrectedDates(effToDt, timeZone)
    }

    const newData = new Data({
      flight,
      depStn,
      std,
      bt,
      sta,
      arrStn,
      variant,
      effFromDt,
      effToDt,
      dow,
      userTag1,
      userTag2,
      remarks1,
      remarks2,
      userId,
      timeZone,
      domINTL: domINTL.toLowerCase()
    });

    newData.userId = userId;
    const data = await newData.save();

    const newSector = new Sector({
      sector1: data.depStn,
      sector2: data.arrStn,
      variant: data.variant,
      bt: data.bt,
      sta: data.sta,
      dow: data.dow,
      flight: data.flight,
      std: data.std,
      domINTL: data.domINTL.toLowerCase(),
      userTag1: data.userTag1,
      userTag2: data.userTag2,
      remarks1: data.remarks1,
      remarks2: data.remarks2,
      fromDt: data.effFromDt,
      toDt: data.effToDt,
      userId: req.user.id,
      networkId: data._id,
    });

    await newSector.save();

    // await createConnections(req.user.id);

    res.status(201).json({ message: "Data created successfully" });
  } catch (error) {
    console.error("Error while saving data:", error);
    res.status(500).json({ error: "An error occurred while creating data" });
  }
};
const AddDataFromRotations = async (req, res, rotationDetailsId) => {
  try {
    let {
      flightNumber, // Optional property
      flight, // Optional property
      depStn,
      std,
      bt,
      sta,
      arrStn,
      variant,
      effFromDt,
      effToDt,
      effFromDate,
      effToDate,
      dow,
      userTag1,
      userTag2,
      remarks1,
      remarks2,
      rotationNumber,
      timeZone,
      domINTL = '',
      domIntl,
      depNumber
    } = req.body;
    const userId = req.user.id;
    domINTL = domINTL.toLowerCase();
    domINTL = domINTL || domIntl;
    flight = flight || flightNumber;
    effFromDt = effFromDt || effFromDate;
    effToDt = effToDate || effToDt

    if (timeZone) {
      effFromDt = timeZoneCorrectedDates(effFromDt, timeZone);
      effToDt = timeZoneCorrectedDates(effToDt, timeZone)
    }

    const newData = new Data({
      flight,
      depStn,
      std,
      bt,
      sta,
      arrStn,
      variant,
      effFromDt,
      effToDt,
      dow,
      userTag1,
      userTag2,
      remarks1,
      remarks2,
      userId,
      timeZone,
      rotationNumber,
      addedByRotation: '' + rotationNumber + '-' + depNumber,
      domINTL: domINTL.toLowerCase()
    });

    newData.userId = userId;
    const data = await newData.save();

    const newSector = new Sector({
      sector1: data.depStn,
      sector2: data.arrStn,
      variant: data.variant,
      bt: data.bt,
      sta: data.sta,
      dow: data.dow,
      flight: data.flight,
      std: data.std,
      domINTL: data.domINTL.toLowerCase(),
      userTag1: data.userTag1,
      userTag2: data.userTag2,
      remarks1: data.remarks1,
      remarks2: data.remarks2,
      fromDt: data.effFromDt,
      toDt: data.effToDt,
      userId: req.user.id,
      networkId: data._id,
      rotationNumber,
      addedByRotation: '' + rotationNumber + '-' + depNumber,
    });

    await newSector.save();
    return { success: true };
  } catch (error) {
    console.error("Error while saving data:", error);
    return { success: false };
  }
};
const getData = async (req, res) => {
  try {
    const id = req.user.id;
    const data = await Data.find({ userId: id });
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
};
const deleteFlightsAndUpdateSectors = async (req, res) => {
  try {
    // Read ids from the request body instead of URL parameters
    const { ids } = req.body;

    // Safety check to ensure ids array is present
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: "No valid IDs provided for deletion" });
    }

    // Fetch the documents being deleted
    const documentsToDelete = await Data.find({ _id: { $in: ids } });

    // Construct an array of unique station names to delete
    const stationNamesToDelete = [...documentsToDelete.flatMap(doc => [doc.arrStn, doc.depStn])];

    const userId = req.user.id;
    // Delete stations only if they are not present in other documents for the same user
    for (const stationName of stationNamesToDelete) {
      const station = await Stations.findOne({ stationName, userId });

      if (station) {
        if (station.freq === 1) {
          // If freq is 1, delete the entry 
          await Stations.deleteOne({ stationName, userId });
        } else {
          // If freq is greater than 1, decrement by 1
          await Stations.updateOne(
            { stationName, userId },
            { $inc: { freq: -1 } }
          );
        }
      }
    }

    const result = await Data.deleteMany({ _id: { $in: ids } });

    const flightsToDelete = await Flights.find({ networkId: { $in: ids } });

    const rotationNumbersToDelete = [...new Set(flightsToDelete
      .filter(flight => flight.rotationNumber !== undefined) // Exclude undefined values
      .map(flight => flight.rotationNumber)
    )];

    const flgtDelCount = await Flights.deleteMany({ networkId: { $in: ids } });

    // Delete entries from RotationDetails model
    await RotationDetails.deleteMany({ rotationNumber: { $in: rotationNumbersToDelete }, userId });

    // Delete entries from RotationSummary model
    await RotationSummary.deleteMany({ rotationNumber: { $in: rotationNumbersToDelete }, userId });

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Data not found" });
    }

    await Sector.deleteMany({
      networkId: { $in: ids }
    });

    // await createConnections(userId);

    res.json({
      message: "Data deleted successfully",
      deletedData: result,
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal server error" });
  }
};
const downloadExpenses = async (req, res) => {
  try {
    const userId = req.user.id;
    const workbook = new exceljs.stream.xlsx.WorkbookWriter({
      stream: res,
      useSharedStrings: true, // Reduce memory footprint
      useStyles: true, // Only enable styles if needed
    });

    const worksheet = workbook.addWorksheet("My-Product");

    // Define columns
    worksheet.columns = [
      { header: "S no.", key: "s_no" },
      { header: "Date", key: "date" },
      { header: "Day", key: "day" },
      { header: "Flight #.", key: "flight" },
      { header: "Dep Stn", key: "depStn" },
      { header: "STD(LT).", key: "std" },
      { header: "BT", key: "bt" },
      { header: "FT", key: "ft" },             // NEW: FT inserted between BT and STA
      { header: "STA(LT)", key: "sta" },
      { header: "Arr Stn.", key: "arrStn" },
      { header: "Sector.", key: "sector" },
      { header: "ACFT", key: "acftType" },
      { header: "BH", key: "bh" },             // NEW: BH inserted between ACFT and FH
      { header: "FH", key: "fh" },
      { header: "Variant.", key: "variant" },
      { header: "Seats.", key: "seats" },
      { header: "Cargo Cap", key: "CargoCapT" },
      { header: "Dist", key: "dist" },
      { header: "Pax", key: "pax" },
      { header: "Cargo T", key: "CargoT" },
      { header: "ASK", key: "ask" },
      { header: "RSK", key: "rsk" },
      { header: "Cargo ATK", key: "cargoAtk" },
      { header: "Cargo RTK", key: "cargoRtk" },
      { header: "Dom / INTL", key: "domIntl" },
      { header: "User Tag 1", key: "userTag1" },
      { header: "User Tag 2", key: "userTag2" },
      { header: "Remarks 1", key: "remarks1" },
      { header: "Remarks 2", key: "remarks2" },
      { header: "Rotations #", key: "rotationNumber" },
    ];

    // Set response headers for download
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", `attachment; filename=FLGTs.xlsx`);

    let count = 1;

    // Use MongoDB cursor to stream data
    const cursor = Flights.find({ userId }).cursor();

    for await (const product of cursor) {
      const excelProduct = {
        s_no: count,
        ...product.toObject(),
        date: product.date ? new Date(product.date).toISOString().split("T")[0] : "",
        ft: product.ft ? parseFloat(product.ft) : 0,       // NEW: Parse FT as float
        acftType: product.acftType || "",
        bh: product.bh ? parseFloat(product.bh) : 0,       // NEW: Parse BH as float
        fh: product.fh ? parseFloat(product.fh) : 0,
        seats: parseFloat(product.seats) || 0,
        CargoCapT: parseFloat(product.CargoCapT) || 0,
        dist: parseFloat(product.dist) || 0,
        pax: parseInt(product.pax, 10) || 0,
        CargoT: parseFloat(product.CargoT) || 0,
        ask: parseInt(product.ask, 10) || 0,
        rsk: parseInt(product.rsk, 10) || 0,
        cargoAtk: parseInt(product.cargoAtk, 10) || 0,
        cargoRtk: parseInt(product.cargoRtk, 10) || 0,
      };

      worksheet.addRow(excelProduct).commit();
      count++;
    }

    worksheet.commit();
    await workbook.commit();
  } catch (error) {
    console.error(error);
    res.status(500).send("An error occurred while generating the file.");
  }
};


// const downloadExpenses = async (req, res) => {
//   try {
//     const userId = req.user.id;
//     const workbook = new exceljs.Workbook();
//     const worksheet = workbook.addWorksheet("My-Product");

//     worksheet.columns = [
//       { header: "S no.", key: "s_no" },
//       { header: "Date", key: "date" },
//       { header: "Day", key: "day" },
//       { header: "Flight #.", key: "flight" },
//       { header: "Dep Stn", key: "depStn" },
//       { header: "STD(LT).", key: "std" },
//       { header: "BT", key: "bt" },
//       { header: "STA(LT)", key: "sta" },
//       { header: "Arr Stn.", key: "arrStn" },
//       { header: "Sector.", key: "sector" },
//       { header: "Variant.", key: "variant" },
//       { header: "Seats.", key: "seats" },
//       { header: "Cargo Cap", key: "CargoCapT" },
//       { header: "Dist", key: "dist" },
//       { header: "Pax", key: "pax" },
//       { header: "Cargo T", key: "CargoT" },
//       { header: "ASK", key: "ask" },
//       { header: "RSK", key: "rsk" },
//       { header: "Cargo ATK", key: "cargoAtk" },
//       { header: "Cargo RTK", key: "cargoRtk" },
//       { header: "Dom / INTL", key: "domIntl" },
//       { header: "User Tag 1", key: "userTag1" },
//       { header: "User Tag 2", key: "userTag2" },
//       { header: "Remarks 1", key: "remarks1" },
//       { header: "Remarks 2", key: "remarks2" },
//       { header: "Rotations #", key: "rotationNumber" },
//     ];

//     let count = 1;
//     const productData = await Flights.find({ userId });
//     productData.forEach((product) => {

//       var excelProduct = {};
//       excelProduct.s_no = count;


//       for (var key in product) {
//         if (key === 'seats' || key === 'CargoCapT' || key === 'dist' || key === 'pax' || key === 'CargoT' || key === 'ask' || key === 'rsk' || key === 'cargoAtk' || key === 'cargoRtk' || key === 'rotationNumber') {
//           // Convert to Float for specific fields
//           excelProduct[key] = parseFloat(product[key]);
//         } else {
//           // Leave other fields as strings
//           excelProduct[key] = product[key];

//         }
//       }

//       worksheet.addRow(excelProduct);
//       worksheet.getCell(`S${count + 1}`).numFmt = '0.00';
//       worksheet.getCell(`T${count + 1}`).numFmt = '0.00';
//       count++;
//     });
//     worksheet.getRow(1).eachCell((cell) => {
//       cell.font = { bold: true };
//     });

//     res.setHeader(
//       "Content-Type",
//       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
//     );
//     res.setHeader("Content-Disposition", `attachment; filename=FLGTs.xlsx`);

//     await workbook.xlsx.write(res);

//     res.status(200).end();
//   } catch (error) {
//     console.log(error);
//   }
// };
const updateData = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;

    if (!id) {
      return res.status(400).json({ message: "ID is required" });
    }

    // Convert comma separated IDs to array
    const idArray = id.split(",").map((item) => item.trim());

    const updatedFlights = [];

    for (const dataId of idArray) {

      // -------------------------------
      // 1️⃣ Handle rotation deletion
      // -------------------------------
      const flightsWithRotation = await Flights.find({
        networkId: dataId,
        rotationNumber: { $exists: true, $ne: null }
      });

      if (flightsWithRotation.length > 0) {

        const rotationNumber = flightsWithRotation[0].rotationNumber;

        const result = await RotationDetails.aggregate([
          { $match: { rotationNumber: rotationNumber } },
          { $sort: { depNumber: -1 } },
          { $limit: 1 },
          { $project: { depNumber: 1 } }
        ]);

        if (result.length > 0) {
          const depNumber = result[0].depNumber;

          await deleteRotation(userId, rotationNumber, depNumber);
        }
      }

      // -------------------------------
      // 2️⃣ Build Safe Update Payload
      // -------------------------------
      const updatePayload = {};

      Object.entries(req.body).forEach(([key, value]) => {
        if (
          value !== undefined &&
          value !== null &&
          value !== "" &&
          typeof value !== "function"
        ) {
          updatePayload[key] = value;
        }
      });

      // Normalize domINTL
      if (updatePayload.domINTL) {
        updatePayload.domINTL = updatePayload.domINTL.toLowerCase();
      }

      // Remove timezone from DB update (if not required in schema)
      delete updatePayload.timeZone;
      delete updatePayload.timezone;

      // If nothing to update
      if (Object.keys(updatePayload).length === 0) {
        continue;
      }

      // -------------------------------
      // 3️⃣ Atomic Safe Update
      // -------------------------------
      const updatedData = await Data.findByIdAndUpdate(
        dataId,
        { $set: updatePayload },
        { new: true, runValidators: true }
      );

      if (!updatedData) {
        return res.status(404).json({ message: "Data not found" });
      }

      updatedFlights.push(updatedData);
    }

    return res.json({
      updatedFlights,
      message: "Data Updated successfully"
    });

  } catch (error) {
    console.error("Update Error:", error);
    return res.status(500).json({ message: "Failed to update data." });
  }
};
const singleData = async (req, res) => {
  try {
    // Ensure req.params.id is defined and not empty
    if (!req.params.id) {
      return res.status(400).json({ message: "Invalid product ID" });
    }

    const product = await Data.findById(req.params.id);

    if (!product) {
      return res.status(404).json({ message: "Product not found" });
    }

    res.status(200).json(product);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};
const getConnections = async (req, res) => {
  try {
    const userId = req.user.id;

    let { label, periodicity, from, to } = req.query;

    if (!periodicity || !label) {
      return res.status(400).json({ error: "Missing label or periodicity" });
    }

    periodicity = periodicity.toLowerCase();
    label = label.toLowerCase();

    const flightsQuery = { userId };

    // Label filter
    if (label === "both") {
      flightsQuery.domIntl = { $in: ["dom", "intl"] };
    } else {
      flightsQuery.domIntl = label;
    }

    if (from && Array.isArray(from) && from.length > 0) {
      flightsQuery.depStn = { $in: from };
    }

    if (to && Array.isArray(to) && to.length > 0) {
      flightsQuery.arrStn = { $in: to };
    }

    const allFlights = await Flights.find(flightsQuery).lean();

    if (!allFlights.length) return res.json([]);

    const minDate = new Date(Math.min(...allFlights.map(f => new Date(f.date))));
    const maxDate = new Date(Math.max(...allFlights.map(f => new Date(f.date))));

    minDate.setUTCHours(0, 0, 0, 0);
    maxDate.setUTCHours(0, 0, 0, 0);

    let periods = [];

    if (periodicity === "daily") {
      periods = generateDailyDates(minDate, maxDate);
    } else if (periodicity === "weekly") {
      periods = generateWeeklyDates(minDate, maxDate);
    } else if (periodicity === "monthly") {
      periods = generateLastDayOfMonths(minDate, maxDate);
    } else if (periodicity === "quarterly") {
      periods = generateQuarterlyDates(minDate, maxDate);
    } else if (periodicity === "annually") {
      periods = generateAnnualDates(minDate, maxDate);
    }

    const result = [];

    for (const periodEndDate of periods) {

      let periodStartDate;

      if (periodicity === "monthly") {
        periodStartDate = new Date(periodEndDate.getFullYear(), periodEndDate.getMonth(), 1);
      }
      else if (periodicity === "quarterly") {
        const qMonth = Math.floor(periodEndDate.getMonth() / 3) * 3;
        periodStartDate = new Date(periodEndDate.getFullYear(), qMonth, 1);
      }
      else if (periodicity === "annually") {
        periodStartDate = new Date(periodEndDate.getFullYear(), 0, 1);
      }
      else if (periodicity === "weekly") {
        const day = periodEndDate.getDay();
        const diff = day === 0 ? 6 : day - 1;
        periodStartDate = new Date(periodEndDate);
        periodStartDate.setDate(periodEndDate.getDate() - diff);
      }
      else {
        periodStartDate = new Date(periodEndDate);
      }

      const flightsInPeriod = allFlights.filter(f => {
        const d = new Date(f.date);
        return d >= periodStartDate && d <= periodEndDate;
      });

      const beyond = flightsInPeriod.filter(f => f.beyondODs);
      const behind = flightsInPeriod.filter(f => f.behindODs);

      const connectingFlights = behind.length;

      const seatCapBeyondFlgts = beyond.reduce((t, f) => t + (Number(f.seats) || 0), 0);
      const seatCapBehindFlgts = behind.reduce((t, f) => t + (Number(f.seats) || 0), 0);

      const cargoCapBeyondFlgts = beyond.reduce((t, f) => t + (Number(f.CargoCapT) || 0), 0);
      const cargoCapBehindFlgts = behind.reduce((t, f) => t + (Number(f.CargoCapT) || 0), 0);

      result.push({
        endDate: periodEndDate,
        connectingFlights,
        seatCapBeyondFlgts,
        seatCapBehindFlgts,
        cargoCapBeyondFlgts,
        cargoCapBehindFlgts
      });
    }

    res.json(result);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch connections" });
  }
};
const getListPageData = async (req, res) => {
  try {
    const {
      label, periodicity, metric, from, to, sector, variant, userTag1, userTag2, rotation,
      depTimeFrom, depTimeTo, arrTimeFrom, arrTimeTo
    } = req.body;

    const userId = req.user.id;

    // =====================================
    // 1️⃣ BUILD MATCH QUERY
    // =====================================
    const matchQuery = { userId };

    if (label && label.value !== "both") {
      matchQuery.domIntl = label.value.toLowerCase();
    }

    const applyArrayFilter = (field, filterArray) => {
      if (filterArray?.length) {
        matchQuery[field] = { $in: filterArray.map(f => f.value) };
      }
    };

    applyArrayFilter("depStn", from);
    applyArrayFilter("arrStn", to);
    applyArrayFilter("sector", sector);
    applyArrayFilter("variant", variant);
    applyArrayFilter("userTag1", userTag1);
    applyArrayFilter("userTag2", userTag2);
    applyArrayFilter("rotationNumber", rotation);

    if (depTimeFrom || depTimeTo) {
      matchQuery.std = {};
      if (depTimeFrom) matchQuery.std.$gte = depTimeFrom;
      if (depTimeTo) matchQuery.std.$lte = depTimeTo;
    }

    if (arrTimeFrom || arrTimeTo) {
      matchQuery.sta = {};
      if (arrTimeFrom) matchQuery.sta.$gte = arrTimeFrom;
      if (arrTimeTo) matchQuery.sta.$lte = arrTimeTo;
    }

    // =====================================
    // 2️⃣ FETCH RAW DATA
    // =====================================
    // We fetch the raw data directly. The React frontend contains a 
    // sophisticated Pivot algorithm that handles Date bucketing, 
    // hierarchical grouping, and metric aggregation dynamically.
    const flights = await Flights.find(matchQuery).lean();

    return res.json({
      flights: flights || []
    });

  } catch (error) {
    console.error("List Page Error:", error);
    res.status(500).json({ message: "Internal Server Error" });
  }
};
const getViewData = async (req, res) => {
  try {
    const { mode, weekStart, station, viewTimezone } = req.query;

    if (!weekStart || !mode) {
      return res.status(400).json({ success: false, message: "Missing parameters" });
    }

    const userId = req.user.id;

    /* ---------------------------------------------------------
       1️⃣  TIMEZONE + WEEK RANGE CALCULATION
    ---------------------------------------------------------- */

    const parseUTCOffsetToMinutes = (tz) => {
      const sign = tz.includes("+") ? 1 : -1;
      const [hours, minutes] = tz
        .replace("UTC", "")
        .replace("+", "")
        .replace("-", "")
        .split(":")
        .map(Number);

      return sign * ((hours || 0) * 60 + (minutes || 0));
    };

    const offsetMinutes = parseUTCOffsetToMinutes(viewTimezone || "UTC+0:00");

    const weekStartInViewTZ = new Date(`${weekStart}T00:00:00.000Z`);

    const startDate = new Date(
      weekStartInViewTZ.getTime() - offsetMinutes * 60000
    );

    const endDate = new Date(startDate);
    endDate.setUTCDate(startDate.getUTCDate() + 6);
    endDate.setUTCHours(23, 59, 59, 999);

    /* ---------------------------------------------------------
       2️⃣  FETCH ALL FLIGHTS IN WEEK
    ---------------------------------------------------------- */

    const weekFlights = await Flights.find({
      userId,
      date: { $gte: startDate, $lte: endDate }
    }).lean();

    if (!weekFlights.length) {
      return res.json({
        success: true,
        timeline: { from: startDate, to: endDate },
        rows: []
      });
    }

    /* ---------------------------------------------------------
       3️⃣  FETCH CONNECTIONS FOR THESE FLIGHTS
    ---------------------------------------------------------- */

    const flightIds = weekFlights.map(f => f._id.toString());

    const connections = await Connections.find({
      userId,
      $or: [
        { flightID: { $in: flightIds } },
        { beyondOD: { $in: flightIds } }
      ]
    }).lean();

    const connectionRightSet = new Set();
    const connectionLeftSet = new Set();

    connections.forEach(conn => {
      connectionRightSet.add(conn.flightID.toString());
      connectionLeftSet.add(conn.beyondOD.toString());
    });

    weekFlights.forEach(flight => {
      const id = flight._id.toString();
      flight.connectionRight = connectionRightSet.has(id);
      flight.connectionLeft = connectionLeftSet.has(id);
    });

    /* ---------------------------------------------------------
       4️⃣  GROUP BY MODE
    ---------------------------------------------------------- */

    let rows = [];

    /* ------------------ ROTATIONS ------------------ */
    if (mode === "Rotations") {
      const grouped = {};

      weekFlights.forEach(f => {
        const key = f.rotationNumber || "Unassigned";

        if (!grouped[key]) {
          grouped[key] = {
            variant: f.variant,
            flights: []
          };
        }

        grouped[key].flights.push(f);
      });

      rows = Object.entries(grouped)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([rot, group]) => ({
          leftColumn: { rot, variant: group.variant },
          flights: group.flights
        }));
    }

    /* ------------------ SECTORS ------------------ */
    else if (mode === "Sectors") {
      const grouped = {};

      weekFlights.forEach(f => {
        const key = f.sector || "Unassigned";

        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(f);
      });

      rows = Object.entries(grouped).map(([sector, flights]) => ({
        leftColumn: { sector },
        flights
      }));
    }

    /* ------------------ STATION ------------------ */
    else if (mode === "Station" && station) {
      const flights = weekFlights.filter(
        f => f.depStn === station || f.arrStn === station
      );

      const grouped = {};

      flights.forEach(f => {
        const isDep = f.depStn === station;
        const key = isDep ? `DEP-${f.arrStn}` : `ARR-${f.depStn}`;
        const type = isDep ? "Departures to" : "Arrivals from";
        const sectorLabel = isDep ? f.arrStn : f.depStn;

        if (!grouped[key]) {
          grouped[key] = { type, sector: sectorLabel, flights: [] };
        }

        grouped[key].flights.push(f);
      });

      rows = Object.values(grouped).map(group => ({
        leftColumn: { type: group.type, sector: group.sector },
        flights: group.flights
      }));
    }

    /* ------------------ AIRCRAFT ------------------ */
    else if (mode === "Aircraft") {
      const grouped = {};

      weekFlights.forEach(f => {
        const key = f.userTag1 || "Unassigned";

        if (!grouped[key]) {
          grouped[key] = {
            variant: f.variant,
            flights: []
          };
        }

        grouped[key].flights.push(f);
      });

      rows = Object.entries(grouped).map(([ac, group]) => ({
        leftColumn: { ac, variant: group.variant },
        flights: group.flights
      }));
    }

    /* ---------------------------------------------------------
       5️⃣  RETURN RESPONSE
    ---------------------------------------------------------- */

    res.json({
      success: true,
      timeline: { from: startDate, to: endDate },
      rows
    });

  } catch (error) {
    console.error("View data error:", error);
    res.status(500).json({ success: false, message: "Server Error" });
  }
};

module.exports = {
  AddData,
  AddDataFromRotations,
  getData,
  deleteFlightsAndUpdateSectors,
  downloadExpenses,
  updateData,
  singleData,
  getConnections,
  getListPageData,
  getViewData
};
