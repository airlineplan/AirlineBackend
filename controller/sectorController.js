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

const getSecors = async (req, res) => {
  try {
    const id = req.user.id;
    const data = await Sector.find({ userId: id });
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
};
const AddSectors = async (req, res) => {
  try {
    const {
      sector1,
      sector2,
      acftType,
      variant,
      bt,
      gcd,
      paxCapacity,
      CargoCapT,
      paxLF,
      cargoLF,
      fromDt,
      toDt,
    } = req.body;
    const userId = req.user.id;
    // const existingData = await Sector.findOne({
    //   fromDt: { $lte: new Date(fromDt) },
    //   toDt: { $gte: new Date(toDt) },
    //   userId,
    // });
    // if (existingData) {
    //   return res
    //     .status(400)
    //     .json({ error: "Data with this combination already exists" });
    // }

    const newSectors = new Sector({
      sector1,
      sector2,
      acftType,
      variant,
      bt,
      gcd,
      paxCapacity,
      CargoCapT,
      paxLF,
      cargoLF,
      fromDt,
      toDt,
      userId: req.user.id,
    });

    await newSectors.save();
    res.status(201).json({ message: "Data created successfully" });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "An error occurred while creating data" });
  }
};
// const deleteSectors = async (req, res) => {
//   try {
//     const id = req.params.id;
//     const sector = await Sector.findById(id);

//     if (!sector) {
//       return res.status(404).json({ error: "Data not found" });
//     }

//     const toDt = sector.toDt;

//     if (toDt !== null && !(toDt instanceof Date && !isNaN(toDt))) {
//       return res.status(400).json({ error: "Invalid date format for 'toDt'" });
//     }

//     if (toDt !== null) {
//       const currentDate = new Date();
//       if (toDt.getTime() > currentDate.getTime()) {
//         return res
//           .status(403)
//           .json({ error: "Permission denied. Data is not expired yet." });
//       }
//     }

//     const deletedSectorData = await Sector.findByIdAndDelete(id);

//     if (!deletedSectorData) {
//       return res.status(404).json({ error: "Data not found" });
//     }

//     // Delete associated Flights records
//     const deletedFlightData = await Flights.deleteMany({ sectorId: id });

//     res.json({
//       message: "Data deleted successfully",
//       deletedSectorData,
//       deletedFlightData,
//     });
//   } catch (error) {
//     console.error(error);
//     res.status(500).json({ error: "Internal server error" });
//   }
// };

const deleteSectors = async (req, res) => {
  try {
    const ids = req.params.ids.split(",");

    // Use find() to retrieve sectors by their IDs
    const sectors = await Sector.find({ _id: { $in: ids } });


    if (sectors.length === 0) {
      return res.status(404).json({ error: "Data not found" });
    }

    //userId should be same for all
    const userId = sectors[0].userId;

    for (const sector of sectors) {
      const toDt = sector.toDt;

      if (toDt !== null && !(toDt instanceof Date && !isNaN(toDt))) {
        return res
          .status(400)
          .json({ error: "Invalid date format for 'toDt'" });
      }

      if (toDt !== null) {
        const currentDate = new Date();
        if (toDt.getTime() > currentDate.getTime()) {
          return res
            .status(403)
            .json({ error: "Permission denied. Data is not expired yet." });
        }
      }
    }

    // Delete associated Flights records
    const deletedFlightData = await Flights.deleteMany({
      sectorId: { $in: ids },
    });

    // Delete sectors
    const deletedSectorData = await Sector.deleteMany({ _id: { $in: ids } });

    // await createConnections(userId);

    res.json({
      message: "Data deleted successfully",
      deletedSectorData,
      deletedFlightData,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal server error" });
  }
};
// const updateSector = async (req, res) => {
//   const { id } = req.params;
//   const {
//     sector1,
//     sector2,
//     acftType,
//     variant,
//     bt,
//     gcd,
//     paxCapacity,
//     CargoCapT,
//     paxLF,
//     cargoLF,
//     fromDt,
//     toDt,
//   } = req.body;

//   try {
//     // const userId = req.user.id;
//     const existingData = await Sector.findOne({
//       fromDt: { $lte: new Date(fromDt) },
//       toDt: { $gte: new Date(toDt) },
//       // userId,
//     });
//     if (existingData && existingData._id != id) {
//       return res
//         .status(400)
//         .json({ error: "Data with this combination already exists" });
//     }
//     const updatedSectore = await Sector.findByIdAndUpdate(
//       id,
//       {
//         sector1,
//         sector2,
//         acftType,
//         variant,
//         bt,
//         gcd,
//         paxCapacity,
//         CargoCapT,
//         paxLF,
//         cargoLF,
//         fromDt,
//         toDt,
//       },
//       { new: true }
//     );

//     if (!updatedSectore) {
//       return res.status(404).json({ message: "Sectore not found" });
//     }
//     res.json({ updatedSectore, message: "Sectore Updated successfully" });
//   } catch (error) {
//     console.error(error);
//     res.status(500).json({ message: "Failed to update data." });
//   }
// };
const updateSector = async (req, res) => {
  const { id } = req.params;
  const {
    acftType,
    gcd,
    paxCapacity,
    CargoCapT,
    paxLF,
    cargoLF,
  } = req.body;

  try {

    const idArray = id.split(',').map((id) => id.trim());

    const updatedSectors = [];

    for (const sectorId of idArray) {
      console.log('Updating sector with ID:', sectorId);

      if (!isValidObjectId(sectorId)) {
        console.log(`Invalid ObjectId: ${sectorId}`);
        return res.status(400).json({ message: `Invalid ObjectId: ${sectorId}` });
      }

      const sectorObjectId = new Types.ObjectId(sectorId);

      const updatedSector = await Sector.findByIdAndUpdate(
        sectorObjectId,
        {
          acftType,
          gcd,
          paxCapacity,
          CargoCapT,
          paxLF,
          cargoLF,
        },
        { new: true }
      );

      console.log('Updated sector:', updatedSector);

      if (!updatedSector) {
        return res.status(404).json({ message: `Sector with ID ${sectorId} not found` });
      }

      updatedSectors.push(updatedSector);
    }

    //  Assuming you want to send a response after updating all sectors
    res.json({ updatedSectors, message: "Sectors updated successfully" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to update data." });
  }
};
const singleSector = async (req, res) => {
  try {
    const product = await Sector.findById(req.params.id);

    if (!product) {
      return res.status(404).json({ message: "Product not found" });
    }

    res.status(200).json(product);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};

module.exports = {
  getSecors,
  AddSectors,
  deleteSectors,
  updateSector,
  singleSector
};
