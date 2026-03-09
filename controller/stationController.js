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

const getStationsTableData = async (req, res) => {
  try {
    const userId = req.user.id;

    // Retrieve user information to get home timezone
    const user = await User.findById(userId);
    const hometimeZone = user ? user.hometimeZone : '';

    // Retrieve station data
    const data = await Stations.find({ userId });

    // Return response with station data and home timezone
    res.json({ data, hometimeZone });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};
const saveStation = async (req, res) => {
  try {
    const { stations, homeTimeZone } = req.body;
    const userId = req.user.id;

    // Update the home timezone of the user
    const user = await User.findById(userId);
    if (user) {
      user.hometimeZone = homeTimeZone;
      await user.save();
    }

    const updatedStations = [];

    // Iterate over stations array and update each station sequentially
    for (const stationData of stations) {
      const { _id, ...updateFields } = stationData;

      const existingStation = await Stations.findById(_id);

      if (!existingStation) {
        updatedStations.push(null);
        continue;
      }

      // 🔍 1. CHECK IF TAXI TIMES ACTUALLY CHANGED
      const taxiTimesChanged =
        (updateFields.avgTaxiOutTime !== undefined && updateFields.avgTaxiOutTime !== existingStation.avgTaxiOutTime) ||
        (updateFields.avgTaxiInTime !== undefined && updateFields.avgTaxiInTime !== existingStation.avgTaxiInTime);

      // Update the existing document
      await existingStation.updateOne(updateFields);

      const updatedStation = await Stations.findById(_id);
      updatedStations.push(updatedStation);

      // 🚀 2. CASCADE THE UPDATE IF TAXI TIMES CHANGED
      if (taxiTimesChanged) {
        console.log(`🚕 Taxi times changed for ${updatedStation.stationName}. Recalculating Flight Hours...`);

        // Find all sectors where this station is either the Departure or Arrival
        const affectedSectors = await Sector.find({
          $or: [{ sector1: updatedStation.stationName }, { sector2: updatedStation.stationName }],
          userId: userId
        });

        // Loop through and save them. 
        // This automatically triggers your `sectorSchema.post("save")` hook, 
        // which fetches the fresh taxi times, does the math, and updates the FLIGHTs!
        for (const sector of affectedSectors) {
          // We use .save() so your Mongoose middleware fires
          await sector.save();
        }

        console.log(`✅ Updated FH for ${affectedSectors.length} sectors and their associated flights.`);
      }
    }

    res.status(200).json(updatedStations);

  } catch (error) {
    console.error('Error updating stations:', error);
    res.status(500).send('Internal Server Error');
  }
};

module.exports = {
  getStationsTableData,
  saveStation
};
