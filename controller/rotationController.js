const User = require("../model/userSchema");
const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const DataHistory = require("../model/dataHistorySchema");
const SectorHistory = require("../model/sectorHistorySchema");
const Flights = require("../model/flight");
const FlightHistory = require("../model/flightHistory")
const RotationSummary = require("../model/rotationSummary");
const RotationDetails = require("../model/rotationDetails");
const RotationOccurrence = require("../model/rotationOccurence");
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

const { AddDataFromRotations } = require('./dataController');
const createNewFlights = async (userId, flightNumber, depStn, std, sta, arrStn, variant, dates, rotationNumber) => {

  const newFlights = dates.map(date => ({
    userId,
    flight: flightNumber,
    depStn,
    std,
    sta,
    arrStn,
    variant,
    date: date,
    day: new Date(date).toLocaleDateString('en-US', { weekday: 'short' }),
    rotationNumber,
  }));

  await Flights.insertMany(newFlights);
  // await populateNetworkTable(newFlights);
};
const eraseAndRepopulateMasterTable = async (req, res, userId, arrStn, bt, depNumber, depStn, datesInRange, flightNumber, std, sta, variant, rotationNumber, existingFlights, rotationDetailsId) => {
  try {


    // Step 1: Delete flights entries alongwith creating copies in FlightHistory
    const flightsToDelete = await Flights.find({
      userId,
      arrStn,
      bt,
      depStn,
      date: { $in: datesInRange },
      flight: flightNumber,
      std,
      sta,
      variant,
    });

    // Create FlightHistory documents for the flights being deleted
    const historyPromises = flightsToDelete.map(async (flight) => {
      // Create a copy of the flight for FlightsHistory
      const flightHistory = new FlightHistory({
        ...flight._doc, // Copy all properties from the original flight
        addedByRotation: `${rotationNumber}-${depNumber - 1}`,
      });

      // Save the flight history document
      await flightHistory.save();
    });

    // Wait for all history operations to complete
    await Promise.all(historyPromises);

    await Flights.deleteMany({
      userId,
      arrStn,
      bt,
      depStn,
      date: { $in: datesInRange },
      flight: flightNumber,
      std,
      sta,
      variant,
    });

    // Step 2: Delete dataSchema entries (assuming networkId is the field to match)
    const networkIdsToDelete = existingFlights.map((flight) => flight.networkId);

    const dataToDelete = await Data.find({
      _id: { $in: networkIdsToDelete },
    });

    // Create DataHistory documents for the entries being deleted
    const dataHistoryPromises = dataToDelete.map(async (dataEntry) => {
      // Create a copy of the data entry for DataHistory
      const dataHistory = new DataHistory({
        ...dataEntry._doc, // Copy all properties from the original entry
        addedByRotation: `${rotationNumber}-${depNumber - 1}`,
      });

      // Save the data history document
      await dataHistory.save();
    });

    // Wait for all data history operations to complete
    await Promise.all(dataHistoryPromises);

    // Delete entries from the Data collection
    await Data.deleteMany({
      _id: { $in: networkIdsToDelete },
    });

    // Step 3: Repopulate Master table
    await AddDataFromRotations(req, res, rotationDetailsId);

    return { success: true };
  } catch (error) {
    console.error('Error erasing and repopulating Master table:', error);
    return { success: false };
  }
};
const addRotationDetails = async (req, res) => {
  const userId = req.user.id;
  const {
    rotationNumber,
    depNumber,
    flightNumber,
    depStn,
    std,
    bt,
    sta,
    arrStn,
    domIntl,
    gt,
    variant,
  } = req.body;

  try {

    const newRotationDetails = new RotationDetails({
      rotationNumber,
      depNumber,
      flightNumber,
      depStn,
      std,
      bt,
      variant,
      sta,
      domIntl,
      gt,
      arrStn,
      userId
    });

    // Save the new entry to the database
    const savedRotationDetails = await newRotationDetails.save();

    console.log("Rotation Details added");
    return savedRotationDetails._id;
    // res.status(201).json({ message: `Rotation Details added` });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "An error occurred while creating data" });
  }
};
const deleteRotation = async (userId, rotationNumber, totalDepNumber) => {

  try {
    for (let depNumber = parseInt(totalDepNumber); depNumber >= 0; depNumber--) {
      const addedByRotationPrev = `${rotationNumber}-${depNumber - 1}`;
      const addedByRotationCurrent = `${rotationNumber}-${depNumber}`;

      const sectorHistoryEntries = await SectorHistory.find({
        addedByRotation: { $in: addedByRotationPrev },
        userId: userId
      });

      const flightHistoryEntries = await FlightHistory.find({
        addedByRotation: { $in: addedByRotationPrev },
        userId: userId
      });

      const dataHistoryEntries = await DataHistory.find({
        addedByRotation: { $in: addedByRotationPrev },
        userId: userId
      });

      const stationHistoryEntries = await StationsHistory.find({
        addedByRotation: { $in: addedByRotationPrev },
        userId: userId
      });

      for (const sectorHistoryEntry of sectorHistoryEntries) {
        let sectorEntryData = { ...sectorHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete sectorEntryData.addedByRotation;
        }

        await Sector.deleteOne({ _id: sectorHistoryEntry.sectorId });

        // If entry exists, add it to the sector schema
        await Sector.create(sectorEntryData);
        await SectorHistory.deleteOne({ _id: sectorHistoryEntry._id });
      }

      for (const flightHistoryEntry of flightHistoryEntries) {
        let flightEntryData = { ...flightHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete flightEntryData.addedByRotation;
        }

        await Flights.deleteOne({ _id: flightHistoryEntry.flightId });

        // If entry exists, add it to the flight schema
        await Flights.create(flightEntryData);
        await FlightHistory.deleteOne({ _id: flightHistoryEntry._id });
      }

      for (const dataHistoryEntry of dataHistoryEntries) {
        let dataEntryData = { ...dataHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete dataEntryData.addedByRotation;
        }

        await Data.deleteOne({ _id: dataHistoryEntry.dataId });

        // If entry exists, add it to the data schema
        await Data.create(dataEntryData);
        await DataHistory.deleteOne({ _id: dataHistoryEntry._id });
      }


      for (const stationHistoryEntry of stationHistoryEntries) {
        let stationEntryData = { ...stationHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete stationEntryData.addedByRotation;
        }

        await Stations.deleteOne({ _id: stationHistoryEntry.stationId });

        // If entry exists, add it to the data schema
        await Stations.create(stationEntryData);
        await StationsHistory.deleteOne({ _id: stationHistoryEntry._id });
      }

      // Always delete the entries with addedByRotation as addedByRotationCurrent from the sector schema
      // await Sector.deleteMany({ addedByRotation: { $in: addedByRotationCurrent } });
      // await Flights.deleteMany({ addedByRotation: { $in: addedByRotationCurrent } });
      // await Data.deleteMany({ addedByRotation: { $in: addedByRotationCurrent } });
      // await Stations.deleteMany({ addedByRotation: { $in: addedByRotationCurrent } });
    }

    // Delete entries from RotationDetails model
    await RotationDetails.deleteMany({ rotationNumber: rotationNumber, userId: userId });

    // Delete entries from RotationSummary model
    await RotationSummary.deleteMany({ rotationNumber: rotationNumber, userId: userId });

    // await createConnections(userId);

  } catch (error) {
    console.error('Error deleting entries:', error);
  }
};
const singleRotationDetail = async (req, res) => {
  try {
    // Fetch data from RotationDetails collection
    const rotationDetails = await RotationDetails.find({ rotationNumber: req.params.id });

    // Fetch data from RotationSummary collection based on rotationNumber
    const rotationSummary = await RotationSummary.findOne({ rotationNumber: req.params.id });

    // Combine rotationDetails and rotationSummary and send as response
    res.status(200).json({ rotationDetails, rotationSummary });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Server error" });
  }
};
const getRotations = async (req, res) => {
  try {
    const userId = req.user.id;

    // Fetch distinct values for the "rotationNumber" field from the RotationSummary model
    const distinctRotationNumbers = await RotationSummary.aggregate([
      { $match: { userId: userId } }, // Filter by user ID
      { $group: { _id: null, rotationNumbers: { $addToSet: '$rotationNumber' } } },
      { $project: { _id: 0, rotationNumbers: 1 } },
    ]);

    // Format the options
    const formatOptions = (values) =>
      values.map((value) => ({ value: value, label: value }));

    // Get the distinct rotationNumbers and format them
    const formattedRotationNumbers = formatOptions(distinctRotationNumbers[0]?.rotationNumbers || []);
    res.json(formattedRotationNumbers); // Send the formatted rotation numbers as response
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};
const getNextRotationNumber = async (req, res) => {
  try {
    const userId = req.user.id;
    // Fetch the latest rotationNumber and increment it for the new rotation
    const latestRotation = await RotationSummary.findOne({ userId: userId }, {}, { sort: { 'rotationNumber': -1 } });
    const nextRotationNumber = latestRotation ? parseInt(latestRotation.rotationNumber) + 1 : 1;

    // Send the nextRotationNumber as a response
    res.json({ nextRotationNumber });
  } catch (error) {
    console.error('Error fetching next rotation number:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
}
const updateRotationSummary = async (req, res) => {
  const userId = req.user.id;
  const rotationNumber = req.body.rotationNumber;
  const rotationTag = req.body.rotationTag;
  const effFromDate = req.body.effFromDate;
  const effToDate = req.body.effToDate;
  const dow = req.body.dow;
  const variant = req.body.selectedVariant;

  try {
    // Find the rotation entry based on rotationNumber
    let rotationEntry = await RotationSummary.findOne({ rotationNumber, userId });

    // If the entry doesn't exist, create a new one with userId
    if (!rotationEntry) {
      rotationEntry = new RotationSummary({ rotationNumber, userId });
    }

    // Update all fields with the new values
    rotationEntry.rotationTag = rotationTag;
    rotationEntry.effFromDt = effFromDate;
    rotationEntry.effToDt = effToDate;
    rotationEntry.dow = dow;
    rotationEntry.variant = variant;

    // Save the updated/created entry to the database
    await rotationEntry.save();

    res.status(201).json({ message: `RotationSummary updated` });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: "An error occurred while updating data" });
  }
};
const addRotationDetailsFlgtChange = async (req, res) => {
  try {

    const userId = req.user.id;
    const {
      arrStn,
      bt,
      depNumber,
      depStn,
      dow,
      effFromDate,
      effToDate,
      flightNumber,
      rotationNumber,
      sta,
      std,
      variant,
    } = req.body;

    console.log("effFromDate " + effFromDate)
    console.log("effToDate " + effToDate)
    // Find dates between effFromDate and effToDate with the given dow
    const startEffDate = new Date(effFromDate);
    const endEffDate = new Date(effToDate);
    // Create separate copies for adjusted date range queries (avoid mutating originals)
    const queryStartDate = new Date(effFromDate);
    const queryEndDate = new Date(effToDate);

    console.log("startEffDate " + startEffDate)
    console.log("endEffDate " + endEffDate)

    const daysOfWeek = dow.split('').map(Number); // Convert dow string to an array of numbers
    const daysOfWeekStrings = daysOfWeek.map(day => {
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
    });


    // timezone = timezone ? timezone : "Asia/Kolkata";

    const datesInRange = [];
    const currentDate = new Date(startEffDate);

    console.log("current Date" + currentDate)

    // Generate dates within the range and filter based on daysOfWeek
    while (currentDate <= endEffDate) {
      if (daysOfWeek.includes(currentDate.getDay() + 1)) {
        datesInRange.push(new Date(currentDate));
      }
      currentDate.setDate(currentDate.getDate() + 1);
    }

    //correction for end Dates - use query copies to avoid mutating originals
    queryEndDate.setDate(queryEndDate.getDate() + 1);
    queryEndDate.setHours(0, 0, 0, 0);

    console.log("After correct startEffDate " + startEffDate)
    console.log("After correct endEffDate " + endEffDate)

    // Step 2: Exclude these networkIds from your query
    const existingFlights = await Flights.find({
      userId,
      arrStn,
      bt,
      depStn,
      day: { $in: daysOfWeekStrings },
      date: { $gte: queryStartDate, $lte: queryEndDate },
      flight: flightNumber,
      std,
      sta,
      variant,
    });

    const hasValidRotationNumber = existingFlights.some(flight => flight.rotationNumber && !isNaN(Number(flight.rotationNumber)));

    if (hasValidRotationNumber) {
      // If any flight already has a valid rotationNumber, no updates should occur
      return res.status(400).json({ message: 'Some flights already have a valid rotationNumber, no updates will be made.' });
    }

    const networkIdToCheck = existingFlights.length > 0 ? existingFlights[0].networkId : "";
    const allFlightsWithSameNetworkId = await Flights.find({
      networkId: networkIdToCheck
    });

    // Use query copies for range comparisons (don't mutate originals)
    queryStartDate.setHours(0, 0, 0, 0);

    const existingFlightsDates = existingFlights.map(flight => flight.date);
    const allFlightsWithSameNetworkIdDates = allFlightsWithSameNetworkId.map(flight => flight.date);

    // Check if existingFlightDates is a subset of allFlightDates
    const isSubset = existingFlightsDates.every(date => allFlightsWithSameNetworkIdDates.some(d => d.getTime() === date.getTime()));

    // check for date ranges
    const minDate = Math.min(...allFlightsWithSameNetworkIdDates.map(d => d.getTime()));
    const maxDate = Math.max(...allFlightsWithSameNetworkIdDates.map(d => d.getTime()));

    // Convert dates to milliseconds for comparison
    const startEffDateInMillis = queryStartDate.getTime();
    const endEffDateInMillis = queryEndDate.getTime();

    // Check if rotation date range fits within the existing flight date range
    const isDateRangeValid = startEffDateInMillis >= minDate && endEffDateInMillis <= maxDate;

    // Check if ALL flights with same networkId fall within the rotation's effective date range
    const allDatesInRange = allFlightsWithSameNetworkId.length > 0 && allFlightsWithSameNetworkId.every((flight) => {
      const flightDate = new Date(flight.date);
      return flightDate >= queryStartDate && flightDate <= queryEndDate;
    });

    console.log("isSubset : " + isSubset)
    console.log("startEffDateInMillis : " + startEffDateInMillis)
    console.log("minDate : " + minDate)
    console.log("endEffDateInMillis : " + endEffDateInMillis)
    console.log("maxDate : " + maxDate)
    console.log("existingFlights.length : " + existingFlights.length)
    console.log("isDateRangeValid : " + isDateRangeValid)
    console.log("allFlightsWithSameNetworkId.length : " + allFlightsWithSameNetworkId.length)
    console.log("allFlightsWithSameNetworkId : " + allFlightsWithSameNetworkIdDates)
    console.log("allDatesInRange : " + allDatesInRange)

    const existingFlightsIds = existingFlights.map(flight => flight._id.toString());
    const allFlightsIds = allFlightsWithSameNetworkId.map(flight => flight._id.toString());

    if (existingFlights.length === 0) {
      // Case A: No row is found — populate new flights in Master table
      const rotationDetailsId = await addRotationDetails(req, res);
      await AddDataFromRotations(req, res, rotationDetailsId);

      // Update RotationSummary totals
      await updateRotationSummaryTotals(userId, rotationNumber);

      return res.status(200).json({ message: 'RotationNumber updated successfully for existing flights.' });

    } else if ((networkIdToCheck && isSubset && isDateRangeValid) || (existingFlightsIds.every(id => allFlightsIds.includes(id)) && allDatesInRange)) {
      // Case B: Rows found for all dates — update rotationNumber in existing flights
      const rotationDetailsId = await addRotationDetails(req, res);

      const historyPromises = existingFlights.map(async (flight) => {
        const flightHistory = new FlightHistory({
          ...flight._doc,
          addedByRotation: `${rotationNumber}-${depNumber - 1}`,
          flightId: flight._id
        });
        await flightHistory.save();

        await Flights.findByIdAndUpdate(flight._id, {
          rotationNumber: rotationNumber,
          addedByRotation: `${rotationNumber}-${depNumber}`
        });
      });

      await Promise.all(historyPromises);

      // Update RotationSummary totals
      await updateRotationSummaryTotals(userId, rotationNumber);

      return res.status(200).json({ message: 'RotationNumber updated successfully for existing flights.' });
    } else {
      // Case C: Rows found on at least one date — erase and repopulate
      const rotationDetailsId = await addRotationDetails(req, res);

      const result = await eraseAndRepopulateMasterTable(
        req, res, userId, arrStn, bt, depNumber, depStn,
        datesInRange, flightNumber, std, sta, variant,
        rotationNumber, existingFlights, rotationDetailsId
      );

      if (result.success) {
        // Update RotationSummary totals
        await updateRotationSummaryTotals(userId, rotationNumber);
        return res.status(200).json({ message: 'Master table erased and repopulated successfully.' });
      } else {
        return res.status(500).json({ message: 'Error erasing and repopulating master table', flightNumber });
      }
    }

  } catch (error) {
    console.error('Error modifying flights:', error);
    return res.status(500).json({ success: false, message: 'An error occurred while modifying flights.' });
  }
};
// Helper: update RotationSummary computed totals after adding a rotation detail
const updateRotationSummaryTotals = async (userId, rotationNumber) => {
  try {
    const details = await RotationDetails.find({ rotationNumber, userId });
    let totalBtMinutes = 0;
    let totalGtMinutes = 0;

    details.forEach(d => {
      if (d.bt && /^\d{2}:\d{2}$/.test(d.bt)) {
        const [h, m] = d.bt.split(':').map(Number);
        totalBtMinutes += h * 60 + m;
      }
      if (d.gt && /^\d{2}:\d{2}$/.test(d.gt)) {
        const [h, m] = d.gt.split(':').map(Number);
        totalGtMinutes += h * 60 + m;
      }
    });

    const bhTotal = `${Math.floor(totalBtMinutes / 60).toString().padStart(2, '0')}:${(totalBtMinutes % 60).toString().padStart(2, '0')}`;
    const gtTotal = `${Math.floor(totalGtMinutes / 60).toString().padStart(2, '0')}:${(totalGtMinutes % 60).toString().padStart(2, '0')}`;
    const totalMinutes = totalBtMinutes + totalGtMinutes;
    const rotationTotalTime = `${Math.floor(totalMinutes / 60).toString().padStart(2, '0')}:${(totalMinutes % 60).toString().padStart(2, '0')}`;

    // Compute firstDepLastArr
    const firstDep = details.length > 0 ? details[0].depStn : '';
    const lastArr = details.length > 0 ? details[details.length - 1].arrStn : '';
    const firstDepLastArr = firstDep && lastArr ? `${firstDep}-${lastArr}` : '';

    await RotationSummary.findOneAndUpdate(
      { rotationNumber, userId },
      { bhTotal, gtTotal, rotationTotalTime, firstDepLastArr },
      { upsert: false }
    );
  } catch (error) {
    console.error('Error updating RotationSummary totals:', error);
  }
};

const deleteCompleteRotation = async (req, res) => {
  const userId = req.user.id;
  const rotationNumber = req.body.rotationNumber;
  const selectedVariant = req.body.selectedVariant;
  const totalDepNumber = req.body.totalDepNumber;

  try {
    for (let depNumber = totalDepNumber; depNumber > 0; depNumber--) {
      const addedByRotationPrev = `${rotationNumber}-${depNumber - 1}`;
      const addedByRotationCurrent = `${rotationNumber}-${depNumber}`;

      const sectorHistoryEntries = await SectorHistory.find({
        addedByRotation: addedByRotationPrev,
        userId: userId
      });

      const flightHistoryEntries = await FlightHistory.find({
        addedByRotation: addedByRotationPrev,
        userId: userId
      });

      const dataHistoryEntries = await DataHistory.find({
        addedByRotation: addedByRotationPrev,
        userId: userId
      });

      const stationHistoryEntries = await StationsHistory.find({
        addedByRotation: addedByRotationPrev,
        userId: userId
      });

      for (const sectorHistoryEntry of sectorHistoryEntries) {
        let sectorEntryData = { ...sectorHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete sectorEntryData.addedByRotation;
        }

        await Sector.deleteOne({ _id: sectorEntryData.sectorId });

        // If entry exists, add it to the sector schema
        await Sector.create(sectorEntryData);
        await SectorHistory.deleteOne({ _id: sectorHistoryEntry._id });
      }

      for (const flightHistoryEntry of flightHistoryEntries) {
        let flightEntryData = { ...flightHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete flightEntryData.addedByRotation;
          flightEntryData.rotationNumber = null;
        }

        await Flights.deleteOne({ _id: flightHistoryEntry.flightId });

        // If entry exists, add it to the flight schema
        await Flights.create(flightEntryData);
        await FlightHistory.deleteOne({ _id: flightHistoryEntry._id });
      }

      for (const dataHistoryEntry of dataHistoryEntries) {
        let dataEntryData = { ...dataHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete dataEntryData.addedByRotation;
        }

        await Data.deleteOne({ _id: dataHistoryEntry.dataId });

        // If entry exists, add it to the data schema
        await Data.create(dataEntryData);
        await DataHistory.deleteOne({ _id: dataHistoryEntry._id });
      }


      for (const stationHistoryEntry of stationHistoryEntries) {
        let stationEntryData = { ...stationHistoryEntry._doc };

        // If depNumber is 1, exclude the addedByRotation field
        if (parseInt(depNumber) === 1) {
          delete stationEntryData.addedByRotation;
        }


        await Stations.deleteOne({ _id: stationHistoryEntry.stationId });

        // If entry exists, add it to the data schema
        await Stations.create(stationEntryData);
        await StationsHistory.deleteOne({ _id: stationHistoryEntry._id });
      }

      // Always delete the entries with addedByRotation as addedByRotationCurrent from the sector schema
      // await Sector.deleteMany({ addedByRotation: addedByRotationCurrent });
      // await Flights.deleteMany({ addedByRotation: addedByRotationCurrent });
      // await Data.deleteMany({ addedByRotation: addedByRotationCurrent });
      // await Stations.deleteMany({ addedByRotation: addedByRotationCurrent });
    }

    // Delete entries from RotationDetails model
    await RotationDetails.deleteMany({ rotationNumber: rotationNumber, userId: userId });

    // Delete entries from RotationSummary model
    await RotationSummary.deleteMany({ rotationNumber: rotationNumber, userId: userId });

    // Delete 'Y' for this rotation in RotationOccurrence table
    const rotNumInt = parseInt(rotationNumber);
    if (rotNumInt >= 1 && rotNumInt <= 7) {
      const rotationField = `rotation_${rotNumInt}`;
      await RotationOccurrence.updateMany(
        { [rotationField]: 'Y', userId: userId }, // <-- Added userId constraint
        { $set: { [rotationField]: 'N' } }
      );
    }

    // await createConnections(userId);

    res.status(200).json({ message: `Entries with rotationNumber ${rotationNumber} and userId ${userId} deleted successfully` });
  } catch (error) {
    console.error('Error deleting entries:', error);
    res.status(500).json({ error: 'An error occurred while deleting entries' });
  }
};
const deletePrevInRotation = async (req, res) => {
  const userId = req.user.id;
  const { rotationNumber, selectedVariant, _id, depNumber } = req.body;
  const addedByRotationPrev = `${rotationNumber}-${depNumber - 1}`;
  const addedByRotationCurrent = `${rotationNumber}-${depNumber}`;

  try {

    const sectorHistoryEntries = await SectorHistory.find({ addedByRotation: addedByRotationPrev, userId: userId });
    const flightHistoryEntries = await FlightHistory.find({ addedByRotation: addedByRotationPrev, userId: userId });
    const dataHistoryEntries = await DataHistory.find({ addedByRotation: addedByRotationPrev, userId: userId });
    const stationHistoryEntries = await StationsHistory.find({ addedByRotation: addedByRotationPrev, userId: userId });

    for (const sectorHistoryEntry of sectorHistoryEntries) {
      let sectorEntryData = { ...sectorHistoryEntry._doc };

      // If depNumber is 1, exclude the addedByRotation field
      if (parseInt(depNumber) === 1) {
        delete sectorEntryData.addedByRotation;
      }

      await Sector.deleteOne({ _id: sectorEntryData.sectorId });

      // If entry exists, add it to the sector schema
      await Sector.create(sectorEntryData);
      await SectorHistory.deleteOne({ _id: sectorHistoryEntry._id });

    }

    for (const flightHistoryEntry of flightHistoryEntries) {
      let flightEntryData = { ...flightHistoryEntry._doc };

      // If depNumber is 1, exclude the addedByRotation field
      if (parseInt(depNumber) === 1) {
        // delete Rotation Number also from flight data


        delete flightEntryData.addedByRotation;
        flightEntryData.rotationNumber = null;
      }

      await Flights.deleteOne({ _id: flightHistoryEntry.flightId });
      // If entry exists, add it to the flight schema
      await Flights.create(flightEntryData);
      await FlightHistory.deleteOne({ _id: flightHistoryEntry._id });
    }


    for (const dataHistoryEntry of dataHistoryEntries) {
      let dataEntryData = { ...dataHistoryEntry._doc };

      // If depNumber is 1, exclude the addedByRotation field
      if (parseInt(depNumber) === 1) {
        delete dataEntryData.addedByRotation;
      }

      await Data.deleteOne({ _id: dataHistoryEntry.dataId });

      // If entry exists, add it to the data schema
      await Data.create(dataEntryData);
      await DataHistory.deleteOne({ _id: dataHistoryEntry._id });
    }

    for (const stationHistoryEntry of stationHistoryEntries) {
      let stationEntryData = { ...stationHistoryEntry._doc };

      // If depNumber is 1, exclude the addedByRotation field
      if (parseInt(depNumber) === 1) {
        delete stationEntryData.addedByRotation;
      }

      await Stations.deleteOne({ _id: stationHistoryEntry.stationId });

      // If entry exists, add it to the data schema
      await Stations.create(stationEntryData);
      await StationsHistory.deleteOne({ _id: stationHistoryEntry._id });
    }

    // Delete the document using its _id and userId
    await RotationDetails.deleteOne({ rotationNumber: rotationNumber, depNumber: depNumber, userId: userId });

    if (parseInt(depNumber) === 1) {
      await RotationSummary.deleteMany({ rotationNumber: rotationNumber, userId: userId });
    }

    // await createConnections(userId);

    res.status(200).json({ message: `Entries with rotationNumber ${rotationNumber} and userId ${userId} deleted successfully` });
  } catch (error) {
    console.error('Error deleting entries:', error);
    res.status(500).json({ error: 'An error occurred while deleting entries' });
  }
};

module.exports = {
  deleteRotation,
  AddDataFromRotations,
  singleRotationDetail,
  getRotations,
  getNextRotationNumber,
  updateRotationSummary,
  addRotationDetailsFlgtChange,
  deleteCompleteRotation,
  deletePrevInRotation
};
