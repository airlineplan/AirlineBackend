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
const moment = require("moment-timezone");
require("dotenv").config();
const { DateTime } = require('luxon');
const { isValidObjectId, Types } = require("mongoose");

// const createConnections = require('../helper/createConnections');


moment.tz.setDefault("America/New_York");

const timeZoneCorrectedDates = (date, tzString) => {
  if (date) {
    return new Date((typeof date === "string" ? new Date(date) : date).toLocaleString("en-US", { timeZone: tzString }));
  } else {
    // Handle the case where date is undefined
    return null; // or whatever you want to return in this case
  }
}

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


const importUser = async (req, res) => {
  const filePath = req.file.path;
  try {
    const SCHEDULE_UPLOAD_LIMIT = parseInt(process.env.SCHEDULE_UPLOAD_LIMIT, 10) || 500; // Default to 500 if not set
    let userData = [];

    if (req.file.originalname.endsWith(".csv")) {
      let rowCount = 0;
      await new Promise((resolve, reject) => {
        const stream = fs
          .createReadStream(req.file.path)
          .pipe(CSV())
          .on("data", (row) => {
            if (rowCount < SCHEDULE_UPLOAD_LIMIT) {
              const formattedData = processCSVRow(row);
              userData.push(formattedData);
              rowCount++;
            } else {
              stream.destroy(); // Stop the stream once we hit the limit
            }
          })
          .on("end", resolve)
          .on("error", reject);
      });
    } else if (req.file.originalname.endsWith(".xlsx")) {
      const workbook = xlsx.readFile(req.file.path);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const jsonData = xlsx.utils.sheet_to_json(worksheet);

      // Take only the first SCHEDULE_UPLOAD_LIMIT rows
      userData = jsonData.slice(0, SCHEDULE_UPLOAD_LIMIT).map(processExcelRow);
    } else {
      throw new Error("Invalid file format");
    }

    userData = userData.map((user) => {
      return { ...user, userId: req.user.id };
    });

    const skippedRows = [];
    let addedRowsCount = 0;
    const userId = req.user.id;

    for (const [index, user] of userData.entries()) {
      const { flight, std, sta, variant, dow, arrStn, depStn, gcd, paxCapacity, cargoCapT, paxSF, cargoLF } = user;

      const flightValidation = isValidFlightNumber(flight);
      const depStnValidation = isValidDepStn(depStn);
      const arrStnValidation = isValidArrStn(arrStn);
      const VariantValidation = isValidVariant(variant);
      const dowValidation = isValidDow(dow);

      if (!flightValidation) {
        skippedRows.push({ ...user, error: "Invalid flight number" });
        continue;
      }
      if (!depStnValidation) {
        skippedRows.push({ ...user, error: "Invalid departure station" });
        continue;
      }
      if (!arrStnValidation) {
        skippedRows.push({ ...user, error: "Invalid arrival station" });
        continue;
      }
      if (!VariantValidation) {
        skippedRows.push({ ...user, error: "Invalid variant" });
        continue;
      }
      if (!dowValidation) {
        skippedRows.push({ ...user, error: "Invalid day of week" });
        continue;
      }

      const isLast = index === userData.length - 1;
      const data = await new Data({ ...user, userId: userId, isScheduled: true, isLast }).save();

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
        fromDt: data.effFromDt,
        toDt: data.effToDt,
        userTag2: data.userTag2,
        remarks1: data.remarks1,
        remarks2: data.remarks2,
        gcd: gcd,
        paxCapacity: paxCapacity,
        CargoCapT: cargoCapT,
        paxLF: paxSF,
        cargoLF: cargoLF,
        networkId: data._id,
        userId: userId,
        isScheduled: true,
      });

      await newSector.save();
      addedRowsCount++;
    }

    // const discardedRowsCount = Math.max(userData.length - SCHEDULE_UPLOAD_LIMIT, 0);
    const skippedMessage = skippedRows.length
      ? `, Skipped ${skippedRows.length} rows due to validation errors`
      : "";

    res.send({
      status: 200,
      success: true,
      msg: `Processed ${addedRowsCount} rows.`,
      skippedRows,
    });
  } catch (error) {
    console.error(error);
    res.send({ status: 400, success: false, msg: error.message });
  } finally {
    // Delete the uploaded file
    fs.unlink(filePath, (err) => {
      if (err) {
        console.error(`Failed to delete file at ${filePath}:`, err.message);
      } else {
        console.log(`File deleted: ${filePath}`);
      }
    });
  }
};


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

//**************************************************************************************** */

const getSecors = async (req, res) => {
  try {
    const id = req.user.id;
    const data = await Sector.find({ userId: id });
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
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

const deleteFlightsAndUpdateSectors = async (req, res) => {
  try {
    const ids = req.params.ids.split(","); // Split the comma-separated IDs

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

    if (result.n === 0) {
      return res.status(404).json({ error: "Data not found" });
    }

    await Sector.updateMany(
      { networkId: { $in: ids } },
      { $set: { toDt: null, fromDt: null } }
    );


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
      { header: "STA(LT)", key: "sta" },
      { header: "Arr Stn.", key: "arrStn" },
      { header: "Sector.", key: "sector" },
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
        seats: parseFloat(product.seats),
        CargoCapT: parseFloat(product.CargoCapT),
        dist: parseFloat(product.dist),
        pax: parseInt(product.pax, 10),
        CargoT: parseFloat(product.CargoT),
        ask: parseInt(product.ask, 10),
        rsk: parseInt(product.rsk, 10),
        cargoAtk: parseInt(product.cargoAtk, 10),
        cargoRtk: parseInt(product.cargoRtk, 10),
      };

      worksheet.addRow(excelProduct).commit();
      count++;
    }

    worksheet.commit(); // Finalize the worksheet
    await workbook.commit(); // Finalize the workbook
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
  const { id } = req.params;
  let {
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
    domINTL,
    timezone
  } = req.body;

  // timezone = timezone ? timezone : "Asia/Kolkata";
  // effFromDt = timeZoneCorrectedDates(effFromDt, timezone);
  // effToDt = timeZoneCorrectedDates(effToDt, timezone);
  domINTL = domINTL?.toLowerCase();

  try {
    const userId = req.user.id;

    const idArray = id.split(',').map((id) => id.trim());

    const updatedFlights = [];

    for (const dataId of idArray) {

      const flightsWithRotation = await Flights.find({
        networkId: dataId,
        rotationNumber: { $exists: true, $ne: null }
      });

      if (flightsWithRotation.length > 0) {

        RotationDetails.aggregate([
          { $match: { rotationNumber: flightsWithRotation[0].rotationNumber } },
          { $sort: { depNumber: -1 } },
          { $limit: 1 },
          { $project: { depNumber: 1 } }
        ])
          .then(result => {
            if (result.length > 0) {
              const depNumber = result[0].depNumber;

              deleteRotation(userId, flightsWithRotation[0].rotationNumber, depNumber)
            } else {
              console.log("No entries found for the given rotation number");
            }
          })

      }

      const updatedData = await Data.findByIdAndUpdate(
        dataId,
        {
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
          domINTL,
          userTag1,
          userTag2,
          remarks1,
          remarks2
        },
        { new: true }
      );

      if (!updatedData) {
        return res.status(404).json({ message: "customer not found" });
      }

      updatedFlights.push(updatedData);

    }

    // await createConnections(req.user.id);

    res.json({ updatedFlights, message: "Data Updated successfully" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to update data." });
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

//**************************Sector*******************************************************/

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

//***************Update Sector***************************/
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

const AdminLogin = (req, res) => {
  const { email, password } = req.body;

  if (email === "admin@airline.com" && password === "12345") {
    const token = jwt.sign({ email }, secretKey, { expiresIn: "1h" });
    return res.status(200).json({ token, message: "Login Successful" });
  } else {
    console.error("Invalid credentials:", email, password);
    res.status(401).json({ error: "Invalid credentials" });
  }
};

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

    console.log("Query finished, page : "+ page + " data length : "+ data.length);

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


function regexForFindingSuperset(inputString) {
  // Create an array of positive lookahead assertions for each letter in the input string
  const lookaheads = inputString.split('').map(letter => `(?=.*${letter})`).join('');

  // Combine the lookaheads with the start and end of string anchors
  const regexPattern = `^${lookaheads}.*$`;

  // Return the regex pattern as a string
  return regexPattern;
}


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
  result.setDate(result.getDate() + days); // Set the date to be days days ahead
  return result; // Return the new date object
}

// const createConnections = async (req, res) => {
//   try {
//     console.log("Create Connection called");
//     const userId = req.user.id;

//     // Fetch user's home timezone
//     const user = await User.findById(userId);
//     // const hometimeZone = user.hometimeZone;

//     // Pre-fetch stations data
//     const stationsMap = {};
//     const stations = await Stations.find({ userId: userId });
//     for (const station of stations) {
//       stationsMap[station.stationName] = station;
//     }

//     // Use cursor for flights to prevent loading all data into memory
//     const flightCursor = Flights.find({ userId: userId }).cursor();
//     const bulkUpdateOperations = [];
//     const behindODsOperations = [];

//     for await (const flight of flightCursor) {
//       if (flight.userId !== userId) continue;

//       // Reset beyondODs and behindODs for the current flight
//       flight.beyondODs = [];
//       flight.behindODs = [];
//       await flight.save();

//       const stationArr = stationsMap[flight.arrStn];
//       const stationDep = stationsMap[flight.depStn];

//       if (!stationArr) {
//         console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
//         continue; // Skip to the next flight
//       }

//       const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);

//       const domQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('dom', 'i') }
//       };

//       const intlQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('intl', 'i') }
//       };

//       if (flight.domIntl.toLowerCase() === 'dom') {
//         const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
//         const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
//         const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
//         const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(flight.date);
//         } else if (nextDayDom) {
//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayDom) {
//           const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//           const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);
//           domQuery.$or = [
//             { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//         if (sameDayInt) {
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {
//           const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//           const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);
//           intlQuery.$or = [
//             { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }
//       } else if (flight.domIntl.toLowerCase() === 'intl') {
//         const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
//         const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
//         const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
//         const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(flight.date);
//         } else if (nextDayDom) {
//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayDom) {
//           const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//           const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);
//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);
//           domQuery.$or = [
//             { std: { $gte: inDMinStdLT, $lte: "23:59" }, date: { $gte: flightDateUTC, $lt: nextDayDateUTC } },
//             { std: { $gte: "00:00", $lte: indmaxMinusB }, date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) } }
//           ];
//         }

//         if (sameDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {
//           const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//           const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);
//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);
//           intlQuery.$or = [
//             { std: { $gte: inInMinStdLT, $lte: "23:59" }, date: { $gte: flightDateUTC, $lt: nextDayDateUTC } },
//             { std: { $gte: "00:00", $lte: ininmaxMinusB }, date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) } }
//           ];
//         }
//       }

//       console.log("domQuery is : " + JSON.stringify(domQuery));
//       console.log("intlQuery is : " + JSON.stringify(intlQuery));

//       const domFlightsCursor = Flights.find(domQuery).cursor();
//       const intlFlightsCursor = Flights.find(intlQuery).cursor();

//       const beyondODs = [];
//       for await (const domFlight of domFlightsCursor) {
//         beyondODs.push(domFlight._id);
//         behindODsOperations.push({
//           updateOne: {
//             filter: { _id: domFlight._id },
//             update: { $addToSet: { behindODs: flight._id } }
//           }
//         });
//       }

//       for await (const intlFlight of intlFlightsCursor) {
//         beyondODs.push(intlFlight._id);
//         behindODsOperations.push({
//           updateOne: {
//             filter: { _id: intlFlight._id },
//             update: { $addToSet: { behindODs: flight._id } }
//           }
//         });
//       }

//       bulkUpdateOperations.push({
//         updateOne: {
//           filter: { _id: flight._id },
//           update: { $set: { beyondODs: beyondODs } }
//         }
//       });

//       if (bulkUpdateOperations.length >= 1000) {
//         await Flights.bulkWrite(bulkUpdateOperations);
//         bulkUpdateOperations.length = 0;
//       }

//       if (behindODsOperations.length >= 1000) {
//         await Flights.bulkWrite(behindODsOperations);
//         behindODsOperations.length = 0;
//       }
//     }

//     if (bulkUpdateOperations.length > 0) {
//       await Flights.bulkWrite(bulkUpdateOperations);
//     }

//     if (behindODsOperations.length > 0) {
//       await Flights.bulkWrite(behindODsOperations);
//     }

//     console.log("Connections Completed");
//     res.status(200).json({ message: "Connections Completed" });
//   } catch (error) {
//     console.error('Error processing flight connections:', error);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// };

const createConnections = async (req, res) => {
  try {
    console.log("Create Connection called");

    const userId = req.user.id;

    // Fetch user's hometimeZone if needed
    const user = await User.findById(userId).lean();
    // const hometimeZone = user.hometimeZone;

    // Pre-fetch stations data
    const stations = await Stations.find({ userId: userId }).lean();
    const stationsMap = {};
    stations.forEach(station => {
      stationsMap[station.stationName] = station;
    });

    // Initialize bulk operations arrays
    const flightBulkOps = [];
    const behindODsBulkOps = [];

    // Create a cursor to iterate over flights without loading all into memory
    const flightCursor = Flights.find({ userId: userId }).cursor();

    let processedCount = 0;
    const BATCH_SIZE = 1000; // Adjust based on your server's capacity

    for await (const flight of flightCursor) {
      processedCount++;
      if (processedCount % 10000 === 0) {
        console.log(`Processed ${processedCount} flights`);
      }

      // Initialize beyondODs and behindODs
      flight.beyondODs = [];
      flight.behindODs = [];

      // Prepare to update the flight's beyondODs
      // (We will set this after finding connecting flights)

      const stationArr = stationsMap[flight.arrStn];
      const stationDep = stationsMap[flight.depStn];

      if (!stationArr) {
        console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
        continue; // Skip to the next flight
      }

      const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
      // const staHTZ = convertTimeToTZ(flight.sta, stationArr.stdtz, hometimeZone);

      // Build queries based on domIntl
      let domQuery = {
        depStn: flight.arrStn,
        arrStn: { $ne: flight.depStn },
        domIntl: { $regex: new RegExp('dom', 'i') }
      };

      let intlQuery = {
        depStn: flight.arrStn,
        arrStn: { $ne: flight.depStn },
        domIntl: { $regex: new RegExp('intl', 'i') }
      };

      if (flight.domIntl.toLowerCase() === 'dom') {
        const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
        const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
        const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
        const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

        const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
        const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
        const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
        const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

        const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
        const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
        const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;


        const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
        const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
        const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

        // B = 23:59 - domConnectingTimeMin
        const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

        if (sameDayDom) {

          domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
          domQuery.date = new Date(flight.date)

        } else if (nextDayDom) {
          // min to max on the next day
          domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
          domQuery.date = new Date(addDays(flight.date, 1))
        } else if (partialDayDom) {
          // minstd to max - B on the same date
          // min + B to max on the next date
          const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
          const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

          domQuery.$or = [
            { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
            { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
          ];
        }

        if (sameDayInt) {
          // min to max on the same day
          intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
          intlQuery.date = new Date(flight.date);
        } else if (nextDayInt) {
          // min to max on the next day
          intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
          intlQuery.date = new Date(addDays(flight.date, 1));
        } else if (partialDayInt) {
          // minstd to max - B on the same date
          // min + B to max on the next date
          const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
          const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

          intlQuery.$or = [
            { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
            { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
          ];
        }

      } else if (flight.domIntl.toLowerCase() === 'intl') {
        const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
        const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
        const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
        const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

        const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
        const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
        const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
        const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

        const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
        const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
        const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

        const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
        const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
        const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

        const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

        if (sameDayDom) {
          domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
          domQuery.date = new Date(flight.date);

        } else if (nextDayDom) {

          domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
          domQuery.date = new Date(addDays(flight.date, 1))
        } else if (partialDayDom) {

          const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
          const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

          const flightDateUTC = new Date(flight.date);
          flightDateUTC.setUTCHours(0, 0, 0, 0);

          // Calculate the next day in UTC for comparison
          const nextDayDateUTC = new Date(flightDateUTC);
          nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

          domQuery.$or = [
            {
              std: { $gte: inDMinStdLT, $lte: "23:59" },
              date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
            },
            {
              std: { $gte: "00:00", $lte: indmaxMinusB },
              date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
            }
          ];
        }

        if (sameDayInt) {
          intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
          intlQuery.date = new Date(flight.date);
        } else if (nextDayInt) {
          intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
          intlQuery.date = new Date(addDays(flight.date, 1));
        } else if (partialDayInt) {

          const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
          const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

          const flightDateUTC = new Date(flight.date);
          flightDateUTC.setUTCHours(0, 0, 0, 0);

          // Calculate the next day in UTC for comparison
          const nextDayDateUTC = new Date(flightDateUTC);
          nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

          intlQuery.$or = [
            {
              std: { $gte: inInMinStdLT, $lte: "23:59" },
              date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
            },
            {
              std: { $gte: "00:00", $lte: ininmaxMinusB },
              date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
            }
          ];
        }
      }

      // Log the queries for debugging
      console.log("domQuery is : " + JSON.stringify(domQuery));
      console.log("intlQuery is : " + JSON.stringify(intlQuery));

      // Find connecting flights using lean queries to improve performance
      const [domFlights, intlFlights] = await Promise.all([
        Flights.find(domQuery).select('_id').lean(),
        Flights.find(intlQuery).select('_id').lean()
      ]);

      const beyondODs = [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)];
      
      // Prepare bulk update for beyondODs
      flightBulkOps.push({
        updateOne: {
          filter: { _id: flight._id },
          update: { $set: { beyondODs: beyondODs } }
        }
      });

      // Prepare bulk updates for behindODs
      domFlights.forEach(f => {
        behindODsBulkOps.push({
          updateOne: {
            filter: { _id: f._id },
            update: { $addToSet: { behindODs: flight._id } }
          }
        });
      });

      intlFlights.forEach(f => {
        behindODsBulkOps.push({
          updateOne: {
            filter: { _id: f._id },
            update: { $addToSet: { behindODs: flight._id } }
          }
        });
      });

      // Execute bulk operations in batches
      if (flightBulkOps.length >= BATCH_SIZE) {
        await Flights.bulkWrite(flightBulkOps);
        flightBulkOps.length = 0; // Clear the array
      }

      if (behindODsBulkOps.length >= BATCH_SIZE) {
        await Flights.bulkWrite(behindODsBulkOps);
        behindODsBulkOps.length = 0; // Clear the array
      }
    }

    // Execute any remaining bulk operations
    if (flightBulkOps.length > 0) {
      await Flights.bulkWrite(flightBulkOps);
    }

    if (behindODsBulkOps.length > 0) {
      await Flights.bulkWrite(behindODsBulkOps);
    }

    console.log("Connections Completed");
    res.status(200).json({ message: "Connections Completed" });
  } catch (error) {
    console.error('Error processing flight connections:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};


// const createConnections = async (req, res) => {
//   try {

//     console.log("Create Connection called")

//     const userId = req.user.id;
//     // Fetch user's hometimeZone
//     const user = await User.findById(userId);
//     // const hometimeZone = user.hometimeZone;

//     // Pre-fetch stations data
//     const stationsMap = {};
//     const stations = await Stations.find({ userId: userId });
//     for (const station of stations) {
//       stationsMap[station.stationName] = station;
//     }

//     // Fetch all flight entries in a single query
//     const allFlights = await Flights.find({ userId: userId });

//     console.log("Flights loaded  : ", allFlights.length);

//     for (const flight of allFlights) {
//       if (flight.userId === userId) {
//         flight.beyondODs = [];
//         flight.behindODs = [];
//         await flight.save();
//       }
//     }

//     console.log("beyondODs , behindODs added  : ");

//     let count  = 0;
//     // Iterate over each flight
//     for (const flight of allFlights) {

//       console.log("Iteration : ", count);
//       count++;

//       const stationArr = stationsMap[flight.arrStn];
//       const stationDep = stationsMap[flight.depStn];

//       if (!stationArr) {
//         console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
//         continue; // Skip to the next flight
//       }

//       const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
//       // const staHTZ = convertTimeToTZ(flight.sta, stationArr.stdtz, hometimeZone);



//       const domQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('dom', 'i') }
//       };

//       const intlQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('intl', 'i') }
//       };

//       if (flight.domIntl.toLowerCase() === 'dom') {
//         const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
//         const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
//         const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
//         const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;


//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         // B = 23:59 - domConnectingTimeMin
//         const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

//         if (sameDayDom) {

//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(flight.date)

//         } else if (nextDayDom) {
//           // min to max on the next day
//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//           const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

//           domQuery.$or = [
//             { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//         if (sameDayInt) {
//           // min to max on the same day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           // min to max on the next day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//           const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

//           intlQuery.$or = [
//             { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//       } else if (flight.domIntl.toLowerCase() === 'intl') {
//         const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
//         const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
//         const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
//         const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(flight.date);

//         } else if (nextDayDom) {

//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {

//           const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//           const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           domQuery.$or = [
//             {
//               std: { $gte: inDMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: indmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];
//         }

//         if (sameDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {

//           const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//           const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           intlQuery.$or = [
//             {
//               std: { $gte: inInMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: ininmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];
//         }
//       }

//       console.log("domQuery is : " + JSON.stringify(domQuery));
//       console.log("intlQuery is : " + JSON.stringify(intlQuery));
//       const domFlights = await Flights.find(domQuery);
//       const intlFlights = await Flights.find(intlQuery);

//       const update = {
//         $set: {
//           beyondODs: [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)],
//         },
//       };

//       await Flights.updateOne({ _id: flight._id }, update);

//       // Update behindODs field in domFlights and intlFlights
//       if (!flight._id) {
//         console.error('Flight _id is undefined or null');
//         // Handle the error accordingly, for example, by skipping this update operation
//       } else {
//         // Update documents with $addToSet only if flight._id is valid
//         for (const f of domFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//         for (const f of intlFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//       }
//     };


//     console.log("Connections Completed")
//     res.status(200).json({ message: "Connections Completed" });
//   } catch (error) {
//     console.error('Error processing flight connections:', error);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// };

const populateDashboardDropDowns = async (req, res) => {
  try {

    const userId = req.user.id;

    // Fetch distinct values for the "sector" field from the Flight model
    const distinctSectors = await Flights.aggregate([
      { $match: { userId: userId } }, // Filter by user ID
      { $group: { _id: null, sector: { $addToSet: '$sector' } } },
      { $project: { _id: 0, sector: 1 } },
    ]);

    const distinctValues = await Data.aggregate([
      { $match: { userId: userId } },
      { $group: { _id: null, from: { $addToSet: '$depStn' }, to: { $addToSet: '$arrStn' }, variant: { $addToSet: '$variant' }, userTag1: { $addToSet: '$userTag1' }, userTag2: { $addToSet: '$userTag2' } } },
      { $project: { _id: 0, from: 1, to: 1, variant: 1, userTag1: 1, userTag2: 1 } },
    ]);

    const formatOptions = (values) =>
      values.map((value) => ({ value: value, label: value }));

    // Filter out undefined-undefined values from sector
    const filteredSectors = distinctSectors?.[0]?.sector?.filter(sector => sector !== 'undefined-undefined') ?? [];
    const formattedSectors = formatOptions(filteredSectors);

    const data = {
      from: formatOptions(distinctValues[0].from),
      to: formatOptions(distinctValues[0].to),
      variant: formatOptions(distinctValues[0].variant),
      sector: formattedSectors,
      userTag1: formatOptions(distinctValues[0].userTag1),
      userTag2: formatOptions(distinctValues[0].userTag2)
    };

    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}

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
              // Check if the behindODs array exists and has at least one entry
              if (flight.behindODs && flight.behindODs.length > 0) {
                flightsWithBehindODs.push(flight);
              }
            });

            return flightsWithBehindODs;
          }

          function getFlightsWithBeyondODs(flightsInPeriod) {
            let flightsWithBeyondODs = [];

            flightsInPeriod.forEach(flight => {
              // Check if the behindODs array exists and has at least one entry
              if (flight.beyondODs && flight.beyondODs.length > 0) {
                flightsWithBeyondODs.push(flight);
              }
            });

            return flightsWithBeyondODs;
          }

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

function roundToLastDateOfNextQuarter(date) {

  // Determine the current quarter
  const currentMonth = date.getMonth();
  const currentQuarter = Math.floor(currentMonth / 3); // Quarters are 0-based

  // Calculate the first month of the next quarter
  const firstMonthOfNextQuarter = (currentQuarter + 1) * 3;

  // Set the date to the first day of the next quarter and subtract one day to get the last day of the current quarter
  date.setMonth(firstMonthOfNextQuarter, 1);
  date.setDate(date.getDate() - 1);

  return date;
}

function generateQuarterlyDates(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);

  while (currentDate <= endDate) {
    const currentMonth = currentDate.getMonth();

    // Check the current quarter and add the last day accordingly
    if (currentMonth >= 0 && currentMonth < 3) {
      // First quarter, end date is March 31
      currentDate = new Date(currentDate.getFullYear(), 2, 31);
    } else if (currentMonth >= 3 && currentMonth < 6) {
      // Second quarter, end date is June 30
      currentDate = new Date(currentDate.getFullYear(), 5, 30);
    } else if (currentMonth >= 6 && currentMonth < 9) {
      // Third quarter, end date is September 30
      currentDate = new Date(currentDate.getFullYear(), 8, 30);
    } else {
      // Fourth quarter, end date is December 31
      currentDate = new Date(currentDate.getFullYear(), 11, 31);
    }



    if (currentDate <= roundToLastDateOfNextQuarter(endDate)) {
      periods.push(new Date(currentDate));

    }

    // Move to the next quarter's start date
    currentDate.setMonth(currentDate.getMonth() + 1);
  }

  return periods;
}

function roundToLastDateOfPresentYear(date) {

  // Set the date to December 31st of the current year
  date.setMonth(11, 31);

  return date;
}

function generateAnnualDates(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);
  endDate = roundToLastDateOfPresentYear(endDate);
  while (currentDate <= endDate) {
    // Calculate the last day of the current year (December 31st)
    const lastDayOfYear = new Date(currentDate.getFullYear(), 11, 31);

    // Push the last day of the year to the periods array
    periods.push(new Date(lastDayOfYear));

    // Move to the next year's start date
    currentDate.setFullYear(currentDate.getFullYear() + 1);
  }

  return periods;
}

function generateLastDayOfMonths(startDate, endDate) {
  const periods = [];
  let currentDate = new Date(startDate);

  while (currentDate <= endDate) {
    const year = currentDate.getFullYear();
    const month = currentDate.getMonth();
    const lastDayOfMonth = new Date(year, month + 1, 0); // Set to the last day of the current month

    // Push the last day of the month to the periods array
    periods.push(lastDayOfMonth);

    // Move to the next month's start date
    currentDate.setMonth(month + 1);
    currentDate.setDate(1);
  }

  return periods;
}

function generateWeeklyDates(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  const dates = [];

  // Loop through the dates from start to end
  for (let current = start; current <= end; current.setDate(current.getDate() + 1)) {
    // Check if the current day is a Sunday (day 0)
    if (current.getDay() === 0) {
      // Push the current date to the array
      dates.push(new Date(current));
    }
  }

  // Check if the endDate is not a Sunday
  if (end.getDay() !== 0) {
    // Find the next Sunday after endDate
    const nextSunday = new Date(end);
    nextSunday.setDate(end.getDate() + (7 - end.getDay()));
    dates.push(nextSunday);
  }

  return dates;
}


function generateDailyDates(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  const dates = [];

  // Loop through the dates from start to end
  for (let current = start; current <= end; current.setDate(current.getDate() + 1)) {
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
      day: { $in: datesInRange },
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
    let startEffDate = new Date(effFromDate);
    let endEffDate = new Date(effToDate);

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

    //correction for end Dates
    endEffDate.setDate(endEffDate.getDate() + 1);
    endEffDate.setHours(0, 0, 0, 0);

    console.log("After correct startEffDate " + startEffDate)
    console.log("After correct endEffDate " + endEffDate)

    // Step 2: Exclude these networkIds from your query
    const existingFlights = await Flights.find({
      userId,
      arrStn,
      bt,
      depStn,
      day: { $in: daysOfWeekStrings },
      date: { $gte: startEffDate, $lte: endEffDate },
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

    startEffDate.setDate(startEffDate.getDate() + 1);
    startEffDate.setHours(0, 0, 0, 0);

    const normalizeDate = (date) => {
      const d = new Date(date);
      d.setHours(0, 0, 0, 0);
      return d;
    };

    const existingFlightsDates = existingFlights.map(flight => flight.date);
    const allFlightsWithSameNetworkIdDates = allFlightsWithSameNetworkId.map(flight => flight.date);

    // Check if existingFlightDates is a subset of allFlightDates
    const isSubset = existingFlightsDates.every(date => allFlightsWithSameNetworkIdDates.some(d => d.getTime() === date.getTime()));

    // check for date ranges
    const minDate = Math.min(...allFlightsWithSameNetworkIdDates.map(d => d.getTime()));
    const maxDate = Math.max(...allFlightsWithSameNetworkIdDates.map(d => d.getTime()));

    // Convert startEffDate and endEffDate to milliseconds for comparison
    const startEffDateInMillis = startEffDate.getTime();
    const endEffDateInMillis = endEffDate.getTime();

    // Check if startEffDate is greater than or equal to the smallest date and endEffDate is less than or equal to the largest date
    const isDateRangeValid = startEffDateInMillis >= minDate && endEffDateInMillis <= maxDate;

    let allDatesInRange = false;

    allFlightsWithSameNetworkId.forEach((flight) => {
      const flightDate = new Date(flight.date);
      if (!(flightDate >= startEffDate && flightDate <= endEffDate)) {
        allDatesInRange = false; // No need to change, as at least one entry is outside the range
      }
      allDatesInRange = true; // If this point is reached, it means all entries so far are within the range
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
      // Add rotation details
      const rotationDetailsId = await addRotationDetails(req, res);

      // a. If no row is found, populate new flights in Master table
      await AddDataFromRotations(req, res, rotationDetailsId);

      return res.status(200).json({ message: 'RotationNumber updated successfully for existing flights.' });

    } else if ((networkIdToCheck && isSubset && isDateRangeValid) || (existingFlightsIds.every(id => allFlightsIds.includes(id)) && allDatesInRange)) {

      // Add rotation details
      const rotationDetailsId = await addRotationDetails(req, res);

      // b. If rows are found for all dates, update rotationNumber in existing flights
      const historyPromises = existingFlights.map(async (flight) => {
        // Create a copy of the flight for FlightsHistory
        const flightHistory = new FlightHistory({
          ...flight._doc, // Copy all properties from the original flight
          addedByRotation: `${rotationNumber}-${depNumber - 1}`,
          flightId: flight._id
        });

        // Save the flight history document
        await flightHistory.save();

        // Update the original flight
        await Flights.findByIdAndUpdate(flight._id, {
          rotationNumber: rotationNumber,
          addedByRotation: `${rotationNumber}-${depNumber}`
        });
      });

      // Wait for all history operations to complete
      await Promise.all(historyPromises);

      return res.status(200).json({ message: 'RotationNumber updated successfully for existing flights.' });
    } else {

      await RotationSummary.deleteMany({ rotationNumber: rotationNumber, userId: userId });
      return res.status(500).json({ message: 'Data Inconsistent', flightNumber });
    }

  } catch (error) {
    console.error('Error modifying flights:', error);
    return res.status(500).json({ success: false, message: 'An error occurred while modifying flights.' });
  }
};


const saveStation = async (req, res) => {
  try {
    const { stations, homeTimeZone } = req.body;

    // Update the home timezone of the user
    const user = await User.findById(req.user.id);
    user.hometimeZone = homeTimeZone; // Corrected property name
    await user.save();

    // Array to store updated stations
    const updatedStations = [];

    // Iterate over stations array and update each station sequentially
    for (const stationData of stations) {
      const { _id, ...updateFields } = stationData;

      // Find the document based on _id (assuming _id is the identifier)
      const existingStation = await Stations.findById(_id);

      if (!existingStation) {
        // Handle the case where the station doesn't exist
        updatedStations.push(null);
        continue; // Move to the next iteration
      }

      // Update the existing document with the new data
      await existingStation.updateOne(updateFields);

      // Push the updated station to the array
      updatedStations.push(await Stations.findById(existingStation._id));
    }


    // Call createConnections after all stations are updated
    res.status(200).json(updatedStations);

    // await createConnections(req.user.id);

  } catch (error) {
    console.error('Error updating stations:', error);
    res.status(500).send('Internal Server Error');
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

const deleteCompleteRotation = async (req, res) => {
  const userId = req.user.id;
  const rotationNumber = req.body.rotationNumber;
  const selectedVariant = req.body.selectedVariant;
  const totalDepNumber = req.body.totalDepNumber;

  try {
    for (let depNumber = totalDepNumber; depNumber >= 0; depNumber--) {
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

    const sectorHistoryEntries = await SectorHistory.find({ addedByRotation: { $in: addedByRotationPrev } });
    const flightHistoryEntries = await FlightHistory.find({ addedByRotation: { $in: addedByRotationPrev } });
    const dataHistoryEntries = await DataHistory.find({ addedByRotation: { $in: addedByRotationPrev } });
    const stationHistoryEntries = await StationsHistory.find({ addedByRotation: { $in: addedByRotationPrev } });

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
  importUser,
  getData,
  downloadExpenses,
  AddData,
  updateData,
  singleData,
  deleteFlightsAndUpdateSectors,
  getSecors,
  AddSectors,
  deleteSectors,
  updateSector,
  singleSector,
  AdminLogin,
  getFlights,
  searchFlights,
  getFlightsWoRotations,
  getDashboardData,
  createConnections,
  populateDashboardDropDowns,
  getVariants,
  getRotations,
  getStationsTableData,
  getNextRotationNumber,
  singleRotationDetail,
  updateRotationSummary,
  addRotationDetailsFlgtChange,
  saveStation,
  deleteCompleteRotation,
  deletePrevInRotation
};
