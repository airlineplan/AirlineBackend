const mongoose = require("mongoose");
const FLIGHT = require("../model/flight");
const Sector = require("../model/sectorSchema");
const Stations = require("../model/stationSchema");
const StationsHistory = require("../model/stationHistorySchema");
const userData = require("../model/userSchema");
const { calculateBH_FH } = require('../utils/calculateFlightHours');
const Schema = mongoose.Schema;
// const createConnections = require('../helper/createConnections');

const dataSchema = new mongoose.Schema({
  flight: {
    type: String,
    required: true,
  },
  depStn: {
    type: String,
    required: true,
  },
  std: {
    type: String,
    required: true,
  },
  bt: {
    type: String,
    required: true,
  },
  sta: {
    type: String,
    required: true,
  },
  arrStn: {
    type: String,
    required: true,
  },
  variant: {
    type: String,
    required: true,
  },
  effFromDt: {
    type: Date,
    required: true,
  },
  effToDt: {
    type: Date,
    required: true,
  },
  dow: {
    type: String,
    required: true,
  },
  domINTL: {
    type: String,
  },
  userTag1: {
    type: String,
  },
  userTag2: {
    type: String,
  },
  remarks1: {
    type: String,
  },
  remarks2: {
    type: String,
  },
  timeZone: {
    type: String,
  },
  userId: {
    type: String,
  },
  isScheduled: {
    type: Boolean,
    default: false, // Set a default value if needed
  },
  rotationNumber: {
    type: String
  },
  addedByRotation: {
    type: String
  },
  isLast: {
    type: Boolean
  }
});

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

function reduceTime(baseTime, reduceBy) {
  const baseTimeParts = baseTime.split(':').map(Number);
  const reduceByParts = reduceBy.split(':').map(Number);

  let resultHours = baseTimeParts[0] - reduceByParts[0];
  let resultMinutes = baseTimeParts[1] - reduceByParts[1];

  if (resultMinutes < 0) {
    resultHours -= 1;
    resultMinutes += 60;
  }

  // Ensure the result is formatted as HH:mm
  const formattedResult = `${String(resultHours).padStart(2, '0')}:${String(resultMinutes).padStart(2, '0')}`;
  return formattedResult;
}

// Function to update beyondODs and behindODs arrays
async function updateBeyondODsAndBehindODs(updatedDoc, originalDoc, domFlights, intlFlights) {
  // Remove original flight IDs from beyondODs in other flights
  await FLIGHT.updateMany(
    { beyondODs: originalDoc._id },
    { $pull: { beyondODs: originalDoc._id } }
  );

  // Remove original flight IDs from behindODs in other flights
  await FLIGHT.updateMany(
    { behindODs: originalDoc._id },
    { $pull: { behindODs: originalDoc._id } }
  );

  // Loop through each updated flight and update beyondODs and behindODs arrays
  for (const updatedFlight of [updatedDoc, ...domFlights, ...intlFlights]) {
    // Update the beyondODs field with the combined array of IDs
    updatedFlight.beyondODs = [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)];

    // Save the updated document
    await updatedFlight.save();

    // Update behindODs field in domFlights and intlFlights
    await FLIGHT.updateMany(
      { _id: { $in: domFlights.map(f => f._id) } },
      { $addToSet: { behindODs: updatedFlight._id } }
    );

    await FLIGHT.updateMany(
      { _id: { $in: intlFlights.map(f => f._id) } },
      { $addToSet: { behindODs: updatedFlight._id } }
    );
  }
}

function getTzMinutes(tzString) {
  if (!tzString || !tzString.startsWith("UTC")) return 0;

  const sign = tzString.includes("-") ? -1 : 1;
  const timePart = tzString.replace(/UTC[+-]/, "");
  if (!timePart) return 0;

  const [hours, minutes] = timePart.split(":").map(Number);
  return sign * ((hours * 60) + (minutes || 0));
}

async function calculateSTA(doc) {
  if (!doc.std || !doc.bt || !doc.depStn || !doc.arrStn) return;

  const depStation = await Stations.findOne({
    stationName: doc.depStn,
    userId: doc.userId
  });

  const arrStation = await Stations.findOne({
    stationName: doc.arrStn,
    userId: doc.userId
  });

  let depTzMins = 0;
  let arrTzMins = 0;

  const selectTz = (station) => {
    if (!station) return 0;

    let tz = station.stdtz;

    if (doc.effFromDt && station.nextDSTStart && station.nextDSTEnd) {
      const fDate = new Date(doc.effFromDt);
      const dStart = new Date(station.nextDSTStart);
      const dEnd = new Date(station.nextDSTEnd);

      if (fDate >= dStart && fDate <= dEnd) {
        tz = station.dsttz || station.stdtz;
      }
    }

    return getTzMinutes(tz);
  };

  depTzMins = selectTz(depStation);
  arrTzMins = selectTz(arrStation);

  const [stdH, stdM] = doc.std.split(":").map(Number);
  const [btH, btM] = doc.bt.split(":").map(Number);

  if (isNaN(stdH) || isNaN(btH)) return;

  const diffInTzMins = arrTzMins - depTzMins;

  let totalMins =
    (stdH * 60 + (stdM || 0)) +
    (btH * 60 + (btM || 0)) +
    diffInTzMins;

  totalMins = ((totalMins % 1440) + 1440) % 1440;

  const staH = Math.floor(totalMins / 60);
  const staM = totalMins % 60;

  doc.sta = `${String(staH).padStart(2, "0")}:${String(staM).padStart(2, "0")}`;
}



async function createStations(doc) {
  const { arrStn, depStn } = doc;

  // Check if entry with stationName value as arrStn exists
  let stationArr = await Stations.findOne({ stationName: arrStn, userId: doc.userId });
  let stationDep = await Stations.findOne({ stationName: depStn, userId: doc.userId });

  if (!doc.addedByRotation) {

    if (stationArr) {
      // If entry exists, increment the freq value by 1
      await Stations.updateOne(
        { stationName: arrStn, userId: doc.userId, },
        { $inc: { freq: 1 } }
      );
    } else {
      // If entry doesn't exist, insert a new entry with freq value set to 1
      await Stations.create({
        stationName: arrStn,
        userId: doc.userId,
        freq: 1
      });
    }


    if (stationDep) {
      // If entry exists, increment the freq value by 1
      await Stations.updateOne(
        { stationName: depStn, userId: doc.userId },
        { $inc: { freq: 1 } }
      );
    } else {
      // If entry doesn't exist, insert a new entry with freq value set to 1
      await Stations.create({
        stationName: depStn,
        userId: doc.userId,
        freq: 1
      });
    }
  } else {
    if (stationArr) {
      // If entry exists, increment the freq value by 1

      const foundDoc = await Stations.findOne({ stationName: arrStn, userId: doc.userId });

      var values = doc.addedByRotation.split('-');
      var lastValue = parseInt(values[1], 10);
      lastValue -= 1;
      var addedByRotationPrev = values[0] + '-' + lastValue;

      const stationsHistoryEntry = new StationsHistory({
        stationName: arrStn,
        userId: doc.userId,
        addedByRotation: addedByRotationPrev,
        freq: foundDoc.freq // Assuming freq is the field being updated
      });

      // Save the historical entry
      await stationsHistoryEntry.save();

      // Update Stations with the incremented freq and addedByRotation field
      await Stations.updateOne(
        { stationName: arrStn, userId: doc.userId },
        { $inc: { freq: 1 }, $set: { addedByRotation: doc.addedByRotation } }
      );

    } else {
      // If entry doesn't exist, insert a new entry with freq value set to 1
      await Stations.create({
        stationName: arrStn,
        userId: doc.userId,
        freq: 1,
        addedByRotation: doc.addedByRotation
      });
    }

    if (stationDep) {

      const foundDoc = await Stations.findOne({ stationName: depStn, userId: doc.userId });

      var values = doc.addedByRotation.split('-');
      var lastValue = parseInt(values[1], 10);
      lastValue -= 1;
      var addedByRotationPrev = values[0] + '-' + lastValue;

      const stationsHistoryEntry = new StationsHistory({
        stationName: depStn,
        userId: doc.userId,
        addedByRotation: addedByRotationPrev,
        freq: foundDoc.freq // Assuming freq is the field being updated
      });

      // Save the historical entry
      await stationsHistoryEntry.save();

      // Update Stations with the incremented freq and addedByRotation field
      await Stations.updateOne(
        { stationName: depStn, userId: doc.userId },
        { $inc: { freq: 1 }, $set: { addedByRotation: doc.addedByRotation } }
      );

    } else {
      // If entry doesn't exist, insert a new entry with freq value set to 1
      await Stations.create({
        stationName: depStn,
        userId: doc.userId,
        freq: 1,
        addedByRotation: doc.addedByRotation
      });
    }
  }
}


async function createFlgts(doc) {
  const startDate = new Date(doc.effFromDt);
  const endDate = new Date(doc.effToDt);
  const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

  // Fetch flight limit from .env
  const FLIGHT_LIMIT = parseInt(process.env.FLIGHT_LIMIT, 10) || 100;
  let currentFlightCount;

  try {

    // Delete existing documents with the same networkId
    // const result = await FLIGHT.deleteMany({ networkId: doc._id });
    // console.log(`Existing flight entries deleted: ${result.deletedCount}`);

  } catch (error) {
    console.error("Error during initial setup:", error);
    return; // Stop further execution if initial setup fails
  }

  // Get the current count of flights for the user
  currentFlightCount = await FLIGHT.countDocuments({ userId: doc.userId });

  // If the user has already reached or exceeded the limit, skip creating flights
  if (currentFlightCount >= FLIGHT_LIMIT) {
    console.log(`Flight limit of ${FLIGHT_LIMIT} reached for user: ${doc.userId}`);
    return; // Exit the function
  }

  const dow = parseInt(doc.dow);
  const digitArray = String(dow).split("").map(Number);

  let currentDate = new Date(startDate);

  const { bh, fh, ft } = await calculateBH_FH(doc.depStn, doc.arrStn, doc.bt, doc.userId);

  while (currentDate <= endDate) {
    // Stop if the overall flight limit is reached
    if (currentFlightCount >= FLIGHT_LIMIT) {
      console.log(`Flight limit of ${FLIGHT_LIMIT} reached while processing.`);
      break;
    }

    // Check if the current date matches the specified days of the week
    if (digitArray.includes(currentDate.getDay() !== 0 ? currentDate.getDay() : 7)) {
      const dayOfWeek = daysOfWeek[currentDate.getDay()];

      const newFlight = new FLIGHT({
        date: currentDate,
        day: dayOfWeek,
        flight: doc.flight,
        depStn: doc.depStn,
        std: doc.std,
        bt: doc.bt,
        sta: doc.sta,
        arrStn: doc.arrStn,
        sector: `${doc.depStn}-${doc.arrStn}`,
        variant: doc.variant,
        domIntl: doc.domINTL.toLowerCase(),
        userTag1: doc.userTag1,
        userTag2: doc.userTag2,
        remarks1: doc.remarks1,
        remarks2: doc.remarks2,
        userId: doc.userId,
        networkId: doc._id,
        rotationNumber: doc.rotationNumber,
        isComplete: !!doc.networkId,
        addedByRotation: doc.addedByRotation,
        effFromDt: doc.effFromDt,
        effToDt: doc.effToDt,
        dow: doc.dow,
        bh: bh,
        fh: fh,
        ft: ft, 
        acftType: doc.variant,
      });

      try {
        await newFlight.save();
        
        //tracker for creating connections
        userData.findByIdAndUpdate(doc.userId, { todoConnection: true });

        currentFlightCount++; // Increment the flight count after successful save
        console.log("New flight entry created.");
      } catch (error) {
        console.error("Error creating new flight entry:", error);
      }
    }

    currentDate.setDate(currentDate.getDate() + 1);
  }
}


dataSchema.pre("save", async function (next) {
  try {
    await calculateSTA(this);
    next();
  } catch (err) {
    console.error("Error calculating STA:", err);
    next(err);
  }
});

dataSchema.pre("findOneAndUpdate", async function (next) {
  try {
    const update = this.getUpdate();
    if (!update) return next();

    const existingDoc = await this.model.findOne(this.getQuery());
    if (!existingDoc) return next();

    // Merge existing doc with incoming update
    const mergedDoc = {
      ...existingDoc.toObject(),
      ...update.$set,
      ...update
    };

    // Calculate new STA
    await calculateSTA(mergedDoc);

    // Inject only STA into update
    this.setUpdate({
      ...update,
      $set: {
        ...update.$set,
        sta: mergedDoc.sta
      }
    });

    next();
  } catch (err) {
    console.error("Error calculating STA on update:", err);
    next(err);
  }
});

dataSchema.post('save', async function (doc) {

  try {

    await createStations(doc);

    await createFlgts(doc);

  } catch (error) {
    console.error("Error in createConnections:", error);
  }
});


dataSchema.post("findOneAndUpdate", async function (doc) {
  const networkId = doc._id;
  try {
    const data = await Sector.findOne({ networkId: networkId });
    const oldArrStn = data.sector2;
    const oldDepStn = data.sector1;

    if (!data.fromDt || !data.toDt) {
      return;
    }

    const updatedFields = [];

    if (doc.effFromDt.toString() !== data.fromDt.toString()) {
      data.fromDt = doc.effFromDt;
      updatedFields.push('fromDt');
    }

    if (doc.flight.toString() !== data.flight.toString()) {
      data.flight = doc.flight;
      updatedFields.push('flight');
    }

    if (doc.effToDt.toString() !== data.toDt.toString()) {
      data.toDt = doc.effToDt;
      updatedFields.push('toDt');
    }

    if (doc.dow.toString() !== data.dow.toString()) {
      data.dow = doc.dow;
      updatedFields.push('dow');
    }

    if (doc.arrStn.toString() !== data.sector2.toString()) {
      data.sector2 = doc.arrStn;
      updatedFields.push('arrStn');
    }

    if (doc.bt.toString() !== data.bt.toString()) {
      data.bt = doc.bt;
      updatedFields.push('bt');
    }

    if (doc.depStn.toLowerCase().toString() !== data.sector1.toLowerCase().toString()) {
      data.sector1 = doc.depStn;
      updatedFields.push('depStn');
    }

    if (doc.sta.toString() !== data.sta.toString()) {
      data.sta = doc.sta;
      updatedFields.push('sta');
    }

    if (doc.std.toString() !== data.std.toString()) {
      data.std = doc.std;
      updatedFields.push('std');
    }

    if (doc.variant.toString() !== data.variant.toString()) {
      data.variant = doc.variant;
      updatedFields.push('variant');
    }

    if (doc.domINTL.toString() !== data.domINTL.toString()) {
      data.domINTL = doc.domINTL;
      updatedFields.push('domINTL');
    }

    if (doc.userTag1 && doc.userTag1.toString() !== data.userTag1 && data.userTag1.toString()) {
      data.userTag1 = doc.userTag1;
      updatedFields.push('userTag1');
    }

    if (doc.userTag2 && doc.userTag2.toString() !== data.userTag2 && data.userTag2.toString()) {
      data.userTag2 = doc.userTag2;
      updatedFields.push('userTag2');
    }

    if (doc.remarks1 && doc.remarks1.toString() !== data.remarks1 && data.remarks1.toString()) {
      data.remarks1 = doc.remarks1;
      updatedFields.push('remarks1');
    }

    if (doc.remarks2 && doc.remarks2.toString() !== data.remarks2 && data.remarks2.toString()) {
      data.remarks2 = doc.remarks2;
      updatedFields.push('remarks2');
    }

    if (updatedFields.length > 0) {
      await data.save();
      console.log(`Updated fields [${updatedFields.join(', ')}] for networkId: ${networkId}`);
    } else {
      console.log(`No fields updated for networkId: ${networkId}`);
    }

    const timeZoneCorrectedDates = (date, tzString) => {
      return new Date((typeof date === "string" ? new Date(date) : date).toLocaleString("en-US", { timeZone: tzString }));
    }

    let startDate = new Date(data.fromDt);
    let endDate = new Date(data.toDt);

    //set to midnight
    startDate.setHours(0, 0, 0, 0);
    endDate.setHours(0, 0, 0, 0);

    const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    const FLIGHT_LIMIT = parseInt(process.env.FLIGHT_LIMIT, 10) || 100;
    let currentFlightCount;

    const result = await FLIGHT.deleteMany({ networkId: networkId });
    console.log(`Existing flight entries deleted: ${result.deletedCount}`);

    // Get the current count of flights for the user
    currentFlightCount = await FLIGHT.countDocuments({ userId: doc.userId });

    // If the user has already reached or exceeded the limit, skip creating flights
    if (currentFlightCount >= FLIGHT_LIMIT) {
      console.log(`Flight limit of ${FLIGHT_LIMIT} reached for user: ${doc.userId}`);
      return; // Exit the function
    }

    const firstElement = parseInt(data.dow.charAt(0));
    const lastElement = parseInt(data.dow.charAt(data.dow.length - 1));

    let currentDate = new Date(startDate);

    while (currentDate <= endDate) {

      // Stop if the overall flight limit is reached
      if (currentFlightCount >= FLIGHT_LIMIT) {
        console.log(`Flight limit of ${FLIGHT_LIMIT} reached while processing.`);
        break;
      }

      const dayOfWeek = daysOfWeek[currentDate.getDay()];

      const currentDayOfWeek = currentDate.getDay() !== 0 ? currentDate.getDay() : 7;
      const allowedDaysOfWeek = Array.from(data.dow).map(Number);

      if (allowedDaysOfWeek.includes(currentDayOfWeek)) {
        const newFlight = {
          date: currentDate.setHours(0, 0, 0, 0),
          day: dayOfWeek,
          flight: doc.flight,
          depStn: data.sector1,
          std: data.std,
          bt: data.bt,
          sta: data.sta,
          arrStn: data.sector2,
          sector: `${data.sector1}-${data.sector2}`,
          variant: data.variant,
          seats: data.paxCapacity,
          CargoCapT: data.CargoCapT,
          dist: data.gcd,
          pax: data.paxCapacity * (data.paxLF / 100),
          CargoT: data.CargoCapT * (data.cargoLF / 100),
          ask: data.paxCapacity * data.gcd,
          rsk: data.paxCapacity * (data.paxLF / 100) * data.gcd,
          cargoAtk: data.CargoCapT * data.gcd,
          cargoRtk: data.CargoCapT * (data.cargoLF / 100) * data.gcd,
          domIntl: data.domINTL.toLowerCase(),
          userTag1: data.userTag1,
          userTag2: data.userTag2,
          remarks1: data.remarks1,
          remarks2: data.remarks2,
          sectorId: data._id,
          userId: data.userId,
          networkId: data.networkId,
          effFromDt: doc.effFromDt,
          effToDt: doc.effToDt,
          dow: doc.dow
        };

        if (
          !isNaN(newFlight.pax) &&
          !isNaN(newFlight.CargoT) &&
          !isNaN(newFlight.rsk) &&
          !isNaN(newFlight.ask) &&
          !isNaN(newFlight.cargoAtk) &&
          !isNaN(newFlight.cargoRtk)
        ) {
          newFlight.isComplete = true;
        } else {
          newFlight.isComplete = false;
        }

        const newFlgt = new FLIGHT(newFlight);

        currentFlightCount++;

        console.log(newFlgt, "this is new value");

        await newFlgt.save();
        console.log("New flight entry created.");
      }

      currentDate.setDate(currentDate.getDate() + 1);
    }

    if (oldArrStn !== doc.arrStn) {
      await updateStationFrequency(doc.arrStn, 1);
      await updateStationFrequency(oldArrStn, -1);
    }

    if (oldDepStn !== doc.depStn) {
      await updateStationFrequency(doc.depStn, 1);
      await updateStationFrequency(oldDepStn, -1);
    }

    // await createConnections(doc.userId);
    // console.log("createConnections completed successfully.");
  } catch (error) {
    console.error("An error occurred:", error);
  }
});

async function updateStationFrequency(stationName, change) {
  await Stations.updateOne(
    { stationName: stationName },
    { $inc: { freq: change } }
  );
}



module.exports = mongoose.model("Data", dataSchema);
