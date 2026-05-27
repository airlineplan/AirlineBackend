const mongoose = require("mongoose");
const Assignment = require("../model/assignment");
const FLIGHT = require("../model/flight");
const Sector = require("../model/sectorSchema");
const Stations = require("../model/stationSchema");
const StationsHistory = require("../model/stationHistorySchema");
const userData = require("../model/userSchema");
const { calculateBH_FH } = require('../utils/calculateFlightHours');
const { purgeStaleAssignmentsForUser, revalidateAssignmentsForUser } = require("../utils/assignmentSync");
const moment = require("moment");
const Schema = mongoose.Schema;
// const createConnections = require('../helper/createConnections');

const dataSchema = new mongoose.Schema({
  flight: {
    type: String,
    required: true,
  },
  sourceSerialNo: {
    type: Number,
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
  beyond1: {
    type: Number,
  },
  beyond2: {
    type: Number,
  },
  behind1: {
    type: Number,
  },
  behind2: {
    type: Number,
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
  if (!tzString || !tzString.startsWith("UTC")) return null;

  if (tzString === "UTC") return 0;

  const sign = tzString.includes("-") ? -1 : 1;
  const timePart = tzString.replace(/UTC[+-]/, "");
  if (!timePart) return 0;

  const [hours, minutes] = timePart.split(":").map(Number);
  return sign * ((hours * 60) + (minutes || 0));
}

function startOfUtcDay(date) {
  const d = new Date(date);
  d.setUTCHours(0, 0, 0, 0);
  return d;
}

function addUtcDays(date, days) {
  const d = startOfUtcDay(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function getUtcDayOfWeek(date) {
  return new Date(date).getUTCDay();
}

function normalizeScheduleString(value) {
  return value === null || value === undefined ? "" : String(value).trim();
}

function normalizeScheduleFlight(value) {
  return normalizeScheduleString(value).toUpperCase();
}

function normalizeScheduleDate(value) {
  if (!value) return "";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";

  return date.toISOString();
}

function hasScheduleChange(originalDoc, updatedDoc) {
  return (
    normalizeScheduleDate(originalDoc.effFromDt) !== normalizeScheduleDate(updatedDoc.effFromDt) ||
    normalizeScheduleDate(originalDoc.effToDt) !== normalizeScheduleDate(updatedDoc.effToDt) ||
    normalizeScheduleString(originalDoc.dow) !== normalizeScheduleString(updatedDoc.dow)
  );
}

function normalizeFlightGenerationDoc(doc) {
  const source = doc || {};

  return {
    flight: source.flight,
    depStn: source.depStn || source.sector1,
    arrStn: source.arrStn || source.sector2,
    std: source.std,
    bt: source.bt,
    sta: source.sta,
    variant: source.variant,
    effFromDt: source.effFromDt || source.fromDt,
    effToDt: source.effToDt || source.toDt,
    dow: source.dow,
    domINTL: source.domINTL || source.domIntl || "",
    userTag1: source.userTag1,
    userTag2: source.userTag2,
    remarks1: source.remarks1,
    remarks2: source.remarks2,
    gcd: source.gcd,
    paxCapacity: source.paxCapacity,
    CargoCapT: source.CargoCapT,
    paxLF: source.paxLF,
    cargoLF: source.cargoLF,
    userId: source.userId,
    networkId: source.networkId || (source._id ? String(source._id) : undefined),
    rotationNumber: source.rotationNumber,
    addedByRotation: source.addedByRotation,
  };
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

  let depTzMins = null;
  let arrTzMins = null;

  const selectTz = (station) => {
    if (!station) return null;

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

  const diffInTzMins = (depTzMins !== null && arrTzMins !== null && !isNaN(depTzMins) && !isNaN(arrTzMins))
    ? arrTzMins - depTzMins
    : 0;

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
  doc = normalizeFlightGenerationDoc(doc);
  const startDate = startOfUtcDay(doc.effFromDt);
  const endDate = startOfUtcDay(doc.effToDt);
  const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

  const toFiniteNumber = (value, fallback = 0) => {
    if (value === null || value === undefined || value === "") return fallback;
    const parsed = typeof value === "number" ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  };

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
  const seats = toFiniteNumber(doc.paxCapacity);
  const cargoCapT = toFiniteNumber(doc.CargoCapT);
  const gcd = toFiniteNumber(doc.gcd);
  const paxLF = toFiniteNumber(doc.paxLF);
  const cargoLF = toFiniteNumber(doc.cargoLF);

  while (currentDate <= endDate) {
    // Stop if the overall flight limit is reached
    if (currentFlightCount >= FLIGHT_LIMIT) {
      console.log(`Flight limit of ${FLIGHT_LIMIT} reached while processing.`);
      break;
    }

    // Check if the current date matches the specified days of the week
    const currentDayOfWeek = getUtcDayOfWeek(currentDate);
    const normalizedDow = currentDayOfWeek === 0 ? 7 : currentDayOfWeek;

    if (digitArray.includes(normalizedDow)) {
      const dayOfWeek = daysOfWeek[currentDayOfWeek];
      const flightDate = new Date(currentDate);
      flightDate.setUTCHours(0, 0, 0, 0);

      const newFlight = new FLIGHT({
        date: flightDate,
        day: dayOfWeek,
        flight: doc.flight,
        sourceSerialNo: doc.sourceSerialNo,
        depStn: doc.depStn,
        std: doc.std,
        bt: doc.bt,
        sta: doc.sta,
        arrStn: doc.arrStn,
        sector: `${doc.depStn}-${doc.arrStn}`,
        variant: doc.variant,
        seats,
        CargoCapT: cargoCapT,
        dist: gcd,
        pax: seats * (paxLF / 100),
        CargoT: cargoCapT * (cargoLF / 100),
        ask: seats * gcd,
        rsk: seats * (paxLF / 100) * gcd,
        cargoAtk: cargoCapT * gcd,
        cargoRtk: cargoCapT * (cargoLF / 100) * gcd,
        domIntl: doc.domINTL.toLowerCase(),
        userTag1: doc.userTag1,
        userTag2: doc.userTag2,
        remarks1: doc.remarks1,
        remarks2: doc.remarks2,
        userId: doc.userId,
        networkId: doc.networkId,
        rotationNumber: doc.rotationNumber,
        isComplete: doc.addedByRotation ? true : !!doc.networkId,
        addedByRotation: doc.addedByRotation,
        effFromDt: doc.effFromDt,
        effToDt: doc.effToDt,
        dow: doc.dow,
        bh: bh,
        fh: fh,
        ft: ft,
        acftType: doc.variant,
        beyond1: doc.beyond1,
        beyond2: doc.beyond2,
        behind1: doc.behind1,
        behind2: doc.behind2,
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

    currentDate = addUtcDays(currentDate, 1);
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
  const shouldSkipAssignmentResync = this.getOptions?.()?.skipAssignmentResync === true;
  try {
    const data = await Sector.findOne({ networkId: networkId });
    if (!data) {
      console.log(`No sector entry found for networkId: ${networkId}`);
      return;
    }

    const originalData = data.toObject ? data.toObject() : { ...data._doc };
    const oldArrStn = originalData.sector2;
    const oldDepStn = originalData.sector1;

    if (!data.fromDt || !data.toDt) {
      return;
    }

    const updatedFields = [];
    const assignIfChanged = (targetField, nextValue, label = targetField, options = {}) => {
      if (nextValue === undefined || nextValue === null) return;

      const currentComparable = options.date
        ? normalizeScheduleDate(data[targetField])
        : String(data[targetField] ?? "");
      const nextComparable = options.date
        ? normalizeScheduleDate(nextValue)
        : String(nextValue ?? "");

      if (currentComparable !== nextComparable) {
        data[targetField] = nextValue;
        updatedFields.push(label);
      }
    };

    assignIfChanged("fromDt", doc.effFromDt, "fromDt", { date: true });
    assignIfChanged("toDt", doc.effToDt, "toDt", { date: true });
    assignIfChanged("flight", doc.flight, "flight");
    ["sourceSerialNo", "beyond1", "beyond2", "behind1", "behind2"].forEach((field) => {
      assignIfChanged(field, doc[field], field);
    });
    assignIfChanged("dow", doc.dow, "dow");
    assignIfChanged("sector2", doc.arrStn, "arrStn");
    assignIfChanged("bt", doc.bt, "bt");
    assignIfChanged("sector1", doc.depStn, "depStn");
    assignIfChanged("sta", doc.sta, "sta");
    assignIfChanged("std", doc.std, "std");
    assignIfChanged("variant", doc.variant, "variant");
    assignIfChanged("domINTL", doc.domINTL, "domINTL");
    assignIfChanged("userTag1", doc.userTag1, "userTag1");
    assignIfChanged("userTag2", doc.userTag2, "userTag2");
    assignIfChanged("remarks1", doc.remarks1, "remarks1");
    assignIfChanged("remarks2", doc.remarks2, "remarks2");

    if (updatedFields.length > 0) {
      await data.save();
      console.log(`Updated fields [${updatedFields.join(", ")}] for networkId: ${networkId}`);
    } else {
      console.log(`No fields updated for networkId: ${networkId}`);
      return;
    }

    if (oldArrStn !== doc.arrStn) {
      await updateStationFrequency(doc.arrStn, 1);
      await updateStationFrequency(oldArrStn, -1);
    }

    if (oldDepStn !== doc.depStn) {
      await updateStationFrequency(doc.depStn, 1);
      await updateStationFrequency(oldDepStn, -1);
    }

    const originalScheduleDoc = {
      effFromDt: originalData.fromDt,
      effToDt: originalData.toDt,
      dow: originalData.dow,
    };

    const updatedScheduleDoc = {
      effFromDt: doc.effFromDt,
      effToDt: doc.effToDt,
      dow: doc.dow,
    };

    const scheduleChanged = hasScheduleChange(originalScheduleDoc, updatedScheduleDoc);

    if (scheduleChanged) {
      const existingFlights = await FLIGHT.find({ networkId: networkId })
        .select("date flight")
        .lean();

      const deletedFlights = await FLIGHT.deleteMany({ networkId: networkId });
      console.log(`Deleted ${deletedFlights.deletedCount} existing flight rows for networkId: ${networkId}`);

      const assignmentOccurrenceFilters = existingFlights
        .filter((flight) => flight.date && flight.flight)
        .map((flight) => ({
          date: moment.utc(flight.date).startOf("day").toDate(),
          flightNumber: normalizeScheduleFlight(flight.flight),
        }));

      if (assignmentOccurrenceFilters.length > 0) {
        const assignmentDeleteResult = await Assignment.deleteMany({
          userId: doc.userId,
          $or: assignmentOccurrenceFilters
        });

        console.log(
          `Deleted ${assignmentDeleteResult.deletedCount} assignment rows for schedule-changed networkId ${networkId}`
        );
      }

      await createFlgts({
        ...originalData,
        ...data.toObject(),
        ...doc,
        depStn: data.sector1,
        arrStn: data.sector2,
        effFromDt: doc.effFromDt,
        effToDt: doc.effToDt,
        dow: doc.dow,
        domINTL: doc.domINTL,
        userId: doc.userId
      });
      await purgeStaleAssignmentsForUser({ userId: doc.userId });
      return;
    }

    const hasNonScheduleChanges = updatedFields.some((field) => !["fromDt", "toDt", "dow"].includes(field));

    if (hasNonScheduleChanges) {
      const flightUpdatePayload = {
        flight: data.flight,
        sourceSerialNo: data.sourceSerialNo,
        depStn: data.sector1,
        std: data.std,
        bt: data.bt,
        sta: data.sta,
        arrStn: data.sector2,
        sector: `${data.sector1}-${data.sector2}`,
        variant: data.variant,
        acftType: data.acftType || data.variant,
        domIntl: String(data.domINTL || "").toLowerCase(),
        userTag1: data.userTag1,
        userTag2: data.userTag2,
        remarks1: data.remarks1,
        remarks2: data.remarks2,
        sectorId: data._id,
        effFromDt: doc.effFromDt,
        effToDt: doc.effToDt,
        dow: doc.dow,
        beyond1: data.beyond1,
        beyond2: data.beyond2,
        behind1: data.behind1,
        behind2: data.behind2,
      };

      Object.keys(flightUpdatePayload).forEach((key) => {
        if (flightUpdatePayload[key] === undefined) delete flightUpdatePayload[key];
      });

      await FLIGHT.updateMany({ networkId: networkId }, { $set: flightUpdatePayload });

      if (!shouldSkipAssignmentResync) {
        await revalidateAssignmentsForUser({ userId: doc.userId });
      }
      return;
    }

    console.log(`Schedule fields unchanged for networkId ${networkId}; flights updated in place.`);
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
