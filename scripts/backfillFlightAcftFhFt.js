require("dotenv").config();
const mongoose = require("mongoose");
const Flight = require("../model/flight");
const Station = require("../model/stationSchema");
const { timeStrToMinutes } = require("../utils/calculateFlightHours");

const decimalHoursToHHMM = (decimalHours) => {
  const value = Number(decimalHours);
  if (!Number.isFinite(value) || value < 0) return "00:00";

  let totalMinutes = Math.round(value * 60);
  if (totalMinutes < 0) totalMinutes = 0;
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
};

const isDifferentNumber = (a, b) => {
  if (a === undefined || a === null) return true;
  if (b === undefined || b === null) return true;
  return Math.abs(Number(a) - Number(b)) > 1e-6;
};

const stationCache = new Map();

const getStationTimesForUser = async (userId) => {
  if (!stationCache.has(userId)) {
    const stations = await Station.find({ userId }).select("stationName avgTaxiOutTime avgTaxiInTime").lean();
    const stationMap = new Map(
      stations.map((station) => [
        String(station.stationName || "").trim().toUpperCase(),
        {
          taxiOut: timeStrToMinutes(station.avgTaxiOutTime || "00:00"),
          taxiIn: timeStrToMinutes(station.avgTaxiInTime || "00:00"),
        },
      ])
    );
    stationCache.set(userId, stationMap);
  }

  return stationCache.get(userId);
};

const calculateFlightMetrics = async (flight) => {
  const stationMap = await getStationTimesForUser(flight.userId);
  const depStation = stationMap.get(String(flight.depStn || "").trim().toUpperCase());
  const arrStation = stationMap.get(String(flight.arrStn || "").trim().toUpperCase());

  const btMins = timeStrToMinutes(flight.bt);
  const fhMins = Math.max(0, btMins - (depStation?.taxiOut || 0) - (arrStation?.taxiIn || 0));

  return {
    bh: btMins / 60,
    fh: fhMins / 60,
    ft: decimalHoursToHHMM(fhMins / 60),
  };
};

async function run() {
  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is not set.");
  }

  await mongoose.connect(process.env.MONGO_URI);
  console.log("Connected to MongoDB");

  const userIdArg = process.argv.find((arg) => arg.startsWith("--userId="));
  const userId = userIdArg ? userIdArg.split("=")[1]?.trim() : "";
  const query = userId ? { userId } : {};
  const cursor = Flight.find(query).sort({ userId: 1 }).cursor();
  const bulkOps = [];

  let scanned = 0;
  let modified = 0;

  for await (const flight of cursor) {
    scanned += 1;
    const patch = {};

    if (!flight.acftType && flight.variant) {
      patch.acftType = flight.variant;
    }

    if (flight.bt && flight.depStn && flight.arrStn && flight.userId) {
      const { bh, fh, ft } = await calculateFlightMetrics(flight);

      if (isDifferentNumber(flight.bh, bh)) patch.bh = bh;
      if (isDifferentNumber(flight.fh, fh)) patch.fh = fh;
      if (!flight.ft || flight.ft !== ft) patch.ft = ft;
    } else if (!flight.ft && Number.isFinite(Number(flight.fh))) {
      patch.ft = decimalHoursToHHMM(Number(flight.fh));
    }

    if (Object.keys(patch).length > 0) {
      modified += 1;
      bulkOps.push({
        updateOne: {
          filter: { _id: flight._id },
          update: { $set: patch },
        },
      });
    }

    if (bulkOps.length >= 500) {
      await Flight.bulkWrite(bulkOps, { ordered: false });
      bulkOps.length = 0;
      console.log(`Processed ${scanned} flights...`);
    }
  }

  if (bulkOps.length > 0) {
    await Flight.bulkWrite(bulkOps, { ordered: false });
  }

  console.log(`Done. User: ${userId || "all"}, Scanned: ${scanned}, Updated: ${modified}`);
  await mongoose.disconnect();
}

run().catch(async (err) => {
  console.error("Backfill failed:", err.message);
  try {
    await mongoose.disconnect();
  } catch (e) {
    // ignore disconnect errors
  }
  process.exit(1);
});
