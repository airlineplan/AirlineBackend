require("dotenv").config();
const mongoose = require("mongoose");
const Flight = require("../model/flight");
const { calculateBH_FH } = require("../utils/calculateFlightHours");

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

async function run() {
  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is not set.");
  }

  await mongoose.connect(process.env.MONGO_URI);
  console.log("Connected to MongoDB");

  const cursor = Flight.find({}).cursor();
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
      const { bh, fh, ft } = await calculateBH_FH(
        flight.depStn,
        flight.arrStn,
        flight.bt,
        flight.userId
      );

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

  console.log(`Done. Scanned: ${scanned}, Updated: ${modified}`);
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
