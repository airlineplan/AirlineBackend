const mongoose = require("mongoose");
const Flight = require("../model/flight");
const { CATEGORIES } = require("../utils/flightCategory");

async function backfillFlightCategory() {
  return Flight.updateMany(
    { category: { $exists: false } },
    { $set: { category: CATEGORIES.PLANNED } },
    { allCategories: true }
  );
}

async function run() {
  require("dotenv").config();
  require("../config/dns");
  if (!process.env.MONGO_URI) throw new Error("MONGO_URI is not set.");

  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log("Connected to MongoDB");
    const result = await backfillFlightCategory();
    console.log(`Done. Matched: ${result.matchedCount}, Updated: ${result.modifiedCount}`);
  } finally {
    await mongoose.disconnect();
  }
}

if (require.main === module) {
  run().catch((err) => {
    console.error("Backfill failed:", err.message);
    process.exitCode = 1;
  });
}

module.exports = { backfillFlightCategory };
