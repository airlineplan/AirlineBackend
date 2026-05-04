require("dotenv").config();
const mongoose = require("mongoose");
const CostConfig = require("../model/costConfigSchema");

async function run() {
  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is not set.");
  }

  await mongoose.connect(process.env.MONGO_URI);
  console.log("Connected to MongoDB");

  const result = await CostConfig.updateMany(
    {
      $or: [
        { maintenanceReserveSchedule: { $exists: false } },
        { maintenanceReserveSchedule: null },
      ],
    },
    { $set: { maintenanceReserveSchedule: [] } }
  );

  console.log(`Maintenance Reserve schedule field ensured. Matched: ${result.matchedCount}, Modified: ${result.modifiedCount}`);
  await mongoose.disconnect();
}

run().catch(async (err) => {
  console.error("Maintenance Reserve schedule field creation failed:", err.message);
  try {
    await mongoose.disconnect();
  } catch (disconnectError) {
    // ignore disconnect errors
  }
  process.exit(1);
});
