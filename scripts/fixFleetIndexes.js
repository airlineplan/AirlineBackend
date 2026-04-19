const mongoose = require("mongoose");
const Fleet = require("../model/fleet");

const DB = process.env.MONGO_URI || "mongodb+srv://neeladrinathsarangi:kBhaZHXuGOIUgt9y@cluster0.n0cx0yj.mongodb.net/?retryWrites=true&w=majority";

async function main() {
  try {
    await mongoose.connect(DB, {});

    const existingIndexes = await Fleet.collection.indexes();
    const legacySnIndex = existingIndexes.find((index) => index.name === "sn_1");

    if (legacySnIndex) {
      console.log("Dropping legacy Fleet index: sn_1");
      await Fleet.collection.dropIndex("sn_1");
    } else {
      console.log("No legacy sn_1 index found on Fleet");
    }

    await Fleet.syncIndexes();
    console.log("Fleet indexes synced successfully");
  } catch (error) {
    console.error("Failed to fix Fleet indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.disconnect();
  }
}

main();
