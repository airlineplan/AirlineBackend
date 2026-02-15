const mongoose = require("mongoose");
const FLIGHT = require("../model/flight");
const FLIGHTHISTORY = require("../model/flightHistory");
// Ensure Station is imported so we can fetch taxi times
const Station = require("../model/stationSchema"); // Update path if necessary

const sectorSchema = new mongoose.Schema({
  sector1: { type: String },
  sector2: { type: String },
  acftType: { type: String },
  variant: { type: String },
  bt: { type: String },
  gcd: { type: String },
  paxCapacity: { type: String },
  CargoCapT: { type: String },
  paxLF: { type: String },
  cargoLF: { type: String },
  fromDt: { type: Date, default: null },
  toDt: { type: Date, default: null },
  flight: { type: String },
  std: { type: String },
  sta: { type: String },
  dow: { type: String },
  domINTL: { type: String },
  userTag1: { type: String },
  userTag2: { type: String },
  remarks1: { type: String },
  remarks2: { type: String },
  networkId: { type: String },
  userId: { type: String },
  isScheduled: { type: Boolean, default: false },
  rotationNumber: { type: String },
  addedByRotation: { type: String },
  
  // ADDED FIELDS TO SECTOR DB
  fh: { type: Number }, // Flight hours (Decimal)
  bh: { type: Number }  // Block hours (Decimal)
});

// --- HELPER FUNCTION: Convert 'HH:MM' to Decimal ---
const timeStrToDecimal = (timeStr) => {
  if (!timeStr || typeof timeStr !== "string") return 0;
  const parts = timeStr.split(":");
  if (parts.length !== 2) return 0;
  const hours = parseInt(parts[0], 10);
  const minutes = parseInt(parts[1], 10);
  if (isNaN(hours) || isNaN(minutes)) return 0;
  return hours + (minutes / 60);
};

// --- CORE UPDATE LOGIC FOR FLIGHTS ---
const syncFlightsForSector = async (doc) => {
  try {
    // 1. Calculate Block Hours
    const bh = timeStrToDecimal(doc.bt);

    // 2. Fetch Taxi times from Station model
    const depStation = await Station.findOne({ stationName: doc.sector1, userId: doc.userId });
    const arrStation = await Station.findOne({ stationName: doc.sector2, userId: doc.userId });

    const taxiOutDec = depStation && depStation.avgTaxiOutTime ? timeStrToDecimal(depStation.avgTaxiOutTime) : 0;
    const taxiInDec = arrStation && arrStation.avgTaxiInTime ? timeStrToDecimal(arrStation.avgTaxiInTime) : 0;

    // 3. Calculate FH (ensure it doesn't go below 0)
    let fh = bh - taxiOutDec - taxiInDec;
    if (fh < 0) fh = 0; 

    // Update the Sector itself with the calculated decimals
    await mongoose.model("Sector").updateOne(
      { _id: doc._id }, 
      { $set: { fh: fh, bh: bh } }
    );

    // 4. Fields to push to FLGTs master table
    const updatedFields = {
      seats: doc.paxCapacity,
      pax: doc.paxCapacity * (doc.paxLF / 100),
      dist: doc.gcd,
      CargoCapT: doc.CargoCapT,
      CargoT: doc.CargoCapT * (doc.cargoLF / 100),
      ask: doc.paxCapacity * doc.gcd,
      rsk: doc.paxCapacity * (doc.paxLF / 100) * doc.gcd,
      cargoAtk: doc.CargoCapT * doc.gcd,
      cargoRtk: doc.CargoCapT * (doc.cargoLF / 100) * doc.gcd,
      acftType: doc.acftType, // Passed directly to master table
      fh: fh,                 // Passed directly to master table
      bh: bh                  // Passing BH as well to keep DB clean
    };

    const allFieldsValid = Object.entries(updatedFields).every(([key, value]) => (
      key in updatedFields && (typeof value === 'string' || typeof value === 'number')
    ));

    updatedFields.isComplete = allFieldsValid;

    // 5. Update flights associated with this sector
    await FLIGHT.updateMany({ networkId: doc.networkId }, { $set: updatedFields });

    console.log(`Flights updated successfully. Calculated FH: ${fh.toFixed(2)}`);
  } catch (error) {
    console.error("Error updating flights:", error);
  }
};

// --- HOOKS ---
sectorSchema.post("findOneAndUpdate", async function (doc) {
  if (doc) await syncFlightsForSector(doc);
});

sectorSchema.post("save", async function (doc) {
  if (doc) await syncFlightsForSector(doc);
});

module.exports = mongoose.model("Sector", sectorSchema);