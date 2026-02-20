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

sectorSchema.index({ userId: 1, sector1: 1, sector2: 1, domINTL: 1, std: 1, date: 1 });

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

const timeStrToMinutes = (timeStr) => {
  if (!timeStr) return 0;
  
  const parts = String(timeStr).split(':');
  if (parts.length === 2) {
    const hours = parseInt(parts[0], 10) || 0;
    const minutes = parseInt(parts[1], 10) || 0;
    return (hours * 60) + minutes;
  }
  
  // Fallback just in case the value is already a decimal or float
  return parseFloat(timeStr) * 60 || 0; 
};

const syncFlightsForSector = async (doc) => {
  try {
    // 1. Fetch Taxi times from Station model
    const depStation = await Station.findOne({ stationName: doc.sector1, userId: doc.userId });
    const arrStation = await Station.findOne({ stationName: doc.sector2, userId: doc.userId });

    // 2. Convert everything to total minutes for accurate math
    const bhMins = timeStrToMinutes(doc.bt);
    const taxiOutMins = depStation && depStation.avgTaxiOutTime ? timeStrToMinutes(depStation.avgTaxiOutTime) : 0;
    const taxiInMins = arrStation && arrStation.avgTaxiInTime ? timeStrToMinutes(arrStation.avgTaxiInTime) : 0;

    // 3. Calculate FH in minutes (ensure it doesn't drop below 0)
    let fhMins = bhMins - taxiOutMins - taxiInMins;
    if (fhMins < 0) fhMins = 0; 

    // 4. Convert back to decimal hours for the database
    // (e.g., 145 mins / 60 = 2.4166...)
    // (e.g., 115 mins / 60 = 1.9166...)
    const bh = bhMins / 60;
    const fh = fhMins / 60;

    // 5. Update the Sector itself with the calculated decimals
    // NOTE: MongoDB will save these as precise floats. You can format them to 2 decimals in your UI.
    await mongoose.model("Sector").updateOne(
      { _id: doc._id }, 
      { $set: { fh: fh, bh: bh } }
    );

    // 6. Fields to push to FLGTs master table
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
      acftType: doc.acftType, 
      fh: fh,                 
      bh: bh                  
    };

    const allFieldsValid = Object.entries(updatedFields).every(([key, value]) => (
      key in updatedFields && (typeof value === 'string' || typeof value === 'number')
    ));

    updatedFields.isComplete = allFieldsValid;

    // 7. Update flights associated with this sector
    await FLIGHT.updateMany({ networkId: doc.networkId }, { $set: updatedFields });

    console.log(`Flights updated successfully. Calculated FH: ${fh.toFixed(2)} | Calculated BH: ${bh.toFixed(2)}`);
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