const mongoose = require("mongoose");
const FLIGHT = require("../model/flight");
const Sector = require("../model/sectorSchema");

const sectorHistorySchema = new mongoose.Schema({
  sector1: {
    type: String,
  },
  sector2: {
    type: String,
  },
  acftType: {
    type: String,
  },
  variant: {
    type: String,
  },
  bt: {
    type: String,
  },
  gcd: {
    type: String,
  },
  paxCapacity: {
    type: String,
  },
  CargoCapT: {
    type: String,
  },
  paxLF: {
    type: String,
  },
  cargoLF: {
    type: String,
  },
  fromDt: {
    type: Date,
    default: null,
  },
  toDt: {
    type: Date,
    default: null,
  },
  flight: {
    type: String,
  },
  std: {
    type: String,
  },
  sta: {
    type: String,
  },
  dow: {
    type: String,
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
  networkId: {
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
  sectorId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Sector',
  }
});

module.exports = mongoose.model("SectorHistory", sectorHistorySchema);
