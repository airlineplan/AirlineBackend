const mongoose = require("mongoose");

const flightSchema = new mongoose.Schema(
  {
    date: { type: Date },
    day: { type: String },
    flight: { type: String },
    depStn: { type: String },
    std: { type: String },
    bt: { type: String },
    sta: { type: String },
    arrStn: { type: String },
    sector: { type: String },
    variant: { type: String },
    
    // CRITICAL FIX: These must all be Numbers, not Strings
    seats: { type: Number },
    CargoCapT: { type: Number },
    dist: { type: Number },
    pax: { type: Number },
    CargoT: { type: Number },
    ask: { type: Number },
    rsk: { type: Number },
    cargoAtk: { type: Number },
    cargoRtk: { type: Number },
    fh: { type: Number }, // Computed Flight Hours
    bh: { type: Number }, // Computed Block Hours
    
    domIntl: { type: String },
    userTag1: { type: String },
    userTag2: { type: String },
    remarks1: { type: String },
    remarks2: { type: String },
    sectorId: { type: String },
    networkId: { type: String },
    userId: { type: String },
    isComplete: { type: Boolean },
    rotationNumber: { type: String },
    beyondODs: { type: Boolean, default: false },
    behindODs: { type: Boolean, default: false },
    addedByRotation : { type: String },
    effFromDt: { type: Date },
    effToDt: { type: Date },
    dow: { type: String },
  }
);

flightSchema.index({ userId: 1, depStn: 1, arrStn: 1, domIntl: 1, std: 1, date: 1 });
module.exports = mongoose.model("FLIGHT", flightSchema);