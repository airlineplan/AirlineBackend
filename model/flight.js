const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const flightSchema = new mongoose.Schema(
  {
    date: {
      type: Date,
    },
    day: {
      type: String,
    },
    flight: {
      type: String,
    },
    depStn: {
      type: String,
    },
    std: {
      type: String,
    },
    bt: {
      type: String,
    },
    sta: {
      type: String,
    },
    arrStn: {
      type: String,
    },
    sector: {
      type: String,
    },
    variant: {
      type: String,
    },
    seats: {
      type: String,
    },
    CargoCapT: {
      type: String,
    },
    dist: {
      type: String,
    },
    pax: {
      type: String,
    },
    CargoT: {
      type: String,
    },
    ask: {
      type: String,
    },
    rsk: {
      type: String,
    },
    cargoAtk: {
      type: String,
    },
    cargoRtk: {
      type: String,
    },
    domIntl: {
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
    sectorId: {
      type: String,
    },
    networkId: {
      type: String,
    },
    userId: {
      type: String,
    },
    isComplete: {
      type: Boolean,
    },
    rotationNumber: {
      type: String
    },
    // pendingConnection: {
    //   type: Boolean,
    //   default : false,
    // },
    beyondODs: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'FLIGHT',
      },
    ],
    behindODs: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'FLIGHT',
      },
    ],
    addedByRotation : {
      type: String 
    },
    effFromDt: {
      type: Date,
    },
    effToDt: {
      type: Date,
    },
    dow: {
      type: String,
    },
  }
);

module.exports = mongoose.model("FLIGHT", flightSchema);
