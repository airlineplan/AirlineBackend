const mongoose = require("mongoose");
const Schema = mongoose.Schema;
const Data = require("../model/dataSchema");

const dataHistorySchema = new mongoose.Schema({
  flight: {
    type: String,
    required: true,
  },
  depStn: {
    type: String,
    required: true,
  },
  std: {
    type: String,
    required: true,
  },
  bt: {
    type: String,
    required: true,
  },
  sta: {
    type: String,
    required: true,
  },
  arrStn: {
    type: String,
    required: true,
  },
  variant: {
    type: String,
    required: true,
  },
  effFromDt: {
    type: Date,
    required: true,
  },
  effToDt: {
    type: Date,
    required: true,
  },
  dow: {
    type: String,
    required: true,
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
  timeZone: {
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
  dataId : {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Data', 
  }
});


module.exports = mongoose.model("DataHistory", dataHistorySchema);
