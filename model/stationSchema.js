const mongoose = require("mongoose");

const stationSchema = new mongoose.Schema({
    stationName: {
        type: String,
        required: true,
        unique: false
    },
    stdtz: {
        type: String,
        required: true,
        default: "UTC+0:00",
    },
    dsttz: {
        type: String,
        required: true,
        default: "UTC+0:00",
    },
    nextDSTStart: {
        type: String,
        default: ''
    },
    nextDSTEnd: {
        type: String,
        default: ''
    },
    ddMinCT: {
        type: String,
        required: true,
        default: "1:30",
    },
    ddMaxCT: {
        type: String,
        required: true,
        default: "7:00",
    },
    dInMinCT: {
        type: String,
        required: true,
        default: "2:00",
    },
    dInMaxCT: {
        type: String,
        required: true,
        default: "7:00",
    },
    inDMinCT: {
        type: String,
        required: true,
        default: "2:00",
    },
    inDMaxCT: {
        type: String,
        default: "7:00",
        required: true,
    },
    inInMinDT: {
        type: String,
        default: "2:00",
        required: true,
    },
    inInMaxDT: {
        type: String,
        default: "7:00",
        required: true,
    },
    userId: {
        type: String,
    },
    freq: {
        type: Number, 
        required: false, 
    },
    addedByRotation : {
      type: String 
    }
});

module.exports = mongoose.model("Station", stationSchema);
