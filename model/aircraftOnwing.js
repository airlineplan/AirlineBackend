// model/aircraftOnwing.js
const mongoose = require("mongoose");

const aircraftOnwingSchema = new mongoose.Schema({
    date: {
        type: Date,
        required: true,
        index: true
    },
    msn: {
        type: String,
        required: true,
        trim: true,
        index: true
    },
    pos1Esn: {
        type: String,
        trim: true
    },
    pos2Esn: {
        type: String,
        trim: true
    },
    apun: {
        type: String,
        trim: true
    }
});

// Compound index for fast lookup of an aircraft's configuration over time
aircraftOnwingSchema.index({ date: 1, msn: 1 });

module.exports = mongoose.model("AircraftOnwing", aircraftOnwingSchema);