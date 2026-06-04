// model/groundDay.js
const mongoose = require("mongoose");

const groundDaySchema = new mongoose.Schema({
    userId: {
        type: String,
        required: true,
        trim: true,
        index: true
    },
    msn: {
        type: String,
        required: true,
        trim: true,
        index: true
    },
    date: {
        type: Date,
        required: true,
        index: true
    },
    event: {
        type: String,
        trim: true
    },
    source: {
        type: String,
        trim: true,
        default: "MANUAL"
    },
    eventSeriesId: {
        type: String,
        trim: true,
        index: true
    },
    occurrenceNumber: {
        type: Number
    },
    occurrenceId: {
        type: String,
        trim: true
    }
});

groundDaySchema.index({ userId: 1, date: 1, msn: 1 });
groundDaySchema.index({ userId: 1, source: 1, eventSeriesId: 1 });

module.exports = mongoose.model("GroundDay", groundDaySchema);
