// model/groundDay.js
const mongoose = require("mongoose");

const groundDaySchema = new mongoose.Schema({
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
        trim: true // e.g., "C-check"
    }
});

// Compound index for super fast lookups
groundDaySchema.index({ date: 1, msn: 1 });

module.exports = mongoose.model("GroundDay", groundDaySchema);