const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceCalendarSchema = new Schema({
    calLabel: { type: String, trim: true },
    lineBase: { type: String, trim: true },
    calMsn: { type: String, trim: true },
    schEvent: { type: String, trim: true },
    calPn: { type: String, trim: true },
    snBn: { type: String, trim: true },
    eTsn: { type: Number },
    eCsn: { type: Number },
    eDsn: { type: Number },
    eTso: { type: Number },
    eCso: { type: Number },
    eDso: { type: Number },
    eTsr: { type: Number },
    eCsr: { type: Number },
    eDsr: { type: Number },
    downDays: { type: Number, default: 0 },
    avgDownda: { type: Number, default: 0 },
    userId: { type: Schema.Types.ObjectId, ref: "User", required: true }
}, { timestamps: true });

// Ensure uniqueness per aircraft SN/BN
maintenanceCalendarSchema.index({ userId: 1, calMsn: 1, calPn: 1, snBn: 1 }, { unique: true });

module.exports = mongoose.model("MaintenanceCalendar", maintenanceCalendarSchema);
