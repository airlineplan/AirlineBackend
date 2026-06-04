const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceCalendarSchema = new Schema({
    calLabel: { type: String, trim: true },
    lineBase: { type: String, trim: true },
    calMsn: { type: String, trim: true },
    schEvent: { type: String, trim: true },
    calPn: { type: String, trim: true },
    snBn: { type: String, trim: true },
    applyToAllSnBn: { type: Boolean, default: false },
    triggerRelationship: {
        type: String,
        enum: ["EARLIEST_OF_EVERY"],
        default: "EARLIEST_OF_EVERY"
    },
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
    lastOccurre: { type: Date },
    nextEstima: { type: Date },
    occurrence: { type: Number, default: 0 },
    postTso: { type: Number },
    postCso: { type: Number },
    postDso: { type: Number },
    postTsr: { type: Number },
    postCsr: { type: Number },
    postDsr: { type: Number },
    soTsr: { type: Number },
    firstOccurrenceDate: { type: Date },
    occurrencesTillExit: { type: Number, default: 0 },
    generatedOccurrences: [{
        occurrenceNumber: Number,
        triggerRelationship: String,
        triggerDate: Date,
        triggeredByMetric: String,
        triggerThreshold: Number,
        triggerMetricValueOnDetectionDate: Number,
        groundStartDate: Date,
        groundEndDate: Date,
        downtimeApplied: Number,
        isFirstOccurrence: Boolean,
        postEventStatusApplied: Schema.Types.Mixed,
        suppressedAlternateThresholds: [{
            metricCode: String,
            suppressedThreshold: Number,
            reason: String
        }],
        createdAt: { type: Date, default: Date.now }
    }],
    suppressedAlternateThresholds: [{
        occurrenceNumber: Number,
        metricCode: String,
        suppressedThreshold: Number,
        reason: String
    }],
    suppressedThresholds: [{
        metricCode: String,
        suppressedThreshold: Number
    }],
    userId: { type: Schema.Types.ObjectId, ref: "User", required: true }
}, { timestamps: true });

// Allow multiple scheduled events for the same part while avoiding duplicate event rows.
maintenanceCalendarSchema.index({ userId: 1, calMsn: 1, calPn: 1, snBn: 1, schEvent: 1 }, { unique: true });

module.exports = mongoose.model("MaintenanceCalendar", maintenanceCalendarSchema);
