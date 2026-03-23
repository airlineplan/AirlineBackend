const mongoose = require("mongoose");

const maintenanceResetSchema = new mongoose.Schema(
    {
        date: {
            type: Date,
            required: true,
            index: true
        },
        msnEsn: { // MSN/ESN column
            type: String,
            required: true,
            trim: true,
            index: true
        },
        pn: { // PN (Part Number) column
            type: String,
            trim: true
        },
        snBn: { // SN/BN (Serial Number / Batch Number) column
            type: String,
            trim: true,
            index: true
        },

        // --- Core Utilization Metrics ---
        tsn: {
            type: Number
        }, // Time Since New
        csn: {
            type: Number
        }, // Cycles Since New
        dsn: {
            type: Number
        }, // Days Since New

        // --- Overhaul / Repair Metrics ---
        tsoTsr: {
            type: Number
        }, // TSO/TSRtrtr (Time Since Overhaul/Repair)
        csoCsr: {
            type: Number
        }, // CSO/CSRtrt (Cycles Since Overhaul/Repair)
        dsoDsr: {
            type: Number
        }, // DSO/DSRtrt (Days Since Overhaul/Repair)

        // --- Replacement Metrics ---
        tsRplmt: {
            type: Number
        }, // TSRplmt (Time Since Replacement)
        csRplmt: {
            type: Number
        }, // CSRplmt (Cycles Since Replacement)
        dsRplmt: {
            type: Number
        }, // DSRplmt (Days Since Replacement)

        // --- Application Time Metric ---
        timeMetric: {
            type: String,
            enum: ["BH", "FH"], // Block Hours or Flight Hours
            default: "BH",      // As noted in the screenshot: "Default is BH"
            trim: true
        }
    },
    {
        timestamps: true // Adds createdAt and updatedAt automatically
    }
);

// Compound index for fast querying when filtering by Date, Aircraft/Engine, and Part
maintenanceResetSchema.index({ date: 1, msnEsn: 1, snBn: 1 });

module.exports = mongoose.model("MaintenanceReset", maintenanceResetSchema);