const mongoose = require("mongoose");

const utilisationSchema = new mongoose.Schema(
    {
        date: {
            type: Date,
            required: true,
            index: true
        },
        msnEsn: { // Maps to "MSN/ESN" column
            type: String,
            required: true,
            trim: true,
            index: true
        },
        pn: { // Maps to "PN" column (Part Number)
            type: String,
            trim: true
        },
        snBn: { // Maps to "SN/BN" column (Serial Number / Batch Number)
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

        // --- Flags & Metadata ---
        setFlag: {
            type: String,
            enum: ["Y", "", null], // Restricts to 'Y' or empty based on the screenshot
            default: null
        },

        remarks: {
            type: String,
            trim: true // Captures the "After populating data (end of day)" notes
        }
    },
    {
        timestamps: true
    }
);

// Compound index for super-fast lookups when filtering by Date, Aircraft/Engine, and Part Serial Number
utilisationSchema.index({ date: 1, msnEsn: 1, snBn: 1 });

module.exports = mongoose.model("Utilisation", utilisationSchema);