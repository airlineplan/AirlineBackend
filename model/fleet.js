// model/fleet.js
const mongoose = require("mongoose");

const fleetSchema = new mongoose.Schema(
    {
        sno: {
            type: Number,
            required: false
        },
        category: {
            type: String,
            required: true,
            enum: ["Aircraft", "Engine", "APU", "Other"], // Based on your table
            default: "Aircraft"
        },
        type: {
            type: String,
            required: true,
            trim: true // e.g., "A320ceo", "CFM56-5B"
        },
        variant: {
            type: String,
            trim: true // e.g., "A320-214", "5B6"
        },
        sn: {
            type: String,
            required: true,
            trim: true,
            unique: true // Asset SN should generally be unique
        },
        // 👇 CRITICAL: These 3 fields must match exactly for the Assignment validation to work
        regn: {
            type: String,
            trim: true,
            index: true, // Indexed for high-speed lookups during assignment upload
            set: (val) => (val ? val.toUpperCase() : val) // Auto-uppercase "vt-dku" to "VT-DKU"
        },
        entry: {
            type: Date,
            required: false // Fleet entry
        },
        exit: {
            type: Date,
            required: false // Fleet exit
        },
        // 👆 --------------------------------------------------------------------------
        titled: {
            type: String,
            trim: true // e.g., "VT-DKU #1", "Spare"
        },
        ownership: {
            type: String,
            trim: true // e.g., "Operating lease"
        },
        mtow: {
            type: Number,
            required: false // MTOW (Kg)
        },

        // Optional: Useful for the frontend FleetTable you showed earlier
        status: {
            type: String,
            enum: ["Active", "Available", "Assigned", "Maintenance", "Retired"],
            default: "Available"
        }
    },
    {
        timestamps: true // Automatically adds createdAt and updatedAt
    }
);

module.exports = mongoose.model("Fleet", fleetSchema);