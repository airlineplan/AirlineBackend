const mongoose = require("mongoose");

const assignmentSchema = new mongoose.Schema(
    {
        userId: {
            type: String,
            required: true,
            index: true
        },

        date: {
            type: Date,
            required: true,
            index: true
        },

        rotationNumber: {
            type: Number,
            required: false
        },

        legNumber: {
            type: Number,
            required: false
        },

        flightNumber: {
            type: String,
            required: true
        },

        aircraft: {
            msn: {
                type: Number,
                required: false,
                index: true
            },
            registration: {
                type: String,
                required: false
            }
        },

        metrics: {
            blockHours: {   // BH
                type: Number,
                required: false
            },
            flightHours: {  // FH
                type: Number,
                required: false
            },
            cycles: {
                type: Number,
                default: 0
            }
        },

        // Validation flags (important for your logic)
        isValid: {
            type: Boolean,
            default: true
        },

        validationErrors: [
            {
                type: String
            }
        ],

        // Optional references for business logic
        removedReason: {
            type: String,
            enum: [
                "OUTSIDE_FLEET_DATES",
                "GROUND_DAY_CONFLICT",
                "VARIANT_MISMATCH",
                "ACFT_ASSIGNMENT_OVERLAP",
                "MANUAL_REMOVAL",
                null
            ],
            default: null
        }
    },
    {
        timestamps: true
    }
);

assignmentSchema.index({ userId: 1, date: 1, flightNumber: 1 }, { unique: true });

module.exports = mongoose.model("Assignment", assignmentSchema);
