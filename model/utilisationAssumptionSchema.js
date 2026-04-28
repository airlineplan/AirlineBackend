const mongoose = require("mongoose");

const utilisationAssumptionSchema = new mongoose.Schema(
    {
        userId: {
            type: String,
            required: true,
            index: true
        },
        msn: {
            type: String,
            required: true,
            trim: true,
            index: true
        },
        fromDate: {
            type: Date,
            required: true,
            index: true
        },
        toDate: {
            type: Date,
            required: true,
            index: true
        },
        hours: {
            type: Number,
            default: 0
        },
        cycles: {
            type: Number,
            default: 0
        },
        avgDowndays: {
            type: Number,
            default: 0
        }
    },
    {
        timestamps: true
    }
);

utilisationAssumptionSchema.index({ userId: 1, msn: 1, fromDate: 1, toDate: 1 });

module.exports = mongoose.model("UtilisationAssumption", utilisationAssumptionSchema);
