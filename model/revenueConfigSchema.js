const mongoose = require("mongoose");

const RevenueFxRateSchema = new mongoose.Schema(
  {
    pair: {
      type: String,
      required: true,
      trim: true,
    },
    dateKey: {
      type: String,
      required: true,
      trim: true,
    },
    rate: {
      type: Number,
      default: 1,
    },
  },
  { _id: false }
);

const RevenueConfigSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    reportingCurrency: {
      type: String,
      default: "USD",
      trim: true,
      uppercase: true,
    },
    currencyCodes: {
      type: [String],
      default: [],
    },
    fxRates: {
      type: [RevenueFxRateSchema],
      default: [],
    },
  },
  { timestamps: true }
);

module.exports = mongoose.model("RevenueConfig", RevenueConfigSchema);
