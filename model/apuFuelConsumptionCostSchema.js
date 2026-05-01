const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const apuFuelConsumptionCostSchema = new Schema({
  userId: {
    type: String,
    required: true,
    index: true,
  },
  rowKey: {
    type: String,
    required: true,
  },
  date: {
    type: Date,
    required: true,
    index: true,
  },
  stn: {
    type: String,
    trim: true,
    default: "",
    index: true,
  },
  acftRegn: {
    type: String,
    trim: true,
    default: "",
    index: true,
  },
  apun: {
    type: String,
    trim: true,
    default: "",
    index: true,
  },
  apuHr: {
    type: Number,
    default: 0,
  },
  consumptionKgPerApuHr: {
    type: Number,
    default: 0,
  },
  consumptionKg: {
    type: Number,
    default: 0,
  },
  consumptionLitres: {
    type: Number,
    default: 0,
  },
  costPerLtr: {
    type: Number,
    default: 0,
  },
  totalFuelCost: {
    type: Number,
    default: 0,
  },
  currency: {
    type: String,
    trim: true,
    default: "INR",
  },
  costSourceType: {
    type: String,
    trim: true,
    default: "ARR_STN",
  },
  costSourceStation: {
    type: String,
    trim: true,
    default: "",
  },
  sourceFlightId: {
    type: String,
    trim: true,
    default: "",
  },
  remarks: {
    type: String,
    trim: true,
    default: "",
  },
  monthKey: {
    type: String,
    trim: true,
    default: "",
    index: true,
  },
}, {
  timestamps: true,
  collection: "apuFuelConsumptionCosts",
});

apuFuelConsumptionCostSchema.index({ userId: 1, rowKey: 1 }, { unique: true });
apuFuelConsumptionCostSchema.index({ userId: 1, date: 1, acftRegn: 1, apun: 1 });

module.exports = mongoose.model("ApuFuelConsumptionCost", apuFuelConsumptionCostSchema);
