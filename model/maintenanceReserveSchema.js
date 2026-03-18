const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceReserveSchema = new Schema({
  date: {
    type: Date,
    required: true,
  },
  msn: {
    type: String,
    required: true,
  },
  mrAccId: {
    type: String,
    required: true,
  },
  acftReg: {
    type: String,
  },
  rate: {
    type: Number,
  },
  driver: {
    type: String, // e.g., BH, FC, Days
  },
  driverVal: {
    type: Number,
  },
  contribution: {
    type: Number,
  },
  openingBal: {
    type: Number,
  },
  drawdown: {
    type: Number,
  },
  closingBal: {
    type: Number,
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("MaintenanceReserve", maintenanceReserveSchema);
