const mongoose = require("mongoose");
const schema = new mongoose.Schema({
  userId: { type: String, required: true }, acftRegn: { type: String, required: true }, msn: String,
  month: { type: Number, required: true }, year: { type: Number, required: true },
  fileName: String, storedFileKey: String,
  lines: [{ _id: false, mrAccId: String, schMxEvent: String, sn: String, rate: Number, utilisation: Number,
    contribution: Number, ccy: String, driver: String, extracted: mongoose.Schema.Types.Mixed }],
  checks: [mongoose.Schema.Types.Mixed], status: { type: String, enum: ["Reconciled", "Unreconciled"], default: "Unreconciled" },
  appliedAt: Date, appliedBy: String,
}, { timestamps: true });
schema.index({ userId: 1, acftRegn: 1, month: 1, year: 1 }, { unique: true });
module.exports = mongoose.model("MaintenanceReserveInvoice", schema);
