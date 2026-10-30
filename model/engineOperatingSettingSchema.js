const mongoose = require("mongoose");
const schema = new mongoose.Schema({
  userId: { type: String, required: true },
  esn: { type: String, required: true, trim: true, uppercase: true },
  metric: { type: String, enum: ["condition", "thrust", "derate"], required: true },
  effectiveDate: { type: Date, required: true },
  value: { type: mongoose.Schema.Types.Mixed, required: true },
}, { timestamps: true });
schema.index({ userId: 1, esn: 1, metric: 1, effectiveDate: 1 }, { unique: true });
module.exports = mongoose.model("EngineOperatingSetting", schema);
