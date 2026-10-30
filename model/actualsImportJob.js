const mongoose = require("mongoose");
const schema = new mongoose.Schema({
  userId: { type: String, required: true }, fileName: String,
  status: { type: String, enum: ["queued", "processing", "completed", "failed"], default: "queued" },
  rawRows: [mongoose.Schema.Types.Mixed], result: mongoose.Schema.Types.Mixed, error: String, lockUntil: Date, lockToken: String,
}, { timestamps: true });
schema.index({ status: 1, lockUntil: 1, createdAt: 1 });
module.exports = mongoose.model("ActualsImportJob", schema);
