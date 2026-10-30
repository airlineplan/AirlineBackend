const mongoose = require("mongoose");
const schema = new mongoose.Schema({
  userId: { type: String, required: true }, fileName: String, storedFileKey: String,
  candidates: mongoose.Schema.Types.Mixed, appliedAt: Date,
}, { timestamps: true });
schema.index({ userId: 1, createdAt: -1 });
module.exports = mongoose.model("InvoiceDraft", schema);
