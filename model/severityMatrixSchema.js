const mongoose = require("mongoose");
const schema = new mongoose.Schema({
  userId: { type: String, required: true },
  mrAccId: { type: String, required: true, trim: true, uppercase: true },
  columns: [{ _id: false, condition: String, thrust: Number, derate: Number }],
  rows: [{ _id: false, hc: Number, rates: [Number] }],
}, { timestamps: true });
schema.index({ userId: 1, mrAccId: 1 }, { unique: true });
module.exports = mongoose.model("SeverityMatrix", schema);
