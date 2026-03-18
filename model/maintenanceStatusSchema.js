const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceStatusSchema = new Schema({
  targetId: {
    type: String, // MSN or SN
    required: true,
  },
  label: {
    type: String,
  },
  category: {
    type: String,
  },
  tsn: {
    type: Number,
  },
  csn: {
    type: Number,
  },
  dsn: {
    type: Number,
  },
  soTsrtrt: {
    type: Number, // SO/TSRtrt
  },
  tsrPlmt: {
    type: Number, // TSRplmt
  },
  csrPlmt: {
    type: Number, // CSRplmt
  },
  dsrPlmt: {
    type: Number, // DSRplmt
  },
  date: {
    type: Date,
    required: true,
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("MaintenanceStatus", maintenanceStatusSchema);
