const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceEventSchema = new Schema({
  msn: {
    type: String,
    required: true,
  },
  eventName: {
    type: String, // e.g., C-check
    required: true,
  },
  date: {
    type: Date,
    required: true,
  },
  hours: {
    type: Number,
  },
  cycles: {
    type: Number,
  },
  days: {
    type: Number,
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("MaintenanceEvent", maintenanceEventSchema);
