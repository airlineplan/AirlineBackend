const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const rotableMovementSchema = new Schema({
  date: {
    type: Date,
    required: true,
  },
  msn: {
    type: String,
    required: true,
  },
  acftReg: {
    type: String,
  },
  pn: {
    type: String,
    required: true,
  },
  position: {
    type: String, // e.g., #1, #2, APU
    required: true,
  },
  removedSN: {
    type: String,
  },
  installedSN: {
    type: String,
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("RotableMovement", rotableMovementSchema);
