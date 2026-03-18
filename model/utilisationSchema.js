const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const utilisationSchema = new Schema({
  targetId: {
    type: String, // MSN or SN
    required: true,
  },
  date: {
    type: Date,
    required: true,
  },
  tsn: {
    type: Number, // Time Since New
  },
  csn: {
    type: Number, // Cycles Since New
  },
  dsn: {
    type: Number, // Days Since New
  },
  hours: {
    type: Number, // Daily increment
  },
  cycles: {
    type: Number, // Daily increment
  },
  isAssumption: {
    type: Boolean,
    default: false,
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("Utilisation", utilisationSchema);
