const mongoose = require("mongoose");

const controlPlaneCounterSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      unique: true,
    },
    value: {
      type: Number,
      required: true,
    },
  },
  { timestamps: true }
);

module.exports = mongoose.model("ControlPlaneCounter", controlPlaneCounterSchema);
