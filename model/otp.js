const mongoose = require("mongoose");

const otpSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
    },
    // SHA-256 of the code, never the code itself.
    code: {
      type: String,
      required: true,
    },
    expireIn: {
      type: Number,
    },
    attempts: {
      type: Number,
      default: 0,
    },
  },
  { timestamps: true }
);

otpSchema.index({ email: 1, createdAt: -1 });

module.exports = mongoose.model("OPT", otpSchema);
