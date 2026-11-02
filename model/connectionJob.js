const mongoose = require("mongoose");

// One row per user: the latest connection build and a lock so concurrent
// "Update Connections" requests (from any instance) join the running build
// instead of deleting its output midway.
const connectionJobSchema = new mongoose.Schema(
  {
    userId: { type: String, required: true, unique: true },
    runId: { type: String, required: true },
    status: {
      type: String,
      enum: ["processing", "completed", "failed"],
      default: "processing",
    },
    startedAt: { type: Date },
    finishedAt: { type: Date, default: null },
    flightCount: { type: Number, default: 0 },
    connectionCount: { type: Number, default: 0 },
    error: { type: String, default: null },
  },
  { timestamps: true }
);

module.exports = mongoose.model("ConnectionJob", connectionJobSchema);
