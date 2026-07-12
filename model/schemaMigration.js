const mongoose = require("mongoose");

const schemaMigrationSchema = new mongoose.Schema(
  {
    migrationId: {
      type: String,
      required: true,
      unique: true,
      trim: true,
    },
    appliedAt: {
      type: Date,
      default: Date.now,
    },
    appVersion: String,
  },
  { versionKey: false }
);

module.exports = mongoose.model("SchemaMigration", schemaMigrationSchema);
