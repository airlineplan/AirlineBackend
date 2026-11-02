const mongoose = require("mongoose");

const connectionSchema = new mongoose.Schema({
    flightID: {
        type: String,
    },
    beyondOD: {
        type: String,
    },
    userId: {
      type: String,
    }
})


connectionSchema.index({ userId: 1, flightID: 1 });
connectionSchema.index({ userId: 1, beyondOD: 1 });

module.exports = mongoose.model("Connections", connectionSchema);
