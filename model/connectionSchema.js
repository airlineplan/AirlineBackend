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


module.exports = mongoose.model("Connections", connectionSchema);
