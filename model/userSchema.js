const mongoose = require("mongoose");

const userSchema = new mongoose.Schema({
  firstName: {
    type: String,
    // required: true,
  },
  lastName: {
    type: String,
  },
  email: {
    type: String,
    required: true,
  },
  password: {
    type: String,
    required: true,
  },
  hometimeZone: {
    type: String,
    default: 'UTC+5:30'
  },
  todoConnection : {
    type: Boolean,
    default: false
  }
});

module.exports = mongoose.model("User", userSchema);
