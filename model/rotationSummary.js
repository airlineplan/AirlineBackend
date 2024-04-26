const mongoose = require('mongoose');

const rotationSummarySchema = new mongoose.Schema({
  rotationNumber: {
    type: String,
  },
  variant: {
    type: String,
  },
  rotationTag: {
    type: String,
  },
  rotationRemark: {
    type: String,
  },
  effFromDt: {
    type: Date,
  },
  effToDt: {
    type: Date,
  },
  dow: {
    type: String,
  },
  bhTotal: {
    type: String, 
  },
  gtTotal: {
    type: String, 
  },
  rotationTotalTime: {
    type: String,
  },
  firstDepLastArr: {
    type: String,
  },
  userId: {
    type: String,
  },
});

const RotationSummary = mongoose.model('RotationSummary', rotationSummarySchema);

module.exports = RotationSummary;
