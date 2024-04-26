const mongoose = require('mongoose');

const rotationOccurrenceSchema = new mongoose.Schema({
  date: {
    type: Date,
    required: true,
  },
  rotation_1: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_2: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_3: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_4: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_5: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_6: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
  rotation_7: {
    type: String,
    enum: ['Y', 'N'],
    default: 'N',
  },
});

const RotationOccurrence = mongoose.model('RotationOccurrence', rotationOccurrenceSchema);

module.exports = RotationOccurrence;
