const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const componentSchema = new Schema({
  pn: {
    type: String,
    required: true,
  },
  sn: {
    type: String,
    required: true,
    unique: true,
  },
  category: {
    type: String,
    enum: ['Conserve', 'Run-down', 'Normal'],
    default: 'Normal'
  },
  description: {
    type: String,
  },
  type: {
    type: String,
    enum: ['ENGINE', 'APU', 'LRU', 'OTHER'],
    default: 'OTHER'
  },
  userId: {
    type: Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model("Component", componentSchema);
