const mongoose = require("mongoose");

const CostConfigSchema = new mongoose.Schema({
  userId: {
    type: String,
    required: true,
    unique: true
  },
  fuelConsum: { type: Array, default: [] },
  fuelConsumIndex: { type: Array, default: [] },
  apuUsage: { type: Array, default: [] },
  plfEffect: { type: Array, default: [] },
  ccyFuel: { type: Array, default: [] },
  
  leasedReserve: { type: Array, default: [] },
  schMxEvents: { type: Array, default: [] },
  transitMx: { type: Array, default: [] },
  otherMx: { type: Array, default: [] },
  rotableChanges: { type: Array, default: [] },
  
  navEnr: { type: Array, default: [] },
  navTerm: { type: Array, default: [] },
  airportLanding: { type: Array, default: [] },
  airportDom: { type: Array, default: [] },
  airportIntl: { type: Array, default: [] },
  airportAvsec: { type: Array, default: [] },

  otherDoc: { type: Array, default: [] }
}, { timestamps: true });

module.exports = mongoose.model("CostConfig", CostConfigSchema);
