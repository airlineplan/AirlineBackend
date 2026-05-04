const mongoose = require("mongoose");

const MaintenanceReserveScheduleRowSchema = new mongoose.Schema({
  mrAccId: { type: String, default: "" },
  schMxEventAccount: { type: String, default: "" },
  acftRegn: { type: String, default: "" },
  pn: { type: String, default: "" },
  sn: { type: String, default: "" },
  date: { type: String, default: "" },
  rate: { type: Number, default: 0 },
  driverValue: { type: Number, default: 0 },
  contribution: { type: Number, default: 0 },
  drawdown: { type: Number, default: 0 },
  balance: { type: Number, default: 0 },
}, { _id: false });

const CostConfigSchema = new mongoose.Schema({
  userId: {
    type: String,
    required: true,
    unique: true
  },
  reportingCurrency: { type: String, default: "USD" },
  fxRates: { type: Array, default: [] },
  allocationTable: { type: Array, default: [] },
  fuelConsum: { type: Array, default: [] },
  fuelConsumIndex: { type: Array, default: [] },
  apuUsage: { type: Array, default: [] },
  plfEffect: { type: Array, default: [] },
  ccyFuel: { type: Array, default: [] },
  
  leasedReserve: { type: Array, default: [] },
  maintenanceReserveSchedule: { type: [MaintenanceReserveScheduleRowSchema], default: [] },
  schMxEvents: { type: Array, default: [] },
  transitMx: { type: Array, default: [] },
  otherMx: { type: Array, default: [] },
  rotableChanges: { type: Array, default: [] },
  
  navEnr: { type: Array, default: [] },
  navTerm: { type: Array, default: [] },
  navMtowTiers: { type: Array, default: [73000, 77000, 78000, 79000] },
  airportLanding: { type: Array, default: [] },
  airportDom: { type: Array, default: [] },
  airportIntl: { type: Array, default: [] },
  airportAvsec: { type: Array, default: [] },
  airportOther: { type: Array, default: [] },

  otherDoc: { type: Array, default: [] }
}, { timestamps: true });

module.exports = mongoose.model("CostConfig", CostConfigSchema);
