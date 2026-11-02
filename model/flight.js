const mongoose = require("mongoose");
const { CATEGORIES } = require("../utils/flightCategory");

const costField = {
  type: Number,
  default: 0,
};

const costCurrencyField = {
  type: String,
  default: "",
};

const flightSchema = new mongoose.Schema(
  {
    date: { type: Date },
    day: { type: String },
    flight: { type: String },
    category: {
      type: String,
      enum: Object.values(CATEGORIES),
      default: CATEGORIES.PLANNED,
    },
    pairId: { type: String },
    actualUploadId: { type: String },
    invoiceId: { type: String },
    sourceSerialNo: { type: Number },
    depStn: { type: String },
    std: { type: String },
    bt: { type: String },
    sta: { type: String },
    arrStn: { type: String },
    sector: { type: String },
    variant: { type: String },
    acftType: { type: String },
    ft: { type: String },
    paxLF: { type: Number },

    // Numbers
    seats: { type: Number },
    CargoCapT: { type: Number },
    dist: { type: Number },
    pax: { type: Number },
    CargoT: { type: Number },
    ask: { type: Number },
    rsk: { type: Number },
    cargoAtk: { type: Number },
    cargoRtk: { type: Number },
    fh: { type: Number },
    bh: { type: Number },
    engineFuelConsumption: costField,
    engineFuelConsumptionKg: costField,
    engineFuelKg: costField,
    engineFuelLitres: costField,
    engineFuelCost: costField,
    engineFuelCostCCY: costCurrencyField,
    engineFuelCostRCCY: costField,
    maintenanceReserveContribution: costField,
    maintenanceReserveContributionCCY: costCurrencyField,
    maintenanceReserveContributionRCCY: costField,
    mrContribution: costField,
    mrContributionCCY: costCurrencyField,
    mrContributionRCCY: costField,
    transitMaintenance: costField,
    transitMaintenanceCCY: costCurrencyField,
    transitMaintenanceRCCY: costField,
    otherMaintenance: costField,
    otherMaintenanceCCY: costCurrencyField,
    otherMaintenanceRCCY: costField,
    navigation: costField,
    navigationCCY: costCurrencyField,
    navigationRCCY: costField,
    airport: costField,
    airportCCY: costCurrencyField,
    airportRCCY: costField,
    otherDoc: costField,
    otherDocCCY: costCurrencyField,
    otherDocRCCY: costField,
    crewAllowances: costField,
    crewAllowancesCCY: costCurrencyField,
    crewAllowancesRCCY: costField,
    layoverCost: costField,
    layoverCostCCY: costCurrencyField,
    layoverCostRCCY: costField,
    crewPositioningCost: costField,
    crewPositioningCostCCY: costCurrencyField,
    crewPositioningCostRCCY: costField,
    apuFuelCost: costField,
    apuFuelConsumptionKg: costField,
    apuFuelKg: costField,
    apuFuelLitres: costField,
    apuFuelCostDirect: costField,
    apuFuelCostAllocated: costField,
    apuFuelCostCCY: costCurrencyField,
    apuFuelCostRCCY: costField,
    mrMonthly: costField,
    mrMonthlyCCY: costCurrencyField,
    mrMonthlyRCCY: costField,
    qualifyingSchMxEvents: costField,
    qualifyingSchMxEventsCCY: costCurrencyField,
    qualifyingSchMxEventsRCCY: costField,
    otherMxExpenses: costField,
    otherMxExpensesCCY: costCurrencyField,
    otherMxExpensesRCCY: costField,
    rotableChanges: costField,
    rotableChangesCCY: costCurrencyField,
    rotableChangesRCCY: costField,
    otherMaintenance1: costField, // Cost-per-BH subtotal for the flight
    otherMaintenance2: costField, // Cost-per-departure subtotal for the flight
    otherMaintenance3: costField, // Cost-per-month subtotal for the flight
    navEnr: costField,
    navTrml: costField,
    aptLandingCost: costField,
    aptHandlingCost: costField,
    aptAvsecCost: costField,
    aptOtherCost: costField,
    otherDoc1: costField,
    otherDoc2: costField,
    otherDoc3: costField,
    totalCost: costField,
    totalCostCCY: costCurrencyField,
    totalCostRCCY: costField,
    ftEffective: costField,
    mtowUsed: costField,

    domIntl: { type: String },
    msn: { type: String, default: "" },
    eng1Esn: { type: String, default: "" },
    eng2Esn: { type: String, default: "" },
    apun: { type: String, default: "" },
    userTag1: { type: String },
    userTag2: { type: String },
    remarks1: { type: String },
    remarks2: { type: String },
    sectorId: { type: String },
    networkId: { type: String },
    userId: { type: String },
    isComplete: { type: Boolean },
    rotationNumber: { type: String },
    beyondODs: { type: Boolean, default: false },
    behindODs: { type: Boolean, default: false },
    beyond1: { type: Number, default: null },
    beyond2: { type: Number, default: null },
    behind1: { type: Number, default: null },
    behind2: { type: Number, default: null },
    addedByRotation: { type: String },
    effFromDt: { type: Date },
    effToDt: { type: Date },
    dow: { type: String },
    aircraft: {
      msn: { type: Number, required: false },
      registration: { type: String, required: false }
    },
  }
);

for (const field of require("../utils/costFields").COST_COMPONENTS) {
  flightSchema.add({ [field]: costField, [`${field}CCY`]: costCurrencyField, [`${field}RCCY`]: costField });
}
flightSchema.index({ userId: 1, depStn: 1, arrStn: 1, domIntl: 1, std: 1, date: 1 });
flightSchema.index({ userId: 1, date: 1, flight: 1 });
flightSchema.index({ userId: 1, category: 1, date: 1, flight: 1 });
flightSchema.index({ userId: 1, isComplete: 1, variant: 1, date: 1, day: 1, std: 1, flight: 1, rotationNumber: 1 });
flightSchema.index({ networkId: 1 });
flightSchema.index(
  { userId: 1, networkId: 1, date: 1, flight: 1 },
  {
    unique: true,
    partialFilterExpression: {
      networkId: { $exists: true, $type: "string" },
    },
  }
);

// Only logical filter branches contain nested predicates for this document.
// Do not mistake a category key in a literal value for a category predicate.
function hasCategoryFilter(filter) {
  if (!filter || typeof filter !== "object") return false;
  if (Object.prototype.hasOwnProperty.call(filter, "category")) return true;
  return ["$and", "$or", "$nor"].some((operator) =>
    Array.isArray(filter[operator]) && filter[operator].some(hasCategoryFilter)
  );
}

function plannedFilter() {
  // Include legacy documents until the category backfill has run.
  return { category: { $nin: [CATEGORIES.ACTUAL, CATEGORIES.INVOICED] } };
}

flightSchema.pre(
  ["find", "findOne", "countDocuments", "distinct", "updateOne", "updateMany", "deleteOne", "deleteMany", "findOneAndUpdate"],
  function guardFlightCategory() {
    if (this.getOptions().allCategories === true || hasCategoryFilter(this.getFilter())) return;
    this.where(plannedFilter());
  }
);

flightSchema.pre("aggregate", function guardFlightAggregateCategory() {
  if (this.options.allCategories === true || hasCategoryFilter(this.pipeline()[0]?.$match)) return;
  this.pipeline().unshift({ $match: plannedFilter() });
});

module.exports = mongoose.model("FLIGHT", flightSchema);
