const COST_COMPONENTS = [
  "otherMaintenance1", "otherMaintenance2", "otherMaintenance3", "navEnr", "navTrml",
  "aptLandingCost", "aptHandlingCost", "aptAvsecCost", "aptOtherCost", "otherDoc1", "otherDoc2", "otherDoc3",
  "otherMxExpenses1", "otherMxExpenses2", "otherMxExpenses3",
];
const COST_TOTALS = [
  "engineFuelCost", "apuFuelCost", "maintenanceReserveContribution", "mrContribution", "mrMonthly",
  "qualifyingSchMxEvents", "transitMaintenance", "otherMaintenance", "otherMxExpenses", "rotableChanges",
  "navigation", "airport", "otherDoc", "crewAllowances", "layoverCost", "crewPositioningCost", "totalCost",
];
const COST_PERSIST_FIELDS = [
  ...[...COST_TOTALS, ...COST_COMPONENTS].flatMap((field) => [field, `${field}CCY`, `${field}RCCY`]),
  "engineFuelConsumptionKg", "engineFuelConsumption", "engineFuelKg", "engineFuelLitres",
  "apuFuelConsumptionKg", "apuFuelKg", "apuFuelLitres", "apuFuelCostDirect", "apuFuelCostAllocated",
  "ftEffective", "mtowUsed", "msn", "eng1Esn", "eng2Esn", "apun", "sn", "snList", "mrDebug",
];
const MR_FIELDS = ["maintenanceReserveContribution", "mrContribution", "mrMonthly"].flatMap((field) => [field, `${field}CCY`, `${field}RCCY`]);
module.exports = { COST_COMPONENTS, COST_PERSIST_FIELDS, MR_FIELDS };
