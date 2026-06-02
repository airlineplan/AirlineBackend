const mongoose = require("mongoose");

const { Schema } = mongoose;

const commonUserField = {
  userId: { type: String, required: true, index: true },
};

const moneyField = {
  type: Number,
  default: 0,
  min: 0,
};

const stringField = {
  type: String,
  default: "",
  trim: true,
};

const CrewUploadBatchSchema = new Schema(
  {
    ...commonUserField,
    uploadType: {
      type: String,
      enum: ["CREW_INFORMATION", "FLIGHT_DUTY", "OTHER_DUTY"],
      required: true,
      index: true,
    },
    fileName: stringField,
    rowsRead: { type: Number, default: 0 },
    rowsInserted: { type: Number, default: 0 },
    rowsUpdated: { type: Number, default: 0 },
    invalidRows: { type: Number, default: 0 },
    warnings: { type: Array, default: [] },
    validationErrors: { type: Array, default: [] },
    uploadedBy: stringField,
  },
  { timestamps: true }
);

const CrewMemberSchema = new Schema(
  {
    ...commonUserField,
    crewCode: { type: String, required: true, trim: true, uppercase: true, index: true },
    name: { type: String, required: true, trim: true },
    crewType: { type: String, default: "", trim: true, uppercase: true },
    role: { type: String, required: true, trim: true, index: true },
    baseStation: { type: String, required: true, trim: true, uppercase: true, index: true },
    dpAllowanceRate: moneyField,
    fdpAllowanceRate: moneyField,
    ftAllowanceRate: moneyField,
    allowanceCurrency: { type: String, required: true, trim: true, uppercase: true, default: "INR" },
    uploadBatchId: { type: Schema.Types.ObjectId, ref: "CrewUploadBatch", default: null },
  },
  { timestamps: true }
);
CrewMemberSchema.index({ userId: 1, crewCode: 1 }, { unique: true });

const CrewFlightAssignmentSchema = new Schema(
  {
    ...commonUserField,
    flightId: { type: Schema.Types.ObjectId, ref: "FLIGHT", default: null, index: true },
    crewMemberId: { type: Schema.Types.ObjectId, ref: "CrewMember", required: true, index: true },
    crewCode: { type: String, required: true, trim: true, uppercase: true, index: true },
    assignedRole: { type: String, required: true, trim: true, index: true },
    flightDate: { type: Date, required: true, index: true },
    flightNumber: { type: String, required: true, trim: true, uppercase: true, index: true },
    departureStation: { type: String, required: true, trim: true, uppercase: true, index: true },
    arrivalStation: { type: String, required: true, trim: true, uppercase: true, index: true },
    sector: { type: String, default: "", trim: true, uppercase: true, index: true },
    std: { type: Date, required: true, index: true },
    sta: { type: Date, required: true, index: true },
    sourceRosterRowId: stringField,
    uploadBatchId: { type: Schema.Types.ObjectId, ref: "CrewUploadBatch", default: null, index: true },
    validationWarnings: { type: Array, default: [] },
  },
  { timestamps: true }
);
CrewFlightAssignmentSchema.index({ userId: 1, crewMemberId: 1, std: 1 });
CrewFlightAssignmentSchema.index(
  { userId: 1, crewCode: 1, flightDate: 1, flightNumber: 1, assignedRole: 1 },
  { unique: true }
);

const CrewOtherDutySchema = new Schema(
  {
    ...commonUserField,
    crewMemberId: { type: Schema.Types.ObjectId, ref: "CrewMember", required: true, index: true },
    crewCode: { type: String, required: true, trim: true, uppercase: true, index: true },
    startDateTime: { type: Date, required: true, index: true },
    endDateTime: { type: Date, required: true, index: true },
    location: { type: String, required: true, trim: true, uppercase: true, index: true },
    category: { type: String, required: true, trim: true, index: true },
    subCategory: { type: String, default: "", trim: true, index: true },
    sourceRosterRowId: stringField,
    isUserEnteredPositioning: { type: Boolean, default: false, index: true },
    uploadBatchId: { type: Schema.Types.ObjectId, ref: "CrewUploadBatch", default: null, index: true },
    validationWarnings: { type: Array, default: [] },
  },
  { timestamps: true }
);
CrewOtherDutySchema.index({ userId: 1, crewMemberId: 1, startDateTime: 1 });

const CrewDutySettingsSchema = new Schema(
  {
    ...commonUserField,
    restThresholdMinutes: { type: Number, required: true, default: 420, min: 0 },
    breakThresholdMinutes: { type: Number, required: true, default: 180, min: 0 },
    preflightNewFdpMinutes: { type: Number, required: true, default: 90, min: 0 },
    preflightExistingDutyMinutes: { type: Number, required: true, default: 45, min: 0 },
    postflightMinutes: { type: Number, required: true, default: 30, min: 0 },
  },
  { timestamps: true }
);
CrewDutySettingsSchema.index({ userId: 1 }, { unique: true });

const CrewPositioningSettingsSchema = new Schema(
  {
    ...commonUserField,
    returnToBaseAfterFdpEnabled: { type: Boolean, default: true },
    hotacCutoffEnabled: { type: Boolean, default: false },
    hotacCutoffLocalTime: { type: String, default: "20:00" },
    positioningWithinCurrentFdpEnabled: { type: Boolean, default: true },
    defaultPositioningMinutes: { type: Number, default: 150, min: 0 },
    hotacToAirportTransferMinutes: { type: Number, default: 60, min: 0 },
  },
  { timestamps: true }
);
CrewPositioningSettingsSchema.index({ userId: 1 }, { unique: true });

const CrewUtilisationTargetSchema = new Schema(
  {
    ...commonUserField,
    role: { type: String, required: true, trim: true, default: "ALL_ROLES", index: true },
    averageDpMinutesPerDay: { type: Number, default: 0, min: 0 },
    averageFdpMinutesPerDay: { type: Number, default: 0, min: 0 },
    averageFtMinutesPerDay: { type: Number, default: 0, min: 0 },
  },
  { timestamps: true }
);
CrewUtilisationTargetSchema.index({ userId: 1, role: 1 }, { unique: true });

const CrewLayoverRuleSchema = new Schema(
  {
    ...commonUserField,
    ruleType: {
      type: String,
      enum: ["CONVENIENCE", "HOTAC"],
      required: true,
      index: true,
    },
    station: { type: String, required: true, trim: true, uppercase: true, index: true },
    role: { type: String, required: true, trim: true, default: "ALL_ROLES", index: true },
    thresholdMinutes: { type: Number, required: true, min: 0 },
    costAmount: moneyField,
    costBasis: {
      type: String,
      enum: ["PER_HOUR", "PER_24_HOURS"],
      required: true,
    },
    currency: { type: String, required: true, trim: true, uppercase: true, default: "INR" },
  },
  { timestamps: true }
);
CrewLayoverRuleSchema.index({ userId: 1, ruleType: 1, station: 1, role: 1 }, { unique: true });

const CrewPositioningCostRuleSchema = new Schema(
  {
    ...commonUserField,
    departureStation: { type: String, required: true, trim: true, uppercase: true, index: true },
    arrivalStation: { type: String, required: true, trim: true, uppercase: true, index: true },
    sector: { type: String, default: "", trim: true, uppercase: true, index: true },
    role: { type: String, required: true, trim: true, default: "ALL_ROLES", index: true },
    costAmount: moneyField,
    currency: { type: String, required: true, trim: true, uppercase: true, default: "INR" },
    isOverride: { type: Boolean, default: true },
  },
  { timestamps: true }
);
CrewPositioningCostRuleSchema.index(
  { userId: 1, departureStation: 1, arrivalStation: 1, role: 1 },
  { unique: true }
);

const CrewCalculationRunSchema = new Schema(
  {
    ...commonUserField,
    status: {
      type: String,
      enum: ["PENDING", "RUNNING", "COMPLETED", "FAILED"],
      default: "PENDING",
      index: true,
    },
    startedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    triggeredBy: stringField,
    settingsSnapshot: { type: Object, default: {} },
    sourceUploadBatchIds: [{ type: Schema.Types.ObjectId, ref: "CrewUploadBatch" }],
    validationWarnings: { type: Array, default: [] },
    errorMessage: stringField,
  },
  { timestamps: true }
);
CrewCalculationRunSchema.index({ userId: 1, createdAt: -1 });

const CrewDiaryEventSchema = new Schema(
  {
    ...commonUserField,
    calculationRunId: { type: Schema.Types.ObjectId, ref: "CrewCalculationRun", required: true, index: true },
    crewMemberId: { type: Schema.Types.ObjectId, ref: "CrewMember", required: true, index: true },
    crewCode: { type: String, required: true, trim: true, uppercase: true, index: true },
    crewName: { type: String, required: true, trim: true },
    role: { type: String, required: true, trim: true, index: true },
    baseStation: { type: String, default: "", trim: true, uppercase: true, index: true },
    startDateTime: { type: Date, required: true, index: true },
    endDateTime: { type: Date, required: true, index: true },
    displayDate: { type: String, default: "", index: true },
    location: { type: String, default: "", trim: true, uppercase: true, index: true },
    departureStation: { type: String, default: "", trim: true, uppercase: true, index: true },
    arrivalStation: { type: String, default: "", trim: true, uppercase: true, index: true },
    flightNumber: { type: String, default: "", trim: true, uppercase: true, index: true },
    category: { type: String, required: true, trim: true, index: true },
    subCategory: { type: String, default: "", trim: true, index: true },
    dpMinutes: { type: Number, default: 0 },
    fdpMinutes: { type: Number, default: 0 },
    ftMinutes: { type: Number, default: 0 },
    rpMinutes: { type: Number, default: 0 },
    dpCost: moneyField,
    fdpCost: moneyField,
    ftCost: moneyField,
    layoverCost: moneyField,
    positioningCost: moneyField,
    currency: { type: String, default: "INR", trim: true, uppercase: true },
    isGenerated: { type: Boolean, default: true },
    sourceType: {
      type: String,
      enum: [
        "FLIGHT_ROSTER",
        "OTHER_DUTY_ROSTER",
        "SYSTEM_PRE_FLIGHT",
        "SYSTEM_POST_FLIGHT",
        "SYSTEM_BREAK",
        "SYSTEM_REST",
        "SYSTEM_POSITIONING",
        "SYSTEM_LAYOVER",
        "SYSTEM_CONTINUING_DUTY",
      ],
      required: true,
      index: true,
    },
    sourceId: { type: Schema.Types.ObjectId, default: null },
    reasonText: stringField,
  },
  { timestamps: true }
);
CrewDiaryEventSchema.index({ userId: 1, calculationRunId: 1, crewMemberId: 1, startDateTime: 1 });
CrewDiaryEventSchema.index({ userId: 1, role: 1, baseStation: 1, category: 1, subCategory: 1 });

const CrewKpiSummarySchema = new Schema(
  {
    ...commonUserField,
    calculationRunId: { type: Schema.Types.ObjectId, ref: "CrewCalculationRun", required: true, index: true },
    crewMemberId: { type: Schema.Types.ObjectId, ref: "CrewMember", default: null, index: true },
    groupingKey: { type: String, default: "", index: true },
    periodStart: { type: Date, required: true, index: true },
    periodEnd: { type: Date, required: true, index: true },
    periodicity: { type: String, default: "MONTHLY", index: true },
    role: { type: String, default: "", trim: true, index: true },
    baseStation: { type: String, default: "", trim: true, uppercase: true, index: true },
    category: { type: String, default: "", trim: true, index: true },
    subCategory: { type: String, default: "", trim: true, index: true },
    totalDpMinutes: { type: Number, default: 0 },
    totalFdpMinutes: { type: Number, default: 0 },
    totalFtMinutes: { type: Number, default: 0 },
    totalRpMinutes: { type: Number, default: 0 },
    totalLandings: { type: Number, default: 0 },
    positioningCount: { type: Number, default: 0 },
    layoverOccurrences: { type: Number, default: 0 },
    layoverDurationMinutes: { type: Number, default: 0 },
    dpUtilisationPercent: { type: Number, default: null },
    fdpUtilisationPercent: { type: Number, default: null },
    ftUtilisationPercent: { type: Number, default: null },
    positioningTotalCost: moneyField,
    positioningAverageCost: moneyField,
    convenienceTotalCost: moneyField,
    convenienceAverageCost: moneyField,
    hotacTotalCost: moneyField,
    hotacAverageCost: moneyField,
    currency: { type: String, default: "", trim: true, uppercase: true },
  },
  { timestamps: true }
);
CrewKpiSummarySchema.index({ userId: 1, calculationRunId: 1, periodStart: 1 });

const model = (name, schema) => mongoose.models[name] || mongoose.model(name, schema);

module.exports = {
  CrewUploadBatch: model("CrewUploadBatch", CrewUploadBatchSchema),
  CrewMember: model("CrewMember", CrewMemberSchema),
  CrewFlightAssignment: model("CrewFlightAssignment", CrewFlightAssignmentSchema),
  CrewOtherDuty: model("CrewOtherDuty", CrewOtherDutySchema),
  CrewDutySettings: model("CrewDutySettings", CrewDutySettingsSchema),
  CrewPositioningSettings: model("CrewPositioningSettings", CrewPositioningSettingsSchema),
  CrewUtilisationTarget: model("CrewUtilisationTarget", CrewUtilisationTargetSchema),
  CrewLayoverRule: model("CrewLayoverRule", CrewLayoverRuleSchema),
  CrewPositioningCostRule: model("CrewPositioningCostRule", CrewPositioningCostRuleSchema),
  CrewCalculationRun: model("CrewCalculationRun", CrewCalculationRunSchema),
  CrewDiaryEvent: model("CrewDiaryEvent", CrewDiaryEventSchema),
  CrewKpiSummary: model("CrewKpiSummary", CrewKpiSummarySchema),
};
