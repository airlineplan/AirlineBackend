const moment = require("moment");
const {
  CrewCalculationRun,
  CrewDiaryEvent,
  CrewDutySettings,
  CrewFlightAssignment,
  CrewKpiSummary,
  CrewLayoverRule,
  CrewMember,
  CrewOtherDuty,
  CrewPositioningCostRule,
  CrewPositioningSettings,
  CrewUtilisationTarget,
} = require("../model/crewSchemas");
const {
  DEFAULT_DUTY_SETTINGS,
  DEFAULT_POSITIONING_SETTINGS,
  calculateKpiResponse,
  runCrewCalculation,
  validatePreconditions,
} = require("../services/crewCalculationService");
const {
  importCrewMembers,
  importFlightDuties,
  importOtherDuties,
} = require("../services/crewUploadService");
const {
  CLOCK_REGEX,
  normalizeText,
  normalizeUpper,
} = require("../services/crewTimeUtils");

const asArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean);
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
};

const sendError = (res, error, fallback = "Crew module request failed") => {
  const status = error.statusCode || error.status || 500;
  return res.status(status).json({
    success: false,
    message: error.message || fallback,
  });
};

const requireUserId = (req) => {
  const userId = req.user?.id;
  if (!userId) {
    const error = new Error("Unauthorized user context missing");
    error.statusCode = 401;
    throw error;
  }
  return userId;
};

const validateMinutesPayload = (payload, fields) => {
  const errors = [];
  fields.forEach((field) => {
    const value = Number(payload[field]);
    if (!Number.isFinite(value) || value < 0) {
      errors.push(`${field} must be a non-negative number of minutes.`);
    }
  });
  return errors;
};

const getDutySettings = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewDutySettings.findOneAndUpdate(
      { userId },
      { $setOnInsert: { ...DEFAULT_DUTY_SETTINGS, userId } },
      { new: true, upsert: true }
    ).lean();
    return res.status(200).json({ success: true, data });
  } catch (error) {
    return sendError(res, error, "Failed to load duty settings");
  }
};

const updateDutySettings = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const fields = [
      "restThresholdMinutes",
      "breakThresholdMinutes",
      "preflightNewFdpMinutes",
      "preflightExistingDutyMinutes",
      "postflightMinutes",
    ];
    const payload = fields.reduce((acc, field) => ({ ...acc, [field]: Number(req.body[field]) }), {});
    const errors = validateMinutesPayload(payload, fields);
    if (payload.restThresholdMinutes <= payload.breakThresholdMinutes) {
      errors.push("Rest threshold should be greater than break threshold.");
    }
    if (errors.length > 0) {
      return res.status(400).json({ success: false, message: errors.join(" "), errors });
    }
    const data = await CrewDutySettings.findOneAndUpdate(
      { userId },
      { $set: payload },
      { new: true, upsert: true }
    ).lean();
    return res.status(200).json({ success: true, data, message: "Duty settings saved." });
  } catch (error) {
    return sendError(res, error, "Failed to save duty settings");
  }
};

const getPositioningSettings = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewPositioningSettings.findOneAndUpdate(
      { userId },
      { $setOnInsert: { ...DEFAULT_POSITIONING_SETTINGS, userId } },
      { new: true, upsert: true }
    ).lean();
    return res.status(200).json({ success: true, data });
  } catch (error) {
    return sendError(res, error, "Failed to load positioning settings");
  }
};

const updatePositioningSettings = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const payload = {
      returnToBaseAfterFdpEnabled: Boolean(req.body.returnToBaseAfterFdpEnabled),
      hotacCutoffEnabled: Boolean(req.body.hotacCutoffEnabled),
      hotacCutoffLocalTime: normalizeText(req.body.hotacCutoffLocalTime || "20:00"),
      positioningWithinCurrentFdpEnabled: Boolean(req.body.positioningWithinCurrentFdpEnabled),
      defaultPositioningMinutes: Number(req.body.defaultPositioningMinutes),
      hotacToAirportTransferMinutes: Number(req.body.hotacToAirportTransferMinutes),
    };
    const errors = validateMinutesPayload(payload, ["defaultPositioningMinutes", "hotacToAirportTransferMinutes"]);
    if (payload.hotacCutoffEnabled && !CLOCK_REGEX.test(payload.hotacCutoffLocalTime)) {
      errors.push("HOTAC cutoff local time must be a valid HH:MM clock time.");
    }
    if (errors.length > 0) {
      return res.status(400).json({ success: false, message: errors.join(" "), errors });
    }
    const data = await CrewPositioningSettings.findOneAndUpdate(
      { userId },
      { $set: payload },
      { new: true, upsert: true }
    ).lean();
    return res.status(200).json({ success: true, data, message: "Positioning settings saved." });
  } catch (error) {
    return sendError(res, error, "Failed to save positioning settings");
  }
};

const validateTarget = (item) => {
  const errors = [];
  if (!normalizeText(item.role)) errors.push("Role is required.");
  ["averageDpMinutesPerDay", "averageFdpMinutesPerDay", "averageFtMinutesPerDay"].forEach((field) => {
    if (!Number.isFinite(Number(item[field])) || Number(item[field]) < 0) errors.push(`${field} must be non-negative minutes.`);
  });
  return errors;
};

const normalizeTarget = (item) => ({
  role: normalizeText(item.role || "ALL_ROLES"),
  averageDpMinutesPerDay: Number(item.averageDpMinutesPerDay || 0),
  averageFdpMinutesPerDay: Number(item.averageFdpMinutesPerDay || 0),
  averageFtMinutesPerDay: Number(item.averageFtMinutesPerDay || 0),
});

const listUtilisationTargets = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewUtilisationTarget.find({ userId }).sort({ role: 1 }).lean();
    return res.status(200).json({ success: true, data });
  } catch (error) {
    return sendError(res, error, "Failed to load utilisation targets");
  }
};

const bulkSaveUtilisationTargets = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const items = Array.isArray(req.body.items) ? req.body.items : [];
    const errors = items.flatMap((item, index) => validateTarget(item).map((message) => `Row ${index + 1}: ${message}`));
    if (errors.length > 0) return res.status(400).json({ success: false, message: errors.join(" "), errors });
    await CrewUtilisationTarget.deleteMany({ userId });
    const data = items.length > 0
      ? await CrewUtilisationTarget.insertMany(items.map((item) => ({ userId, ...normalizeTarget(item) })))
      : [];
    return res.status(200).json({ success: true, data, message: "Utilisation targets saved." });
  } catch (error) {
    return sendError(res, error, "Failed to save utilisation targets");
  }
};

const deleteUtilisationTarget = async (req, res) => {
  try {
    const userId = requireUserId(req);
    await CrewUtilisationTarget.deleteOne({ userId, _id: req.params.id });
    return res.status(200).json({ success: true, message: "Utilisation target deleted." });
  } catch (error) {
    return sendError(res, error, "Failed to delete utilisation target");
  }
};

const validateLayoverRule = (item) => {
  const errors = [];
  const ruleType = normalizeUpper(item.ruleType);
  if (!["CONVENIENCE", "HOTAC"].includes(ruleType)) errors.push("Rule type must be CONVENIENCE or HOTAC.");
  if (!normalizeText(item.station)) errors.push("Station is required.");
  if (!normalizeText(item.role)) errors.push("Role is required.");
  if (!Number.isFinite(Number(item.thresholdMinutes)) || Number(item.thresholdMinutes) < 0) errors.push("Threshold must be non-negative minutes.");
  if (!Number.isFinite(Number(item.costAmount)) || Number(item.costAmount) < 0) errors.push("Cost amount must be non-negative.");
  if (!normalizeText(item.currency)) errors.push("Currency is required.");
  return errors;
};

const normalizeLayoverRule = (item) => {
  const ruleType = normalizeUpper(item.ruleType);
  return {
    ruleType,
    station: normalizeUpper(item.station),
    role: normalizeText(item.role || "ALL_ROLES"),
    thresholdMinutes: Number(item.thresholdMinutes || 0),
    costAmount: Number(item.costAmount || 0),
    costBasis: ruleType === "HOTAC" ? "PER_24_HOURS" : "PER_HOUR",
    currency: normalizeUpper(item.currency || "INR"),
  };
};

const listLayoverRules = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewLayoverRule.find({ userId }).sort({ ruleType: 1, station: 1, role: 1 }).lean();
    return res.status(200).json({ success: true, data });
  } catch (error) {
    return sendError(res, error, "Failed to load layover rules");
  }
};

const bulkSaveLayoverRules = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const items = Array.isArray(req.body.items) ? req.body.items : [];
    const errors = items.flatMap((item, index) => validateLayoverRule(item).map((message) => `Row ${index + 1}: ${message}`));
    if (errors.length > 0) return res.status(400).json({ success: false, message: errors.join(" "), errors });
    await CrewLayoverRule.deleteMany({ userId });
    const data = items.length > 0
      ? await CrewLayoverRule.insertMany(items.map((item) => ({ userId, ...normalizeLayoverRule(item) })))
      : [];
    return res.status(200).json({ success: true, data, message: "Layover rules saved." });
  } catch (error) {
    return sendError(res, error, "Failed to save layover rules");
  }
};

const deleteLayoverRule = async (req, res) => {
  try {
    const userId = requireUserId(req);
    await CrewLayoverRule.deleteOne({ userId, _id: req.params.id });
    return res.status(200).json({ success: true, message: "Layover rule deleted." });
  } catch (error) {
    return sendError(res, error, "Failed to delete layover rule");
  }
};

const validatePositioningCostRule = (item) => {
  const errors = [];
  if (!normalizeText(item.departureStation)) errors.push("Departure station is required.");
  if (!normalizeText(item.arrivalStation)) errors.push("Arrival station is required.");
  if (!normalizeText(item.role)) errors.push("Role is required.");
  if (!Number.isFinite(Number(item.costAmount)) || Number(item.costAmount) < 0) errors.push("Cost amount must be non-negative.");
  if (!normalizeText(item.currency)) errors.push("Currency is required.");
  return errors;
};

const normalizePositioningCostRule = (item) => {
  const departureStation = normalizeUpper(item.departureStation);
  const arrivalStation = normalizeUpper(item.arrivalStation);
  return {
    departureStation,
    arrivalStation,
    sector: normalizeUpper(item.sector) || `${departureStation}-${arrivalStation}`,
    role: normalizeText(item.role || "ALL_ROLES"),
    costAmount: Number(item.costAmount || 0),
    currency: normalizeUpper(item.currency || "INR"),
    isOverride: item.isOverride !== false,
  };
};

const listPositioningCostRules = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewPositioningCostRule.find({ userId }).sort({ departureStation: 1, arrivalStation: 1, role: 1 }).lean();
    return res.status(200).json({ success: true, data });
  } catch (error) {
    return sendError(res, error, "Failed to load positioning cost rules");
  }
};

const bulkSavePositioningCostRules = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const items = Array.isArray(req.body.items) ? req.body.items : [];
    const errors = items.flatMap((item, index) => validatePositioningCostRule(item).map((message) => `Row ${index + 1}: ${message}`));
    if (errors.length > 0) return res.status(400).json({ success: false, message: errors.join(" "), errors });
    await CrewPositioningCostRule.deleteMany({ userId });
    const data = items.length > 0
      ? await CrewPositioningCostRule.insertMany(items.map((item) => ({ userId, ...normalizePositioningCostRule(item) })))
      : [];
    return res.status(200).json({ success: true, data, message: "Positioning costs saved." });
  } catch (error) {
    return sendError(res, error, "Failed to save positioning cost rules");
  }
};

const deletePositioningCostRule = async (req, res) => {
  try {
    const userId = requireUserId(req);
    await CrewPositioningCostRule.deleteOne({ userId, _id: req.params.id });
    return res.status(200).json({ success: true, message: "Positioning cost rule deleted." });
  } catch (error) {
    return sendError(res, error, "Failed to delete positioning cost rule");
  }
};

const sendUploadSummary = (res, summary, label) => {
  const invalidRows = Number(summary?.invalidRows || 0);
  const changedRows = Number(summary?.rowsInserted || 0) + Number(summary?.rowsUpdated || 0);

  if (invalidRows > 0 && changedRows === 0) {
    return res.status(422).json({
      success: false,
      data: summary,
      message: `${label} import failed. ${invalidRows} invalid row${invalidRows === 1 ? "" : "s"} found.`,
    });
  }

  if (invalidRows > 0) {
    return res.status(200).json({
      success: true,
      data: summary,
      message: `${label} imported with ${invalidRows} invalid row${invalidRows === 1 ? "" : "s"}.`,
    });
  }

  return res.status(200).json({ success: true, data: summary, message: `${label} import completed.` });
};

const uploadCrewInformation = async (req, res) => {
  try {
    const userId = requireUserId(req);
    if (!req.file) return res.status(400).json({ success: false, message: "No file uploaded." });
    const summary = await importCrewMembers({ userId, file: req.file, uploadedBy: req.user.email });
    return sendUploadSummary(res, summary, "Crew Information");
  } catch (error) {
    return sendError(res, error, "Failed to import Crew Information");
  }
};

const uploadFlightDuties = async (req, res) => {
  try {
    const userId = requireUserId(req);
    if (!req.file) return res.status(400).json({ success: false, message: "No file uploaded." });
    const summary = await importFlightDuties({ userId, file: req.file, uploadedBy: req.user.email });
    return sendUploadSummary(res, summary, "Flight Duty roster");
  } catch (error) {
    return sendError(res, error, "Failed to import Flight Duty roster");
  }
};

const uploadOtherDuties = async (req, res) => {
  try {
    const userId = requireUserId(req);
    if (!req.file) return res.status(400).json({ success: false, message: "No file uploaded." });
    const summary = await importOtherDuties({ userId, file: req.file, uploadedBy: req.user.email });
    return sendUploadSummary(res, summary, "Other Duty roster");
  } catch (error) {
    return sendError(res, error, "Failed to import Other Duty roster");
  }
};

const updatePlan = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const result = await runCrewCalculation({ userId, triggeredBy: req.user.email || req.user.id });
    return res.status(200).json({
      success: true,
      message: "Crew plan updated successfully.",
      data: {
        calculationRun: result.run,
        eventCount: result.eventCount,
        kpiSummaryCount: result.kpiSummaryCount,
        warnings: result.warnings,
      },
    });
  } catch (error) {
    return sendError(res, error, "Failed to update Crew plan");
  }
};

const getLatestRun = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const data = await CrewCalculationRun.findOne({ userId }).sort({ createdAt: -1 }).lean();
    const preconditions = await validatePreconditions({ userId });
    return res.status(200).json({ success: true, data, preconditions });
  } catch (error) {
    return sendError(res, error, "Failed to load calculation status");
  }
};

const getCrewDiary = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const page = Math.max(1, Number(req.query.page || 1));
    const requestedView = String(req.query.view || req.query.mode || "").toLowerCase();
    const maxLimit = requestedView === "summary" ? 5000 : 200;
    const limit = Math.min(maxLimit, Math.max(10, Number(req.query.limit || 50)));
    const latestRun = req.query.calculationRunId
      ? { _id: req.query.calculationRunId }
      : await CrewCalculationRun.findOne({ userId, status: "COMPLETED" }).sort({ createdAt: -1 }).select("_id").lean();
    if (!latestRun?._id) {
      return res.status(200).json({ success: true, data: [], pagination: { page, limit, total: 0 }, calculationRun: null });
    }

    const query = { userId, calculationRunId: latestRun._id };
    if (req.query.startDate || req.query.endDate) {
      const startDate = req.query.startDate ? moment.utc(req.query.startDate).startOf("day").toDate() : null;
      const endDate = req.query.endDate ? moment.utc(req.query.endDate).add(1, "day").startOf("day").toDate() : null;
      if (startDate && endDate) {
        query.startDateTime = { $lt: endDate };
        query.endDateTime = { $gt: startDate };
      } else if (startDate) {
        query.endDateTime = { $gt: startDate };
      } else if (endDate) {
        query.startDateTime = { $lt: endDate };
      }
    }
    if (req.query.crewCode) query.crewCode = new RegExp(normalizeText(req.query.crewCode), "i");
    if (req.query.crewName) query.crewName = new RegExp(normalizeText(req.query.crewName), "i");
    if (req.query.role) query.role = new RegExp(normalizeText(req.query.role), "i");
    if (req.query.category) query.category = new RegExp(normalizeText(req.query.category), "i");
    if (req.query.subCategory) query.subCategory = new RegExp(normalizeText(req.query.subCategory), "i");
    if (req.query.location) query.location = new RegExp(normalizeText(req.query.location), "i");

    const [data, total] = await Promise.all([
      CrewDiaryEvent.find(query)
        .sort({ startDateTime: 1, crewCode: 1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),
      CrewDiaryEvent.countDocuments(query),
    ]);

    return res.status(200).json({
      success: true,
      data,
      pagination: { page, limit, total, totalPages: Math.ceil(total / limit) },
      calculationRun: latestRun,
    });
  } catch (error) {
    return sendError(res, error, "Failed to load Crew Diary");
  }
};

const getCrewKpis = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const latestRun = req.query.calculationRunId
      ? { _id: req.query.calculationRunId }
      : await CrewCalculationRun.findOne({ userId, status: "COMPLETED" }).sort({ createdAt: -1 }).select("_id").lean();
    if (!latestRun?._id) {
      return res.status(200).json({ success: true, data: { periods: [], metrics: [] }, calculationRun: null });
    }

    const [events, targets] = await Promise.all([
      CrewDiaryEvent.find({ userId, calculationRunId: latestRun._id }).lean(),
      CrewUtilisationTarget.find({ userId }).lean(),
    ]);
    const data = calculateKpiResponse({
      events,
      targets,
      periodicity: req.query.periodicity || "MONTHLY",
      startDate: req.query.startDate,
      filters: {
        roles: asArray(req.query.roles),
        bases: asArray(req.query.bases),
        categories: asArray(req.query.categories),
        subCategories: asArray(req.query.subCategories),
      },
    });
    return res.status(200).json({ success: true, data, calculationRun: latestRun });
  } catch (error) {
    return sendError(res, error, "Failed to load Crew KPIs");
  }
};

const getCrewOptions = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const [members, diaryEvents] = await Promise.all([
      CrewMember.find({ userId }).select("role baseStation").lean(),
      CrewDiaryEvent.find({ userId }).select("category subCategory location").limit(5000).lean(),
    ]);
    const unique = (values) => Array.from(new Set(values.map(normalizeText).filter(Boolean))).sort();
    return res.status(200).json({
      success: true,
      data: {
        roles: unique(members.map((member) => member.role)),
        bases: unique(members.map((member) => member.baseStation)),
        categories: unique(diaryEvents.map((event) => event.category)),
        subCategories: unique(diaryEvents.map((event) => event.subCategory)),
        stations: unique([
          ...members.map((member) => member.baseStation),
          ...diaryEvents.map((event) => event.location),
        ]),
      },
    });
  } catch (error) {
    return sendError(res, error, "Failed to load Crew options");
  }
};

const getCrewBootstrap = async (req, res) => {
  try {
    const userId = requireUserId(req);
    const [
      dutySettings,
      positioningSettings,
      utilisationTargets,
      layoverRules,
      positioningCostRules,
      latestRun,
      preconditions,
      counts,
    ] = await Promise.all([
      CrewDutySettings.findOneAndUpdate(
        { userId },
        { $setOnInsert: { ...DEFAULT_DUTY_SETTINGS, userId } },
        { new: true, upsert: true }
      ).lean(),
      CrewPositioningSettings.findOneAndUpdate(
        { userId },
        { $setOnInsert: { ...DEFAULT_POSITIONING_SETTINGS, userId } },
        { new: true, upsert: true }
      ).lean(),
      CrewUtilisationTarget.find({ userId }).sort({ role: 1 }).lean(),
      CrewLayoverRule.find({ userId }).sort({ ruleType: 1, station: 1 }).lean(),
      CrewPositioningCostRule.find({ userId }).sort({ departureStation: 1 }).lean(),
      CrewCalculationRun.findOne({ userId }).sort({ createdAt: -1 }).lean(),
      validatePreconditions({ userId }),
      Promise.all([
        CrewMember.countDocuments({ userId }),
        CrewFlightAssignment.countDocuments({ userId }),
        CrewOtherDuty.countDocuments({ userId }),
        CrewDiaryEvent.countDocuments({ userId }),
        CrewKpiSummary.countDocuments({ userId }),
      ]),
    ]);

    return res.status(200).json({
      success: true,
      data: {
        dutySettings,
        positioningSettings,
        utilisationTargets,
        layoverRules,
        positioningCostRules,
        latestRun,
        preconditions,
        counts: {
          crewMembers: counts[0],
          flightAssignments: counts[1],
          otherDuties: counts[2],
          diaryEvents: counts[3],
          kpiSummaries: counts[4],
        },
      },
    });
  } catch (error) {
    return sendError(res, error, "Failed to load Crew module");
  }
};

module.exports = {
  bulkSaveLayoverRules,
  bulkSavePositioningCostRules,
  bulkSaveUtilisationTargets,
  deleteLayoverRule,
  deletePositioningCostRule,
  deleteUtilisationTarget,
  getCrewBootstrap,
  getCrewDiary,
  getCrewKpis,
  getCrewOptions,
  getDutySettings,
  getLatestRun,
  getPositioningSettings,
  listLayoverRules,
  listPositioningCostRules,
  listUtilisationTargets,
  updateDutySettings,
  updatePlan,
  updatePositioningSettings,
  uploadCrewInformation,
  uploadFlightDuties,
  uploadOtherDuties,
};
