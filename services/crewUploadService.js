const xlsx = require("xlsx");
const moment = require("moment");
const Flight = require("../model/flight");
const {
  CrewFlightAssignment,
  CrewMember,
  CrewOtherDuty,
  CrewUploadBatch,
} = require("../model/crewSchemas");
const {
  combineDateAndClock,
  dateKey,
  diffMinutes,
  endAfterStartWithOvernight,
  getRowValue,
  normalizeKey,
  normalizeText,
  normalizeUpper,
  parseDateTime,
  parseDurationToMinutes,
  parseExcelDate,
  roundMoney,
} = require("./crewTimeUtils");

const READ_ROW_NUMBER_KEY = "__excelRowNumber";

const headerAliases = [
  "id",
  "date",
  "day",
  "crew id",
  "crew code",
  "name",
  "fc/cc",
  "role",
  "base",
  "dp allw",
  "fdp allw",
  "ft allw",
  "allowance ccy",
  "allowance currency",
  "flight #",
  "flight number",
  "dep stn",
  "arr stn",
  "sector",
  "captain",
  "fo",
  "cc1",
  "cc2",
  "cc3",
  "cc4",
  "location",
  "category",
  "sub-category",
  "start date",
  "start time",
  "duty time",
  "finish date",
  "finish time",
];

const headerAliasSet = new Set(headerAliases.map(normalizeKey));

const isBlankRow = (row = []) => row.every((cell) => !normalizeText(cell));

const scoreHeaderRow = (row = []) => row.reduce((score, cell) => (
  headerAliasSet.has(normalizeKey(cell)) ? score + 1 : score
), 0);

const makeUniqueHeader = (value, index, used) => {
  const base = normalizeText(value) || `__EMPTY_${index}`;
  const normalizedBase = normalizeKey(base);
  const count = used.get(normalizedBase) || 0;
  used.set(normalizedBase, count + 1);
  return count === 0 ? base : `${base}_${count}`;
};

const readRows = (filePath) => {
  const workbook = xlsx.readFile(filePath, { cellDates: true });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const matrix = xlsx.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: "", blankrows: false });
  if (matrix.length === 0) return [];

  const scoredRows = matrix
    .map((row, index) => ({ row, index, score: scoreHeaderRow(row) }))
    .filter(({ row }) => !isBlankRow(row));
  const bestHeader = scoredRows.reduce((best, candidate) => (
    candidate.score > best.score ? candidate : best
  ), { row: matrix[0] || [], index: 0, score: 0 });
  const headerIndex = bestHeader.score > 1 ? bestHeader.index : 0;
  const usedHeaders = new Map();
  const headers = (matrix[headerIndex] || []).map((value, index) => makeUniqueHeader(value, index, usedHeaders));

  return matrix.slice(headerIndex + 1).reduce((rows, row, offset) => {
    if (isBlankRow(row)) return rows;
    const item = {};
    headers.forEach((header, index) => {
      item[header] = row[index] ?? "";
    });
    Object.defineProperty(item, READ_ROW_NUMBER_KEY, {
      value: headerIndex + offset + 2,
      enumerable: false,
    });
    rows.push(item);
    return rows;
  }, []);
};

const cleanNumber = (value) => {
  if (value === "" || value === null || value === undefined) return 0;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
};

const rowError = (rowNumber, message, row = {}) => ({ rowNumber, message, row });

const getUploadRowNumber = (row, fallbackIndex) => Number(row?.[READ_ROW_NUMBER_KEY]) || fallbackIndex + 2;

const crewMemberColumnAliases = {
  crewCode: ["id", "crew id", "crew code", "crewid", "crew no", "crewno", "employee id", "emp id"],
  name: ["name", "crew name", "crewname", "employee name"],
  crewType: ["fc/cc", "fc cc", "fc or cc", "crew type", "crewtype", "crew category"],
  role: ["role", "rank", "position", "designation"],
  baseStation: ["base", "base station", "basestation", "home base"],
  dpAllowanceRate: ["dp allowance", "dp allowance rate", "dp allw", "dp allw rate", "dp allow", "dp"],
  fdpAllowanceRate: ["fdp allowance", "fdp allowance rate", "fdp allw", "fdp allw rate", "fdp allow", "fdp"],
  ftAllowanceRate: ["ft allowance", "ft allowance rate", "ft allw", "ft allw rate", "ft allow", "ft"],
  allowanceCurrency: [
    "allowance currency",
    "allowance ccy",
    "allowance currency code",
    "allowance cur",
    "allw ccy",
    "currency",
    "currency code",
    "ccy",
  ],
};

const normalizeCrewMemberUploadRow = (row) => ({
  crewCode: normalizeUpper(getRowValue(row, crewMemberColumnAliases.crewCode)),
  name: normalizeText(getRowValue(row, crewMemberColumnAliases.name)),
  crewType: normalizeUpper(getRowValue(row, crewMemberColumnAliases.crewType)),
  role: normalizeText(getRowValue(row, crewMemberColumnAliases.role)),
  baseStation: normalizeUpper(getRowValue(row, crewMemberColumnAliases.baseStation)),
  dpAllowanceRate: cleanNumber(getRowValue(row, crewMemberColumnAliases.dpAllowanceRate)),
  fdpAllowanceRate: cleanNumber(getRowValue(row, crewMemberColumnAliases.fdpAllowanceRate)),
  ftAllowanceRate: cleanNumber(getRowValue(row, crewMemberColumnAliases.ftAllowanceRate)),
  allowanceCurrency: normalizeUpper(getRowValue(row, crewMemberColumnAliases.allowanceCurrency)),
});

const buildBatch = async ({ userId, uploadType, fileName, uploadedBy }) => CrewUploadBatch.create({
  userId,
  uploadType,
  fileName,
  uploadedBy,
});

const finishBatch = async (batch, summary) => {
  Object.assign(batch, {
    rowsRead: summary.rowsRead,
    rowsInserted: summary.rowsInserted,
    rowsUpdated: summary.rowsUpdated,
    invalidRows: summary.invalidRows,
    warnings: summary.warnings,
    validationErrors: summary.errors,
  });
  await batch.save();
};

const defaultSummary = (batch) => ({
  batchId: batch?._id,
  rowsRead: 0,
  rowsInserted: 0,
  rowsUpdated: 0,
  invalidRows: 0,
  warnings: [],
  errors: [],
  unresolvedCrew: [],
  unresolvedFlights: [],
});

const shouldApplyReplacement = (rows, validRows, summary) => (
  rows.length === 0 || validRows.length > 0 || summary.invalidRows === 0
);

const importCrewMembers = async ({ userId, file, uploadedBy }) => {
  const batch = await buildBatch({
    userId,
    uploadType: "CREW_INFORMATION",
    fileName: file.originalname,
    uploadedBy,
  });
  const summary = defaultSummary(batch);
  const rows = readRows(file.path);
  summary.rowsRead = rows.length;
  const seenCrewCodes = new Set();
  const validMembers = [];

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const rowNumber = getUploadRowNumber(row, index);
    const {
      crewCode,
      name,
      crewType,
      role,
      baseStation,
      dpAllowanceRate,
      fdpAllowanceRate,
      ftAllowanceRate,
      allowanceCurrency,
    } = normalizeCrewMemberUploadRow(row);

    const errors = [];
    if (!crewCode) errors.push("Crew ID is required.");
    if (!name) errors.push("Crew name is required.");
    if (!role) errors.push("Role is required.");
    if (!baseStation) errors.push("Base station is required.");
    if (!allowanceCurrency) errors.push("Allowance currency is required.");
    if (dpAllowanceRate === null || dpAllowanceRate < 0) errors.push("DP allowance must be numeric and non-negative.");
    if (fdpAllowanceRate === null || fdpAllowanceRate < 0) errors.push("FDP allowance must be numeric and non-negative.");
    if (ftAllowanceRate === null || ftAllowanceRate < 0) errors.push("FT allowance must be numeric and non-negative.");
    if (crewCode && seenCrewCodes.has(crewCode)) errors.push("Duplicate Crew ID within this import.");

    if (errors.length > 0) {
      summary.invalidRows += 1;
      summary.errors.push(rowError(rowNumber, errors.join(" "), row));
      continue;
    }

    seenCrewCodes.add(crewCode);
    validMembers.push({
      crewCode,
      name,
      crewType,
      role,
      baseStation,
      dpAllowanceRate,
      fdpAllowanceRate,
      ftAllowanceRate,
      allowanceCurrency,
    });
  }

  if (!shouldApplyReplacement(rows, validMembers, summary)) {
    await finishBatch(batch, summary);
    return summary;
  }

  const validCrewCodes = validMembers.map((member) => member.crewCode);
  if (validCrewCodes.length > 0) {
    await Promise.all([
      CrewMember.deleteMany({ userId, crewCode: { $nin: validCrewCodes } }),
      CrewFlightAssignment.deleteMany({ userId, crewCode: { $nin: validCrewCodes } }),
      CrewOtherDuty.deleteMany({ userId, crewCode: { $nin: validCrewCodes } }),
    ]);
  } else {
    await Promise.all([
      CrewMember.deleteMany({ userId }),
      CrewFlightAssignment.deleteMany({ userId }),
      CrewOtherDuty.deleteMany({ userId }),
    ]);
  }

  for (const member of validMembers) {
    const existing = await CrewMember.findOne({ userId, crewCode: member.crewCode }).lean();
    await CrewMember.findOneAndUpdate(
      { userId, crewCode: member.crewCode },
      {
        $set: {
          name: member.name,
          crewType: member.crewType,
          role: member.role,
          baseStation: member.baseStation,
          dpAllowanceRate: member.dpAllowanceRate,
          fdpAllowanceRate: member.fdpAllowanceRate,
          ftAllowanceRate: member.ftAllowanceRate,
          allowanceCurrency: member.allowanceCurrency,
          uploadBatchId: batch._id,
        },
      },
      { upsert: true, new: true }
    );

    if (existing) summary.rowsUpdated += 1;
    else summary.rowsInserted += 1;
  }

  await finishBatch(batch, summary);
  return summary;
};

const assignmentColumns = [
  { role: "Captain", aliases: ["captain", "capt", "cpt"] },
  { role: "First Officer", aliases: ["fo", "first officer", "firstofficer", "f/o"] },
  { role: "Cabin Crew 1", aliases: ["cc1", "cabin crew 1", "cabincrew1"] },
  { role: "Cabin Crew 2", aliases: ["cc2", "cabin crew 2", "cabincrew2"] },
  { role: "Cabin Crew 3", aliases: ["cc3", "cabin crew 3", "cabincrew3"] },
  { role: "Cabin Crew 4", aliases: ["cc4", "cabin crew 4", "cabincrew4"] },
];

const otherDutyColumnAliases = {
  crewCode: ["crew id", "crewid", "crew code"],
  sourceRosterRowId: ["row id", "rowid", "s.no", "s no", "id"],
};

const findScheduleFlight = async ({ userId, date, flightNumber, departureStation, arrivalStation }) => {
  const start = moment.utc(date).startOf("day").toDate();
  const end = moment.utc(date).endOf("day").toDate();
  const flightRegex = new RegExp(`^${String(flightNumber).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i");
  const candidates = await Flight.find({
    userId,
    date: { $gte: start, $lte: end },
    flight: flightRegex,
  }).lean();

  if (candidates.length === 0) return null;

  return candidates.find((flight) => (
    (!departureStation || normalizeUpper(flight.depStn) === departureStation) &&
    (!arrivalStation || normalizeUpper(flight.arrStn) === arrivalStation)
  )) || candidates[0];
};

const buildFlightTimes = async ({ userId, date, flightNumber, departureStation, arrivalStation, row }) => {
  const scheduleFlight = await findScheduleFlight({ userId, date, flightNumber, departureStation, arrivalStation });
  const stdValue = scheduleFlight?.std || getRowValue(row, ["std", "std lt", "scheduled departure"]);
  const staValue = scheduleFlight?.sta || getRowValue(row, ["sta", "sta lt", "scheduled arrival"]);
  // The flight duty roster date is authoritative for crew diary placement; schedule STD/STA only provide the clock.
  const std = combineDateAndClock(date, stdValue);
  const rawSta = combineDateAndClock(date, staValue);
  const sta = endAfterStartWithOvernight(std, rawSta);

  return {
    scheduleFlight,
    std,
    sta,
    warning: scheduleFlight ? "" : "Flight not found in master schedule; using uploaded STD/STA.",
  };
};

const importFlightDuties = async ({ userId, file, uploadedBy }) => {
  const batch = await buildBatch({
    userId,
    uploadType: "FLIGHT_DUTY",
    fileName: file.originalname,
    uploadedBy,
  });
  const summary = defaultSummary(batch);
  const rows = readRows(file.path);
  summary.rowsRead = rows.length;
  const crewMembers = await CrewMember.find({ userId }).lean();
  const crewByCode = new Map(crewMembers.map((member) => [member.crewCode, member]));
  const validAssignments = [];

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const rowNumber = getUploadRowNumber(row, index);
    const rawDate = getRowValue(row, ["date", "flight date", "flightdate"]);
    const flightDate = parseExcelDate(rawDate);
    const flightNumber = normalizeUpper(getRowValue(row, ["flight number", "flight no", "flight #", "flight", "flightnumber"]));
    const departureStation = normalizeUpper(getRowValue(row, ["departure station", "dep stn", "dep", "from"]));
    const arrivalStation = normalizeUpper(getRowValue(row, ["arrival station", "arr stn", "arr", "to"]));
    const sector = normalizeUpper(getRowValue(row, ["sector"])) || [departureStation, arrivalStation].filter(Boolean).join("-");
    const sourceRosterRowId = normalizeText(getRowValue(row, ["row id", "rowid", "s.no", "s no", "id"])) || String(rowNumber - 1);

    const errors = [];
    if (!flightDate) errors.push("Date is required and must be valid.");
    if (!flightNumber) errors.push("Flight number is required.");
    if (!departureStation) errors.push("Departure station is required.");
    if (!arrivalStation) errors.push("Arrival station is required.");

    const crewAssignments = assignmentColumns
      .map((column) => ({
        role: column.role,
        crewCode: normalizeUpper(getRowValue(row, column.aliases)),
      }))
      .filter((assignment) => assignment.crewCode);

    if (crewAssignments.length === 0) {
      errors.push("At least one crew assignment column is required.");
    }

    if (errors.length > 0) {
      summary.invalidRows += 1;
      summary.errors.push(rowError(rowNumber, errors.join(" "), row));
      continue;
    }

    const { scheduleFlight, std, sta, warning } = await buildFlightTimes({
      userId,
      date: flightDate,
      flightNumber,
      departureStation,
      arrivalStation,
      row,
    });

    if (!std || !sta) {
      summary.invalidRows += 1;
      summary.unresolvedFlights.push({ rowNumber, flightNumber, date: dateKey(flightDate), sector });
      summary.errors.push(rowError(rowNumber, "Could not resolve valid STD/STA from schedule or upload.", row));
      continue;
    }

    if (warning) {
      summary.warnings.push({ rowNumber, message: warning, flightNumber, date: dateKey(flightDate), sector });
      summary.unresolvedFlights.push({ rowNumber, flightNumber, date: dateKey(flightDate), sector });
    }

    for (const assignment of crewAssignments) {
      const crew = crewByCode.get(assignment.crewCode);
      if (!crew) {
        summary.invalidRows += 1;
        summary.unresolvedCrew.push({ rowNumber, crewCode: assignment.crewCode, flightNumber });
        summary.errors.push(rowError(rowNumber, `Crew ID ${assignment.crewCode} has not been imported.`, row));
        continue;
      }

      const existing = await CrewFlightAssignment.findOne({
        userId,
        crewCode: crew.crewCode,
        flightDate,
        flightNumber,
        assignedRole: assignment.role,
      }).lean();

      validAssignments.push({
        crewCode: crew.crewCode,
        flightDate,
        flightNumber,
        assignedRole: assignment.role,
        flightId: scheduleFlight?._id || null,
        crewMemberId: crew._id,
        departureStation,
        arrivalStation,
        sector,
        std,
        sta,
        sourceRosterRowId,
        validationWarnings: warning ? [warning] : [],
      });

      if (existing) summary.rowsUpdated += 1;
      else summary.rowsInserted += 1;
    }
  }

  if (!shouldApplyReplacement(rows, validAssignments, summary)) {
    await finishBatch(batch, summary);
    return summary;
  }

  await CrewFlightAssignment.deleteMany({ userId });
  for (const assignment of validAssignments) {
    await CrewFlightAssignment.findOneAndUpdate(
      {
        userId,
        crewCode: assignment.crewCode,
        flightDate: assignment.flightDate,
        flightNumber: assignment.flightNumber,
        assignedRole: assignment.assignedRole,
      },
      {
        $set: {
          flightId: assignment.flightId,
          crewMemberId: assignment.crewMemberId,
          departureStation: assignment.departureStation,
          arrivalStation: assignment.arrivalStation,
          sector: assignment.sector,
          std: assignment.std,
          sta: assignment.sta,
          sourceRosterRowId: assignment.sourceRosterRowId,
          uploadBatchId: batch._id,
          validationWarnings: assignment.validationWarnings,
        },
      },
      { upsert: true, new: true }
    );
  }

  await finishBatch(batch, summary);
  return summary;
};

const parseOtherDutyTimes = (row) => {
  const startDateTimeValue = getRowValue(row, ["start datetime", "start date time", "start"]);
  const startDateTime = startDateTimeValue ? parseDateTime(startDateTimeValue) : null;
  let start = startDateTime;

  if (!start) {
    start = combineDateAndClock(
      getRowValue(row, ["date", "start date", "duty date"]),
      getRowValue(row, ["start time", "starttime"])
    );
  }

  const endDateTimeValue = getRowValue(row, ["finish datetime", "finish date time", "end datetime", "end"]);
  let end = endDateTimeValue ? parseDateTime(endDateTimeValue) : null;

  if (!end) {
    end = combineDateAndClock(
      getRowValue(row, ["finish date", "end date", "date", "start date", "duty date"]),
      getRowValue(row, ["finish time", "finishtime", "end time", "endtime"])
    );
  }

  const duration = parseDurationToMinutes(getRowValue(row, ["duty duration", "duration", "duty time"]));
  if (start && !end && duration !== null) {
    end = moment.utc(start).add(duration, "minutes").toDate();
  }

  return { start, end: endAfterStartWithOvernight(start, end) };
};

const isPositioningDuty = (category, subCategory) => (
  `${category} ${subCategory}`.toLowerCase().match(/position|deadhead|travel|return to base/) !== null
);

const importOtherDuties = async ({ userId, file, uploadedBy }) => {
  const batch = await buildBatch({
    userId,
    uploadType: "OTHER_DUTY",
    fileName: file.originalname,
    uploadedBy,
  });
  const summary = defaultSummary(batch);
  const rows = readRows(file.path);
  summary.rowsRead = rows.length;
  const crewMembers = await CrewMember.find({ userId }).lean();
  const crewByCode = new Map(crewMembers.map((member) => [member.crewCode, member]));
  const validDuties = [];

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const rowNumber = getUploadRowNumber(row, index);
    const crewCode = normalizeUpper(getRowValue(row, otherDutyColumnAliases.crewCode));
    const crew = crewByCode.get(crewCode);
    const { start, end } = parseOtherDutyTimes(row);
    const location = normalizeUpper(getRowValue(row, ["location", "stn", "station"]));
    const category = normalizeText(getRowValue(row, ["category", "duty category"]));
    const subCategory = normalizeText(getRowValue(row, ["sub-category", "subcategory", "sub category"]));
    const sourceRosterRowId = normalizeText(getRowValue(row, otherDutyColumnAliases.sourceRosterRowId)) || String(rowNumber - 1);
    const errors = [];

    if (!crewCode) errors.push("Crew ID is required.");
    if (crewCode && !crew) errors.push(`Crew ID ${crewCode} has not been imported.`);
    if (!start || !end) errors.push("Start and finish/duration must form a valid datetime range.");
    if (start && end && diffMinutes(start, end) <= 0) errors.push("Finish must be after start.");
    if (!location) errors.push("Location is required.");
    if (!category) errors.push("Category is required.");
    if (!subCategory) errors.push("Sub-category is required.");

    if (errors.length > 0) {
      summary.invalidRows += 1;
      if (crewCode && !crew) summary.unresolvedCrew.push({ rowNumber, crewCode });
      summary.errors.push(rowError(rowNumber, errors.join(" "), row));
      continue;
    }

    const overlaps = validDuties.filter((duty) => (
      String(duty.crewMemberId) === String(crew._id) &&
      new Date(duty.startDateTime) < end &&
      new Date(duty.endDateTime) > start
    ));
    const validationWarnings = overlaps.length > 0
      ? [`Overlaps ${overlaps.length} uploaded other duty row(s) for this crew member.`]
      : [];
    if (validationWarnings.length > 0) {
      summary.warnings.push({ rowNumber, crewCode, message: validationWarnings[0] });
    }

    const existing = await CrewOtherDuty.findOne({
      userId,
      crewCode,
      sourceRosterRowId,
      startDateTime: start,
    }).lean();

    validDuties.push({
      crewCode,
      sourceRosterRowId,
      startDateTime: start,
      crewMemberId: crew._id,
      endDateTime: end,
      location,
      category,
      subCategory,
      isUserEnteredPositioning: isPositioningDuty(category, subCategory),
      validationWarnings,
    });

    if (existing) summary.rowsUpdated += 1;
    else summary.rowsInserted += 1;
  }

  if (!shouldApplyReplacement(rows, validDuties, summary)) {
    await finishBatch(batch, summary);
    summary.rowsInserted = roundMoney(summary.rowsInserted);
    return summary;
  }

  await CrewOtherDuty.deleteMany({ userId });
  for (const duty of validDuties) {
    await CrewOtherDuty.findOneAndUpdate(
      {
        userId,
        crewCode: duty.crewCode,
        sourceRosterRowId: duty.sourceRosterRowId,
        startDateTime: duty.startDateTime,
      },
      {
        $set: {
          crewMemberId: duty.crewMemberId,
          endDateTime: duty.endDateTime,
          location: duty.location,
          category: duty.category,
          subCategory: duty.subCategory,
          isUserEnteredPositioning: duty.isUserEnteredPositioning,
          uploadBatchId: batch._id,
          validationWarnings: duty.validationWarnings,
        },
      },
      { upsert: true, new: true }
    );
  }

  await finishBatch(batch, summary);
  summary.rowsInserted = roundMoney(summary.rowsInserted);
  return summary;
};

module.exports = {
  importCrewMembers,
  importFlightDuties,
  importOtherDuties,
  readRows,
  __testables__: {
    buildFlightTimes,
    cleanNumber,
    crewMemberColumnAliases,
    isPositioningDuty,
    normalizeCrewMemberUploadRow,
    otherDutyColumnAliases,
    parseOtherDutyTimes,
  },
};
