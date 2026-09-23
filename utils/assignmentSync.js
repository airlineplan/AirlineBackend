const Assignment = require("../model/assignment");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const GroundDay = require("../model/groundDay");
const Station = require("../model/stationSchema");
const User = require("../model/userSchema");
const moment = require("moment");

const escapeRegExp = (value) => String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const normalizeSnForCompare = (value) => {
  if (value === null || value === undefined) return "";
  const raw = String(value).trim().toUpperCase();
  if (!raw) return "";
  const digitsOnly = raw.replace(/\D/g, "");
  return digitsOnly || raw;
};

const normalizeVariantForCompare = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).trim().toUpperCase();
};

const normalizeVariantBase = (value) => {
  const normalized = normalizeVariantForCompare(value);
  if (!normalized) return "";
  return normalized.split("-")[0].trim();
};

const variantsMatch = (flightVariant, fleetVariant) => {
  const flightBase = normalizeVariantBase(flightVariant);
  const fleetBase = normalizeVariantBase(fleetVariant);

  if (!flightBase || !fleetBase) return false;
  if (flightBase === fleetBase) return true;

  return normalizeVariantForCompare(flightVariant) === normalizeVariantForCompare(fleetVariant);
};

const parseTimeToMinutes = (value) => {
  if (value === null || value === undefined) return null;

  if (typeof value === "number" && Number.isFinite(value)) {
    const totalMinutes = Math.round(value * 24 * 60);
    return ((totalMinutes % (24 * 60)) + (24 * 60)) % (24 * 60);
  }

  const text = String(value).trim();
  if (!text) return null;

  const formats = ["HH:mm", "H:mm", "HH:mm:ss", "H:mm:ss", "h:mm A", "h:mmA", "HHmm"];
  const parsed = moment.utc(text, formats, true);
  if (!parsed.isValid()) return null;
  return parsed.hours() * 60 + parsed.minutes();
};

const parseUtcOffsetToMinutes = (value) => {
  if (typeof value !== "string") return null;
  const match = value.trim().match(/^UTC\s*([+-])\s*(\d{1,2})(?::([0-5]\d))?$/i);
  if (!match) return null;

  const hours = Number(match[2]);
  const minutes = Number(match[3] || 0);
  if (!Number.isInteger(hours) || hours > 14 || (hours === 14 && minutes !== 0)) return null;

  const total = (hours * 60) + minutes;
  return match[1] === "-" ? -total : total;
};

const parseBusinessDateKey = (value) => {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return moment.utc(value).format("YYYY-MM-DD");
  }

  const text = String(value).trim();
  if (!text) return null;
  const isoDatePrefix = text.match(/^(\d{4}-\d{2}-\d{2})/);
  if (isoDatePrefix && moment.utc(isoDatePrefix[1], "YYYY-MM-DD", true).isValid()) {
    return isoDatePrefix[1];
  }

  const parsed = moment.utc(text, [
    "YYYY-MM-DD",
    "DD-MM-YYYY",
    "D-MM-YYYY",
    "DD/MM/YYYY",
    "D/MM/YYYY",
    "DD-MMM-YYYY",
    "D-MMM-YYYY",
  ], true);
  return parsed.isValid() ? parsed.format("YYYY-MM-DD") : null;
};

const addDaysToDateKey = (dateKey, days) => (
  moment.utc(dateKey, "YYYY-MM-DD", true).add(days, "days").format("YYYY-MM-DD")
);

const getEffectiveStationOffsetForDate = (station, localDateKey) => {
  if (!station || !localDateKey) return null;

  let offsetText = station.stdtz;
  const hasStart = Boolean(String(station.nextDSTStart || "").trim());
  const hasEnd = Boolean(String(station.nextDSTEnd || "").trim());

  if (hasStart !== hasEnd) return null;
  if (hasStart && hasEnd) {
    const startKey = parseBusinessDateKey(station.nextDSTStart);
    const endKey = parseBusinessDateKey(station.nextDSTEnd);
    if (!startKey || !endKey) return null;

    const isInDst = startKey <= endKey
      ? localDateKey >= startKey && localDateKey <= endKey
      : localDateKey >= startKey || localDateKey <= endKey;
    if (isInDst) offsetText = station.dsttz || station.stdtz;
  }

  return parseUtcOffsetToMinutes(offsetText);
};

const buildStationLocalInstant = ({ localDateKey, localTime, station }) => {
  const timeMinutes = parseTimeToMinutes(localTime);
  const offsetMinutes = getEffectiveStationOffsetForDate(station, localDateKey);
  if (timeMinutes === null || offsetMinutes === null) return null;

  const localMidnightMs = moment.utc(localDateKey, "YYYY-MM-DD", true).valueOf();
  if (!Number.isFinite(localMidnightMs)) return null;

  return {
    instantUtcMs: localMidnightMs + (timeMinutes - offsetMinutes) * 60 * 1000,
    localDateKey,
    localTimeMinutes: timeMinutes,
    effectiveOffsetMinutes: offsetMinutes,
  };
};

const buildFlightInterval = ({
  flightRecord,
  assignDate,
  departureStation,
  arrivalStation,
  homeTimezone,
}) => {
  const sourceDateKey = parseBusinessDateKey(assignDate);
  const homeOffsetMinutes = parseUtcOffsetToMinutes(homeTimezone);
  if (!flightRecord || !sourceDateKey || homeOffsetMinutes === null) return null;

  const departure = buildStationLocalInstant({
    localDateKey: sourceDateKey,
    localTime: flightRecord.std,
    station: departureStation,
  });
  if (!departure) return null;

  let arrivalDateKey = sourceDateKey;
  let arrival = null;
  for (let dayOffset = 0; dayOffset <= 2; dayOffset += 1) {
    arrivalDateKey = addDaysToDateKey(sourceDateKey, dayOffset);
    arrival = buildStationLocalInstant({
      localDateKey: arrivalDateKey,
      localTime: flightRecord.sta,
      station: arrivalStation,
    });
    if (!arrival) return null;
    if (arrival.instantUtcMs > departure.instantUtcMs) break;
  }
  if (!arrival || arrival.instantUtcMs <= departure.instantUtcMs) return null;

  const homeOffsetMs = homeOffsetMinutes * 60 * 1000;
  const stdHomeDateTime = moment.utc(departure.instantUtcMs + homeOffsetMs);
  const staHomeDateTime = moment.utc(arrival.instantUtcMs + homeOffsetMs);

  return {
    start: moment.utc(departure.instantUtcMs),
    end: moment.utc(arrival.instantUtcMs),
    departureInstantUtc: new Date(departure.instantUtcMs),
    arrivalInstantUtc: new Date(arrival.instantUtcMs),
    stdHomeDateTime,
    staHomeDateTime,
    stdHomeDateKey: stdHomeDateTime.format("YYYY-MM-DD"),
    staHomeDateKey: staHomeDateTime.format("YYYY-MM-DD"),
    departureEffectiveOffset: departure.effectiveOffsetMinutes,
    arrivalEffectiveOffset: arrival.effectiveOffsetMinutes,
    homeTimezoneOffset: homeOffsetMinutes,
    arrivalLocalDateKey: arrivalDateKey,
  };
};

const intervalsOverlap = (a, b) => {
  if (!a || !b) return false;
  return a.start.isBefore(b.end) && b.start.isBefore(a.end);
};

const parseLegNumberFromFlight = (flightRecord) => {
  if (!flightRecord) return null;

  if (flightRecord.legNumber !== undefined && flightRecord.legNumber !== null) {
    const leg = parseInt(String(flightRecord.legNumber).replace(/\D/g, ""), 10);
    if (!Number.isNaN(leg)) return leg;
  }

  if (flightRecord.addedByRotation) {
    const addedBy = String(flightRecord.addedByRotation);
    const parts = addedBy.split("-");
    if (parts.length >= 2) {
      const leg = parseInt(String(parts[1]).replace(/\D/g, ""), 10);
      if (!Number.isNaN(leg)) return leg;
    }
  }

  return null;
};

const pickFleetRecordForDate = (fleetRecordsForRegn, assignDate) => {
  if (!Array.isArray(fleetRecordsForRegn) || fleetRecordsForRegn.length === 0) {
    return null;
  }

  const assignMom = moment.utc(assignDate).startOf("day");
  for (const record of fleetRecordsForRegn) {
    const entryMom = record.entry ? moment.utc(record.entry).startOf("day") : null;
    const exitMom = record.exit ? moment.utc(record.exit).endOf("day") : null;
    const isBeforeEntry = entryMom && assignMom.isBefore(entryMom);
    const isAfterExit = exitMom && assignMom.isAfter(exitMom);
    if (!isBeforeEntry && !isAfterExit) return record;
  }

  return fleetRecordsForRegn[0];
};

const buildRejectionSummary = (row, errors) => {
  const rejection = {
    date: row?.dateKey || null,
    flight: row?.flight || null,
    acft: row?.acft || null,
    errors: Array.isArray(errors) ? errors.slice(0, 3) : [],
  };

  return rejection;
};

const normalizeAssignmentRow = (row) => {
  if (!row) return null;

  const assignDate = row.assignDate instanceof Date ? row.assignDate : new Date(row.assignDate);
  if (!assignDate || Number.isNaN(assignDate.getTime())) return null;

  const flight = String(row.flight || "").trim().toUpperCase();
  if (!flight) return null;

  const dateKey = row.dateKey || moment.utc(assignDate).format("YYYY-MM-DD");
  const acftText = row.acft === null || row.acft === undefined ? "" : String(row.acft).trim().toUpperCase();

  return {
    assignDate,
    dateKey,
    flight,
    acft: acftText || null,
    sourceOrder: Number.isInteger(row.sourceOrder) ? row.sourceOrder : undefined,
    sourceUploadedAt: row.sourceUploadedAt ? new Date(row.sourceUploadedAt) : undefined,
    sourceCreatedAt: row.sourceCreatedAt ? new Date(row.sourceCreatedAt) : undefined,
    sourceId: row.sourceId ? String(row.sourceId) : undefined,
  };
};

const buildDateFlightKey = (dateKey, flight) => (
  `${dateKey}_${String(flight || "").trim().toUpperCase()}`
);

const buildEmptyDiagnostics = (duplicateComboCount = 0) => ({
  totalRows: 0,
  uniqueRows: 0,
  duplicateComboCount,
  successfullyAssigned: 0,
  appliedCount: 0,
  discardedCount: 0,
  discardedDateCount: 0,
  revalidatedCount: 0,
  rejections: {
    missingFromFleetDB: 0,
    preEntryDates: 0,
    postExitDates: 0,
    variantMismatches: 0,
    groundConflicts: 0,
    acftOverlaps: 0,
    flightNotFound: 0,
    outsideMasterDateRange: 0,
    invalidTimezoneData: 0,
  },
  rejectedRows: [],
  discardedRows: [],
});

const buildValidationContext = async ({ userId, rows, includeGroundDays = true }) => {
  const normalizedRows = [];
  const seenRowKeys = new Set();
  const flightSet = new Set();
  const dateSet = new Set();
  const acftSet = new Set();
  let duplicateComboCount = 0;

  let inputIndex = 0;
  for (const row of rows || []) {
    const normalized = normalizeAssignmentRow(row);
    if (!normalized) {
      inputIndex += 1;
      continue;
    }

    const rowKey = buildDateFlightKey(normalized.dateKey, normalized.flight);
    if (seenRowKeys.has(rowKey)) {
      duplicateComboCount++;
      inputIndex += 1;
      continue;
    }

    seenRowKeys.add(rowKey);
    normalized.sequence = inputIndex;
    if (!Number.isInteger(normalized.sourceOrder)) normalized.sourceOrder = inputIndex;
    inputIndex += 1;
    normalizedRows.push(normalized);
    flightSet.add(normalized.flight);
    dateSet.add(normalized.assignDate.getTime());
    if (normalized.acft) acftSet.add(normalized.acft);
  }

  if (normalizedRows.length === 0) {
    return {
      normalizedRows,
      duplicateComboCount,
      flights: [],
      fleetData: [],
      groundDays: [],
      stations: [],
      user: null,
      masterDateRange: null,
    };
  }

  const minDate = new Date(Math.min(...dateSet));
  const maxDate = new Date(Math.max(...dateSet));
  const flightRegexArray = [...flightSet].map((flight) => new RegExp(`^${escapeRegExp(flight)}$`, "i"));
  const acftRegexArray = [...acftSet].map((acft) => new RegExp(`^${escapeRegExp(acft)}$`, "i"));

  const [flights, fleetData, groundDays, stations, user, masterDateRangeRows] = await Promise.all([
    Flight.find({
      userId,
      date: { $gte: minDate, $lte: maxDate },
      flight: { $in: flightRegexArray },
    })
      .select("_id flight date depStn arrStn std sta variant rotationNumber addedByRotation legNumber")
      .lean(),
    Fleet.find({
      userId,
      category: "Aircraft",
      regn: { $in: acftRegexArray },
    })
      .select("sn regn entry exit variant")
      .lean(),
    includeGroundDays
      ? GroundDay.find({
        userId,
        date: { $gte: minDate, $lte: maxDate },
      })
        .select("msn date event")
        .lean()
      : Promise.resolve([]),
    Station.find({ userId })
      .select("stationName stdtz dsttz nextDSTStart nextDSTEnd")
      .lean(),
    User.findById(userId)
      .select("hometimeZone")
      .lean(),
    Flight.aggregate([
      { $match: { userId: String(userId) } },
      { $group: { _id: null, minDate: { $min: "$date" }, maxDate: { $max: "$date" } } },
    ]),
  ]);

  return {
    normalizedRows,
    duplicateComboCount,
    flights,
    fleetData,
    groundDays,
    stations,
    user,
    masterDateRange: masterDateRangeRows[0] || null,
  };
};

const buildAssignmentSyncPlan = async ({
  userId,
  rows,
  priorityKeys = [],
  includeGroundDays = true,
  allowCreate = true,
}) => {
  const {
    normalizedRows,
    duplicateComboCount,
    flights,
    fleetData,
    groundDays,
    stations,
    user,
    masterDateRange,
  } = await buildValidationContext({
    userId,
    rows,
    includeGroundDays
  });

  if (normalizedRows.length === 0) {
    return {
      assignmentBulkOps: [],
      flightBulkOps: [],
      diagnostics: buildEmptyDiagnostics(duplicateComboCount),
    };
  }

  const flightMap = new Map();
  for (const flight of flights) {
    if (!flight.flight) continue;
    const key = buildDateFlightKey(moment.utc(flight.date).format("YYYY-MM-DD"), flight.flight);
    flightMap.set(key, flight);
  }

  const fleetMap = new Map();
  for (const asset of fleetData) {
    if (!asset.regn) continue;
    const regnKey = String(asset.regn).trim().toUpperCase();
    if (!fleetMap.has(regnKey)) fleetMap.set(regnKey, []);
    fleetMap.get(regnKey).push(asset);
  }

  const groundDayMap = new Map();
  for (const gd of groundDays) {
    if (!gd.msn) continue;
    const normalizedSn = normalizeSnForCompare(gd.msn);
    if (!normalizedSn) continue;
    const key = `${moment.utc(gd.date).format("YYYY-MM-DD")}_${normalizedSn}`;
    groundDayMap.set(key, gd);
  }

  const stationMap = new Map();
  for (const station of stations) {
    const stationKey = String(station.stationName || "").trim().toUpperCase();
    if (stationKey) stationMap.set(stationKey, station);
  }

  const masterMinDateKey = parseBusinessDateKey(masterDateRange?.minDate);
  const masterMaxDateKey = parseBusinessDateKey(masterDateRange?.maxDate);
  const homeTimezone = user?.hometimeZone;

  const processedRowsByFlightKey = new Map();
  const acceptedIntervalsByAcft = new Map();
  const rejectedRows = [];
  const discardedDateKeys = new Set();

  let notFoundCount = 0;
  let missingFleetDBCount = 0;
  let preEntryCount = 0;
  let postExitCount = 0;
  let groundConflictCount = 0;
  let variantMismatchCount = 0;
  let overlapConflictCount = 0;
  let outsideMasterDateRangeCount = 0;
  let invalidTimezoneDataCount = 0;
  let successfulAcftLinks = 0;

  const priorityKeySet = new Set(
    (priorityKeys || []).map((key) => String(key || "").trim().toUpperCase()).filter(Boolean)
  );
  const rowsForValidation = [...normalizedRows].sort((a, b) => {
    const aPriority = priorityKeySet.has(buildDateFlightKey(a.dateKey, a.flight)) ? 1 : 0;
    const bPriority = priorityKeySet.has(buildDateFlightKey(b.dateKey, b.flight)) ? 1 : 0;
    if (aPriority !== bPriority) return bPriority - aPriority;
    return (a.sequence || 0) - (b.sequence || 0);
  });

  const validationRows = rowsForValidation.map((row) => {
    const flightKey = buildDateFlightKey(row.dateKey, row.flight);
    const flightRecord = flightMap.get(flightKey);
    const fleetRecordsForRegn = row.acft ? (fleetMap.get(row.acft) || []) : [];
    const fleetRecord = pickFleetRecordForDate(fleetRecordsForRegn, row.assignDate);
    const departureStation = flightRecord
      ? stationMap.get(String(flightRecord.depStn || "").trim().toUpperCase())
      : null;
    const arrivalStation = flightRecord
      ? stationMap.get(String(flightRecord.arrStn || "").trim().toUpperCase())
      : null;
    const interval = flightRecord
      ? buildFlightInterval({
        flightRecord,
        assignDate: row.assignDate,
        departureStation,
        arrivalStation,
        homeTimezone,
      })
      : null;

    return {
      ...row,
      flightKey,
      flightRecord,
      fleetRecord,
      departureStation,
      arrivalStation,
      homeTimezone,
      interval,
      flightVariant: normalizeVariantForCompare(flightRecord?.variant),
      fleetVariant: normalizeVariantForCompare(fleetRecord?.variant),
    };
  });

  for (const row of validationRows) {
    const { flightKey, flightRecord, fleetRecord } = row;

    const errors = [];
    let isValid = true;
    let removedReason = null;
    let assignedAcft = row.acft;

    const outsideMasterDateRange = !masterMinDateKey || !masterMaxDateKey ||
      row.dateKey < masterMinDateKey || row.dateKey > masterMaxDateKey;

    if (outsideMasterDateRange) {
      outsideMasterDateRangeCount++;
      isValid = false;
      removedReason = "OUTSIDE_MASTER_DATE_RANGE";
      errors.push("Assignment date is outside the master schedule date range");
    } else if (!flightRecord) {
      notFoundCount++;
      isValid = false;
      errors.push("Flight not found in master schedule");
    }

    if (!fleetRecord) {
      isValid = false;
      assignedAcft = null;
      missingFleetDBCount++;
      errors.push(`Aircraft ${row.acft || "N/A"} not found in Fleet master`);
    } else {
      const assignMom = moment.utc(row.assignDate);
      const entryMom = fleetRecord.entry ? moment.utc(fleetRecord.entry).startOf("day") : null;
      const exitMom = fleetRecord.exit ? moment.utc(fleetRecord.exit).endOf("day") : null;
      const msn = normalizeSnForCompare(fleetRecord.sn);

      if (entryMom && assignMom.isBefore(entryMom)) {
        isValid = false;
        assignedAcft = null;
        removedReason = "OUTSIDE_FLEET_DATES";
        preEntryCount++;
        errors.push("Date precedes fleet entry");
      } else if (exitMom && assignMom.isAfter(exitMom)) {
        isValid = false;
        assignedAcft = null;
        removedReason = "OUTSIDE_FLEET_DATES";
        postExitCount++;
        errors.push("Date succeeds fleet exit");
      } else if (!flightRecord || outsideMasterDateRange) {
        assignedAcft = null;
      } else {
        const { flightVariant, fleetVariant } = row;

        if (!variantsMatch(flightVariant, fleetVariant)) {
          isValid = false;
          assignedAcft = null;
          removedReason = "VARIANT_MISMATCH";
          variantMismatchCount++;
          errors.push(
            `Aircraft variant ${fleetVariant || "N/A"} does not match flight variant ${flightVariant || "N/A"}`
          );
        } else {
          const groundKey = `${row.dateKey}_${msn}`;
          const groundRecord = groundDayMap.get(groundKey);

          if (groundRecord) {
            isValid = false;
            removedReason = "GROUND_DAY_CONFLICT";
            groundConflictCount++;
            errors.push(`Aircraft ${msn} is on ground for this date`);
          } else {
            const { interval } = row;
            if (!interval) {
              isValid = false;
              assignedAcft = null;
              removedReason = "INVALID_TIMEZONE_DATA";
              invalidTimezoneDataCount++;
              errors.push("Flight interval could not be normalized using station and home timezone data");
            } else {
              const priorIntervals = acceptedIntervalsByAcft.get(row.acft) || [];
              const hasOverlap = priorIntervals.some((existingInterval) => intervalsOverlap(interval, existingInterval));

              if (hasOverlap) {
                isValid = false;
                assignedAcft = null;
                removedReason = "ACFT_ASSIGNMENT_OVERLAP";
                overlapConflictCount++;
                errors.push(`Assignment overlaps with a previous assignment for aircraft ${row.acft}`);
              } else {
                priorIntervals.push(interval);
                acceptedIntervalsByAcft.set(row.acft, priorIntervals);
              }
            }
          }
        }
      }
    }

    if (isValid && assignedAcft) successfulAcftLinks++;
    if (!isValid || !assignedAcft) discardedDateKeys.add(row.dateKey);
    if (errors.length > 0 && rejectedRows.length < 10) {
      rejectedRows.push(buildRejectionSummary(row, errors));
    }

    let rotationNum = null;
    if (flightRecord?.rotationNumber) {
      rotationNum = parseInt(String(flightRecord.rotationNumber).replace(/\D/g, ""), 10) || null;
    }
    const legNumber = parseLegNumberFromFlight(flightRecord);

    let msnVal = null;
    if (fleetRecord && assignedAcft && fleetRecord.sn) {
      const strippedSn = String(fleetRecord.sn).replace(/\D/g, "");
      if (strippedSn.length > 0) msnVal = Number(strippedSn);
    }

    processedRowsByFlightKey.set(flightKey, {
      row,
      flightRecord,
      assignedAcft,
      msnVal,
      rotationNum,
      legNumber,
      isValid,
      errors,
      removedReason,
    });
  }

  const assignmentBulkOps = [];
  const flightBulkOps = [];

  for (const item of processedRowsByFlightKey.values()) {
    const {
      row,
      flightRecord,
      assignedAcft,
      msnVal,
      rotationNum,
      legNumber,
      isValid,
      errors,
      removedReason,
    } = item;

    if (isValid && assignedAcft) {
      assignmentBulkOps.push({
        updateOne: {
          filter: { userId, date: row.assignDate, flightNumber: row.flight },
          update: {
            $set: {
              userId,
              date: row.assignDate,
              flightNumber: row.flight,
              "aircraft.registration": assignedAcft,
              "aircraft.msn": msnVal,
              rotationNumber: rotationNum,
              legNumber,
              isValid: true,
              validationErrors: [],
              removedReason: null,
              ...(Number.isInteger(row.sourceOrder) ? { sourceOrder: row.sourceOrder } : {}),
              ...(row.sourceUploadedAt && !Number.isNaN(row.sourceUploadedAt.getTime())
                ? { sourceUploadedAt: row.sourceUploadedAt }
                : {}),
            },
          },
          upsert: allowCreate,
        },
      });
    } else if (removedReason === "GROUND_DAY_CONFLICT" && assignedAcft && msnVal !== null) {
      // Keep the planned assignment as the source used to forecast the
      // maintenance threshold on a later recomputation. Normal assignment
      // views only return valid records, so the grounded aircraft remains
      // unavailable and the flight link below is still cleared.
      assignmentBulkOps.push({
        updateOne: {
          filter: { userId, date: row.assignDate, flightNumber: row.flight },
          update: {
            $set: {
              userId,
              date: row.assignDate,
              flightNumber: row.flight,
              "aircraft.registration": assignedAcft,
              "aircraft.msn": msnVal,
              rotationNumber: rotationNum,
              legNumber,
              isValid: false,
              validationErrors: errors,
              removedReason,
              ...(Number.isInteger(row.sourceOrder) ? { sourceOrder: row.sourceOrder } : {}),
              ...(row.sourceUploadedAt && !Number.isNaN(row.sourceUploadedAt.getTime())
                ? { sourceUploadedAt: row.sourceUploadedAt }
                : {}),
            },
          },
          upsert: allowCreate,
        },
      });
    } else {
      assignmentBulkOps.push({
        deleteOne: {
          filter: { userId, date: row.assignDate, flightNumber: row.flight },
        },
      });
    }

    if (flightRecord && flightRecord._id) {
      flightBulkOps.push({
        updateOne: {
          filter: { _id: flightRecord._id, userId },
          update: isValid && assignedAcft
            ? {
              $set: {
                "aircraft.registration": assignedAcft,
                "aircraft.msn": msnVal,
              },
            }
            : {
              $unset: {
                "aircraft.registration": "",
                "aircraft.msn": "",
              },
            },
        },
      });
    }
  }

  return {
    assignmentBulkOps,
    flightBulkOps,
    diagnostics: {
      totalRows: normalizedRows.length,
      uniqueRows: processedRowsByFlightKey.size,
      duplicateComboCount,
      successfullyAssigned: successfulAcftLinks,
      appliedCount: successfulAcftLinks,
      discardedCount: processedRowsByFlightKey.size - successfulAcftLinks,
      discardedDateCount: discardedDateKeys.size,
      revalidatedCount: 0,
      rejections: {
        missingFromFleetDB: missingFleetDBCount,
        preEntryDates: preEntryCount,
        postExitDates: postExitCount,
        variantMismatches: variantMismatchCount,
        groundConflicts: groundConflictCount,
        acftOverlaps: overlapConflictCount,
        flightNotFound: notFoundCount,
        outsideMasterDateRange: outsideMasterDateRangeCount,
        invalidTimezoneData: invalidTimezoneDataCount,
      },
      rejectedRows,
      discardedRows: rejectedRows,
    },
  };
};

const applyAssignmentSyncPlan = async (result = {}) => {
  const writes = [];
  if (result.assignmentBulkOps?.length > 0) {
    writes.push(Assignment.bulkWrite(result.assignmentBulkOps, { ordered: false }));
  }
  if (result.flightBulkOps?.length > 0) {
    writes.push(Flight.bulkWrite(result.flightBulkOps, { ordered: false }));
  }

  if (writes.length === 0) {
    return { assignmentWriteResult: null, flightWriteResult: null };
  }

  const [assignmentWriteResult = null, flightWriteResult = null] = await Promise.all(writes);
  return { assignmentWriteResult, flightWriteResult };
};

const revalidateAssignmentsForUser = async ({ userId, priorityKeys = [], includeGroundDays = true }) => {
  if (Assignment.db?.readyState !== 1) {
    return buildEmptyDiagnostics();
  }

  const assignments = await Assignment.find({
    userId,
  })
    .select("date flightNumber aircraft.registration sourceOrder sourceUploadedAt createdAt")
    .lean();

  assignments.sort((a, b) => {
    const aUploadedAt = a.sourceUploadedAt || a.createdAt;
    const bUploadedAt = b.sourceUploadedAt || b.createdAt;
    const uploadedDifference = new Date(aUploadedAt || 0).getTime() - new Date(bUploadedAt || 0).getTime();
    if (uploadedDifference !== 0) return uploadedDifference;

    const aOrder = Number.isInteger(a.sourceOrder) ? a.sourceOrder : Number.MAX_SAFE_INTEGER;
    const bOrder = Number.isInteger(b.sourceOrder) ? b.sourceOrder : Number.MAX_SAFE_INTEGER;
    if (aOrder !== bOrder) return aOrder - bOrder;
    return String(a._id).localeCompare(String(b._id));
  });

  const rows = assignments.map((assignment) => ({
    assignDate: assignment.date,
    dateKey: moment.utc(assignment.date).format("YYYY-MM-DD"),
    flight: assignment.flightNumber,
    acft: assignment.aircraft?.registration || null,
    sourceOrder: assignment.sourceOrder,
    sourceUploadedAt: assignment.sourceUploadedAt,
    sourceCreatedAt: assignment.createdAt,
    sourceId: assignment._id,
  }));

  const result = await buildAssignmentSyncPlan({
    userId,
    rows,
    priorityKeys,
    includeGroundDays,
    allowCreate: false,
  });
  await applyAssignmentSyncPlan(result);

  return {
    ...result.diagnostics,
    revalidatedCount: assignments.length,
  };
};

const purgeStaleAssignmentsForUser = async ({ userId }) => {
  const assignments = await Assignment.find({ userId })
    .select("_id date flightNumber")
    .lean();

  if (assignments.length === 0) {
    return { deletedCount: 0 };
  }

  const dates = assignments
    .map((assignment) => assignment.date)
    .filter(Boolean)
    .map((date) => moment.utc(date).startOf("day").toDate());

  if (dates.length === 0) {
    const result = await Assignment.deleteMany({
      userId,
      _id: { $in: assignments.map((assignment) => assignment._id) },
    });
    return { deletedCount: result.deletedCount || 0 };
  }

  const minDate = moment.utc(Math.min(...dates.map((date) => date.getTime()))).startOf("day").toDate();
  const maxDate = moment.utc(Math.max(...dates.map((date) => date.getTime()))).endOf("day").toDate();
  const flightRegexArray = [...new Set(assignments
    .map((assignment) => String(assignment.flightNumber || "").trim().toUpperCase())
    .filter(Boolean))]
    .map((flight) => new RegExp(`^${escapeRegExp(flight)}$`, "i"));

  const flights = flightRegexArray.length > 0
    ? await Flight.find({
      userId,
      date: { $gte: minDate, $lte: maxDate },
      flight: { $in: flightRegexArray },
    })
      .select("date flight")
      .lean()
    : [];

  const activeFlightKeys = new Set(flights.map((flight) => (
    buildDateFlightKey(moment.utc(flight.date).format("YYYY-MM-DD"), flight.flight)
  )));

  const staleAssignmentIds = assignments
    .filter((assignment) => {
      const key = buildDateFlightKey(moment.utc(assignment.date).format("YYYY-MM-DD"), assignment.flightNumber);
      return !activeFlightKeys.has(key);
    })
    .map((assignment) => assignment._id);

  if (staleAssignmentIds.length === 0) {
    return { deletedCount: 0 };
  }

  const result = await Assignment.deleteMany({
    userId,
    _id: { $in: staleAssignmentIds },
  });

  return { deletedCount: result.deletedCount || 0 };
};

const deleteAssignmentsForAircraftDate = async ({ userId, msn, date, reason = "GROUND_DAY_CONFLICT" }) => {
  const msnNumber = Number(msn);
  if (!userId || !date || !Number.isFinite(msnNumber)) {
    return { deletedCount: 0, clearedFlightCount: 0, reason };
  }

  const start = moment.utc(date).startOf("day");
  const end = start.clone().add(1, "day");
  const assignments = await Assignment.find({
    userId: String(userId),
    date: { $gte: start.toDate(), $lt: end.toDate() },
    "aircraft.msn": msnNumber,
  })
    .select("_id date flightNumber")
    .lean();

  if (assignments.length === 0) {
    return { deletedCount: 0, clearedFlightCount: 0, reason };
  }

  const flightBulkOps = assignments
    .filter((assignment) => assignment.date && assignment.flightNumber)
    .map((assignment) => ({
      updateOne: {
        filter: {
          userId: String(userId),
          date: {
            $gte: moment.utc(assignment.date).startOf("day").toDate(),
            $lt: moment.utc(assignment.date).startOf("day").add(1, "day").toDate(),
          },
          flight: new RegExp(`^${escapeRegExp(assignment.flightNumber)}$`, "i"),
        },
        update: {
          $unset: {
            "aircraft.registration": "",
            "aircraft.msn": "",
          },
        },
      },
    }));

  const [deleteResult] = await Promise.all([
    Assignment.deleteMany({
      userId: String(userId),
      _id: { $in: assignments.map((assignment) => assignment._id) },
    }),
    flightBulkOps.length > 0 ? Flight.bulkWrite(flightBulkOps, { ordered: false }) : Promise.resolve(null),
  ]);

  return {
    deletedCount: deleteResult.deletedCount || 0,
    clearedFlightCount: flightBulkOps.length,
    reason,
  };
};

module.exports = {
  buildAssignmentSyncPlan,
  applyAssignmentSyncPlan,
  revalidateAssignmentsForUser,
  purgeStaleAssignmentsForUser,
  deleteAssignmentsForAircraftDate,
  buildDateFlightKey,
  __testables__: {
    parseUtcOffsetToMinutes,
    parseBusinessDateKey,
    getEffectiveStationOffsetForDate,
    buildStationLocalInstant,
    buildFlightInterval,
    intervalsOverlap,
  },
};
