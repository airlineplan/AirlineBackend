const Aircraft = require("../model/aircraftSchema.js");
const Utilisation = require("../model/utilisation.js");
const MaintenanceStatus = require("../model/maintenanceStatusSchema.js");
const RotableMovement = require("../model/rotableMovementSchema.js");
const MaintenanceTarget = require("../model/maintenanceTargetSchema.js");
const MaintenanceReset = require('../model/maintenanceReset');
const AircraftOnwing = require("../model/aircraftOnwing.js");
const Fleet = require("../model/fleet.js");
const Assignment = require("../model/assignment.js");
const Flight = require("../model/flight.js");
const MaintenanceCalendar = require("../model/maintenanceCalendarSchema.js");
const UtilisationAssumption = require("../model/utilisationAssumptionSchema.js");
const GroundDay = require("../model/groundDay.js");
const moment = require('moment'); // <-- Added missing moment import

const getUserIdFromReq = (req) => req.user?.id || req.userId || req.user?.userId || req.user?._id;
const isValidObjectId = (value) => /^[a-f\d]{24}$/i.test(String(value || ""));
const parseUtcIsoDate = (value) => {
    const parsed = moment.utc(value, moment.ISO_8601, true);
    return parsed.isValid() ? parsed : null;
};
const getUtcDayBounds = (value) => {
    const parsed = parseUtcIsoDate(value);
    if (!parsed) return null;
    const start = parsed.clone().startOf("day");
    return {
        start,
        endExclusive: start.clone().add(1, "day")
    };
};
const isSameUtcDay = (left, right) => {
    const leftMoment = moment.utc(left);
    const rightMoment = moment.utc(right);
    return leftMoment.isValid() && rightMoment.isValid() && leftMoment.isSame(rightMoment, "day");
};

const escapeRegex = (value = "") =>
    String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const buildResetGroupKey = ({ msnEsn, pn, snBn } = {}) => [
    String(msnEsn || "").trim().toUpperCase(),
    String(pn || "").trim().toUpperCase(),
    String(snBn || "").trim().toUpperCase()
].join("|");

const resetNumericFields = [
    { payloadKey: "tsn", label: "TSN", modelKey: "tsn" },
    { payloadKey: "csn", label: "CSN", modelKey: "csn" },
    { payloadKey: "dsn", label: "DSN", modelKey: "dsn" },
    { payloadKey: "tso", label: "TSO/TSRtrtn", modelKey: "tsoTsr" },
    { payloadKey: "cso", label: "CSO/CSRtrtn", modelKey: "csoCsr" },
    { payloadKey: "dso", label: "DSO/DSRtrtn", modelKey: "dsoDsr" },
    { payloadKey: "tsr", label: "TSRplmt", modelKey: "tsRplmt" },
    { payloadKey: "csr", label: "CSRplmt", modelKey: "csRplmt" },
    { payloadKey: "dsr", label: "DSRplmt", modelKey: "dsRplmt" },
];

const parseResetNumericValue = (value, label) => {
    const normalizedValue = typeof value === "string" ? value.trim() : value;

    if (normalizedValue === "" || normalizedValue === null || normalizedValue === undefined) {
        return { ok: true, value: null };
    }

    const numericValue = Number(normalizedValue);
    if (!Number.isFinite(numericValue)) {
        return {
            ok: false,
            message: `${label} must be a valid number.`
        };
    }

    return { ok: true, value: numericValue };
};

const normalizeResetTimeMetric = (value) => {
    const metric = String(value || "BH").trim().toUpperCase();
    return ["BH", "FH"].includes(metric) ? metric : null;
};

const getMongooseValidationMessage = (error) => {
    if (error?.name === "ValidationError") {
        return Object.values(error.errors || {})
            .map(validationError => validationError.message)
            .filter(Boolean)
            .join(" ") || error.message;
    }

    const validationErrors = error?.validationErrors || error?.result?.result?.writeErrors;
    if (Array.isArray(validationErrors) && validationErrors.length > 0) {
        return validationErrors
            .map(validationError => validationError?.message || validationError?.errmsg || validationError?.err?.errmsg)
            .filter(Boolean)
            .join(" ");
    }

    return null;
};

const maintenanceMetricDefinitions = [
    { limitKey: "eTsn", valueKey: "tsn", metricCode: "TSN", group: "sinceNew", label: "TSN" },
    { limitKey: "eCsn", valueKey: "csn", metricCode: "CSN", group: "sinceNew", label: "CSN" },
    { limitKey: "eDsn", valueKey: "dsn", metricCode: "DSN", group: "sinceNew", label: "DSN" },
    { limitKey: "eTso", valueKey: "tsoTsr", metricCode: "TSO/TSRTRTN", group: "restoration", label: "TSO/TSRtrtn" },
    { limitKey: "eCso", valueKey: "csoCsr", metricCode: "CSO/CSRTRTN", group: "restoration", label: "CSO/CSRtrtn" },
    { limitKey: "eDso", valueKey: "dsoDsr", metricCode: "DSO/DSRTRTN", group: "restoration", label: "DSO/DSRtrtn" },
    { limitKey: "eTsr", valueKey: "tsRplmt", metricCode: "TSRPLMT", group: "replacement", label: "TSRplmt" },
    { limitKey: "eCsr", valueKey: "csRplmt", metricCode: "CSRPLMT", group: "replacement", label: "CSRplmt" },
    { limitKey: "eDsr", valueKey: "dsRplmt", metricCode: "DSRPLMT", group: "replacement", label: "DSRplmt" },
];

const normalizeThresholdNumber = (value) => {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? Number(numericValue.toFixed(6)) : null;
};

const thresholdSetKey = (value) => {
    const normalized = normalizeThresholdNumber(value);
    return normalized === null ? "" : String(normalized);
};

const nextMultipleAfter = (value, interval) => {
    const numericInterval = Number(interval);
    if (!Number.isFinite(numericInterval) || numericInterval <= 0) return null;

    const numericValue = Number(value || 0);
    const multiplier = Math.floor((numericValue + Number.EPSILON) / numericInterval) + 1;
    return normalizeThresholdNumber(multiplier * numericInterval);
};

const getCalendarTriggerDefinitions = (cal = {}) => maintenanceMetricDefinitions
    .map(definition => {
        const interval = Number(cal?.[definition.limitKey]);
        if (!Number.isFinite(interval) || interval <= 0) return null;
        return {
            ...definition,
            interval: normalizeThresholdNumber(interval)
        };
    })
    .filter(Boolean);

const getCalendarTriggeredGroups = (triggerDefinitions = []) => [
    ...new Set(triggerDefinitions.map(definition => definition.group))
];

const hasSuppressedThreshold = (state, metricCode, threshold) => {
    const set = state?.suppressedThresholdsByMetric?.get(metricCode);
    return Boolean(set?.has(thresholdSetKey(threshold)));
};

const addSuppressedThreshold = (state, metricCode, threshold) => {
    if (!state || threshold === null || threshold === undefined) return false;
    if (!state.suppressedThresholdsByMetric.has(metricCode)) {
        state.suppressedThresholdsByMetric.set(metricCode, new Set());
    }
    const key = thresholdSetKey(threshold);
    const set = state.suppressedThresholdsByMetric.get(metricCode);
    const wasMissing = !set.has(key);
    set.add(key);
    return wasMissing;
};

const getNextUnsuppressedThreshold = ({ state, metricCode, currentValue, interval }) => {
    let threshold = nextMultipleAfter(currentValue, interval);
    let guard = 0;

    while (threshold !== null && hasSuppressedThreshold(state, metricCode, threshold) && guard < 1000) {
        threshold = normalizeThresholdNumber(threshold + Number(interval));
        guard += 1;
    }

    return threshold;
};

const findCalendarTriggerCandidate = ({ cal, state, currentValues, projectedValues }) => {
    const triggerDefinitions = getCalendarTriggerDefinitions(cal);
    if (triggerDefinitions.length === 0) return null;

    const candidates = [];
    for (const definition of triggerDefinitions) {
        const currentValue = normalizeMetricNumber(currentValues?.[definition.valueKey]);
        const projectedValue = normalizeMetricNumber(projectedValues?.[definition.valueKey]);
        if (projectedValue === null) continue;

        if (definition.group === "sinceNew") {
            const alreadyTriggered = state?.triggeredSinceNewMetrics?.has(definition.metricCode);
            const threshold = definition.interval;
            if (!alreadyTriggered && projectedValue >= threshold && (currentValue === null || currentValue < threshold)) {
                candidates.push({
                    ...definition,
                    threshold,
                    projectedValue
                });
            }
            continue;
        }

        const threshold = getNextUnsuppressedThreshold({
            state,
            metricCode: definition.metricCode,
            currentValue,
            interval: definition.interval
        });

        if (threshold !== null && projectedValue >= threshold) {
            candidates.push({
                ...definition,
                threshold,
                projectedValue
            });
        }
    }

    if (candidates.length === 0) return null;
    return candidates.sort((left, right) => {
        const leftOrder = maintenanceMetricDefinitions.findIndex(definition => definition.metricCode === left.metricCode);
        const rightOrder = maintenanceMetricDefinitions.findIndex(definition => definition.metricCode === right.metricCode);
        return leftOrder - rightOrder;
    })[0];
};

const getCalendarDowntimeDaysForOccurrence = (cal, occurrenceNumber) => {
    const firstOccurrenceDownDays = Number(cal?.downDays);
    const laterOccurrenceDownDays = Number(cal?.avgDownda);
    const selectedValue = occurrenceNumber <= 1
        ? firstOccurrenceDownDays
        : laterOccurrenceDownDays;

    if (Number.isFinite(selectedValue) && selectedValue > 0) return Math.ceil(selectedValue);
    return 0;
};

const getMetricValuesFromCurrent = ({
    currentTsn,
    currentCsn,
    currentDsn,
    currentTso,
    currentCso,
    currentDso,
    currentTsr,
    currentCsr,
    currentDsr
}) => ({
    tsn: currentTsn,
    csn: currentCsn,
    dsn: currentDsn,
    tsoTsr: currentTso,
    csoCsr: currentCso,
    dsoDsr: currentDso,
    tsRplmt: currentTsr,
    csRplmt: currentCsr,
    dsRplmt: currentDsr,
});

const getCalendarState = (calendarEventState, cal) => {
    const key = String(cal._id);
    if (!calendarEventState.has(key)) {
        calendarEventState.set(key, {
            id: cal._id,
            occurrence: 0,
            lastOccurre: null,
            nextEstima: null,
            firstOccurrenceDate: null,
            occurrencesTillExit: 0,
            soTsr: null,
            triggeredSinceNew: false,
            triggeredSinceNewMetrics: new Set(),
            suppressedThresholdsByMetric: new Map(),
            generatedOccurrences: [],
            suppressedAlternateThresholds: []
        });
    }
    return calendarEventState.get(key);
};

const serializeSuppressedThresholds = (suppressedThresholdsByMetric = new Map()) => {
    const rows = [];
    for (const [metricCode, thresholds] of suppressedThresholdsByMetric.entries()) {
        for (const threshold of thresholds.values()) {
            rows.push({
                metricCode,
                suppressedThreshold: normalizeThresholdNumber(threshold)
            });
        }
    }
    return rows;
};

const getCalendarGroundDateRows = ({ userId, msn, cal, occurrenceNumber, startDate, downtimeDays }) => {
    const rows = [];
    const days = Number(downtimeDays);
    if (!Number.isFinite(days) || days <= 0) return rows;

    const occurrenceId = `${cal._id}:${occurrenceNumber}`;
    for (let offset = 0; offset < days; offset += 1) {
        const date = moment.utc(startDate).add(offset, "days").startOf("day").toDate();
        rows.push({
            userId: String(userId),
            msn: String(msn || cal.calMsn || "").trim(),
            date,
            event: cal.schEvent || "",
            source: "SCHEDULED_MAINTENANCE",
            eventSeriesId: String(cal._id),
            occurrenceNumber,
            occurrenceId
        });
    }
    return rows;
};

const getPostEventValuesForTriggeredMetric = (cal, triggerDefinition) => {
    if (!triggerDefinition) return {};
    return collectPostEventValues(cal, [triggerDefinition.group]);
};

const getExplicitPostEventValue = (cal, key) => {
    if (cal?.[key] === "" || cal?.[key] === null || cal?.[key] === undefined) return null;
    const value = Number(cal[key]);
    return Number.isFinite(value) ? value : null;
};

const collectPostEventValues = (cal, triggeredGroups = []) => {
    const values = {};

    if (triggeredGroups.includes("restoration")) {
        const postTso = getExplicitPostEventValue(cal, "postTso");
        const postCso = getExplicitPostEventValue(cal, "postCso");
        const postDso = getExplicitPostEventValue(cal, "postDso");

        if (postTso !== null) values.tsoTsr = postTso;
        if (postCso !== null) values.csoCsr = postCso;
        if (postDso !== null) values.dsoDsr = postDso;
    }

    if (triggeredGroups.includes("replacement")) {
        const postTsr = getExplicitPostEventValue(cal, "postTsr");
        const postCsr = getExplicitPostEventValue(cal, "postCsr");
        const postDsr = getExplicitPostEventValue(cal, "postDsr");

        if (postTsr !== null) values.tsRplmt = postTsr;
        if (postCsr !== null) values.csRplmt = postCsr;
        if (postDsr !== null) values.dsRplmt = postDsr;
    }

    return values;
};

const applyPostEventValuesToCurrent = (currentValues, postEventValues = {}) => ({
    currentTso: Object.prototype.hasOwnProperty.call(postEventValues, "tsoTsr") ? postEventValues.tsoTsr : currentValues.currentTso,
    currentCso: Object.prototype.hasOwnProperty.call(postEventValues, "csoCsr") ? postEventValues.csoCsr : currentValues.currentCso,
    currentDso: Object.prototype.hasOwnProperty.call(postEventValues, "dsoDsr") ? postEventValues.dsoDsr : currentValues.currentDso,
    currentTsr: Object.prototype.hasOwnProperty.call(postEventValues, "tsRplmt") ? postEventValues.tsRplmt : currentValues.currentTsr,
    currentCsr: Object.prototype.hasOwnProperty.call(postEventValues, "csRplmt") ? postEventValues.csRplmt : currentValues.currentCsr,
    currentDsr: Object.prototype.hasOwnProperty.call(postEventValues, "dsRplmt") ? postEventValues.dsRplmt : currentValues.currentDsr,
});

let maintenanceCalendarIndexesEnsured = false;
const ensureMaintenanceCalendarIndexes = async () => {
    if (maintenanceCalendarIndexesEnsured) return;

    try {
        let indexes = [];
        try {
            indexes = await MaintenanceCalendar.collection.indexes();
        } catch (error) {
            const namespaceMissing = error?.codeName === "NamespaceNotFound" ||
                /ns does not exist/i.test(error?.message || "");
            if (!namespaceMissing) throw error;
        }

        const legacyIndex = indexes.find((index) => {
            const keys = Object.keys(index.key || {});
            return index.unique === true &&
                keys.length === 4 &&
                index.key.userId === 1 &&
                index.key.calMsn === 1 &&
                index.key.calPn === 1 &&
                index.key.snBn === 1;
        });

        if (legacyIndex?.name) {
            await MaintenanceCalendar.collection.dropIndex(legacyIndex.name);
        }

        await MaintenanceCalendar.collection.createIndex(
            { userId: 1, calMsn: 1, calPn: 1, snBn: 1, schEvent: 1 },
            { unique: true }
        );
        maintenanceCalendarIndexesEnsured = true;
    } catch (error) {
        console.warn("Could not ensure maintenance calendar indexes:", error.message);
    }
};

const formatCalendarDate = (value) => value ? moment.utc(value).format("YYYY-MM-DD") : "";

const formatCalendarOccurrence = (occurrence = {}) => ({
    occurrenceNumber: occurrence.occurrenceNumber || 0,
    triggerRelationship: occurrence.triggerRelationship || "EARLIEST_OF_EVERY",
    triggerDate: formatCalendarDate(occurrence.triggerDate),
    triggeredByMetric: occurrence.triggeredByMetric || "",
    triggerThreshold: occurrence.triggerThreshold ?? "",
    triggerMetricValueOnDetectionDate: occurrence.triggerMetricValueOnDetectionDate ?? "",
    groundStartDate: formatCalendarDate(occurrence.groundStartDate),
    groundEndDate: formatCalendarDate(occurrence.groundEndDate),
    downtimeApplied: occurrence.downtimeApplied ?? 0,
    isFirstOccurrence: Boolean(occurrence.isFirstOccurrence),
    postEventStatusApplied: occurrence.postEventStatusApplied || {},
    suppressedAlternateThresholds: Array.isArray(occurrence.suppressedAlternateThresholds)
        ? occurrence.suppressedAlternateThresholds
        : []
});

const normalizeResetGroup = ({ msnEsn, pn, snBn } = {}) => ({
    msnEsn: String(msnEsn || "").trim(),
    pn: String(pn || "").trim(),
    snBn: String(snBn || "").trim()
});

const normalizeMetricNumber = (value) => {
    if (value === "" || value === null || value === undefined) return null;
    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : null;
};

const getFlightDateBounds = async ({ userId } = {}) => {
    const userMatch = userId ? { userId: String(userId) } : {};
    const [datedBounds, effectiveBounds] = await Promise.all([
        Flight.aggregate([
            { $match: { ...userMatch, date: { $type: "date" } } },
            {
                $group: {
                    _id: null,
                    firstDate: { $min: "$date" },
                    lastDate: { $max: "$date" }
                }
            }
        ]),
        Flight.aggregate([
            { $match: { ...userMatch, effFromDt: { $type: "date" } } },
            {
                $group: {
                    _id: null,
                    firstDate: { $min: "$effFromDt" },
                    lastFromDate: { $max: "$effFromDt" },
                    lastToDate: { $max: "$effToDt" }
                }
            }
        ])
    ]);

    const dateCandidates = [
        datedBounds[0]?.firstDate,
        effectiveBounds[0]?.firstDate
    ].filter(Boolean).map(date => moment.utc(date));
    const endCandidates = [
        datedBounds[0]?.lastDate,
        effectiveBounds[0]?.lastToDate,
        effectiveBounds[0]?.lastFromDate
    ].filter(Boolean).map(date => moment.utc(date));

    if (dateCandidates.length === 0 || endCandidates.length === 0) {
        return null;
    }

    return {
        firstDate: moment.min(dateCandidates).toDate(),
        lastDate: moment.max(endCandidates).toDate()
    };
};

const onwingFields = ["pos1Esn", "pos2Esn", "apun"];
const normalizeAssetIdentity = (value) => String(value || "").trim().toUpperCase();

const onwingRowHasAsset = (row, assetKey) => {
    const normalizedAssetKey = normalizeAssetIdentity(assetKey);
    if (!row || !normalizedAssetKey) return false;
    return onwingFields.some(field => normalizeAssetIdentity(row[field]) === normalizedAssetKey);
};

const getRotablePositionField = (position) => {
    const normalizedPosition = String(position || "").trim();
    if (normalizedPosition === "#1") return "pos1Esn";
    if (normalizedPosition === "#2") return "pos2Esn";
    return null;
};

const getRotableEffectiveMoment = (record) => {
    const movementDate = moment.utc(record?.date, moment.ISO_8601, true);
    return movementDate.isValid() ? movementDate.add(1, "day").startOf("day") : null;
};

const compareRotableEvents = (left, right) => {
    const leftEffective = left?.effectiveMoment?.valueOf() || 0;
    const rightEffective = right?.effectiveMoment?.valueOf() || 0;
    if (leftEffective !== rightEffective) return leftEffective - rightEffective;

    const leftCreated = moment.utc(left?.record?.createdAt).isValid()
        ? moment.utc(left.record.createdAt).valueOf()
        : 0;
    const rightCreated = moment.utc(right?.record?.createdAt).isValid()
        ? moment.utc(right.record.createdAt).valueOf()
        : 0;
    if (leftCreated !== rightCreated) return leftCreated - rightCreated;

    return String(left?.record?._id || "").localeCompare(String(right?.record?._id || ""));
};

const getRotableOwnershipContextForDate = async ({ userId, assetKey, date }) => {
    const normalizedAssetKey = String(assetKey || "").trim();
    const normalizedAssetIdentity = normalizeAssetIdentity(assetKey);
    const targetDay = date ? moment.utc(date).startOf("day") : null;
    if (!normalizedAssetKey || !targetDay?.isValid()) return null;

    const movementDateLimit = targetDay.clone().subtract(1, "day").endOf("day").toDate();
    const assetRegex = new RegExp(`^${escapeRegex(normalizedAssetKey)}$`, "i");
    const movementFilter = {
        $or: [
            { installedSN: assetRegex },
            { removedSN: assetRegex }
        ],
        date: { $lte: movementDateLimit }
    };
    if (userId) movementFilter.userId = String(userId);

    const movementRecords = await RotableMovement.find(movementFilter)
        .sort({ date: -1, createdAt: -1, _id: -1 })
        .lean();

    const events = [];
    movementRecords.forEach((record) => {
        const effectiveMoment = getRotableEffectiveMoment(record);
        if (!effectiveMoment || effectiveMoment.isAfter(targetDay, "day")) return;

        if (normalizeAssetIdentity(record.installedSN) === normalizedAssetIdentity) {
            events.push({ record, effectiveMoment, type: "installed" });
        }
        if (normalizeAssetIdentity(record.removedSN) === normalizedAssetIdentity) {
            events.push({ record, effectiveMoment, type: "removed" });
        }
    });

    events.sort((left, right) => compareRotableEvents(right, left));
    const latestAssetEvent = events[0];
    if (!latestAssetEvent) return null;

    if (latestAssetEvent.type === "removed") {
        return { effectiveMsn: normalizedAssetKey, isExplicitSpare: true };
    }

    const latestInstalledMsn = String(latestAssetEvent.record?.msn || "").trim();
    const latestInstalledPosition = String(latestAssetEvent.record?.position || "").trim();
    if (!latestInstalledMsn || !latestInstalledPosition) {
        return { effectiveMsn: normalizedAssetKey, isExplicitSpare: true };
    }

    const slotFilter = {
        msn: latestInstalledMsn,
        position: latestInstalledPosition,
        date: {
            $gte: moment.utc(latestAssetEvent.record.date).startOf("day").toDate(),
            $lte: movementDateLimit
        }
    };
    if (userId) slotFilter.userId = String(userId);

    const slotEvents = (await RotableMovement.find(slotFilter)
        .sort({ date: -1, createdAt: -1, _id: -1 })
        .lean())
        .map((record) => ({ record, effectiveMoment: getRotableEffectiveMoment(record) }))
        .filter(({ effectiveMoment }) => effectiveMoment && effectiveMoment.isSameOrBefore(targetDay, "day"));

    slotEvents.sort((left, right) => compareRotableEvents(right, left));
    const latestSlotEvent = slotEvents[0];
    const isStillInstalled = latestSlotEvent &&
        normalizeAssetIdentity(latestSlotEvent.record?.installedSN) === normalizedAssetIdentity;

    return isStillInstalled
        ? { effectiveMsn: latestInstalledMsn, isExplicitSpare: false }
        : { effectiveMsn: normalizedAssetKey, isExplicitSpare: true };
};

const buildOnwingSnapshot = ({ userId, msn, date, priorConfig, updateField, value }) => ({
    userId: String(userId),
    msn: String(msn || "").trim(),
    date,
    pos1Esn: priorConfig?.pos1Esn || "",
    pos2Esn: priorConfig?.pos2Esn || "",
    apun: priorConfig?.apun || "",
    [updateField]: value
});

const getOnwingSnapshotBase = async ({ userId, msn, date, fallbackConfig }) => {
    const existingConfig = await AircraftOnwing.findOne({
        userId: String(userId),
        msn: String(msn || "").trim(),
        date
    }).lean();

    return existingConfig || fallbackConfig || {};
};

const rebuildOnwingTimelineForRotableMovement = async ({ userId, movement }) => {
    const updateField = getRotablePositionField(movement?.position);
    const effectiveMoment = getRotableEffectiveMoment(movement);
    const msn = String(movement?.msn || "").trim();

    if (!userId || !updateField || !effectiveMoment || !msn) return;

    const userKey = String(userId);
    const effectiveDate = effectiveMoment.toDate();
    const priorConfig = await AircraftOnwing.findOne({
        userId: userKey,
        msn,
        date: { $lt: effectiveDate }
    }).sort({ date: -1 }).lean();

    const restoredValue = String(movement?.removedSN || priorConfig?.[updateField] || "").trim();
    const remainingMovements = await RotableMovement.find({
        userId: userKey,
        msn,
        position: movement.position,
        date: { $gte: moment.utc(movement.date).startOf("day").toDate() }
    }).sort({ date: 1, createdAt: 1, _id: 1 }).lean();

    const futureEvents = remainingMovements
        .map(record => ({
            record,
            effectiveMoment: getRotableEffectiveMoment(record)
        }))
        .filter(({ effectiveMoment: eventMoment }) => eventMoment && eventMoment.isSameOrAfter(effectiveMoment, "day"));

    let segmentStart = effectiveMoment.clone();
    let currentValue = restoredValue;
    let currentPriorConfig = {
        ...(priorConfig || {}),
        [updateField]: restoredValue
    };

    const effectiveSnapshotBase = await getOnwingSnapshotBase({
        userId: userKey,
        msn,
        date: effectiveDate,
        fallbackConfig: priorConfig
    });

    await AircraftOnwing.updateOne(
        { userId: userKey, msn, date: effectiveDate },
        {
            $set: buildOnwingSnapshot({
                userId: userKey,
                msn,
                date: effectiveDate,
                priorConfig: effectiveSnapshotBase,
                updateField,
                value: restoredValue
            })
        },
        { upsert: true }
    );

    for (const { record, effectiveMoment: eventMoment } of futureEvents) {
        const eventDate = eventMoment.toDate();
        if (eventMoment.isAfter(segmentStart, "day")) {
            await AircraftOnwing.updateMany(
                {
                    userId: userKey,
                    msn,
                    date: {
                        $gte: segmentStart.toDate(),
                        $lt: eventDate
                    }
                },
                { $set: { [updateField]: currentValue } }
            );
        }

        currentValue = String(record.installedSN || "").trim();
        const eventPriorConfig = await AircraftOnwing.findOne({
            userId: userKey,
            msn,
            date: { $lt: eventDate }
        }).sort({ date: -1 }).lean();
        const eventSnapshotBase = await getOnwingSnapshotBase({
            userId: userKey,
            msn,
            date: eventDate,
            fallbackConfig: eventPriorConfig || currentPriorConfig
        });
        currentPriorConfig = {
            ...(eventSnapshotBase || currentPriorConfig || {}),
            [updateField]: currentValue
        };

        await AircraftOnwing.updateOne(
            { userId: userKey, msn, date: eventDate },
            {
                $set: buildOnwingSnapshot({
                    userId: userKey,
                    msn,
                    date: eventDate,
                    priorConfig: eventSnapshotBase,
                    updateField,
                    value: currentValue
                })
            },
            { upsert: true }
        );

        segmentStart = eventMoment.clone();
    }

    await AircraftOnwing.updateMany(
        {
            userId: userKey,
            msn,
            date: { $gte: segmentStart.toDate() }
        },
        { $set: { [updateField]: currentValue } }
    );
};

const getEffectiveUtilisationContext = async ({ userId, msnEsn, date }) => {
    const assetKey = String(msnEsn || "").trim();
    const lookupDate = date ? moment.utc(date).endOf("day").toDate() : null;
    const rotableOwnership = await getRotableOwnershipContextForDate({
        userId,
        assetKey,
        date
    });

    if (rotableOwnership?.isExplicitSpare) {
        const fleetFilter = assetKey ? { sn: assetKey } : {};
        if (userId) {
            fleetFilter.userId = String(userId);
        }

        return {
            assetKey,
            effectiveMsn: assetKey,
            fleet: assetKey ? await Fleet.findOne(fleetFilter).lean() : null,
        };
    }

    if (rotableOwnership?.effectiveMsn) {
        const fleetFilter = { sn: rotableOwnership.effectiveMsn };
        if (userId) {
            fleetFilter.userId = String(userId);
        }

        return {
            assetKey,
            effectiveMsn: rotableOwnership.effectiveMsn,
            fleet: await Fleet.findOne(fleetFilter).lean(),
        };
    }

    const historicalOwnershipFilter = {
        $or: [
            { pos1Esn: assetKey },
            { pos2Esn: assetKey },
            { apun: assetKey }
        ]
    };

    if (userId) {
        historicalOwnershipFilter.userId = String(userId);
    }

    const candidateMsns = assetKey
        ? await AircraftOnwing.distinct("msn", historicalOwnershipFilter)
        : [];

    const matchingConfigs = [];
    for (const candidateMsn of candidateMsns) {
        const latestConfigFilter = { msn: candidateMsn };
        if (userId) {
            latestConfigFilter.userId = String(userId);
        }
        if (lookupDate) {
            latestConfigFilter.date = { $lte: lookupDate };
        }

        const latestConfig = await AircraftOnwing.findOne(latestConfigFilter).sort({ date: -1 }).lean();
        if (onwingRowHasAsset(latestConfig, assetKey)) {
            matchingConfigs.push(latestConfig);
        }
    }
    matchingConfigs.sort((a, b) => moment.utc(b.date).valueOf() - moment.utc(a.date).valueOf());
    const owningAircraft = matchingConfigs[0] || null;

    const effectiveMsn = String(owningAircraft?.msn || assetKey).trim();
    const fleetFilter = effectiveMsn ? { sn: effectiveMsn } : {};
    if (userId) {
        fleetFilter.userId = String(userId);
    }

    const fleet = effectiveMsn ? await Fleet.findOne(fleetFilter).lean() : null;

    return {
        assetKey,
        effectiveMsn,
        fleet,
    };
};

const getAssumptionUsageForDate = ({ assumptions = [], effectiveMsn, date }) => {
    const msn = String(effectiveMsn || "").trim();
    if (!msn || !date) {
        return { timeUsage: 0, cycleUsage: 0, hasUsage: false };
    }

    const targetDate = moment.utc(date).startOf("day");
    const match = assumptions.find((assumption) => (
        String(assumption.msn || "").trim() === msn &&
        targetDate.isSameOrAfter(moment.utc(assumption.fromDate).startOf("day")) &&
        targetDate.isSameOrBefore(moment.utc(assumption.toDate).endOf("day"))
    ));

    if (!match) {
        return { timeUsage: 0, cycleUsage: 0, hasUsage: false };
    }

    return {
        timeUsage: Number(match.hours || 0),
        cycleUsage: Number(match.cycles || 0),
        hasUsage: true
    };
};

const getEffectiveUsageForDate = async ({ userId, effectiveMsn, date, metric, assumptions = [] }) => {
    if (!effectiveMsn) {
        return { timeUsage: 0, cycleUsage: 0, hasUsage: false };
    }

    const msnNumber = Number(effectiveMsn);
    if (!Number.isFinite(msnNumber)) {
        return getAssumptionUsageForDate({ assumptions, effectiveMsn, date });
    }

    const assignmentFilter = {
        ...(userId ? { userId: String(userId) } : {}),
        date: {
            $gte: date.toDate(),
            $lt: moment.utc(date).endOf("day").toDate()
        },
        "aircraft.msn": msnNumber
    };

    const assignments = await Assignment.find(assignmentFilter);

    if (assignments.length > 0) {
        const primaryTimeKey = metric === "FH" ? "flightHours" : "blockHours";
        const fallbackTimeKey = metric === "FH" ? "blockHours" : "flightHours";
        const flightNumbers = [...new Set(assignments
            .map((assignment) => String(assignment.flightNumber || "").trim().toUpperCase())
            .filter(Boolean))];
        const flightRegexArray = flightNumbers.map((flightNumber) => new RegExp(`^${escapeRegex(flightNumber)}$`, "i"));
        const flightRecords = flightRegexArray.length > 0
            ? await Flight.find({
                ...(userId ? { userId: String(userId) } : {}),
                date: {
                    $gte: date.toDate(),
                    $lt: moment.utc(date).endOf("day").toDate()
                },
                flight: { $in: flightRegexArray }
            }).select("flight bh fh").lean()
            : [];
        const flightsByNumber = new Map();
        flightRecords.forEach((flight) => {
            const flightNumber = String(flight.flight || "").trim().toUpperCase();
            if (!flightNumber) return;
            if (!flightsByNumber.has(flightNumber)) flightsByNumber.set(flightNumber, []);
            flightsByNumber.get(flightNumber).push(flight);
        });

        const sumAssignmentTime = (key) => assignments.reduce((sum, assignment) => {
            const metricValue = Number(assignment.metrics?.[key]);
            if (Number.isFinite(metricValue) && metricValue > 0) {
                return sum + metricValue;
            }

            const flightNumber = String(assignment.flightNumber || "").trim().toUpperCase();
            const matchedFlights = flightsByNumber.get(flightNumber) || [];
            const flightKey = key === "flightHours" ? "fh" : "bh";
            return sum + matchedFlights.reduce((flightSum, flight) => {
                const flightValue = Number(flight?.[flightKey]);
                return flightSum + (Number.isFinite(flightValue) ? flightValue : 0);
            }, 0);
        }, 0);

        const primaryTimeUsage = sumAssignmentTime(primaryTimeKey);
        const fallbackTimeUsage = sumAssignmentTime(fallbackTimeKey);
        const timeUsage = primaryTimeUsage || fallbackTimeUsage;
        const cycleUsage = assignments.reduce((sum, a) => {
            const cycles = Number(a.metrics?.cycles);
            return sum + (Number.isFinite(cycles) && cycles > 0 ? cycles : 1);
        }, 0);

        return {
            timeUsage,
            cycleUsage,
            hasUsage: true
        };
    }

    return getAssumptionUsageForDate({ assumptions, effectiveMsn, date });
};

const recomputeMaintenanceTimeline = async ({ userId, resetGroups: requestedResetGroups } = {}) => {
    const flightBounds = await getFlightDateBounds({ userId });

    if (!flightBounds?.firstDate || !flightBounds?.lastDate) {
        return {
            resetGroupCount: 0,
            message: "No flights found to compute.",
        };
    }

    const masterStartDate = moment.utc(flightBounds.firstDate).startOf("day");
    const masterEndDate = moment.utc(flightBounds.lastDate).endOf("day");

    const allCalendars = await MaintenanceCalendar.find({ userId: String(userId) }).lean();
    const utilisationAssumptions = await UtilisationAssumption.find({ userId: String(userId) }).lean();

    const resetGroups = Array.isArray(requestedResetGroups) && requestedResetGroups.length > 0
        ? [...new Map(
            requestedResetGroups
                .map(normalizeResetGroup)
                .filter(group => group.msnEsn && group.pn && group.snBn)
                .map(group => [buildResetGroupKey(group), { _id: group }])
        ).values()]
        : await MaintenanceReset.aggregate([
            { $match: { userId: String(userId) } },
            { $group: { _id: { msnEsn: "$msnEsn", pn: "$pn", snBn: "$snBn" } } }
        ]);

    const totalOps = [];
    const groundDayOps = [];
    const calendarEventState = new Map();
    const calendarIdsTouched = new Set();
    const assignmentImpact = {
        deletedCount: 0,
        clearedFlightCount: 0,
        daysTouched: 0
    };

    const affectedResetGroupKeys = new Set(resetGroups.map(group => buildResetGroupKey(group._id)));
    const affectedCalendarIds = allCalendars
        .filter(cal => affectedResetGroupKeys.has(buildResetGroupKey({
            msnEsn: cal.calMsn,
            pn: cal.calPn,
            snBn: cal.snBn
        })))
        .map(cal => String(cal._id));

    const isScopedRecompute = Array.isArray(requestedResetGroups) && requestedResetGroups.length > 0;
    if (allCalendars.length > 0 && (!isScopedRecompute || affectedCalendarIds.length > 0)) {
        const deleteGeneratedGroundDaysFilter = {
            userId: String(userId),
            source: "SCHEDULED_MAINTENANCE"
        };
        if (affectedCalendarIds.length > 0) {
            deleteGeneratedGroundDaysFilter.eventSeriesId = { $in: affectedCalendarIds };
        }
        await GroundDay.deleteMany(deleteGeneratedGroundDaysFilter);
    }

    const recordCalendarEvent = ({
        cal,
        state,
        date,
        triggerDefinition,
        preOccurrenceValues,
        projectedValues,
        downtimeApplied,
        postEventValues
    }) => {
        const key = String(cal._id);
        const previous = state || getCalendarState(calendarEventState, cal);

        const eventDate = moment.utc(date).startOf("day");
        const occurrence = previous.occurrence + 1;
        const triggeredGroups = getCalendarTriggeredGroups([triggerDefinition]);
        const soTsr = triggerDefinition.group === "replacement"
            ? projectedValues.tsRplmt
            : triggerDefinition.group === "restoration"
                ? projectedValues.tsoTsr
                : projectedValues.tsn;
        const suppressedAlternateThresholds = [];
        const triggerThreshold = normalizeThresholdNumber(triggerDefinition.threshold);

        const triggerMetricResetByPostEvent = Object.prototype.hasOwnProperty.call(
            postEventValues || {},
            triggerDefinition.valueKey
        );
        if (!triggerMetricResetByPostEvent) {
            addSuppressedThreshold(previous, triggerDefinition.metricCode, triggerThreshold);
        }
        if (triggerDefinition.group === "sinceNew") {
            previous.triggeredSinceNewMetrics.add(triggerDefinition.metricCode);
        }

        for (const alternateDefinition of getCalendarTriggerDefinitions(cal)) {
            if (alternateDefinition.metricCode === triggerDefinition.metricCode) continue;
            if (Object.prototype.hasOwnProperty.call(postEventValues || {}, alternateDefinition.valueKey)) continue;

            const preOccurrenceValue = normalizeMetricNumber(preOccurrenceValues?.[alternateDefinition.valueKey]);
            const firstSuppressedThreshold = nextMultipleAfter(preOccurrenceValue, alternateDefinition.interval);
            const thresholdsToSuppress = [];
            if (firstSuppressedThreshold !== null) {
                thresholdsToSuppress.push(firstSuppressedThreshold);
                if (
                    occurrence === 1 &&
                    preOccurrenceValue !== null &&
                    preOccurrenceValue > 0 &&
                    preOccurrenceValue < alternateDefinition.interval
                ) {
                    thresholdsToSuppress.push(normalizeThresholdNumber(firstSuppressedThreshold + alternateDefinition.interval));
                }
            }

            for (const suppressedThreshold of thresholdsToSuppress) {
                const wasMissing = addSuppressedThreshold(previous, alternateDefinition.metricCode, suppressedThreshold);
                if (wasMissing) {
                    suppressedAlternateThresholds.push({
                        metricCode: alternateDefinition.label,
                        suppressedThreshold,
                        reason: `Same ${cal.schEvent || "maintenance"} requirement already satisfied by ${triggerDefinition.label} on ${eventDate.format("YYYY-MM-DD")}.`
                    });
                }
            }
        }

        const groundStartDate = downtimeApplied > 0 ? eventDate.toDate() : null;
        const groundEndDate = downtimeApplied > 0
            ? eventDate.clone().add(downtimeApplied - 1, "days").toDate()
            : null;
        const generatedOccurrence = {
            occurrenceNumber: occurrence,
            triggerRelationship: cal.triggerRelationship || "EARLIEST_OF_EVERY",
            triggerDate: eventDate.toDate(),
            triggeredByMetric: triggerDefinition.label,
            triggerThreshold,
            triggerMetricValueOnDetectionDate: normalizeMetricNumber(projectedValues?.[triggerDefinition.valueKey]),
            groundStartDate,
            groundEndDate,
            downtimeApplied,
            isFirstOccurrence: occurrence === 1,
            postEventStatusApplied: postEventValues || {},
            suppressedAlternateThresholds
        };

        calendarEventState.set(key, {
            ...previous,
            occurrence,
            lastOccurre: eventDate.toDate(),
            nextEstima: previous.nextEstima || eventDate.toDate(),
            firstOccurrenceDate: previous.firstOccurrenceDate || eventDate.toDate(),
            occurrencesTillExit: Math.max(0, occurrence - 1),
            soTsr: Number.isFinite(Number(soTsr)) ? Number(soTsr) : previous.soTsr,
            triggeredSinceNew: previous.triggeredSinceNew || triggeredGroups.includes("sinceNew"),
            generatedOccurrences: [...previous.generatedOccurrences, generatedOccurrence],
            suppressedAlternateThresholds: [
                ...previous.suppressedAlternateThresholds,
                ...suppressedAlternateThresholds.map(item => ({
                    occurrenceNumber: occurrence,
                    ...item
                }))
            ]
        });

        return generatedOccurrence;
    };

    for (const group of resetGroups) {
        const { msnEsn, pn, snBn } = group._id;

        const resets = await MaintenanceReset.find({
            userId: String(userId),
            msnEsn,
            pn,
            snBn
        }).sort({ date: 1 }).lean();
        if (resets.length === 0) continue;

        for (let i = 0; i < resets.length; i++) {
            const currentReset = resets[i];
            const nextReset = resets[i + 1];
            const utilizationContext = await getEffectiveUtilisationContext({
                userId,
                msnEsn,
                date: currentReset.date
            });
            const effectiveMsn = utilizationContext.effectiveMsn || msnEsn;
            const fleet = utilizationContext.fleet || await Fleet.findOne({
                ...(userId ? { userId: String(userId) } : {}),
                sn: effectiveMsn
            }).lean();
            const utilisationWindow = getUtilisationWindow({
                masterStartDate,
                masterEndDate,
                fleet
            });
            const startBoundaryDate = utilisationWindow.startBoundaryDate || masterStartDate;
            let endBoundaryDate = utilisationWindow.endBoundaryDate || masterEndDate;

            const resetDate = moment.utc(currentReset.date).startOf("day");

            if (i === 0) {
                let backfillCursor = moment.utc(resetDate);
                let currentTsn = normalizeMetricNumber(currentReset.tsn);
                let currentCsn = normalizeMetricNumber(currentReset.csn);
                let currentDsn = normalizeMetricNumber(currentReset.dsn);
                let currentTso = normalizeMetricNumber(currentReset.tsoTsr);
                let currentCso = normalizeMetricNumber(currentReset.csoCsr);
                let currentDso = normalizeMetricNumber(currentReset.dsoDsr);
                let currentTsr = normalizeMetricNumber(currentReset.tsRplmt);
                let currentCsr = normalizeMetricNumber(currentReset.csRplmt);
                let currentDsr = normalizeMetricNumber(currentReset.dsRplmt);

                while (backfillCursor.isAfter(startBoundaryDate)) {
                    const targetDate = moment.utc(backfillCursor).subtract(1, "day").startOf("day");
                    const backfillUtilizationContext = await getEffectiveUtilisationContext({
                        userId,
                        msnEsn,
                        date: backfillCursor
                    });

                    const { timeUsage, cycleUsage } = await getEffectiveUsageForDate({
                        userId,
                        effectiveMsn: backfillUtilizationContext.effectiveMsn || msnEsn,
                        date: backfillCursor,
                        metric: currentReset.timeMetric,
                        assumptions: utilisationAssumptions
                    });
                    const dayUsage = 1;

                    if (currentTsn !== null) currentTsn = Number((currentTsn - timeUsage).toFixed(2));
                    if (currentCsn !== null) currentCsn -= cycleUsage;
                    if (currentDsn !== null) currentDsn -= dayUsage;
                    if (currentTso !== null) currentTso = Number((currentTso - timeUsage).toFixed(2));
                    if (currentCso !== null) currentCso -= cycleUsage;
                    if (currentDso !== null) currentDso -= dayUsage;
                    if (currentTsr !== null) currentTsr = Number((currentTsr - timeUsage).toFixed(2));
                    if (currentCsr !== null) currentCsr -= cycleUsage;
                    if (currentDsr !== null) currentDsr -= dayUsage;

                    totalOps.push({
                        updateOne: {
                            filter: { userId: String(userId), date: targetDate.toDate(), msnEsn, pn, snBn },
                            update: {
                                $set: {
                                    userId: String(userId),
                                    date: targetDate.toDate(),
                                    msnEsn, pn, snBn,
                                    tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                    tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                    tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                    timeMetric: currentReset.timeMetric
                                },
                                $unset: { setFlag: "", remarks: "" }
                            },
                            upsert: true
                        }
                    });

                    backfillCursor = targetDate;
                }
            }

            totalOps.push({
                updateOne: {
                    filter: { userId: String(userId), date: resetDate.toDate(), msnEsn, pn, snBn },
                    update: {
                        $set: {
                            userId: String(userId),
                            date: resetDate.toDate(),
                            msnEsn, pn, snBn,
                            tsn: normalizeMetricNumber(currentReset.tsn),
                            csn: normalizeMetricNumber(currentReset.csn),
                            dsn: normalizeMetricNumber(currentReset.dsn),
                            tsoTsr: normalizeMetricNumber(currentReset.tsoTsr),
                            csoCsr: normalizeMetricNumber(currentReset.csoCsr),
                            dsoDsr: normalizeMetricNumber(currentReset.dsoDsr),
                            tsRplmt: normalizeMetricNumber(currentReset.tsRplmt),
                            csRplmt: normalizeMetricNumber(currentReset.csRplmt),
                            dsRplmt: normalizeMetricNumber(currentReset.dsRplmt),
                            timeMetric: currentReset.timeMetric, setFlag: "Y", remarks: "(reset point)"
                        }
                    },
                    upsert: true
                }
            });

            const segmentEnd = nextReset ? moment.utc(nextReset.date).subtract(1, "day").startOf("day") : endBoundaryDate;
            let currDate = moment.utc(resetDate).add(1, "days").startOf("day");

            let currentTsn = normalizeMetricNumber(currentReset.tsn);
            let currentCsn = normalizeMetricNumber(currentReset.csn);
            let currentDsn = normalizeMetricNumber(currentReset.dsn);
            let currentTso = normalizeMetricNumber(currentReset.tsoTsr);
            let currentCso = normalizeMetricNumber(currentReset.csoCsr);
            let currentDso = normalizeMetricNumber(currentReset.dsoDsr);
            let currentTsr = normalizeMetricNumber(currentReset.tsRplmt);
            let currentCsr = normalizeMetricNumber(currentReset.csRplmt);
            let currentDsr = normalizeMetricNumber(currentReset.dsRplmt);

            const assetCalendars = allCalendars.filter(c =>
                String(c.calMsn) === String(msnEsn) &&
                String(c.calPn) === String(pn) &&
                String(c.snBn) === String(snBn)
            );
            assetCalendars.forEach(c => calendarIdsTouched.add(String(c._id)));
            let inMaintenanceUntil = null;
            let pendingPostEvents = [];

            while (currDate.isSameOrBefore(segmentEnd)) {
                const currentUtilizationContext = await getEffectiveUtilisationContext({
                    userId,
                    msnEsn,
                    date: currDate
                });
                const currentEffectiveMsn = currentUtilizationContext.effectiveMsn || msnEsn;

                if (inMaintenanceUntil && currDate.isSameOrBefore(inMaintenanceUntil)) {
                    if (currentDsn !== null) currentDsn += 1;
                    if (currentDso !== null) currentDso += 1;
                    if (currentDsr !== null) currentDsr += 1;

                    const duePostEvents = pendingPostEvents.filter(event => currDate.isSame(event.date, "day"));
                    if (duePostEvents.length > 0) {
                        for (const postEvent of duePostEvents) {
                            ({ currentTso, currentCso, currentDso, currentTsr, currentCsr, currentDsr } = applyPostEventValuesToCurrent({
                                currentTso,
                                currentCso,
                                currentDso,
                                currentTsr,
                                currentCsr,
                                currentDsr
                            }, postEvent.values));
                        }
                        pendingPostEvents = pendingPostEvents.filter(event => !currDate.isSame(event.date, "day"));
                    }

                    totalOps.push({
                        updateOne: {
                            filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                            update: {
                                $set: {
                                    userId: String(userId),
                                    date: currDate.toDate(),
                                    msnEsn, pn, snBn,
                                    tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                    tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                    tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                    timeMetric: currentReset.timeMetric,
                                    remarks: "Maintenance Downtime"
                                },
                                $unset: { setFlag: "" }
                            },
                            upsert: true
                        }
                    });
                    currDate.add(1, "days");
                    continue;
                }

                const { timeUsage, cycleUsage } = await getEffectiveUsageForDate({
                    userId,
                    effectiveMsn: currentEffectiveMsn,
                    date: currDate,
                    metric: currentReset.timeMetric,
                    assumptions: utilisationAssumptions
                });
                const dayUsage = 1;

                const projectedTsn = currentTsn !== null ? Number((currentTsn + timeUsage).toFixed(2)) : null;
                const projectedCsn = currentCsn !== null ? currentCsn + cycleUsage : null;
                const projectedDsn = currentDsn !== null ? currentDsn + dayUsage : null;

                const projectedTso = currentTso !== null ? Number((currentTso + timeUsage).toFixed(2)) : null;
                const projectedCso = currentCso !== null ? currentCso + cycleUsage : null;
                const projectedDso = currentDso !== null ? currentDso + dayUsage : null;

                const projectedTsr = currentTsr !== null ? Number((currentTsr + timeUsage).toFixed(2)) : null;
                const projectedCsr = currentCsr !== null ? currentCsr + cycleUsage : null;
                const projectedDsr = currentDsr !== null ? currentDsr + dayUsage : null;

                const projectedValues = {
                    tsn: projectedTsn,
                    csn: projectedCsn,
                    dsn: projectedDsn,
                    tsoTsr: projectedTso,
                    csoCsr: projectedCso,
                    dsoDsr: projectedDso,
                    tsRplmt: projectedTsr,
                    csRplmt: projectedCsr,
                    dsRplmt: projectedDsr
                };

                const currentValues = getMetricValuesFromCurrent({
                    currentTsn,
                    currentCsn,
                    currentDsn,
                    currentTso,
                    currentCso,
                    currentDso,
                    currentTsr,
                    currentCsr,
                    currentDsr
                });
                const triggeredCalendars = [];
                let maxDownDaysToApply = 0;

                for (const cal of assetCalendars) {
                    const state = getCalendarState(calendarEventState, cal);
                    const triggerDefinition = findCalendarTriggerCandidate({
                        cal,
                        state,
                        currentValues,
                        projectedValues
                    });

                    if (triggerDefinition) {
                        const occurrenceNumber = state.occurrence + 1;
                        const downDaysToApply = getCalendarDowntimeDaysForOccurrence(cal, occurrenceNumber);
                        triggeredCalendars.push({
                            cal,
                            state,
                            triggerDefinition,
                            downDaysToApply
                        });
                        maxDownDaysToApply = Math.max(maxDownDaysToApply, downDaysToApply);
                    }
                }

                if (triggeredCalendars.length > 0) {
                    if (maxDownDaysToApply > 0) {
                        inMaintenanceUntil = moment.utc(currDate).add(maxDownDaysToApply - 1, "days");
                    }

                    if (maxDownDaysToApply <= 0) {
                        currentTsn = projectedTsn;
                        currentCsn = projectedCsn;
                        currentDsn = projectedDsn;
                        currentTso = projectedTso;
                        currentCso = projectedCso;
                        currentDso = projectedDso;
                        currentTsr = projectedTsr;
                        currentCsr = projectedCsr;
                        currentDsr = projectedDsr;
                    }

                    const immediatePostEventValues = {};
                    for (const { cal, state, triggerDefinition, downDaysToApply } of triggeredCalendars) {
                        const postEventValues = getPostEventValuesForTriggeredMetric(cal, triggerDefinition);
                        const occurrence = recordCalendarEvent({
                            cal,
                            state,
                            date: currDate,
                            triggerDefinition,
                            preOccurrenceValues: currentValues,
                            projectedValues,
                            downtimeApplied: downDaysToApply,
                            postEventValues
                        });

                        groundDayOps.push(...getCalendarGroundDateRows({
                            userId,
                            msn: currentEffectiveMsn,
                            cal,
                            occurrenceNumber: occurrence.occurrenceNumber,
                            startDate: currDate,
                            downtimeDays: downDaysToApply
                        }).map(row => ({
                            updateOne: {
                                filter: {
                                    userId: row.userId,
                                    msn: row.msn,
                                    date: row.date,
                                    eventSeriesId: row.eventSeriesId,
                                    occurrenceNumber: row.occurrenceNumber
                                },
                                update: { $set: row },
                                upsert: true
                            }
                        })));

                        if (Object.keys(postEventValues).length > 0) {
                            const postEventApplyDate = moment.utc(currDate)
                                .add(Math.max(1, downDaysToApply) - 1, "days")
                                .startOf("day");

                            if (postEventApplyDate.isSame(currDate, "day")) {
                                Object.assign(immediatePostEventValues, postEventValues);
                            } else {
                                pendingPostEvents.push({
                                    date: postEventApplyDate,
                                    values: postEventValues
                                });
                            }
                        }
                    }

                    if (Object.keys(immediatePostEventValues).length > 0) {
                        ({ currentTso, currentCso, currentDso, currentTsr, currentCsr, currentDsr } = applyPostEventValuesToCurrent({
                            currentTso,
                            currentCso,
                            currentDso,
                            currentTsr,
                            currentCsr,
                            currentDsr
                        }, immediatePostEventValues));
                    }

                    totalOps.push({
                        updateOne: {
                            filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                            update: {
                                $set: {
                                    userId: String(userId),
                                    date: currDate.toDate(),
                                    msnEsn, pn, snBn,
                                    tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                    tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                    tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                    timeMetric: currentReset.timeMetric,
                                    remarks: "Maintenance Check Triggered"
                                },
                                $unset: { setFlag: "" }
                            },
                            upsert: true
                        }
                    });

                    currDate.add(1, "days");
                    continue;
                }

                currentTsn = projectedTsn;
                currentCsn = projectedCsn;
                currentDsn = projectedDsn;

                currentTso = projectedTso;
                currentCso = projectedCso;
                currentDso = projectedDso;

                currentTsr = projectedTsr;
                currentCsr = projectedCsr;
                currentDsr = projectedDsr;

                totalOps.push({
                    updateOne: {
                        filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                        update: {
                            $set: {
                                userId: String(userId),
                                date: currDate.toDate(),
                                msnEsn, pn, snBn,
                                tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                timeMetric: currentReset.timeMetric
                            },
                            $unset: { setFlag: "", remarks: "" }
                        },
                        upsert: true
                    }
                });

                currDate.add(1, "days");
            }
        }
    }

    if (totalOps.length > 0) {
        const chunkSize = 1000;
        for (let i = 0; i < totalOps.length; i += chunkSize) {
            const chunk = totalOps.slice(i, i + chunkSize);
            await Utilisation.bulkWrite(chunk, { ordered: false });
        }
    }

    if (groundDayOps.length > 0) {
        const chunkSize = 1000;
        for (let i = 0; i < groundDayOps.length; i += chunkSize) {
            const chunk = groundDayOps.slice(i, i + chunkSize);
            await GroundDay.bulkWrite(chunk, { ordered: false });
        }
    }

    if (calendarIdsTouched.size > 0) {
        await MaintenanceCalendar.bulkWrite([...calendarIdsTouched].map(calendarId => {
            const event = calendarEventState.get(calendarId) || {
                id: calendarId,
                lastOccurre: null,
                nextEstima: null,
                firstOccurrenceDate: null,
                occurrence: 0,
                occurrencesTillExit: 0,
                soTsr: null
            };

            return {
                updateOne: {
                    filter: { _id: event.id, userId: String(userId) },
                    update: {
                        $set: {
                            lastOccurre: event.lastOccurre,
                            nextEstima: event.nextEstima,
                            occurrence: event.occurrence,
                            firstOccurrenceDate: event.firstOccurrenceDate,
                            occurrencesTillExit: event.occurrencesTillExit || Math.max(0, Number(event.occurrence || 0) - 1),
                            soTsr: event.soTsr,
                            generatedOccurrences: event.generatedOccurrences || [],
                            suppressedAlternateThresholds: event.suppressedAlternateThresholds || [],
                            suppressedThresholds: serializeSuppressedThresholds(event.suppressedThresholdsByMetric)
                        }
                    }
                }
            };
        }));
    }

    return {
        resetGroupCount: resetGroups.length,
        message: `Maintenance logic computed for ${resetGroups.length} assets.`,
        assignmentImpact,
    };
};

const getAssignmentUsageForDate = async ({ userId, effectiveMsn, date, metric }) => {
    if (!effectiveMsn) {
        return { timeUsage: 0, cycleUsage: 0 };
    }

    const msnNumber = Number(effectiveMsn);
    if (!Number.isFinite(msnNumber)) {
        return { timeUsage: 0, cycleUsage: 0 };
    }

    const assignments = await Assignment.find({
        ...(userId ? { userId: String(userId) } : {}),
        date: {
            $gte: date.toDate(),
            $lt: moment(date).endOf("day").toDate()
        },
        "aircraft.msn": msnNumber
    });

    const primaryTimeKey = metric === "FH" ? "flightHours" : "blockHours";
    const fallbackTimeKey = metric === "FH" ? "blockHours" : "flightHours";
    const primaryTimeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.[primaryTimeKey] || 0), 0);
    const fallbackTimeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.[fallbackTimeKey] || 0), 0);
    const timeUsage = primaryTimeUsage || fallbackTimeUsage;

    return {
        timeUsage,
        cycleUsage: assignments.reduce((sum, a) => {
            const cycles = Number(a.metrics?.cycles);
            return sum + (Number.isFinite(cycles) && cycles > 0 ? cycles : 1);
        }, 0)
    };
};

const getUtilisationWindow = ({ masterStartDate, masterEndDate, fleet }) => {
    let startBoundaryDate = masterStartDate ? moment.utc(masterStartDate) : null;
    let endBoundaryDate = masterEndDate ? moment.utc(masterEndDate) : null;

    if (fleet?.entry) {
        const fleetEntry = moment.utc(fleet.entry).startOf("day");
        startBoundaryDate = startBoundaryDate ? moment.max(startBoundaryDate, fleetEntry) : fleetEntry;
    }

    if (fleet?.exit) {
        const fleetExit = moment.utc(fleet.exit).endOf("day");
        endBoundaryDate = endBoundaryDate ? moment.min(endBoundaryDate, fleetExit) : fleetExit;
    }

    return { startBoundaryDate, endBoundaryDate };
};

const buildMaintenanceStatusFromReset = async ({ userId, reset, selectedDate, assumptions = [] }) => {
    if (!reset || !selectedDate) return null;

    const resetDate = moment.utc(reset.date).startOf("day");
    const targetDate = moment.utc(selectedDate).startOf("day");
    if (!resetDate.isValid() || !targetDate.isValid()) return null;

    let currentTsn = normalizeMetricNumber(reset.tsn);
    let currentCsn = normalizeMetricNumber(reset.csn);
    let currentDsn = normalizeMetricNumber(reset.dsn);
    let currentTso = normalizeMetricNumber(reset.tsoTsr);
    let currentCso = normalizeMetricNumber(reset.csoCsr);
    let currentDso = normalizeMetricNumber(reset.dsoDsr);
    let currentTsr = normalizeMetricNumber(reset.tsRplmt);
    let currentCsr = normalizeMetricNumber(reset.csRplmt);
    let currentDsr = normalizeMetricNumber(reset.dsRplmt);

    const applyUsage = async (date, direction) => {
        const utilizationContext = await getEffectiveUtilisationContext({
            userId,
            msnEsn: reset.msnEsn,
            date
        });
        const { timeUsage, cycleUsage } = await getEffectiveUsageForDate({
            userId,
            effectiveMsn: utilizationContext.effectiveMsn || reset.msnEsn,
            date,
            metric: reset.timeMetric,
            assumptions
        });
        const dayUsage = 1;

        if (currentTsn !== null) currentTsn = Number((currentTsn + (direction * timeUsage)).toFixed(2));
        if (currentCsn !== null) currentCsn += direction * cycleUsage;
        if (currentDsn !== null) currentDsn += direction * dayUsage;
        if (currentTso !== null) currentTso = Number((currentTso + (direction * timeUsage)).toFixed(2));
        if (currentCso !== null) currentCso += direction * cycleUsage;
        if (currentDso !== null) currentDso += direction * dayUsage;
        if (currentTsr !== null) currentTsr = Number((currentTsr + (direction * timeUsage)).toFixed(2));
        if (currentCsr !== null) currentCsr += direction * cycleUsage;
        if (currentDsr !== null) currentDsr += direction * dayUsage;
    };

    if (targetDate.isBefore(resetDate)) {
        for (let cursor = resetDate.clone(); cursor.isAfter(targetDate); cursor.subtract(1, "day")) {
            await applyUsage(cursor.clone(), -1);
        }
    } else if (targetDate.isAfter(resetDate)) {
        for (let cursor = resetDate.clone().add(1, "day"); cursor.isSameOrBefore(targetDate); cursor.add(1, "day")) {
            await applyUsage(cursor.clone(), 1);
        }
    }

    return {
        tsn: currentTsn,
        csn: currentCsn,
        dsn: currentDsn,
        tsoTsr: currentTso,
        csoCsr: currentCso,
        dsoDsr: currentDso,
        tsRplmt: currentTsr,
        csRplmt: currentCsr,
        dsRplmt: currentDsr,
        timeMetric: reset.timeMetric
    };
};

const targetMetricFields = [
    { key: "tsn", utilKey: "tsn" },
    { key: "csn", utilKey: "csn" },
    { key: "dsn", utilKey: "dsn" },
    { key: "tso", utilKey: "tsoTsr" },
    { key: "cso", utilKey: "csoCsr" },
    { key: "dso", utilKey: "dsoDsr" },
    { key: "tsRplmt", utilKey: "tsRplmt" },
    { key: "csRplmt", utilKey: "csRplmt" },
    { key: "dsRplmt", utilKey: "dsRplmt" },
];

const parseMetricValue = normalizeMetricNumber;

const roundMetricDelta = (value) => Number(value.toFixed(2));

const getMaintenanceDashboardDateWindow = async ({ userId } = {}) => {
    const flightBounds = await getFlightDateBounds({ userId });
    if (!flightBounds?.firstDate || !flightBounds?.lastDate) {
        return null;
    }

    const startDate = moment.utc(flightBounds.firstDate).startOf("day");
    const endDate = moment.utc(flightBounds.lastDate).endOf("day");

    return {
        startDate,
        openingBalanceDate: startDate.clone().subtract(1, "day").startOf("day"),
        endDate
    };
};

/**
 * 1. GET: Fetch Main Dashboard Data
 */
exports.getMaintenanceDashboard = async (req, res) => {
    try {
        // Assuming verifyToken middleware attaches the user to req.user
        const userId = getUserIdFromReq(req);

        const aircraft = [];

        // 2. Fetch recent Utilisation
        const utilisation = await Utilisation.find({ userId })
            .sort({ date: -1 })
            .limit(10)
            .lean();

        // 3. Fetch Maintenance Status
        const status = await MaintenanceStatus.find({ userId }).lean();

        // 4. Fetch Rotable Movements
        const rotables = await RotableMovement.find({ userId })
            .sort({ date: -1 })
            .limit(10)
            .lean();

        // 5. Build maintenance dashboard rows from the reset/status model
        let maintenanceData = [];
        const { date, msnEsn } = req.query;

        if (date) {
            const selectedDateBounds = getUtcDayBounds(date);
            if (!selectedDateBounds) {
                return res.status(400).json({ success: false, message: "Invalid date" });
            }

            const dashboardWindow = await getMaintenanceDashboardDateWindow({ userId });
            if (dashboardWindow) {
                const requestedDate = selectedDateBounds.start.clone();
                const isBeforeWindow = requestedDate.isBefore(dashboardWindow.openingBalanceDate);
                const isAfterWindow = requestedDate.isAfter(dashboardWindow.endDate);

                if (isBeforeWindow || isAfterWindow) {
                    return res.status(200).json({
                        success: true,
                        data: {
                            maintenanceData: [],
                            aircraft,
                            utilisation,
                            status,
                            rotables
                        }
                    });
                }
            }

            const startOfDay = selectedDateBounds.start.toDate();
            const endOfDay = selectedDateBounds.endExclusive.toDate();
            const utilFilter = {
                date: { $gte: startOfDay, $lt: endOfDay }
            };
            if (userId) {
                utilFilter.userId = String(userId);
            }
            if (msnEsn) {
                utilFilter.msnEsn = { $regex: `^${escapeRegex(msnEsn.trim())}$`, $options: "i" };
            }

            const resetFilter = {};
            if (msnEsn) {
                resetFilter.msnEsn = { $regex: `^${escapeRegex(msnEsn.trim())}$`, $options: "i" };
            }
            if (userId) {
                resetFilter.userId = String(userId);
            }

            const fleetFilter = {};
            if (userId) {
                fleetFilter.userId = String(userId);
            }
            fleetFilter.$and = [
                { $or: [{ entry: { $exists: false } }, { entry: null }, { entry: { $lte: endOfDay } }] },
                { $or: [{ exit: { $exists: false } }, { exit: null }, { exit: { $gte: startOfDay } }] }
            ];

            const allFleetFilter = {};
            if (userId) {
                allFleetFilter.userId = String(userId);
            }

            const [utils, resetRecords, fleetAssets, allFleetAssets, utilisationAssumptions, calendarRows] = await Promise.all([
                Utilisation.find(utilFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
                MaintenanceReset.find(resetFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
                Fleet.find(fleetFilter).select("sn titled regn").lean(),
                Fleet.find(allFleetFilter).select("sn titled regn").lean(),
                UtilisationAssumption.find({ userId: String(userId) }).lean(),
                MaintenanceCalendar.find({ userId: String(userId) }).select("calMsn calPn snBn").lean(),
            ]);

            const getFleetTitledDisplay = (asset = {}) =>
                String(asset.titled || asset.regn || "").trim();
            const addTitledBySn = (map, asset) => {
                const sn = String(asset.sn || "").trim().toUpperCase();
                const titledDisplay = getFleetTitledDisplay(asset);
                if (sn && titledDisplay && !map.has(sn)) {
                    map.set(sn, titledDisplay);
                }
            };
            const titledBySn = new Map();
            fleetAssets.forEach(asset => addTitledBySn(titledBySn, asset));
            allFleetAssets.forEach(asset => addTitledBySn(titledBySn, asset));
            const activeFleetSnSet = new Set(
                allFleetAssets
                    .map(asset => String(asset.sn || "").trim().toUpperCase())
                    .filter(Boolean)
            );

            const utilByKey = new Map();
            utils.forEach(record => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                if (!utilByKey.has(key)) {
                    utilByKey.set(key, record);
                }
            });

            const resetRecordsByKey = new Map();
            resetRecords.forEach(record => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                const recordsForKey = resetRecordsByKey.get(key) || [];
                recordsForKey.push(record);
                resetRecordsByKey.set(key, recordsForKey);
            });

            const calendarKeys = new Set((calendarRows || []).map(record => [
                String(record.calMsn || "").trim().toUpperCase(),
                String(record.calPn || "").trim().toUpperCase(),
                String(record.snBn || "").trim().toUpperCase()
            ].join("|")));

            const selectedDateMoment = selectedDateBounds.start.clone().endOf("day");
            const selectedDate = selectedDateMoment.format("YYYY-MM-DD");
            const rowSourcesByKey = new Map(utilByKey);
            resetRecords.forEach(record => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                if (!rowSourcesByKey.has(key)) {
                    rowSourcesByKey.set(key, record);
                }
            });

            const rowSourceEntries = Array.from(rowSourcesByKey.entries()).filter(([utilKey]) => {
                if (activeFleetSnSet.size === 0) return true;
                const [msnEsn, , snBn] = utilKey.split("|");
                return activeFleetSnSet.has(msnEsn) || activeFleetSnSet.has(snBn);
            });

            const rows = await Promise.all(rowSourceEntries.map(async ([utilKey, util]) => {
                const resetRecordsForKey = resetRecordsByKey.get(utilKey) || [];
                const record = resetRecordsForKey.find(reset =>
                    moment.utc(reset.date).isSameOrBefore(selectedDateMoment)
                ) || [...resetRecordsForKey].reverse().find(reset =>
                    moment.utc(reset.date).isAfter(selectedDateMoment)
                ) || util;
                const exactResetRecord = resetRecordsForKey.find(reset =>
                    isSameUtcDay(reset.date, selectedDateMoment)
                );
                const sourceRecord = exactResetRecord || record || util;
                const savedResetDate = sourceRecord?.date ? moment.utc(sourceRecord.date).format("YYYY-MM-DD") : "";
                const computedMetricSource = sourceRecord
                    ? await buildMaintenanceStatusFromReset({
                        userId,
                        reset: sourceRecord,
                        selectedDate: selectedDateMoment,
                        assumptions: utilisationAssumptions
                    })
                    : null;
                const hasCalendarSchedule = calendarKeys.has(utilKey);
                const metricSource = exactResetRecord
                    || (hasCalendarSchedule && util ? util : null)
                    || computedMetricSource
                    || util
                    || sourceRecord;

                return {
                    id: sourceRecord?._id,
                    msn: sourceRecord?.msnEsn || "",
                    msnEsn: sourceRecord?.msnEsn || "",
                    pn: sourceRecord?.pn || "",
                    sn: sourceRecord?.snBn || "",
                    snBn: sourceRecord?.snBn || "",
                    titled: titledBySn.get(String(sourceRecord?.msnEsn || "").trim().toUpperCase()) || "",
                    date: savedResetDate,
                    savedResetDate,
                    asOnDate: selectedDate,
                    resetDate: savedResetDate,
                    timeMetric: sourceRecord?.timeMetric || "BH",
                    tsn: metricSource?.tsn ?? sourceRecord?.tsn ?? "",
                    csn: metricSource?.csn ?? sourceRecord?.csn ?? "",
                    dsn: metricSource?.dsn ?? sourceRecord?.dsn ?? "",
                    tso: metricSource?.tsoTsr ?? sourceRecord?.tsoTsr ?? "",
                    cso: metricSource?.csoCsr ?? sourceRecord?.csoCsr ?? "",
                    dso: metricSource?.dsoDsr ?? sourceRecord?.dsoDsr ?? "",
                    tsr: metricSource?.tsRplmt ?? sourceRecord?.tsRplmt ?? "",
                    csr: metricSource?.csRplmt ?? sourceRecord?.csRplmt ?? "",
                    dsr: metricSource?.dsRplmt ?? sourceRecord?.dsRplmt ?? "",
                    allDisplay: ""
                };
            }));

            maintenanceData = rows;
        }

        res.status(200).json({
            success: true,
            data: {
                maintenanceData, // The aggregated frontend status table
                aircraft,
                utilisation,
                status,
                rotables
            }
        });
    } catch (error) {
        console.error("Error fetching maintenance dashboard:", error);
        res.status(500).json({ success: false, message: "Server Error" });
    }
};

/**
 * 2. GET: Fetch records for the "Set/Reset Maintenance status" Modal
 */
exports.getResetRecords = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { date, msnEsn } = req.query;
        const filter = {};

        if (date) {
            const selectedDateBounds = getUtcDayBounds(date);
            if (!selectedDateBounds) {
                return res.status(400).json({ message: "Invalid reset date." });
            }
            filter.date = {
                $gte: selectedDateBounds.start.toDate(),
                $lt: selectedDateBounds.endExclusive.toDate()
            };
        }

        if (msnEsn) {
            filter.msnEsn = { $regex: `^${escapeRegex(msnEsn.trim())}$`, $options: "i" };
        }
        if (userId) {
            filter.userId = String(userId);
        }

        const resetRecords = await MaintenanceReset.find(filter)
            .sort({ date: -1, updatedAt: -1, createdAt: -1, msnEsn: 1 })
            .lean();

        // Format data for the React frontend (map _id to id, format dates)
        const formattedRecords = resetRecords.map(record => ({
            id: record._id,
            date: moment(record.date).format("YYYY-MM-DD"),
            msnEsn: record.msnEsn || "",
            pn: record.pn || "",
            snBn: record.snBn || "",
            tsn: record.tsn || "",
            csn: record.csn || "",
            dsn: record.dsn || "",
            tso: record.tsoTsr || "",
            cso: record.csoCsr || "",
            dso: record.dsoDsr || "",
            tsr: record.tsRplmt || "",
            csr: record.csRplmt || "",
            dsr: record.dsRplmt || "",
            metric: record.timeMetric || "BH"
        }));

        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("🔥 Error fetching reset records:", error);
        res.status(500).json({ message: "Failed to fetch reset records", error: error.message });
    }
};

/**
 * 3. POST: Save/Update records from the "Set/Reset Maintenance status" Modal
 * Handles bulk upserts when the user clicks the "Update" button.
 */
exports.bulkSaveResetRecords = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const resetData = Array.isArray(req.body) ? req.body : req.body?.resetData;
        const fallbackResetDate = req.body?.resetDate || req.body?.date || req.body?.asOnDate || "";

        if (!resetData || !Array.isArray(resetData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const replaceExistingResetOps = [];
        const saveDateStrings = new Set();
        const changedResetGroups = new Map();
        const seenResetGroups = new Set();

        // Use a for...of loop to handle async Await calls
        for (const record of resetData) {
            const msnEsn = String(record.msnEsn || "").trim();
            const pn = String(record.pn || "").trim();
            const snBn = String(record.snBn || "").trim();
            const resetGroup = { msnEsn, pn, snBn };
            const values = [
                record.msnEsn, record.pn, record.snBn, record.tsn, record.csn, record.dsn,
                record.tso, record.cso, record.dso, record.tsr, record.csr, record.dsr
            ];
            const hasAnyValue = values.some(value => String(value ?? "").trim() !== "");
            if (!hasAnyValue) continue;

            if (!msnEsn || !pn || !snBn) {
                return res.status(400).json({
                    success: false,
                    message: "MSN/ESN, PN, and SN/BN are required before saving reset records."
                });
            }

            const resetGroupKey = buildResetGroupKey(resetGroup);
            if (seenResetGroups.has(resetGroupKey)) {
                return res.status(400).json({
                    success: false,
                    message: `Only one maintenance status setting is allowed for ${msnEsn}/${pn}/${snBn}.`
                });
            }
            seenResetGroups.add(resetGroupKey);

            const rawDate = fallbackResetDate || record.date;
            const parsedDate = moment.utc(rawDate, moment.ISO_8601, true);

            if (!parsedDate.isValid()) {
                return res.status(400).json({
                    success: false,
                    message: "Invalid reset date. Please select a valid date before saving."
                });
            }

            const numericFields = {};
            for (const { payloadKey, label, modelKey } of resetNumericFields) {
                const parsedValue = parseResetNumericValue(record[payloadKey], label);
                if (!parsedValue.ok) {
                    return res.status(400).json({
                        success: false,
                        message: parsedValue.message
                    });
                }
                numericFields[modelKey] = parsedValue.value;
            }

            const timeMetric = normalizeResetTimeMetric(record.metric);
            if (!timeMetric) {
                return res.status(400).json({
                    success: false,
                    message: "Application time metric must be BH or FH."
                });
            }

            const normalizedDate = parsedDate.startOf("day").toDate();
            const updateFields = {
                date: normalizedDate,
                msnEsn,
                pn,
                snBn,
                ...numericFields,
            };
            saveDateStrings.add(moment(normalizedDate).format("YYYY-MM-DD"));
            changedResetGroups.set(resetGroupKey, resetGroup);

            replaceExistingResetOps.push({
                deleteMany: {
                    filter: {
                        userId: String(userId),
                        msnEsn,
                        pn,
                        snBn,
                        date: { $ne: normalizedDate }
                    }
                }
            });

            // 1. MaintenanceReset mapping (for the explicit reset date)
            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        pn: updateFields.pn,
                        snBn: updateFields.snBn
                    },
                    update: {
                        $set: {
                            ...updateFields,
                            userId: String(userId),
                            timeMetric
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await MaintenanceReset.bulkWrite(bulkOperations, { ordered: false });
        }

        if (replaceExistingResetOps.length > 0) {
            await MaintenanceReset.bulkWrite(replaceExistingResetOps, { ordered: false });
        }

        if (changedResetGroups.size > 0) {
            await Promise.all([...changedResetGroups.values()].map(group => Utilisation.deleteMany({
                userId: String(userId),
                msnEsn: group.msnEsn,
                pn: group.pn,
                snBn: group.snBn
            })));
        }

        if (saveDateStrings.size > 0) {
            await recomputeMaintenanceTimeline({
                userId,
                resetGroups: [...changedResetGroups.values()]
            });
        }

        res.status(200).json({ success: true, message: "Maintenance reset records updated successfully!" });
    } catch (error) {
        console.error("🔥 Error saving reset records:", error);
        const validationMessage = getMongooseValidationMessage(error);
        if (validationMessage) {
            return res.status(400).json({
                success: false,
                message: validationMessage
            });
        }
        res.status(500).json({ message: "Failed to save records", error: error.message });
    }
};

exports.deleteResetRecord = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { id } = req.params;

        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });
        if (!isValidObjectId(id)) return res.status(400).json({ message: "Invalid reset record id." });

        const deletedRecord = await MaintenanceReset.findOneAndDelete({ _id: id, userId: String(userId) });

        if (!deletedRecord) {
            return res.status(404).json({ message: "Maintenance reset record not found." });
        }

        const resetGroup = {
            msnEsn: deletedRecord.msnEsn,
            pn: deletedRecord.pn,
            snBn: deletedRecord.snBn
        };

        await Utilisation.deleteMany({
            userId: String(userId),
            msnEsn: resetGroup.msnEsn,
            pn: resetGroup.pn,
            snBn: resetGroup.snBn
        });

        const remainingReset = await MaintenanceReset.exists({
            userId: String(userId),
            msnEsn: resetGroup.msnEsn,
            pn: resetGroup.pn,
            snBn: resetGroup.snBn
        });

        if (remainingReset) {
            await recomputeMaintenanceTimeline({
                userId,
                resetGroups: [resetGroup]
            });
        }

        res.status(200).json({ success: true, message: "Maintenance reset record deleted successfully." });
    } catch (error) {
        console.error("Error deleting reset record:", error);
        res.status(500).json({ message: "Failed to delete maintenance reset record", error: error.message });
    }
};

/**
 * 4. POST: Trigger to compute maintenance logic (The green "Compute" button)
 */
/**
 * 4. POST: Trigger to compute maintenance logic (The green "Compute" button)
 * This function performs a full recalculation for all assets that have Maintenance Reset records.
 * It propagates metrics forwards and backwards from these reset points.
 */
exports.computeMaintenanceLogic = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const result = await recomputeMaintenanceTimeline({ userId });

        if (result.message === "No flights found to compute.") {
            return res.status(200).json({
                success: true,
                message: result.message,
                assignmentImpact: result.assignmentImpact || { deletedCount: 0, clearedFlightCount: 0, daysTouched: 0 }
            });
        }

        res.status(200).json({
            success: true,
            message: result.message,
            assignmentImpact: result.assignmentImpact
        });
    } catch (error) {
        console.error("🔥 Error computing maintenance logic:", error);
        res.status(500).json({ message: "Failed to recalculate maintenance logic", error: error.message });
    }
};

/**
 * 5. GET: Fetch Major Rotable Movements for Modal
 */
exports.getRotables = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const records = await RotableMovement.find({ userId: String(userId) }).sort({ date: -1 }).lean();
        const formattedRecords = records.map(record => ({
            id: record._id,
            label: record.label || "",
            date: moment(record.date).format("YYYY-MM-DD"),
            pn: record.pn || "",
            msn: record.msn || "",
            acftRegn: record.acftReg || "",
            position: record.position || "",
            removedSN: record.removedSN || "",
            installedSN: record.installedSN || ""
        }));
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching rotable movements:", error);
        res.status(500).json({ message: "Failed to fetch rotable movements", error: error.message });
    }
};

/**
 * 6. POST: Bulk Save/Update Major Rotable Movements
 */
exports.bulkSaveRotables = async (req, res) => {
    try {
        const { rotablesData } = req.body;
        const userId = getUserIdFromReq(req);

        if (!rotablesData || !Array.isArray(rotablesData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const onwingOps = [];
        const affectedOnwingRebuilds = new Map();

        for (const record of rotablesData) {
            const msn = String(record.msn || "").trim();
            const pn = String(record.pn || "").trim();
            const position = String(record.position || "").trim();
            const movementDate = record.date
                ? moment.utc(record.date, moment.ISO_8601, true).startOf("day")
                : moment.utc().startOf("day");
            const persistedMovementDate = movementDate.isValid() ? movementDate.toDate() : moment.utc().startOf("day").toDate();
            const movementFilter = isValidObjectId(record.id)
                ? { _id: record.id, userId: String(userId) }
                : {
                    userId: String(userId),
                    msn,
                    pn,
                    position,
                    date: persistedMovementDate
                };

            bulkOperations.push({
                updateOne: {
                    filter: movementFilter,
                    update: {
                        $set: {
                            label: record.label,
                            date: persistedMovementDate,
                            pn,
                            msn,
                            acftReg: record.acftRegn,
                            position,
                            removedSN: record.removedSN,
                            installedSN: record.installedSN,
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });

            // Update AircraftOnwing if an Engine is assigned to Position #1 or #2
            if ((position === "#1" || position === "#2") && record.date) {
                const effectiveDate = moment.utc(record.date, moment.ISO_8601, true).add(1, "day").startOf("day").toDate();
                if (!isNaN(effectiveDate.getTime())) {

                    const updateField = position === "#1" ? "pos1Esn" : "pos2Esn";
                    const priorConfig = await AircraftOnwing.findOne({
                        userId: String(userId),
                        msn,
                        date: { $lt: effectiveDate }
                    }).sort({ date: -1 }).lean();
                    const effectiveSnapshot = {
                        userId: String(userId),
                        msn,
                        date: effectiveDate,
                        pos1Esn: priorConfig?.pos1Esn || "",
                        pos2Esn: priorConfig?.pos2Esn || "",
                        apun: priorConfig?.apun || "",
                        [updateField]: record.installedSN
                    };

                    // 1. Update all future chronological configurations for this MSN
                    onwingOps.push({
                        updateMany: {
                            filter: { userId: String(userId), msn, date: { $gte: effectiveDate } },
                            update: {
                                $set: {
                                    [updateField]: record.installedSN
                                }
                            }
                        }
                    });

                    // 2. Explicitly log the new configuration timeline starting on the effective date
                    onwingOps.push({
                        updateOne: {
                            filter: { userId: String(userId), msn, date: effectiveDate },
                            update: {
                                $set: effectiveSnapshot
                            },
                            upsert: true
                        }
                    });

                    const rebuildKey = `${msn}|${position}`;
                    const currentRebuild = affectedOnwingRebuilds.get(rebuildKey);
                    if (!currentRebuild || movementDate.isBefore(moment.utc(currentRebuild.date), "day")) {
                        affectedOnwingRebuilds.set(rebuildKey, {
                            ...record,
                            msn,
                            pn,
                            position,
                            date: persistedMovementDate
                        });
                    }
                }
            }
        }

        if (bulkOperations.length > 0) {
            await RotableMovement.bulkWrite(bulkOperations);
        }

        if (onwingOps.length > 0) {
            await AircraftOnwing.bulkWrite(onwingOps, { ordered: false });
        }

        for (const movement of affectedOnwingRebuilds.values()) {
            await rebuildOnwingTimelineForRotableMovement({ userId, movement });
        }

        if (bulkOperations.length > 0 || affectedOnwingRebuilds.size > 0) {
            await recomputeMaintenanceTimeline({ userId });
        }

        res.status(200).json({ success: true, message: "Rotable movements and Aircraft configurations updated successfully." });
    } catch (error) {
        console.error("Error saving rotables data:", error);
        res.status(500).json({ message: "Failed to update rotables data", error: error.message });
    }
};

exports.deleteRotable = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { id } = req.params;

        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });
        if (!isValidObjectId(id)) return res.status(400).json({ message: "Invalid rotable movement id." });

        const deletedRecord = await RotableMovement.findOneAndDelete({ _id: id, userId: String(userId) });

        if (!deletedRecord) {
            return res.status(404).json({ message: "Rotable movement not found." });
        }

        await rebuildOnwingTimelineForRotableMovement({ userId, movement: deletedRecord });
        await recomputeMaintenanceTimeline({ userId });

        res.status(200).json({ success: true, message: "Rotable movement deleted successfully." });
    } catch (error) {
        console.error("Error deleting rotable movement:", error);
        res.status(500).json({ message: "Failed to delete rotable movement", error: error.message });
    }
};

/**
 * 7. GET: Fetch Target Maintenance Status Records
 */
exports.getTargets = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { msnEsn } = req.query;
        const filter = { userId: String(userId) };

        if (msnEsn) {
            filter.msnEsn = String(msnEsn).trim();
        }

        const records = await MaintenanceTarget.find(filter).sort({ date: -1 }).lean();
        const utilisationQueries = records.map(record => {
            const startOfTargetDay = moment.utc(record.date).startOf("day").toDate();
            const endOfTargetDay = moment.utc(record.date).endOf("day").toDate();

            return Utilisation.findOne({
                userId: String(userId),
                date: { $gte: startOfTargetDay, $lte: endOfTargetDay },
                msnEsn: record.msnEsn || "",
                pn: record.pn || "",
                snBn: record.snBn || ""
            }).sort({ updatedAt: -1, createdAt: -1 }).lean();
        });
        const forecastRows = await Promise.all(utilisationQueries);

        const formattedRecords = records.map((record, index) => {
            const forecast = forecastRows[index] || {};
            const deltas = {};
            const highlights = [];
            const category = String(record.category || "").trim().toLowerCase();

            targetMetricFields.forEach(({ key, utilKey }) => {
                const targetValue = parseMetricValue(record[key]);
                const forecastValue = parseMetricValue(forecast[utilKey]);

                if (targetValue === null || forecastValue === null) {
                    deltas[key] = "";
                    return;
                }

                const delta = roundMetricDelta(targetValue - forecastValue);
                deltas[key] = delta;

                if ((category === "conserve" && delta < 0) || (category === "run-down" && delta > 0)) {
                    highlights.push(key);
                }
            });

            const base = {
                id: record._id,
                label: record.label || "",
                msnEsn: record.msnEsn || "",
                pn: record.pn || "",
                snBn: record.snBn || "",
                category: record.category || "",
                date: moment.utc(record.date).format("YYYY-MM-DD"),
                displayDate: moment.utc(record.date).format("DD MMM YY"),
                tsn: record.tsn || "",
                csn: record.csn || "",
                dsn: record.dsn || "",
                tso: record.tso || "",
                cso: record.cso || "",
                dso: record.dso || "",
                tsRplmt: record.tsRplmt || "",
                csRplmt: record.csRplmt || "",
                dsRplmt: record.dsRplmt || "",
                highlights,
            };

            return {
                ...base,
                targetLabel: base.label,
                targetMsn: base.msnEsn,
                targetPn: base.pn,
                targetSn: base.snBn,
                tsr: base.tsRplmt,
                csr: base.csRplmt,
                dsr: base.dsRplmt,
                fTsn: deltas.tsn,
                fCsn: deltas.csn,
                fDsn: deltas.dsn,
                fTso: deltas.tso,
                fCso: deltas.cso,
                fDso: deltas.dso,
                fTsr: deltas.tsRplmt,
                fCsr: deltas.csRplmt,
                fDsr: deltas.dsRplmt,
            };
        });
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching target maintenance data:", error);
        res.status(500).json({ message: "Failed to fetch targets", error: error.message });
    }
};

/**
 * 8. POST: Bulk Save/Update Target Maintenance Status
 */
exports.bulkSaveTargets = async (req, res) => {
    try {
        const { targetData } = req.body;
        const userId = getUserIdFromReq(req);

        if (!targetData || !Array.isArray(targetData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];

        for (const record of targetData) {
            const values = [
                record.label, record.msnEsn, record.pn, record.snBn, record.category, record.date,
                record.tsn, record.csn, record.dsn, record.tso, record.cso, record.dso,
                record.tsRplmt, record.csRplmt, record.dsRplmt
            ];
            const hasAnyValue = values.some(value => String(value ?? "").trim() !== "");
            if (!hasAnyValue) continue;

            const targetDate = moment.utc(record.date, moment.ISO_8601, true);
            if (!targetDate.isValid()) {
                return res.status(400).json({ message: "Target maintenance status requires a valid date." });
            }

            const normalizedDate = targetDate.startOf("day").toDate();
            const msnEsn = String(record.msnEsn || "").trim();
            const pn = String(record.pn || "").trim();
            const snBn = String(record.snBn || "").trim();

            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        msnEsn,
                        pn,
                        snBn,
                        date: normalizedDate
                    },
                    update: {
                        $set: {
                            label: String(record.label || "").trim(),
                            msnEsn,
                            pn,
                            snBn,
                            category: String(record.category || "").trim(),
                            date: normalizedDate,
                            tsn: record.tsn,
                            csn: record.csn,
                            dsn: record.dsn,
                            tso: record.tso,
                            cso: record.cso,
                            dso: record.dso,
                            tsRplmt: record.tsRplmt,
                            csRplmt: record.csRplmt,
                            dsRplmt: record.dsRplmt,
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await MaintenanceTarget.bulkWrite(bulkOperations);
        }

        res.status(200).json({ success: true, message: "Target maintenance status updated successfully." });
    } catch (error) {
        console.error("Error saving target maintenance data:", error);
        res.status(500).json({ message: "Failed to update target maintenance status", error: error.message });
    }
};

exports.deleteTarget = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { id } = req.params;

        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });
        if (!isValidObjectId(id)) return res.status(400).json({ message: "Invalid target maintenance status id." });

        const deletedRecord = await MaintenanceTarget.findOneAndDelete({ _id: id, userId: String(userId) });

        if (!deletedRecord) {
            return res.status(404).json({ message: "Target maintenance status not found." });
        }

        res.status(200).json({ success: true, message: "Target maintenance status deleted successfully." });
    } catch (error) {
        console.error("Error deleting target maintenance status:", error);
        res.status(500).json({ message: "Failed to delete target maintenance status", error: error.message });
    }
};

exports.getUtilisationAssumptions = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const records = await UtilisationAssumption.find({ userId: String(userId) })
            .sort({ msn: 1, fromDate: 1 })
            .lean();

        const formattedRecords = records.map(record => ({
            id: record._id,
            msn: record.msn || "",
            fromDate: record.fromDate ? moment.utc(record.fromDate).format("YYYY-MM-DD") : "",
            toDate: record.toDate ? moment.utc(record.toDate).format("YYYY-MM-DD") : "",
            hours: record.hours ?? "",
            cycles: record.cycles ?? "",
            avgDowndays: record.avgDowndays ?? ""
        }));

        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching utilisation assumptions:", error);
        res.status(500).json({ message: "Failed to fetch utilisation assumptions", error: error.message });
    }
};

exports.bulkSaveUtilisationAssumptions = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { utilisationAssumptions } = req.body;

        if (!Array.isArray(utilisationAssumptions)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const parseNum = (value) => {
            if (value === "" || value === null || value === undefined) return 0;
            const parsed = Number(value);
            return Number.isFinite(parsed) ? parsed : 0;
        };

        const bulkOperations = [];

        for (const record of utilisationAssumptions) {
            const values = [record.msn, record.fromDate, record.toDate, record.hours, record.cycles, record.avgDowndays];
            const hasAnyValue = values.some(value => String(value ?? "").trim() !== "");
            if (!hasAnyValue) continue;

            const msn = String(record.msn || "").trim();
            const fromDate = moment.utc(record.fromDate, moment.ISO_8601, true);
            const toDate = moment.utc(record.toDate, moment.ISO_8601, true);

            if (!msn || !fromDate.isValid() || !toDate.isValid()) {
                return res.status(400).json({
                    success: false,
                    message: "MSN, From date, and To date are required for utilisation assumptions."
                });
            }

            if (toDate.isBefore(fromDate, "day")) {
                return res.status(400).json({
                    success: false,
                    message: "Utilisation assumption To date cannot be before From date."
                });
            }

            const normalizedFromDate = fromDate.startOf("day").toDate();
            const normalizedToDate = toDate.startOf("day").toDate();
            const filter = isValidObjectId(record.id)
                ? { _id: record.id, userId: String(userId) }
                : {
                    userId: String(userId),
                    msn,
                    fromDate: normalizedFromDate,
                    toDate: normalizedToDate
                };

            bulkOperations.push({
                updateOne: {
                    filter,
                    update: {
                        $set: {
                            userId: String(userId),
                            msn,
                            fromDate: normalizedFromDate,
                            toDate: normalizedToDate,
                            hours: parseNum(record.hours),
                            cycles: parseNum(record.cycles),
                            avgDowndays: parseNum(record.avgDowndays)
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await UtilisationAssumption.bulkWrite(bulkOperations, { ordered: false });
        }

        await recomputeMaintenanceTimeline({ userId });

        res.status(200).json({ success: true, message: "Utilisation assumptions updated successfully." });
    } catch (error) {
        console.error("Error saving utilisation assumptions:", error);
        res.status(500).json({ message: "Failed to update utilisation assumptions", error: error.message });
    }
};

exports.deleteUtilisationAssumption = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { id } = req.params;

        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });
        if (!isValidObjectId(id)) return res.status(400).json({ message: "Invalid utilisation assumption id." });

        const deletedRecord = await UtilisationAssumption.findOneAndDelete({ _id: id, userId: String(userId) });

        if (!deletedRecord) {
            return res.status(404).json({ message: "Utilisation assumption not found." });
        }

        await recomputeMaintenanceTimeline({ userId });

        res.status(200).json({ success: true, message: "Utilisation assumption deleted successfully." });
    } catch (error) {
        console.error("Error deleting utilisation assumption:", error);
        res.status(500).json({ message: "Failed to delete utilisation assumption", error: error.message });
    }
};

exports.getGroundDays = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const filter = { userId: String(userId) };
        const { msn, fromDate, toDate } = req.query || {};

        if (msn) {
            filter.msn = { $regex: `^${escapeRegex(String(msn).trim())}$`, $options: "i" };
        }

        if (fromDate || toDate) {
            filter.date = {};
            if (fromDate) {
                const parsedFrom = parseUtcIsoDate(fromDate);
                if (!parsedFrom) return res.status(400).json({ message: "Invalid fromDate." });
                filter.date.$gte = parsedFrom.startOf("day").toDate();
            }
            if (toDate) {
                const parsedTo = parseUtcIsoDate(toDate);
                if (!parsedTo) return res.status(400).json({ message: "Invalid toDate." });
                filter.date.$lte = parsedTo.endOf("day").toDate();
            }
        }

        const rows = await GroundDay.find(filter)
            .sort({ date: 1, msn: 1, event: 1 })
            .lean();

        res.status(200).json({
            success: true,
            data: rows.map(row => ({
                id: row._id,
                msn: row.msn || "",
                date: formatCalendarDate(row.date),
                event: row.event || "",
                source: row.source || "",
                eventSeriesId: row.eventSeriesId || "",
                occurrenceNumber: row.occurrenceNumber || "",
                occurrenceId: row.occurrenceId || ""
            }))
        });
    } catch (error) {
        console.error("Error fetching generated ground days:", error);
        res.status(500).json({ message: "Failed to fetch ground days", error: error.message });
    }
};

/**
 * 9. GET: Fetch Calendar Inputs
 */
exports.getCalendar = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const records = await MaintenanceCalendar.find({ userId: String(userId) }).lean();
        const formattedRecords = records.map(record => ({
            id: record._id,
            calLabel: record.calLabel || "",
            lineBase: record.lineBase || "",
            calMsn: record.calMsn || "",
            schEvent: record.schEvent || "",
            calPn: record.calPn || "",
            snBn: record.snBn || "",
            applyToAllSnBn: Boolean(record.applyToAllSnBn),
            triggerRelationship: record.triggerRelationship || "EARLIEST_OF_EVERY",
            eTsn: record.eTsn || "",
            eCsn: record.eCsn || "",
            eDsn: record.eDsn || "",
            eTso: record.eTso || "",
            eCso: record.eCso || "",
            eDso: record.eDso || "",
            eTsr: record.eTsr || "",
            eCsr: record.eCsr || "",
            eDsr: record.eDsr || "",
            downDays: record.downDays || 0,
            avgDownda: record.avgDownda || 0,
            lastOccurre: formatCalendarDate(record.lastOccurre),
            nextEstima: formatCalendarDate(record.nextEstima),
            firstOccurrenceDate: formatCalendarDate(record.firstOccurrenceDate || record.nextEstima),
            occurrence: record.occurrence || "",
            occurrencesTillExit: record.occurrencesTillExit ?? Math.max(0, Number(record.occurrence || 0) - 1),
            postTso: record.postTso ?? "",
            postCso: record.postCso ?? "",
            postDso: record.postDso ?? "",
            postTsr: record.postTsr ?? "",
            postCsr: record.postCsr ?? "",
            postDsr: record.postDsr ?? "",
            soTsr: record.soTsr ?? "",
            generatedOccurrences: Array.isArray(record.generatedOccurrences)
                ? record.generatedOccurrences.map(formatCalendarOccurrence)
                : [],
            suppressedAlternateThresholds: Array.isArray(record.suppressedAlternateThresholds)
                ? record.suppressedAlternateThresholds
                : [],
            suppressedThresholds: Array.isArray(record.suppressedThresholds)
                ? record.suppressedThresholds
                : []
        }));
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching calendar data:", error);
        res.status(500).json({ message: "Failed to fetch calendar data", error: error.message });
    }
};

/**
 * 10. POST: Bulk Save/Update Calendar Inputs
 */
exports.bulkSaveCalendar = async (req, res) => {
    try {
        const { calendarData } = req.body;
        const userId = getUserIdFromReq(req);

        if (!calendarData || !Array.isArray(calendarData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const affectedResetGroups = new Map();

        await ensureMaintenanceCalendarIndexes();

        for (const record of calendarData) {
            const hasAnyValue = [
                record.calLabel,
                record.lineBase,
                record.schEvent,
                record.calMsn,
                record.calPn,
                record.snBn,
                ...maintenanceMetricDefinitions.map(definition => record[definition.limitKey]),
                record.downDays,
                record.avgDownda,
                record.postTso,
                record.postCso,
                record.postDso,
                record.postTsr,
                record.postCsr,
                record.postDsr,
            ].some(value => String(value ?? "").trim() !== "");

            if (!hasAnyValue) continue;

            const parseNum = (val, label, { required = false, positive = false, whole = false, nonNegative = false } = {}) => {
                if (val === "" || val === null || val === undefined) {
                    if (required) {
                        return { ok: false, message: `${label} is required.` };
                    }
                    return { ok: true, value: null };
                }

                const value = Number(val);
                if (!Number.isFinite(value)) {
                    return { ok: false, message: `${label} must be a valid number.` };
                }
                if (positive && value <= 0) {
                    return { ok: false, message: `${label} must be greater than zero.` };
                }
                if (nonNegative && value < 0) {
                    return { ok: false, message: `${label} cannot be negative.` };
                }
                if (whole && !Number.isInteger(value)) {
                    return { ok: false, message: `${label} must be a whole number.` };
                }
                return { ok: true, value };
            };

            const resetGroup = normalizeResetGroup({
                msnEsn: record.calMsn,
                pn: record.calPn,
                snBn: record.snBn
            });

            const scheduledEvent = String(record.schEvent || "").trim();
            if (!resetGroup.msnEsn || !resetGroup.pn || !resetGroup.snBn || !scheduledEvent) {
                return res.status(400).json({
                    success: false,
                    message: "Calendar inputs require MSN/ESN, PN, SN/BN, and Scheduled Maintenance Event."
                });
            }

            const parsedMetricValues = {};
            let triggerCount = 0;
            for (const definition of maintenanceMetricDefinitions) {
                const rawMetricValue = record[definition.limitKey];
                const parsed = parseNum(rawMetricValue, definition.label, { positive: String(rawMetricValue ?? "").trim() !== "" });
                if (!parsed.ok) {
                    return res.status(400).json({ success: false, message: parsed.message });
                }
                parsedMetricValues[definition.limitKey] = parsed.value;
                if (parsed.value !== null) triggerCount += 1;
            }

            if (triggerCount === 0) {
                return res.status(400).json({
                    success: false,
                    message: "Calendar inputs require at least one positive Earliest of, every trigger."
                });
            }

            const parsedDownDays = parseNum(record.downDays, "Down days", { nonNegative: true, whole: true });
            if (!parsedDownDays.ok) return res.status(400).json({ success: false, message: parsedDownDays.message });
            const parsedAvgDowndays = parseNum(record.avgDownda, "Avg Downdays", { nonNegative: true, whole: true });
            if (!parsedAvgDowndays.ok) return res.status(400).json({ success: false, message: parsedAvgDowndays.message });

            const parsedPostValues = {};
            for (const postField of ["postTso", "postCso", "postDso", "postTsr", "postCsr", "postDsr", "soTsr"]) {
                const parsed = parseNum(record[postField], postField);
                if (!parsed.ok) return res.status(400).json({ success: false, message: parsed.message });
                parsedPostValues[postField] = parsed.value;
            }

            if (resetGroup.msnEsn && resetGroup.pn && resetGroup.snBn) {
                affectedResetGroups.set(buildResetGroupKey(resetGroup), resetGroup);
            }

            const update = {
                $set: {
                    calLabel: record.calLabel,
                    lineBase: record.lineBase,
                    schEvent: scheduledEvent,
                    calMsn: resetGroup.msnEsn,
                    calPn: resetGroup.pn,
                    snBn: resetGroup.snBn,
                    applyToAllSnBn: Boolean(record.applyToAllSnBn),
                    triggerRelationship: "EARLIEST_OF_EVERY",
                    eTsn: parsedMetricValues.eTsn,
                    eCsn: parsedMetricValues.eCsn,
                    eDsn: parsedMetricValues.eDsn,
                    eTso: parsedMetricValues.eTso,
                    eCso: parsedMetricValues.eCso,
                    eDso: parsedMetricValues.eDso,
                    eTsr: parsedMetricValues.eTsr,
                    eCsr: parsedMetricValues.eCsr,
                    eDsr: parsedMetricValues.eDsr,
                    downDays: parsedDownDays.value,
                    avgDownda: parsedAvgDowndays.value,
                    lastOccurre: null,
                    nextEstima: null,
                    firstOccurrenceDate: null,
                    occurrence: 0,
                    occurrencesTillExit: 0,
                    generatedOccurrences: [],
                    suppressedAlternateThresholds: [],
                    suppressedThresholds: [],
                    postTso: parsedPostValues.postTso,
                    postCso: parsedPostValues.postCso,
                    postDso: parsedPostValues.postDso,
                    postTsr: parsedPostValues.postTsr,
                    postCsr: parsedPostValues.postCsr,
                    postDsr: parsedPostValues.postDsr,
                    soTsr: parsedPostValues.soTsr,
                    userId: String(userId)
                }
            };

            if (isValidObjectId(record.id)) {
                bulkOperations.push({
                    updateOne: {
                        filter: { _id: record.id, userId: String(userId) },
                        update
                    }
                });
            } else {
                bulkOperations.push({
                    updateOne: {
                        filter: {
                            userId: String(userId),
                            calMsn: resetGroup.msnEsn,
                            calPn: resetGroup.pn,
                            snBn: resetGroup.snBn,
                            schEvent: scheduledEvent
                        },
                        update,
                        upsert: true
                    },
                });
            }
        }

        if (bulkOperations.length > 0) {
            await MaintenanceCalendar.bulkWrite(bulkOperations);
        }

        if (affectedResetGroups.size > 0) {
            await recomputeMaintenanceTimeline({
                userId,
                resetGroups: [...affectedResetGroups.values()]
            });
        }

        res.status(200).json({ success: true, message: "Calendar inputs updated successfully." });
    } catch (error) {
        console.error("Error saving calendar data:", error);
        res.status(500).json({ message: "Failed to update calendar inputs", error: error.message });
    }
};

exports.deleteCalendar = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { id } = req.params;

        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });
        if (!isValidObjectId(id)) return res.status(400).json({ message: "Invalid calendar input id." });

        const deletedRecord = await MaintenanceCalendar.findOneAndDelete({ _id: id, userId: String(userId) });

        if (!deletedRecord) {
            return res.status(404).json({ message: "Calendar input not found." });
        }

        await GroundDay.deleteMany({
            userId: String(userId),
            source: "SCHEDULED_MAINTENANCE",
            eventSeriesId: String(deletedRecord._id)
        });

        await recomputeMaintenanceTimeline({
            userId,
            resetGroups: [{
                msnEsn: deletedRecord.calMsn,
                pn: deletedRecord.calPn,
                snBn: deletedRecord.snBn
            }]
        });

        res.status(200).json({ success: true, message: "Calendar input deleted successfully." });
    } catch (error) {
        console.error("Error deleting calendar input:", error);
        res.status(500).json({ message: "Failed to delete calendar input", error: error.message });
    }
};
