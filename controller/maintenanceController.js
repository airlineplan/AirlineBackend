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

const calendarMetricGroups = {
    sinceNew: [
        { limitKey: "eTsn", valueKey: "tsn" },
        { limitKey: "eCsn", valueKey: "csn" },
        { limitKey: "eDsn", valueKey: "dsn" }
    ],
    restoration: [
        { limitKey: "eTso", valueKey: "tsoTsr" },
        { limitKey: "eCso", valueKey: "csoCsr" },
        { limitKey: "eDso", valueKey: "dsoDsr" }
    ],
    replacement: [
        { limitKey: "eTsr", valueKey: "tsRplmt" },
        { limitKey: "eCsr", valueKey: "csRplmt" },
        { limitKey: "eDsr", valueKey: "dsRplmt" }
    ]
};

const hasCalendarLimitHit = (cal, projectedValues, metricGroup) => metricGroup.some(({ limitKey, valueKey }) => {
    const limit = Number(cal?.[limitKey]);
    const projected = projectedValues?.[valueKey];
    return Number.isFinite(limit) && limit > 0 && projected !== null && projected !== undefined && projected >= limit;
});

const getCalendarDowntimeDays = (cal) => {
    const downDays = Number(cal?.downDays);
    if (Number.isFinite(downDays) && downDays > 0) return Math.ceil(downDays);

    const avgDowndays = Number(cal?.avgDownda);
    if (Number.isFinite(avgDowndays) && avgDowndays > 0) return Math.ceil(avgDowndays);

    return 0;
};

const formatCalendarDate = (value) => value ? moment.utc(value).format("YYYY-MM-DD") : "";

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

const onwingRowHasAsset = (row, assetKey) => {
    const normalizedAssetKey = String(assetKey || "").trim();
    if (!row || !normalizedAssetKey) return false;
    return onwingFields.some(field => String(row[field] || "").trim() === normalizedAssetKey);
};

const getEffectiveUtilisationContext = async ({ userId, msnEsn, date }) => {
    const assetKey = String(msnEsn || "").trim();
    const lookupDate = date ? moment.utc(date).endOf("day").toDate() : null;
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
    const assignmentFilter = {
        ...(userId ? { userId: String(userId) } : {}),
        date: {
            $gte: date.toDate(),
            $lt: moment.utc(date).endOf("day").toDate()
        }
    };

    if (Number.isFinite(msnNumber)) {
        assignmentFilter["aircraft.msn"] = msnNumber;
    } else {
        assignmentFilter["aircraft.msn"] = effectiveMsn;
    }

    const assignments = await Assignment.find(assignmentFilter);

    if (assignments.length > 0) {
        const primaryTimeKey = metric === "FH" ? "flightHours" : "blockHours";
        const fallbackTimeKey = metric === "FH" ? "blockHours" : "flightHours";
        const primaryTimeUsage = assignments.reduce((sum, a) =>
            sum + (a.metrics?.[primaryTimeKey] || 0), 0);
        const fallbackTimeUsage = assignments.reduce((sum, a) =>
            sum + (a.metrics?.[fallbackTimeKey] || 0), 0);
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
    const calendarEventState = new Map();
    const calendarIdsTouched = new Set();

    const recordCalendarEvent = ({ cal, date, triggeredGroups, projectedValues }) => {
        const key = String(cal._id);
        const previous = calendarEventState.get(key) || {
            id: cal._id,
            occurrence: 0,
            lastOccurre: null,
            nextEstima: null,
            soTsr: null,
            triggeredSinceNew: false
        };

        const eventDate = moment.utc(date).startOf("day");
        const occurrence = previous.occurrence + 1;
        const soTsr = triggeredGroups.includes("replacement")
            ? projectedValues.tsRplmt
            : triggeredGroups.includes("restoration")
                ? projectedValues.tsoTsr
                : projectedValues.tsn;

        calendarEventState.set(key, {
            ...previous,
            occurrence,
            lastOccurre: eventDate.toDate(),
            nextEstima: previous.nextEstima || eventDate.toDate(),
            soTsr: Number.isFinite(Number(soTsr)) ? Number(soTsr) : previous.soTsr,
            triggeredSinceNew: previous.triggeredSinceNew || triggeredGroups.includes("sinceNew")
        });
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

                    if (currentTsn !== null) currentTsn = Number((currentTsn - timeUsage).toFixed(2));
                    if (currentCsn !== null) currentCsn -= cycleUsage;
                    if (currentDsn !== null) currentDsn -= 1;
                    if (currentTso !== null) currentTso = Number((currentTso - timeUsage).toFixed(2));
                    if (currentCso !== null) currentCso -= cycleUsage;
                    if (currentDso !== null) currentDso -= 1;
                    if (currentTsr !== null) currentTsr = Number((currentTsr - timeUsage).toFixed(2));
                    if (currentCsr !== null) currentCsr -= cycleUsage;
                    if (currentDsr !== null) currentDsr -= 1;

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

            while (currDate.isSameOrBefore(segmentEnd)) {
                const currentUtilizationContext = await getEffectiveUtilisationContext({
                    userId,
                    msnEsn,
                    date: currDate
                });
                const currentEffectiveMsn = currentUtilizationContext.effectiveMsn || msnEsn;

                if (inMaintenanceUntil && currDate.isSameOrBefore(inMaintenanceUntil)) {
                    await Assignment.updateMany({
                        userId: String(userId),
                        date: {
                            $gte: currDate.toDate(),
                            $lt: moment.utc(currDate).endOf("day").toDate()
                        },
                        "aircraft.msn": Number(currentEffectiveMsn)
                    }, {
                        $unset: { "aircraft.msn": "", "aircraft.registration": "" },
                        $set: { removedReason: "GROUND_DAY_CONFLICT" }
                    });

                    if (currentDsn !== null) currentDsn += 1;
                    if (currentDso !== null) currentDso += 1;
                    if (currentDsr !== null) currentDsr += 1;

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

                const { timeUsage, cycleUsage, hasUsage } = await getEffectiveUsageForDate({
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

                const triggeredCalendars = [];
                let downDaysToApply = 0;

                for (const cal of assetCalendars) {
                    const state = calendarEventState.get(String(cal._id));
                    const triggeredGroups = [];

                    if (!state?.triggeredSinceNew && hasCalendarLimitHit(cal, projectedValues, calendarMetricGroups.sinceNew)) {
                        triggeredGroups.push("sinceNew");
                    }

                    if (hasCalendarLimitHit(cal, projectedValues, calendarMetricGroups.restoration)) {
                        triggeredGroups.push("restoration");
                    }

                    if (hasCalendarLimitHit(cal, projectedValues, calendarMetricGroups.replacement)) {
                        triggeredGroups.push("replacement");
                    }

                    if (triggeredGroups.length > 0) {
                        triggeredCalendars.push({ cal, triggeredGroups });
                        downDaysToApply = Math.max(downDaysToApply, getCalendarDowntimeDays(cal));
                    }
                }

                if (triggeredCalendars.length > 0) {
                    await Assignment.updateMany({
                        userId: String(userId),
                        date: {
                            $gte: currDate.toDate(),
                            $lt: moment.utc(currDate).endOf("day").toDate()
                        },
                        "aircraft.msn": Number(currentEffectiveMsn)
                    }, {
                        $unset: { "aircraft.msn": "", "aircraft.registration": "" },
                        $set: { removedReason: "GROUND_DAY_CONFLICT" }
                    });

                    if (downDaysToApply > 0) {
                        inMaintenanceUntil = moment.utc(currDate).add(downDaysToApply - 1, "days");
                    }

                    if (currentDsn !== null) currentDsn += 1;
                    if (currentDso !== null) currentDso += 1;
                    if (currentDsr !== null) currentDsr += 1;

                    for (const { cal, triggeredGroups } of triggeredCalendars) {
                        recordCalendarEvent({
                            cal,
                            date: currDate,
                            triggeredGroups,
                            projectedValues
                        });

                        if (triggeredGroups.includes("restoration")) {
                            currentTso = 0;
                            currentCso = 0;
                            currentDso = 0;
                        }

                        if (triggeredGroups.includes("replacement")) {
                            currentTsr = 0;
                            currentCsr = 0;
                            currentDsr = 0;
                        }
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

    if (calendarIdsTouched.size > 0) {
        await MaintenanceCalendar.bulkWrite([...calendarIdsTouched].map(calendarId => {
            const event = calendarEventState.get(calendarId) || {
                id: calendarId,
                lastOccurre: null,
                nextEstima: null,
                occurrence: 0,
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
                            soTsr: event.soTsr
                        }
                    }
                }
            };
        }));
    }

    return {
        resetGroupCount: resetGroups.length,
        message: `Maintenance logic computed for ${resetGroups.length} assets.`,
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

        if (currentTsn !== null) currentTsn = Number((currentTsn + (direction * timeUsage)).toFixed(2));
        if (currentCsn !== null) currentCsn += direction * cycleUsage;
        if (currentDsn !== null) currentDsn += direction;
        if (currentTso !== null) currentTso = Number((currentTso + (direction * timeUsage)).toFixed(2));
        if (currentCso !== null) currentCso += direction * cycleUsage;
        if (currentDso !== null) currentDso += direction;
        if (currentTsr !== null) currentTsr = Number((currentTsr + (direction * timeUsage)).toFixed(2));
        if (currentCsr !== null) currentCsr += direction * cycleUsage;
        if (currentDsr !== null) currentDsr += direction;
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

            const [utils, resetRecords, fleetAssets, utilisationAssumptions] = await Promise.all([
                Utilisation.find(utilFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
                MaintenanceReset.find(resetFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
                Fleet.find(fleetFilter).select("sn titled").lean(),
                UtilisationAssumption.find({ userId: String(userId) }).lean(),
            ]);

            const titledBySn = new Map();
            fleetAssets.forEach(asset => {
                const sn = String(asset.sn || "").trim().toUpperCase();
                if (sn && !titledBySn.has(sn)) {
                    titledBySn.set(sn, asset.titled || "");
                }
            });

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

            const rows = await Promise.all(Array.from(rowSourcesByKey.entries()).map(async ([utilKey, util]) => {
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
                const metricSource = computedMetricSource || exactResetRecord || util || sourceRecord;

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
        const utilisationOps = [];
        const saveDateStrings = new Set();
        const changedResetGroups = new Map();

        // Use a for...of loop to handle async Await calls
        for (const record of resetData) {
            // Clean up empty string values to null for numeric fields
            const parseNum = (val) => (val === "" || val === undefined) ? null : Number(val);
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

            const rawDate = fallbackResetDate || record.date;
            const parsedDate = moment.utc(rawDate, moment.ISO_8601, true);

            if (!parsedDate.isValid()) {
                return res.status(400).json({
                    success: false,
                    message: "Invalid reset date. Please select a valid date before saving."
                });
            }

            const normalizedDate = parsedDate.startOf("day").toDate();
            const updateFields = {
                date: normalizedDate,
                msnEsn,
                pn,
                snBn,
                tsn: parseNum(record.tsn),
                csn: parseNum(record.csn),
                dsn: parseNum(record.dsn),
                tsoTsr: parseNum(record.tso),
                csoCsr: parseNum(record.cso),
                dsoDsr: parseNum(record.dso),
                tsRplmt: parseNum(record.tsr),
                csRplmt: parseNum(record.csr),
                dsRplmt: parseNum(record.dsr),
            };
            saveDateStrings.add(moment(normalizedDate).format("YYYY-MM-DD"));
            changedResetGroups.set(buildResetGroupKey(resetGroup), resetGroup);

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
                            timeMetric: record.metric || "BH"
                        }
                    },
                    upsert: true
                }
            });

            // 2. Utilisation table mapping (Sync for the explicit reset date)
            utilisationOps.push({
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
                            timeMetric: record.metric || "BH",
                            setFlag: "Y",
                            remarks: "(end of day)"
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await Promise.all([
                MaintenanceReset.bulkWrite(bulkOperations, { ordered: false }),
                Utilisation.bulkWrite(utilisationOps, { ordered: false })
            ]);
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

        await Utilisation.deleteOne({
            userId: String(userId),
            date: deletedRecord.date,
            msnEsn: deletedRecord.msnEsn,
            pn: deletedRecord.pn,
            snBn: deletedRecord.snBn
        });

        await recomputeMaintenanceTimeline({
            userId,
            resetGroups: [deletedRecord]
        });

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
            return res.status(200).json({ success: true, message: result.message });
        }

        res.status(200).json({ success: true, message: result.message });
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

        for (const record of rotablesData) {
            const movementDate = record.date
                ? moment.utc(record.date, moment.ISO_8601, true).startOf("day")
                : moment.utc().startOf("day");
            const persistedMovementDate = movementDate.isValid() ? movementDate.toDate() : moment.utc().startOf("day").toDate();

            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        msn: record.msn,
                        pn: record.pn,
                        position: record.position,
                        date: persistedMovementDate
                    },
                    update: {
                        $set: {
                            label: record.label,
                            date: persistedMovementDate,
                            pn: record.pn,
                            msn: record.msn,
                            acftReg: record.acftRegn,
                            position: record.position,
                            removedSN: record.removedSN,
                            installedSN: record.installedSN,
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });

            // Update AircraftOnwing if an Engine is assigned to Position #1 or #2
            if ((record.position === "#1" || record.position === "#2") && record.date) {
                const effectiveDate = moment.utc(record.date, moment.ISO_8601, true).add(1, "day").startOf("day").toDate();
                if (!isNaN(effectiveDate.getTime())) {

                    const updateField = record.position === "#1" ? "pos1Esn" : "pos2Esn";
                    const priorConfig = await AircraftOnwing.findOne({
                        userId: String(userId),
                        msn: record.msn,
                        date: { $lt: effectiveDate }
                    }).sort({ date: -1 }).lean();
                    const effectiveSnapshot = {
                        userId: String(userId),
                        msn: record.msn,
                        date: effectiveDate,
                        pos1Esn: priorConfig?.pos1Esn || "",
                        pos2Esn: priorConfig?.pos2Esn || "",
                        apun: priorConfig?.apun || "",
                        [updateField]: record.installedSN
                    };

                    // 1. Update all future chronological configurations for this MSN
                    onwingOps.push({
                        updateMany: {
                            filter: { userId: String(userId), msn: record.msn, date: { $gte: effectiveDate } },
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
                            filter: { userId: String(userId), msn: record.msn, date: effectiveDate },
                            update: {
                                $set: effectiveSnapshot
                            },
                            upsert: true
                        }
                    });
                }
            }
        }

        if (bulkOperations.length > 0) {
            await RotableMovement.bulkWrite(bulkOperations);
        }

        if (onwingOps.length > 0) {
            await AircraftOnwing.bulkWrite(onwingOps, { ordered: false });
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
            occurrence: record.occurrence || "",
            soTsr: record.soTsr ?? ""
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

        for (const record of calendarData) {
            const parseNum = (val) => (val === "" || val === undefined) ? null : Number(val);

            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        calMsn: record.calMsn,
                        calPn: record.calPn,
                        snBn: record.snBn
                    },
                    update: {
                        $set: {
                            calLabel: record.calLabel,
                            lineBase: record.lineBase,
                            schEvent: record.schEvent,
                            calMsn: record.calMsn,
                            calPn: record.calPn,
                            snBn: record.snBn,
                            eTsn: parseNum(record.eTsn),
                            eCsn: parseNum(record.eCsn),
                            eDsn: parseNum(record.eDsn),
                            eTso: parseNum(record.eTso),
                            eCso: parseNum(record.eCso),
                            eDso: parseNum(record.eDso),
                            eTsr: parseNum(record.eTsr),
                            eCsr: parseNum(record.eCsr),
                            eDsr: parseNum(record.eDsr),
                            downDays: parseNum(record.downDays),
                            avgDownda: parseNum(record.avgDownda),
                            lastOccurre: record.lastOccurre ? moment.utc(record.lastOccurre, moment.ISO_8601, true).startOf("day").toDate() : null,
                            nextEstima: record.nextEstima ? moment.utc(record.nextEstima, moment.ISO_8601, true).startOf("day").toDate() : null,
                            occurrence: parseNum(record.occurrence),
                            soTsr: parseNum(record.soTsr),
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await MaintenanceCalendar.bulkWrite(bulkOperations);
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

        res.status(200).json({ success: true, message: "Calendar input deleted successfully." });
    } catch (error) {
        console.error("Error deleting calendar input:", error);
        res.status(500).json({ message: "Failed to delete calendar input", error: error.message });
    }
};
