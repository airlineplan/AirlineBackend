const moment = require("moment");

const metricKeys = Object.freeze({
    time: ["tsn", "tsoTsr", "tsRplmt"],
    cycles: ["csn", "csoCsr", "csRplmt"],
    days: ["dsn", "dsoDsr", "dsRplmt"]
});

const toMetric = (value) => {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
};

const toDateOnly = (value) => {
    const parsed = moment.utc(value, moment.ISO_8601, true);
    return parsed.isValid() ? parsed.startOf("day") : null;
};

const assumptionCoversDate = (assumption, effectiveMsn, date) => {
    const targetDate = toDateOnly(date);
    const fromDate = toDateOnly(assumption?.fromDate);
    const toDate = toDateOnly(assumption?.toDate);
    return Boolean(
        targetDate && fromDate && toDate &&
        String(assumption?.msn || "").trim() === String(effectiveMsn || "").trim() &&
        targetDate.isSameOrAfter(fromDate, "day") &&
        targetDate.isSameOrBefore(toDate, "day")
    );
};

const selectAssumptionForDate = ({ assumptions = [], effectiveMsn, date }) => {
    const matches = assumptions
        .filter(assumption => assumptionCoversDate(assumption, effectiveMsn, date))
        .sort((left, right) => {
            const fromDifference = toDateOnly(right.fromDate).valueOf() - toDateOnly(left.fromDate).valueOf();
            if (fromDifference !== 0) return fromDifference;
            return moment.utc(right.updatedAt || right.createdAt || 0).valueOf() -
                moment.utc(left.updatedAt || left.createdAt || 0).valueOf();
        });
    return matches[0] || null;
};

const getAssumptionUsageForDate = ({ assumptions = [], effectiveMsn, date }) => {
    const assumption = selectAssumptionForDate({ assumptions, effectiveMsn, date });
    if (!assumption) {
        return {
            timeUsage: 0,
            cycleUsage: 0,
            hasUsage: false,
            source: "NONE",
            assumption: null,
            assumptionId: null
        };
    }

    return {
        timeUsage: Number(assumption.hours || 0),
        cycleUsage: Number(assumption.cycles || 0),
        hasUsage: true,
        source: "ASSUMPTION",
        assumption,
        assumptionId: assumption._id ? String(assumption._id) : null
    };
};

const projectDailyState = (state = {}, { canOperate = true, timeUsage = 0, cycleUsage = 0 } = {}) => {
    const nextState = {};
    for (const key of [...metricKeys.time, ...metricKeys.cycles, ...metricKeys.days]) {
        nextState[key] = toMetric(state[key]);
    }

    const appliedTime = canOperate ? Number(timeUsage || 0) : 0;
    const appliedCycles = canOperate ? Number(cycleUsage || 0) : 0;

    for (const key of metricKeys.time) {
        if (nextState[key] !== null) nextState[key] = Number((nextState[key] + appliedTime).toFixed(2));
    }
    for (const key of metricKeys.cycles) {
        if (nextState[key] !== null) nextState[key] += appliedCycles;
    }
    // Days-since counters represent elapsed calendar days for a continuously
    // installed serialized component, including maintenance/ground days.
    for (const key of metricKeys.days) {
        if (nextState[key] !== null) nextState[key] += 1;
    }

    return {
        state: nextState,
        appliedTime,
        appliedCycles
    };
};

const applyMaintenanceEvent = (state = {}, postEventValues = {}) => ({
    ...state,
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "tsoTsr")
        ? { tsoTsr: toMetric(postEventValues.tsoTsr) }
        : {}),
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "csoCsr")
        ? { csoCsr: toMetric(postEventValues.csoCsr) }
        : {}),
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "dsoDsr")
        ? { dsoDsr: toMetric(postEventValues.dsoDsr) }
        : {}),
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "tsRplmt")
        ? { tsRplmt: toMetric(postEventValues.tsRplmt) }
        : {}),
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "csRplmt")
        ? { csRplmt: toMetric(postEventValues.csRplmt) }
        : {}),
    ...(Object.prototype.hasOwnProperty.call(postEventValues, "dsRplmt")
        ? { dsRplmt: toMetric(postEventValues.dsRplmt) }
        : {})
});

module.exports = {
    applyMaintenanceEvent,
    assumptionCoversDate,
    getAssumptionUsageForDate,
    projectDailyState,
    selectAssumptionForDate,
    toDateOnly,
    toMetric
};
