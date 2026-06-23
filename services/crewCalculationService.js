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
  CrewUploadBatch,
  CrewUtilisationTarget,
} = require("../model/crewSchemas");
const {
  addMinutes,
  clockToMinutes,
  dateKey,
  diffMinutes,
  monthKey,
  normalizeText,
  normalizeUpper,
  roundMoney,
} = require("./crewTimeUtils");

const DEFAULT_DUTY_SETTINGS = {
  restThresholdMinutes: 420,
  breakThresholdMinutes: 180,
  preflightNewFdpMinutes: 90,
  preflightExistingDutyMinutes: 45,
  postflightMinutes: 30,
};

const DEFAULT_POSITIONING_SETTINGS = {
  returnToBaseAfterFdpEnabled: true,
  hotacCutoffEnabled: false,
  hotacCutoffLocalTime: "20:00",
  positioningWithinCurrentFdpEnabled: true,
  defaultPositioningMinutes: 150,
  hotacToAirportTransferMinutes: 60,
};

const DEFAULT_UTILISATION_TARGETS = [
  {
    role: "ALL_ROLES",
    averageDpMinutesPerDay: 0,
    averageFdpMinutesPerDay: 0,
    averageFtMinutesPerDay: 0,
  },
];

const DEFAULT_LAYOVER_RULES = [
  {
    ruleType: "CONVENIENCE",
    station: "ALL_STATIONS",
    role: "ALL_ROLES",
    thresholdMinutes: 180,
    costAmount: 0,
    costBasis: "PER_HOUR",
    currency: "INR",
  },
  {
    ruleType: "HOTAC",
    station: "ALL_STATIONS",
    role: "ALL_ROLES",
    thresholdMinutes: 420,
    costAmount: 0,
    costBasis: "PER_24_HOURS",
    currency: "INR",
  },
];

const DEFAULT_POSITIONING_COST_RULES = [
  {
    departureStation: "ALL_STATIONS",
    arrivalStation: "ALL_STATIONS",
    sector: "ALL_STATIONS-ALL_STATIONS",
    role: "ALL_ROLES",
    costAmount: 0,
    currency: "INR",
    isOverride: true,
  },
];

const toPlainId = (value) => (value?._id ? value._id : value);

const sortByStart = (a, b) => {
  const startDiff = new Date(a.start).getTime() - new Date(b.start).getTime();
  if (startDiff !== 0) return startDiff;
  return new Date(a.end).getTime() - new Date(b.end).getTime();
};

const eventDuration = (event) => diffMinutes(event.startDateTime, event.endDateTime);

const eventCurrency = (crewMember, overrideCurrency = "") => (
  normalizeUpper(overrideCurrency) || normalizeUpper(crewMember.allowanceCurrency) || "INR"
);

const startOfUtcDay = (date) => moment.utc(date).startOf("day").toDate();

const startOfNextUtcDay = (date) => moment.utc(date).add(1, "day").startOf("day").toDate();

const splitByUtcDay = (start, end) => {
  const periods = [];
  let cursor = new Date(start);
  const finalEnd = new Date(end);

  while (cursor < finalEnd) {
    const nextMidnight = moment.utc(cursor).add(1, "day").startOf("day").toDate();
    const segmentEnd = nextMidnight < finalEnd ? nextMidnight : finalEnd;
    if (diffMinutes(cursor, segmentEnd) > 0) {
      periods.push({ start: cursor, end: segmentEnd });
    }
    cursor = segmentEnd;
  }

  return periods;
};

const splitEventByUtcDay = (event) => {
  const duration = eventDuration(event);
  if (duration <= 0) return [event];

  const periods = splitByUtcDay(event.startDateTime, event.endDateTime);
  if (periods.length <= 1) return [event];

  const minuteFields = ["dpMinutes", "fdpMinutes", "ftMinutes", "rpMinutes"];
  const costFields = ["dpCost", "fdpCost", "ftCost", "layoverCost", "positioningCost"];

  return periods.map((period, index) => {
    const segmentDuration = diffMinutes(period.start, period.end);
    const factor = segmentDuration / duration;
    const segment = {
      ...event,
      startDateTime: period.start,
      endDateTime: period.end,
      displayDate: dateKey(period.start),
    };

    minuteFields.forEach((field) => {
      segment[field] = Math.round((Number(event[field]) || 0) * factor);
    });
    costFields.forEach((field) => {
      segment[field] = roundMoney((Number(event[field]) || 0) * factor);
    });

    if (event._id) segment._id = `${event._id}-${index + 1}`;
    return segment;
  });
};

const makeEvent = ({
  crewMember,
  start,
  end,
  category,
  subCategory = "",
  sourceType,
  sourceId = null,
  location = "",
  departureStation = "",
  arrivalStation = "",
  flightNumber = "",
  dpMinutes = 0,
  fdpMinutes = 0,
  ftMinutes = 0,
  rpMinutes = 0,
  layoverCost = 0,
  positioningCost = 0,
  currency = "",
  reasonText = "",
  isGenerated = true,
}) => {
  const safeStart = new Date(start);
  const safeEnd = new Date(end);
  const resolvedCurrency = eventCurrency(crewMember, currency);

  return {
    crewMemberId: toPlainId(crewMember._id || crewMember.id),
    crewCode: normalizeUpper(crewMember.crewCode),
    crewName: crewMember.name,
    role: crewMember.role,
    baseStation: normalizeUpper(crewMember.baseStation),
    startDateTime: safeStart,
    endDateTime: safeEnd,
    displayDate: dateKey(safeStart),
    location: normalizeUpper(location || arrivalStation || departureStation),
    departureStation: normalizeUpper(departureStation),
    arrivalStation: normalizeUpper(arrivalStation),
    flightNumber: normalizeUpper(flightNumber),
    category,
    subCategory,
    dpMinutes,
    fdpMinutes,
    ftMinutes,
    rpMinutes,
    dpCost: roundMoney((dpMinutes / 60) * (Number(crewMember.dpAllowanceRate) || 0)),
    fdpCost: roundMoney((fdpMinutes / 60) * (Number(crewMember.fdpAllowanceRate) || 0)),
    ftCost: roundMoney((ftMinutes / 60) * (Number(crewMember.ftAllowanceRate) || 0)),
    layoverCost: roundMoney(layoverCost),
    positioningCost: roundMoney(positioningCost),
    currency: resolvedCurrency,
    isGenerated,
    sourceType,
    sourceId,
    reasonText,
  };
};

const getRuleScore = (rule, station, role) => {
  const stationMatch = normalizeUpper(rule.station) === normalizeUpper(station);
  const stationAll = normalizeUpper(rule.station) === "ALL_STATIONS";
  const roleMatch = normalizeText(rule.role).toLowerCase() === normalizeText(role).toLowerCase();
  const roleAll = normalizeUpper(rule.role) === "ALL_ROLES";

  if (!(stationMatch || stationAll) || !(roleMatch || roleAll)) return -1;
  return (stationMatch ? 2 : 1) + (roleMatch ? 2 : 1);
};

const findLayoverRule = (rules, ruleType, station, role) => (
  (rules || [])
    .filter((rule) => rule.ruleType === ruleType)
    .map((rule) => ({ rule, score: getRuleScore(rule, station, role) }))
    .filter((entry) => entry.score >= 0)
    .sort((a, b) => b.score - a.score)[0]?.rule || null
);

const findPositioningCostRule = (rules, departureStation, arrivalStation, role) => {
  const departure = normalizeUpper(departureStation);
  const arrival = normalizeUpper(arrivalStation);
  const roleLower = normalizeText(role).toLowerCase();

  return (rules || [])
    .map((rule) => {
      const ruleDeparture = normalizeUpper(rule.departureStation);
      const ruleArrival = normalizeUpper(rule.arrivalStation);
      const ruleSector = normalizeUpper(rule.sector);
      const departureExact = ruleDeparture === departure;
      const arrivalExact = ruleArrival === arrival;
      const departureAll = ruleDeparture === "ALL_STATIONS";
      const arrivalAll = ruleArrival === "ALL_STATIONS";
      const sectorExact = ruleSector === `${departure}-${arrival}`;
      const sectorAll = ruleSector === "ALL_STATIONS-ALL_STATIONS";
      const roleExact = normalizeText(rule.role).toLowerCase() === roleLower;
      const roleAll = normalizeUpper(rule.role) === "ALL_ROLES";
      const sectorMatches = sectorExact || ((departureExact || departureAll) && (arrivalExact || arrivalAll)) || sectorAll;
      if (!sectorMatches || !(roleExact || roleAll)) return null;
      return {
        rule,
        score: (departureExact ? 2 : 1) + (arrivalExact ? 2 : 1) + (roleExact ? 2 : 1),
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score)[0]?.rule || null;
};

const isAtOrBeforeCutoff = (date, clock) => {
  const cutoffMinutes = clockToMinutes(clock);
  if (cutoffMinutes === null) return false;
  const current = moment.utc(date);
  const currentMinutes = current.hour() * 60 + current.minute();
  return currentMinutes <= cutoffMinutes;
};

const isPositioningActivity = (activity) => (
  activity.type === "OTHER" &&
  `${activity.category} ${activity.subCategory}`.toLowerCase().match(/position|deadhead|travel|return to base/) !== null
);

const otherDutyLocationAfter = (activity, crewMember) => {
  const combined = `${activity.category} ${activity.subCategory}`.toLowerCase();
  if (combined.includes("return to base")) return normalizeUpper(crewMember.baseStation);
  return normalizeUpper(activity.location);
};

const hasMatchingUserPositioning = ({ activities, fromStation, toStation, beforeStart, afterEnd }) => (
  activities.some((activity) => (
    isPositioningActivity(activity) &&
    new Date(activity.start).getTime() >= new Date(afterEnd || 0).getTime() &&
    new Date(activity.end).getTime() <= new Date(beforeStart).getTime() &&
    (
      normalizeUpper(activity.departureStation) === normalizeUpper(fromStation) ||
      normalizeUpper(activity.location) === "FLIGHT" ||
      normalizeUpper(activity.location) === normalizeUpper(fromStation)
    ) &&
    (
      normalizeUpper(activity.arrivalStation) === normalizeUpper(toStation) ||
      `${activity.category} ${activity.subCategory}`.toLowerCase().includes("return to base") ||
      normalizeUpper(activity.location) === "FLIGHT"
    )
  ))
);

const addBreakEvent = ({ events, crewMember, start, end, location, activeFdp, layoverRules, costDurationMinutes = null }) => {
  const duration = diffMinutes(start, end);
  if (duration <= 0) return;
  const eligibleDuration = costDurationMinutes === null ? duration : Math.max(0, Number(costDurationMinutes) || 0);
  const rule = findLayoverRule(layoverRules, "CONVENIENCE", location, crewMember.role);
  let layoverCost = 0;
  let subCategory = "Between Duties";
  let reasonText = `Break inserted because the gap of ${duration} minutes meets the configured break threshold.`;

  if (rule && eligibleDuration >= Number(rule.thresholdMinutes || 0)) {
    layoverCost = (eligibleDuration / 60) * Number(rule.costAmount || 0);
    subCategory = "Convenience";
    reasonText += ` Convenience rule matched for ${normalizeUpper(rule.station)} / ${rule.role}; threshold ${rule.thresholdMinutes} minutes; ${eligibleDuration} minutes were cost eligible.`;
  }

  events.push(makeEvent({
    crewMember,
    start,
    end,
    location,
    category: "Break",
    subCategory,
    sourceType: "SYSTEM_BREAK",
    dpMinutes: activeFdp ? duration : 0,
    fdpMinutes: activeFdp ? duration : 0,
    layoverCost,
    currency: rule?.currency,
    reasonText,
  }));
};

const addContinuingDutyEvent = ({ events, crewMember, start, end, location, activeFdp }) => {
  const duration = diffMinutes(start, end);
  if (duration <= 0) return;

  events.push(makeEvent({
    crewMember,
    start,
    end,
    location,
    category: "Flight Duty",
    subCategory: "Between Flights in FDP",
    sourceType: "SYSTEM_CONTINUING_DUTY",
    dpMinutes: activeFdp ? duration : 0,
    fdpMinutes: activeFdp ? duration : 0,
    reasonText: `Continuing duty inserted because the gap of ${duration} minutes is below the configured break threshold.`,
  }));
};

const addRestEvent = ({ events, crewMember, start, end, location, layoverRules, reasonPrefix = "Rest inserted" }) => {
  const duration = diffMinutes(start, end);
  if (duration <= 0) return;
  const baseStation = normalizeUpper(crewMember.baseStation);
  const restStation = normalizeUpper(location);
  const isBase = restStation === baseStation;
  const rule = !isBase ? findLayoverRule(layoverRules, "HOTAC", restStation, crewMember.role) : null;
  let layoverCost = 0;
  let subCategory = isBase ? "Rest at Base" : "Away Rest";
  let reasonText = `${reasonPrefix} because the duty gap of ${duration} minutes meets the configured rest threshold.`;

  if (isBase) {
    reasonText += " Rest location equals crew base, so HOTAC was not applied.";
  } else if (rule && duration >= Number(rule.thresholdMinutes || 0)) {
    layoverCost = (duration / (24 * 60)) * Number(rule.costAmount || 0);
    subCategory = "Layover HOTAC";
    reasonText += ` HOTAC rule matched for ${normalizeUpper(rule.station)} / ${rule.role}; threshold ${rule.thresholdMinutes} minutes.`;
  } else {
    reasonText += " No matching HOTAC threshold was met.";
  }

  for (const period of splitByUtcDay(start, end)) {
    const segmentDuration = diffMinutes(period.start, period.end);
    const segmentLayoverCost = layoverCost > 0 ? (layoverCost * segmentDuration) / duration : 0;
    events.push(makeEvent({
      crewMember,
      start: period.start,
      end: period.end,
      location: restStation,
      category: "Rest",
      subCategory,
      sourceType: "SYSTEM_REST",
      rpMinutes: segmentDuration,
      layoverCost: segmentLayoverCost,
      currency: rule?.currency,
      reasonText,
    }));
  }
};

const addPostflightEvent = ({ events, crewMember, start, dutySettings, location, limitEnd }) => {
  const duration = Number(dutySettings.postflightMinutes || 0);
  if (duration <= 0) return start;
  const end = addMinutes(start, duration);
  if (limitEnd && new Date(end).getTime() > new Date(limitEnd).getTime()) return start;
  events.push(makeEvent({
    crewMember,
    start,
    end,
    location,
    category: "Flight Duty",
    subCategory: "Post-flight",
    sourceType: "SYSTEM_POST_FLIGHT",
    dpMinutes: duration,
    fdpMinutes: duration,
    reasonText: "Post-flight duty inserted after the last operated flight before a qualifying rest/end of sequence.",
  }));
  return end;
};

const addPreflightEvent = ({ events, crewMember, end, duration, location, activeDuty }) => {
  if (duration <= 0) return end;
  const start = addMinutes(end, -duration);
  events.push(makeEvent({
    crewMember,
    start,
    end,
    location,
    category: "Flight Duty",
    subCategory: "Pre-flight",
    sourceType: "SYSTEM_PRE_FLIGHT",
    dpMinutes: duration,
    fdpMinutes: duration,
    reasonText: activeDuty
      ? "Reduced pre-flight duration applied because crew duty is already in effect due to preceding duty/positioning."
      : "Pre-flight duty inserted because this is the first operated flight after qualifying rest.",
  }));
  return start;
};

const addPositioningEvent = ({
  events,
  crewMember,
  start,
  duration,
  fromStation,
  toStation,
  positioningCostRules,
  withinFdp,
  reasonText,
}) => {
  if (duration <= 0 || normalizeUpper(fromStation) === normalizeUpper(toStation)) return start;
  const end = addMinutes(start, duration);
  const rule = findPositioningCostRule(positioningCostRules, fromStation, toStation, crewMember.role);
  events.push(makeEvent({
    crewMember,
    start,
    end,
    location: normalizeUpper(toStation),
    departureStation: fromStation,
    arrivalStation: toStation,
    category: "Positioning",
    subCategory: "Deadheading",
    sourceType: "SYSTEM_POSITIONING",
    dpMinutes: duration,
    fdpMinutes: withinFdp ? duration : 0,
    ftMinutes: 0,
    positioningCost: Number(rule?.costAmount || 0),
    currency: rule?.currency,
    reasonText: reasonText || `Positioning inserted because crew location ${normalizeUpper(fromStation)} differs from required station ${normalizeUpper(toStation)}.`,
  }));
  return end;
};

const addTransferEvent = ({
  events,
  crewMember,
  start,
  duration,
  location,
  subCategory,
  withinFdp,
  reasonText,
}) => {
  if (duration <= 0) return start;
  const end = addMinutes(start, duration);
  events.push(makeEvent({
    crewMember,
    start,
    end,
    location,
    category: "Positioning",
    subCategory,
    sourceType: "SYSTEM_POSITIONING",
    dpMinutes: duration,
    fdpMinutes: withinFdp ? duration : 0,
    reasonText,
  }));
  return end;
};

const normalizeActivities = ({ flightAssignments, otherDuties }) => {
  const flights = (flightAssignments || []).map((assignment) => ({
    type: "FLIGHT",
    sourceId: toPlainId(assignment._id || assignment.id),
    start: assignment.std,
    end: assignment.sta,
    flightNumber: assignment.flightNumber,
    departureStation: assignment.departureStation,
    arrivalStation: assignment.arrivalStation,
    sector: assignment.sector,
  }));

  const duties = (otherDuties || []).map((duty) => ({
    type: "OTHER",
    sourceId: toPlainId(duty._id || duty.id),
    start: duty.startDateTime,
    end: duty.endDateTime,
    location: duty.location,
    category: duty.category,
    subCategory: duty.subCategory,
    isUserEnteredPositioning: duty.isUserEnteredPositioning,
  }));

  return [...flights, ...duties]
    .filter((activity) => activity.start && activity.end)
    .sort(sortByStart);
};

const calculateCrewMemberEvents = ({
  crewMember,
  flightAssignments = [],
  otherDuties = [],
  dutySettings = DEFAULT_DUTY_SETTINGS,
  positioningSettings = DEFAULT_POSITIONING_SETTINGS,
  layoverRules = [],
  positioningCostRules = [],
  coverageStart = null,
  coverageEnd = null,
}) => {
  const settings = { ...DEFAULT_DUTY_SETTINGS, ...(dutySettings || {}) };
  const posSettings = { ...DEFAULT_POSITIONING_SETTINGS, ...(positioningSettings || {}) };
  const activities = normalizeActivities({ flightAssignments, otherDuties });
  const events = [];
  let currentLocation = normalizeUpper(crewMember.baseStation);
  let lastDutyEnd = null;
  let lastWasFlight = false;
  let activeFdp = false;
  let activeDuty = false;
  let postflightAlreadyAddedForLastFlight = false;

  const getLeadIn = (activity, location, activeDutyState, activeFdpState, afterEnd, previousDutyWasNonFlight = false) => {
    if (activity.type !== "FLIGHT") {
      return { duration: 0, preflightMinutes: 0, positioningMinutes: 0, transferMinutes: 0, needsPositioning: false };
    }

    const departure = normalizeUpper(activity.departureStation);
    const matchingUploadedPositioning = hasMatchingUserPositioning({
      activities,
      fromStation: location,
      toStation: departure,
      beforeStart: activity.start,
      afterEnd,
    });
    const previousDutyWasUnspecifiedTravel = normalizeUpper(location) === "FLIGHT";
    const needsPositioning = Boolean(
      posSettings.positioningWithinCurrentFdpEnabled &&
      !previousDutyWasNonFlight &&
      normalizeUpper(location) &&
      normalizeUpper(location) !== departure &&
      !matchingUploadedPositioning &&
      !previousDutyWasUnspecifiedTravel
    );
    const positioningMinutes = needsPositioning ? Number(posSettings.defaultPositioningMinutes || 0) : 0;
    const transferMinutes = needsPositioning && normalizeUpper(location) !== normalizeUpper(crewMember.baseStation)
      ? Number(posSettings.hotacToAirportTransferMinutes || 0)
      : 0;
    let preflightMinutes = 0;
    if (!activeFdpState) {
      preflightMinutes = (needsPositioning || activeDutyState)
        ? Number(settings.preflightExistingDutyMinutes || 0)
        : Number(settings.preflightNewFdpMinutes || 0);
    } else if (needsPositioning) {
      preflightMinutes = Number(settings.preflightExistingDutyMinutes || 0);
    }

    return {
      duration: preflightMinutes + positioningMinutes + transferMinutes,
      preflightMinutes,
      positioningMinutes,
      transferMinutes,
      needsPositioning,
      matchingUploadedPositioning,
    };
  };

  const insertLeadInAndActivity = (activity, leadIn) => {
    if (activity.type === "FLIGHT") {
      const departure = normalizeUpper(activity.departureStation);
      const arrival = normalizeUpper(activity.arrivalStation);
      let preflightEnd = new Date(activity.start);
      const preflightStart = addMinutes(preflightEnd, -Number(leadIn.preflightMinutes || 0));
      let positioningStart = addMinutes(preflightStart, -Number(leadIn.positioningMinutes || 0));
      let cursor = addMinutes(positioningStart, -Number(leadIn.transferMinutes || 0));

      if (leadIn.matchingUploadedPositioning) {
        currentLocation = departure;
      } else if (leadIn.needsPositioning) {
        if (leadIn.transferMinutes > 0) {
          cursor = addTransferEvent({
            crewMember,
            events,
            start: cursor,
            duration: leadIn.transferMinutes,
            location: currentLocation,
            subCategory: "HOTAC to Airport Transfer",
            withinFdp: true,
            reasonText: "Airport transfer inserted before positioning from HOTAC/rest station.",
          });
        }
        positioningStart = cursor;
        cursor = addPositioningEvent({
          events,
          crewMember,
          start: positioningStart,
          duration: leadIn.positioningMinutes,
          fromStation: currentLocation,
          toStation: departure,
          positioningCostRules,
          withinFdp: true,
          reasonText: `Positioning inserted because crew is at ${currentLocation} and next operated flight departs ${departure}.`,
        });
        currentLocation = departure;
      }

      if (leadIn.preflightMinutes > 0) {
        addPreflightEvent({
          events,
          crewMember,
          end: preflightEnd,
          duration: leadIn.preflightMinutes,
          location: departure,
          activeDuty: activeDuty || leadIn.needsPositioning || leadIn.matchingUploadedPositioning,
        });
      }

      const flightMinutes = diffMinutes(activity.start, activity.end);
      events.push(makeEvent({
        crewMember,
        start: activity.start,
        end: activity.end,
        location: arrival,
        departureStation: departure,
        arrivalStation: arrival,
        flightNumber: activity.flightNumber,
        category: "Flight Duty",
        subCategory: "Operated Flight",
        sourceType: "FLIGHT_ROSTER",
        sourceId: activity.sourceId,
        dpMinutes: flightMinutes,
        fdpMinutes: flightMinutes,
        ftMinutes: flightMinutes,
        isGenerated: false,
        reasonText: "Operated flight imported from Crew Roster - Flight Duty.",
      }));

      currentLocation = arrival;
      lastDutyEnd = new Date(activity.end);
      lastWasFlight = true;
      activeFdp = true;
      activeDuty = true;
      postflightAlreadyAddedForLastFlight = false;
      return;
    }

    const duration = diffMinutes(activity.start, activity.end);
    const location = normalizeUpper(activity.location);
    events.push(makeEvent({
      crewMember,
      start: activity.start,
      end: activity.end,
      location,
      category: activity.category,
      subCategory: activity.subCategory,
      sourceType: "OTHER_DUTY_ROSTER",
      sourceId: activity.sourceId,
      dpMinutes: duration,
      fdpMinutes: 0,
      ftMinutes: 0,
      isGenerated: false,
      reasonText: "Duty imported from Crew Roster - Other Duty.",
    }));

    currentLocation = otherDutyLocationAfter(activity, crewMember);
    lastDutyEnd = new Date(activity.end);
    lastWasFlight = false;
    activeDuty = true;
    postflightAlreadyAddedForLastFlight = false;
  };

  for (const activity of activities) {
    if (!lastDutyEnd) {
      const leadIn = getLeadIn(activity, currentLocation, activeDuty, activeFdp, null);
      if (coverageStart && new Date(coverageStart).getTime() < new Date(addMinutes(activity.start, -leadIn.duration)).getTime()) {
        addRestEvent({
          events,
          crewMember,
          start: coverageStart,
          end: addMinutes(activity.start, -leadIn.duration),
          location: currentLocation,
          layoverRules,
          reasonPrefix: "Rest inserted before the first duty in the calculation coverage window",
        });
      }
      insertLeadInAndActivity(activity, leadIn);
      continue;
    }

    const previousDutyWasNonFlight = !lastWasFlight && lastDutyEnd;
    let leadIn = getLeadIn(activity, currentLocation, activeDuty, activeFdp, lastDutyEnd, previousDutyWasNonFlight);
    let nextDutyStart = addMinutes(activity.start, -leadIn.duration);
    let gapMinutes = diffMinutes(lastDutyEnd, nextDutyStart);

    if (gapMinutes >= Number(settings.restThresholdMinutes || 0)) {
      let restStart = lastDutyEnd;
      if (lastWasFlight) {
        restStart = addPostflightEvent({
          events,
          crewMember,
          start: lastDutyEnd,
          dutySettings: settings,
          location: currentLocation,
          limitEnd: nextDutyStart,
        });
        postflightAlreadyAddedForLastFlight = true;
      }

      const activityDeparture = activity.type === "FLIGHT" ? normalizeUpper(activity.departureStation) : normalizeUpper(activity.location);
      const baseStation = normalizeUpper(crewMember.baseStation);
      const restPrePositionStart = restStart;
      const shouldReturnBase = (
        activity.type === "FLIGHT" &&
        lastWasFlight &&
        posSettings.returnToBaseAfterFdpEnabled &&
        currentLocation !== baseStation &&
        activityDeparture === baseStation
      );
      const shouldCutoffPosition = (
        activity.type === "FLIGHT" &&
        lastWasFlight &&
        posSettings.hotacCutoffEnabled &&
        currentLocation !== baseStation &&
        activityDeparture !== baseStation &&
        currentLocation !== activityDeparture &&
        isAtOrBeforeCutoff(restStart, posSettings.hotacCutoffLocalTime)
      );

      if (shouldReturnBase || shouldCutoffPosition) {
        const toStation = shouldReturnBase ? baseStation : activityDeparture;
        let posEnd = addPositioningEvent({
          events,
          crewMember,
          start: restStart,
          duration: Number(posSettings.defaultPositioningMinutes || 0),
          fromStation: currentLocation,
          toStation,
          positioningCostRules,
          withinFdp: false,
          reasonText: shouldReturnBase
            ? "Return-to-base positioning inserted because a qualifying rest follows and the next duty starts at crew base."
            : "HOTAC cutoff positioning inserted before rest because FDP ended at or before the configured cutoff and next duty starts at another non-base station.",
        });
        if (shouldCutoffPosition && Number(posSettings.hotacToAirportTransferMinutes || 0) > 0) {
          posEnd = addTransferEvent({
            events,
            crewMember,
            start: posEnd,
            duration: Number(posSettings.hotacToAirportTransferMinutes || 0),
            location: toStation,
            subCategory: "Airport to HOTAC Transfer",
            withinFdp: false,
            reasonText: "Airport transfer inserted after cutoff positioning so rest starts at HOTAC in the next duty station.",
          });
        }
        currentLocation = toStation;
        restStart = posEnd;
      }

      activeFdp = false;
      activeDuty = false;
      leadIn = getLeadIn(activity, currentLocation, activeDuty, activeFdp, restStart, previousDutyWasNonFlight);
      nextDutyStart = addMinutes(activity.start, -leadIn.duration);
      if (new Date(nextDutyStart).getTime() < new Date(restStart).getTime()) {
        nextDutyStart = restStart;
      }
      addRestEvent({
        events,
        crewMember,
        start: restStart,
        end: nextDutyStart,
        location: currentLocation,
        layoverRules,
      });
      lastDutyEnd = nextDutyStart;
      lastWasFlight = false;
      if (diffMinutes(restPrePositionStart, restStart) > 0) {
        activeDuty = false;
      }
    } else if (gapMinutes >= Number(settings.breakThresholdMinutes || 0)) {
      addBreakEvent({
        events,
        crewMember,
        start: lastDutyEnd,
        end: nextDutyStart,
        location: currentLocation,
        activeFdp,
        layoverRules,
        costDurationMinutes: lastWasFlight
          ? Math.max(0, gapMinutes - Number(settings.postflightMinutes || 0) - 1)
          : gapMinutes,
      });
      lastDutyEnd = nextDutyStart;
      activeDuty = true;
      lastWasFlight = false;
    } else if (gapMinutes > 0) {
      addContinuingDutyEvent({
        events,
        crewMember,
        start: lastDutyEnd,
        end: nextDutyStart,
        location: currentLocation,
        activeFdp,
      });
      lastDutyEnd = nextDutyStart;
      activeDuty = true;
      lastWasFlight = false;
    }

    insertLeadInAndActivity(activity, leadIn);
  }

  if (lastWasFlight && !postflightAlreadyAddedForLastFlight) {
    lastDutyEnd = addPostflightEvent({
      events,
      crewMember,
      start: lastDutyEnd,
      dutySettings: settings,
      location: currentLocation,
    });
  }

  if (coverageEnd) {
    if (!lastDutyEnd && coverageStart && new Date(coverageStart).getTime() < new Date(coverageEnd).getTime()) {
      addRestEvent({
        events,
        crewMember,
        start: coverageStart,
        end: coverageEnd,
        location: currentLocation,
        layoverRules,
        reasonPrefix: "Rest inserted for a crew member with no duties in the calculation coverage window",
      });
    } else if (lastDutyEnd && new Date(lastDutyEnd).getTime() < new Date(coverageEnd).getTime()) {
      addRestEvent({
        events,
        crewMember,
        start: lastDutyEnd,
        end: coverageEnd,
        location: currentLocation,
        layoverRules,
        reasonPrefix: "Rest inserted after the last duty in the calculation coverage window",
      });
    }
  }

  return events
    .flatMap(splitEventByUtcDay)
    .sort((a, b) => new Date(a.startDateTime).getTime() - new Date(b.startDateTime).getTime());
};

const loadSettings = async (userId) => {
  const [dutySettings, positioningSettings] = await Promise.all([
    CrewDutySettings.findOne({ userId }).lean(),
    CrewPositioningSettings.findOne({ userId }).lean(),
  ]);

  return {
    dutySettings: { ...DEFAULT_DUTY_SETTINGS, ...(dutySettings || {}) },
    positioningSettings: { ...DEFAULT_POSITIONING_SETTINGS, ...(positioningSettings || {}) },
  };
};

const validatePreconditions = async ({ userId }) => {
  const [crewCount, flightCount, otherDutyCount] = await Promise.all([
    CrewMember.countDocuments({ userId }),
    CrewFlightAssignment.countDocuments({ userId }),
    CrewOtherDuty.countDocuments({ userId }),
  ]);

  const errors = [];
  if (crewCount === 0) errors.push("Crew Information must be uploaded before calculation.");
  if (flightCount === 0 && otherDutyCount === 0) errors.push("Upload a Flight Duty or Other Duty roster before calculation.");

  return { valid: errors.length === 0, errors, crewCount, flightCount, otherDutyCount };
};

const getTargetForRole = (targets, role) => {
  const roleLower = normalizeText(role).toLowerCase();
  return (targets || []).find((target) => normalizeText(target.role).toLowerCase() === roleLower)
    || (targets || []).find((target) => normalizeUpper(target.role) === "ALL_ROLES")
    || null;
};

const lowerEventText = (event) => `${normalizeText(event.category)} ${normalizeText(event.subCategory)}`.toLowerCase();

const isPositioningEvent = (event) => {
  const category = normalizeText(event.category).toLowerCase();
  const subCategory = normalizeText(event.subCategory).toLowerCase();
  return category.includes("position") || /position|deadhead|return to base/.test(subCategory);
};

const isConvenienceEvent = (event) => (
  normalizeText(event.subCategory).toLowerCase() === "convenience" ||
  (event.sourceType === "SYSTEM_LAYOVER" && lowerEventText(event).includes("convenience"))
);

const isHotacEvent = (event) => (
  normalizeText(event.subCategory).toLowerCase() === "layover hotac" ||
  (event.sourceType === "SYSTEM_LAYOVER" && lowerEventText(event).includes("hotac"))
);

const isLayoverEvent = (event) => (
  event.sourceType === "SYSTEM_LAYOVER" ||
  isConvenienceEvent(event) ||
  isHotacEvent(event)
);

const addCrewRoleForTarget = (crewRoles, event) => {
  const crewKey = normalizeUpper(event.crewCode) || String(event.crewMemberId || "");
  if (crewKey && event.role && !crewRoles.has(crewKey)) {
    crewRoles.set(crewKey, event.role);
  }
};

const rolewiseTargetMinutes = (targets, crewRoles, field, days) => (
  Array.from(crewRoles.values()).reduce((total, role) => {
    const target = getTargetForRole(targets, role);
    return total + (Number(target?.[field] || 0) * days);
  }, 0)
);

const buildMonthlyKpiSummaries = ({ userId, calculationRunId, events, targets }) => {
  const groups = new Map();

  for (const event of events) {
    const key = [
      monthKey(event.startDateTime),
      event.role || "",
      event.baseStation || "",
      event.currency || "",
    ].join("|");
    if (!groups.has(key)) {
      const start = moment.utc(event.startDateTime).startOf("month");
      groups.set(key, {
        userId,
        calculationRunId,
        groupingKey: key,
        periodStart: start.toDate(),
        periodEnd: start.clone().endOf("month").toDate(),
        periodicity: "MONTHLY",
        role: event.role || "",
        baseStation: event.baseStation || "",
        totalDpMinutes: 0,
        totalFdpMinutes: 0,
        totalFtMinutes: 0,
        totalRpMinutes: 0,
        totalLandings: 0,
        positioningCount: 0,
        layoverOccurrences: 0,
        layoverDurationMinutes: 0,
        layoverTotalCost: 0,
        layoverAverageCost: 0,
        positioningTotalCost: 0,
        positioningAverageCost: 0,
        convenienceCount: 0,
        convenienceTotalCost: 0,
        convenienceAverageCost: 0,
        hotacCount: 0,
        hotacTotalCost: 0,
        hotacAverageCost: 0,
        currency: event.currency || "",
        crewRoles: new Map(),
      });
    }
    const row = groups.get(key);
    addCrewRoleForTarget(row.crewRoles, event);
    row.totalDpMinutes += Number(event.dpMinutes || 0);
    row.totalFdpMinutes += Number(event.fdpMinutes || 0);
    row.totalFtMinutes += Number(event.ftMinutes || 0);
    row.totalRpMinutes += Number(event.rpMinutes || 0);
    if (event.sourceType === "FLIGHT_ROSTER") row.totalLandings += 1;
    if (isPositioningEvent(event)) row.positioningCount += 1;
    if (isLayoverEvent(event)) {
      row.layoverOccurrences += 1;
      row.layoverDurationMinutes += eventDuration(event);
      row.layoverTotalCost += Number(event.layoverCost || 0);
    }
    if (isConvenienceEvent(event)) {
      row.convenienceCount += 1;
      row.convenienceTotalCost += Number(event.layoverCost || 0);
    }
    if (isHotacEvent(event)) {
      row.hotacCount += 1;
      row.hotacTotalCost += Number(event.layoverCost || 0);
    }
    row.positioningTotalCost += Number(event.positioningCost || 0);
  }

  return Array.from(groups.values()).map((row) => {
    const days = Math.max(1, moment.utc(row.periodEnd).diff(moment.utc(row.periodStart), "days") + 1);
    const dpTarget = rolewiseTargetMinutes(targets, row.crewRoles, "averageDpMinutesPerDay", days);
    const fdpTarget = rolewiseTargetMinutes(targets, row.crewRoles, "averageFdpMinutesPerDay", days);
    const ftTarget = rolewiseTargetMinutes(targets, row.crewRoles, "averageFtMinutesPerDay", days);
    const { crewRoles, ...summaryRow } = row;
    return {
      ...summaryRow,
      dpUtilisationPercent: dpTarget > 0 ? roundMoney((row.totalDpMinutes / dpTarget) * 100) : null,
      fdpUtilisationPercent: fdpTarget > 0 ? roundMoney((row.totalFdpMinutes / fdpTarget) * 100) : null,
      ftUtilisationPercent: ftTarget > 0 ? roundMoney((row.totalFtMinutes / ftTarget) * 100) : null,
      positioningAverageCost: row.positioningCount > 0 ? roundMoney(row.positioningTotalCost / row.positioningCount) : 0,
      layoverAverageCost: row.layoverOccurrences > 0 ? roundMoney(row.layoverTotalCost / row.layoverOccurrences) : 0,
      convenienceAverageCost: row.convenienceCount > 0 ? roundMoney(row.convenienceTotalCost / row.convenienceCount) : 0,
      hotacAverageCost: row.hotacCount > 0 ? roundMoney(row.hotacTotalCost / row.hotacCount) : 0,
      layoverTotalCost: roundMoney(row.layoverTotalCost),
      positioningTotalCost: roundMoney(row.positioningTotalCost),
      convenienceTotalCost: roundMoney(row.convenienceTotalCost),
      hotacTotalCost: roundMoney(row.hotacTotalCost),
    };
  });
};

const runCrewCalculation = async ({ userId, triggeredBy }) => {
  const preconditions = await validatePreconditions({ userId });
  if (!preconditions.valid) {
    const error = new Error(preconditions.errors.join(" "));
    error.statusCode = 400;
    throw error;
  }

  const run = await CrewCalculationRun.create({
    userId,
    status: "RUNNING",
    startedAt: new Date(),
    triggeredBy,
  });

  try {
    const [
      crewMembers,
      flightAssignments,
      otherDuties,
      layoverRules,
      positioningCostRules,
      utilisationTargets,
      uploadBatches,
      settings,
    ] = await Promise.all([
      CrewMember.find({ userId }).lean(),
      CrewFlightAssignment.find({ userId }).sort({ std: 1 }).lean(),
      CrewOtherDuty.find({ userId }).sort({ startDateTime: 1 }).lean(),
      CrewLayoverRule.find({ userId }).lean(),
      CrewPositioningCostRule.find({ userId }).lean(),
      CrewUtilisationTarget.find({ userId }).lean(),
      CrewUploadBatch.find({ userId }).sort({ createdAt: -1 }).limit(5).lean(),
      loadSettings(userId),
    ]);

    run.settingsSnapshot = settings;
    run.sourceUploadBatchIds = uploadBatches.map((batch) => batch._id);
    await run.save();

    const flightsByCrew = new Map();
    const dutiesByCrew = new Map();
    const coverageDates = [];
    for (const assignment of flightAssignments) {
      const key = String(assignment.crewMemberId);
      if (!flightsByCrew.has(key)) flightsByCrew.set(key, []);
      flightsByCrew.get(key).push(assignment);
      if (assignment.std) coverageDates.push(new Date(assignment.std));
      if (assignment.sta) coverageDates.push(new Date(assignment.sta));
    }
    for (const duty of otherDuties) {
      const key = String(duty.crewMemberId);
      if (!dutiesByCrew.has(key)) dutiesByCrew.set(key, []);
      dutiesByCrew.get(key).push(duty);
      if (duty.startDateTime) coverageDates.push(new Date(duty.startDateTime));
      if (duty.endDateTime) coverageDates.push(new Date(duty.endDateTime));
    }
    const coverageStart = coverageDates.length
      ? startOfUtcDay(coverageDates.reduce((min, date) => (date < min ? date : min), coverageDates[0]))
      : null;
    const coverageEnd = coverageDates.length
      ? startOfNextUtcDay(coverageDates.reduce((max, date) => (date > max ? date : max), coverageDates[0]))
      : null;

    const allEvents = [];
    for (const crewMember of crewMembers) {
      const key = String(crewMember._id);
      const crewEvents = calculateCrewMemberEvents({
        crewMember,
        flightAssignments: flightsByCrew.get(key) || [],
        otherDuties: dutiesByCrew.get(key) || [],
        dutySettings: settings.dutySettings,
        positioningSettings: settings.positioningSettings,
        layoverRules,
        positioningCostRules,
        coverageStart,
        coverageEnd,
      });
      allEvents.push(...crewEvents.map((event) => ({
        ...event,
        userId,
        calculationRunId: run._id,
      })));
    }

    await CrewDiaryEvent.deleteMany({ userId, calculationRunId: run._id });
    if (allEvents.length > 0) {
      await CrewDiaryEvent.insertMany(allEvents);
    }

    const kpiRows = buildMonthlyKpiSummaries({
      userId,
      calculationRunId: run._id,
      events: allEvents,
      targets: utilisationTargets,
    });
    await CrewKpiSummary.deleteMany({ userId, calculationRunId: run._id });
    if (kpiRows.length > 0) {
      await CrewKpiSummary.insertMany(kpiRows);
    }

    run.status = "COMPLETED";
    run.completedAt = new Date();
    await run.save();

    return {
      run,
      eventCount: allEvents.length,
      kpiSummaryCount: kpiRows.length,
      warnings: run.validationWarnings || [],
    };
  } catch (error) {
    run.status = "FAILED";
    run.completedAt = new Date();
    run.errorMessage = error.message;
    await run.save();
    throw error;
  }
};

const getFirstWeekEnd = (date) => {
  const weekEnd = date.clone().startOf("day");
  if (weekEnd.day() !== 0) {
    weekEnd.add(7 - weekEnd.day(), "days");
  }
  return weekEnd;
};

const buildWeeklyPeriods = ({ base, endDate, periods }) => {
  const rangeStart = base.clone().startOf("day");
  const rangeEnd = endDate && moment.utc(endDate).isValid()
    ? moment.utc(endDate).endOf("day")
    : null;
  const firstWeekEnd = getFirstWeekEnd(rangeStart);
  const lastWeekEnd = rangeEnd ? getFirstWeekEnd(rangeEnd) : null;
  const weeklyPeriods = [];

  for (let index = 0; ; index += 1) {
    const weekEnd = firstWeekEnd.clone().add(index, "week");
    if (lastWeekEnd && weekEnd.isAfter(lastWeekEnd)) break;
    if (!lastWeekEnd && index >= periods) break;

    const weekStart = weekEnd.clone().subtract(6, "days").startOf("day");
    const start = moment.max(weekStart, rangeStart);
    const end = rangeEnd ? moment.min(weekEnd.clone().endOf("day"), rangeEnd) : weekEnd.clone().endOf("day");

    weeklyPeriods.push({
      label: `Week ${weekEnd.format("DD MMM")}`,
      start: start.toDate(),
      end: end.toDate(),
    });
  }

  return weeklyPeriods;
};

const buildPeriods = ({ events = [], periodicity = "MONTHLY", startDate, endDate, periods = 6 }) => {
  const type = normalizeUpper(periodicity || "MONTHLY");
  const firstEventDate = events.length > 0
    ? events.reduce((min, event) => (new Date(event.startDateTime) < min ? new Date(event.startDateTime) : min), new Date(events[0].startDateTime))
    : new Date();
  const base = startDate ? moment.utc(startDate) : moment.utc(firstEventDate);
  const unit = type === "DAILY" ? "day" : (type === "WEEKLY" ? "week" : "month");

  if (type === "WEEKLY") {
    return buildWeeklyPeriods({ base, endDate, periods });
  }

  if (endDate && moment.utc(endDate).isValid()) {
    const rangeStart = base.clone().startOf("day");
    const rangeEnd = moment.utc(endDate).endOf("day");
    const result = [];
    const current = base.clone().startOf(unit);

    while (current.isSameOrBefore(rangeEnd)) {
      const periodStart = moment.max(current.clone().startOf(unit), rangeStart);
      const periodEnd = moment.min(current.clone().endOf(unit), rangeEnd);
      result.push({
        label: type === "DAILY" ? current.format("DD MMM YYYY") : current.format("MMM YYYY"),
        start: periodStart.toDate(),
        end: periodEnd.toDate(),
      });
      current.add(1, unit);
    }

    return result;
  }

  return Array.from({ length: periods }, (_, index) => {
    const start = base.clone().startOf(unit).add(index, unit);
    const end = start.clone().endOf(unit);
    return {
      label: type === "DAILY" ? start.format("DD MMM YYYY") : start.format("MMM YYYY"),
      start: start.toDate(),
      end: end.toDate(),
    };
  });
};

const filterEventForKpi = (event, filters = {}) => {
  const includes = (values, value, normalizer = normalizeText) => {
    if (!values || values.length === 0) return true;
    const set = new Set(values.map((item) => normalizer(item).toLowerCase()));
    return set.has(normalizer(value).toLowerCase());
  };

  return (
    includes(filters.roles, event.role) &&
    includes(filters.bases, event.baseStation, normalizeUpper) &&
    includes(filters.categories, event.category) &&
    includes(filters.subCategories, event.subCategory)
  );
};

const calculateKpiResponse = ({ events, targets, periodicity = "MONTHLY", startDate, endDate, filters = {} }) => {
  const filteredEvents = (events || []).filter((event) => filterEventForKpi(event, filters));
  const periods = buildPeriods({ events: filteredEvents.length ? filteredEvents : events, periodicity, startDate, endDate, periods: 6 });
  const metrics = [
    { key: "totalFtMinutes", label: "Total Flight Time", type: "duration" },
    { key: "totalFdpMinutes", label: "Total Flight Duty Period", type: "duration" },
    { key: "totalDpMinutes", label: "Total Duty Period", type: "duration" },
    { key: "totalRpMinutes", label: "Total Rest Period", type: "duration" },
    { key: "totalLandings", label: "Total Landings", type: "number" },
    { key: "positioningCount", label: "Positionings", type: "number" },
    { key: "layoverOccurrences", label: "Layover occurrences", type: "number" },
    { key: "layoverDurationMinutes", label: "Layover duration", type: "duration" },
    { key: "dpUtilisationPercent", label: "Crew utilisation % (DP/Target)", type: "percent" },
    { key: "fdpUtilisationPercent", label: "Crew utilisation % (FDP/Target)", type: "percent" },
    { key: "ftUtilisationPercent", label: "Crew utilisation % (FT/Target)", type: "percent" },
    { key: "layoverTotalCost", label: "Layover total cost", type: "currency" },
    { key: "layoverAverageCost", label: "Layover average cost", type: "currency" },
    { key: "positioningTotalCost", label: "Positioning total cost", type: "currency" },
    { key: "positioningAverageCost", label: "Positioning average cost", type: "currency" },
    { key: "convenienceTotalCost", label: "Convenience total cost", type: "currency" },
    { key: "convenienceAverageCost", label: "Convenience average cost", type: "currency" },
    { key: "hotacTotalCost", label: "HOTAC + Airport Transfer total cost", type: "currency" },
    { key: "hotacAverageCost", label: "HOTAC + Airport Transfer average cost", type: "currency" },
  ];

  const periodValues = periods.map((period) => {
    const rows = filteredEvents.filter((event) => (
      new Date(event.startDateTime) >= period.start && new Date(event.startDateTime) <= period.end
    ));
    const totals = rows.reduce((acc, event) => {
      acc.totalDpMinutes += Number(event.dpMinutes || 0);
      acc.totalFdpMinutes += Number(event.fdpMinutes || 0);
      acc.totalFtMinutes += Number(event.ftMinutes || 0);
      acc.totalRpMinutes += Number(event.rpMinutes || 0);
      if (event.sourceType === "FLIGHT_ROSTER") acc.totalLandings += 1;
      if (isPositioningEvent(event)) acc.positioningCount += 1;
      if (isLayoverEvent(event)) {
        acc.layoverOccurrences += 1;
        acc.layoverDurationMinutes += eventDuration(event);
        acc.layoverTotalCost += Number(event.layoverCost || 0);
      }
      if (isConvenienceEvent(event)) {
        acc.convenienceCount += 1;
        acc.convenienceTotalCost += Number(event.layoverCost || 0);
      }
      if (isHotacEvent(event)) {
        acc.hotacCount += 1;
        acc.hotacTotalCost += Number(event.layoverCost || 0);
      }
      acc.positioningTotalCost += Number(event.positioningCost || 0);
      if (event.currency) acc.currencies.add(event.currency);
      addCrewRoleForTarget(acc.crewRoles, event);
      return acc;
    }, {
      totalDpMinutes: 0,
      totalFdpMinutes: 0,
      totalFtMinutes: 0,
      totalRpMinutes: 0,
      totalLandings: 0,
      positioningCount: 0,
      layoverOccurrences: 0,
      layoverDurationMinutes: 0,
      layoverTotalCost: 0,
      layoverAverageCost: 0,
      positioningTotalCost: 0,
      positioningAverageCost: 0,
      convenienceCount: 0,
      convenienceTotalCost: 0,
      convenienceAverageCost: 0,
      hotacCount: 0,
      hotacTotalCost: 0,
      hotacAverageCost: 0,
      currencies: new Set(),
      crewRoles: new Map(),
    });

    const days = Math.max(1, moment.utc(period.end).diff(moment.utc(period.start), "days") + 1);
    const dpTarget = rolewiseTargetMinutes(targets, totals.crewRoles, "averageDpMinutesPerDay", days);
    const fdpTarget = rolewiseTargetMinutes(targets, totals.crewRoles, "averageFdpMinutesPerDay", days);
    const ftTarget = rolewiseTargetMinutes(targets, totals.crewRoles, "averageFtMinutesPerDay", days);

    totals.dpUtilisationPercent = dpTarget > 0 ? roundMoney((totals.totalDpMinutes / dpTarget) * 100) : null;
    totals.fdpUtilisationPercent = fdpTarget > 0 ? roundMoney((totals.totalFdpMinutes / fdpTarget) * 100) : null;
    totals.ftUtilisationPercent = ftTarget > 0 ? roundMoney((totals.totalFtMinutes / ftTarget) * 100) : null;
    totals.positioningAverageCost = totals.positioningCount > 0 ? roundMoney(totals.positioningTotalCost / totals.positioningCount) : 0;
    totals.layoverAverageCost = totals.layoverOccurrences > 0 ? roundMoney(totals.layoverTotalCost / totals.layoverOccurrences) : 0;
    totals.convenienceAverageCost = totals.convenienceCount > 0 ? roundMoney(totals.convenienceTotalCost / totals.convenienceCount) : 0;
    totals.hotacAverageCost = totals.hotacCount > 0 ? roundMoney(totals.hotacTotalCost / totals.hotacCount) : 0;
    totals.layoverTotalCost = roundMoney(totals.layoverTotalCost);
    totals.positioningTotalCost = roundMoney(totals.positioningTotalCost);
    totals.convenienceTotalCost = roundMoney(totals.convenienceTotalCost);
    totals.hotacTotalCost = roundMoney(totals.hotacTotalCost);
    totals.currency = totals.currencies.size > 1 ? "MIXED" : (Array.from(totals.currencies)[0] || "");
    delete totals.currencies;
    delete totals.crewRoles;
    delete totals.convenienceCount;
    delete totals.hotacCount;
    return totals;
  });

  return {
    periods: periods.map((period) => ({ label: period.label, start: period.start, end: period.end })),
    metrics: metrics.map((metric) => ({
      ...metric,
      values: periodValues.map((values) => ({
        value: values[metric.key],
        currency: values.currency,
      })),
    })),
  };
};

module.exports = {
  DEFAULT_DUTY_SETTINGS,
  DEFAULT_LAYOVER_RULES,
  DEFAULT_POSITIONING_SETTINGS,
  DEFAULT_POSITIONING_COST_RULES,
  DEFAULT_UTILISATION_TARGETS,
  buildMonthlyKpiSummaries,
  calculateCrewMemberEvents,
  calculateKpiResponse,
  runCrewCalculation,
  validatePreconditions,
  __testables__: {
    addBreakEvent,
    addRestEvent,
    findLayoverRule,
    findPositioningCostRule,
    getTargetForRole,
    isAtOrBeforeCutoff,
    normalizeActivities,
    splitEventByUtcDay,
  },
};
