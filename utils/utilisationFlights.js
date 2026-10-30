const Flight = require("../model/flight");
const { CATEGORIES } = require("./flightCategory");

function requireUserId(userId) {
  if (!userId || (typeof userId === "string" && !userId.trim())) {
    throw new TypeError("userId is required for utilisation flights.");
  }
}

function utcDay(value, name) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new RangeError(`${name} must be a valid date.`);
  date.setUTCHours(0, 0, 0, 0);
  return date;
}

function windowFilter(from, to) {
  const date = { $type: "date" };
  if (from != null) date.$gte = utcDay(from, "from");
  if (to != null) {
    date.$lt = utcDay(to, "to");
    date.$lt.setUTCDate(date.$lt.getUTCDate() + 1);
  }
  if (date.$gte && date.$lt && date.$gte >= date.$lt) {
    throw new RangeError("from must be on or before to.");
  }
  return date;
}

const dateKey = (date) => date.toISOString().slice(0, 10);
const normalizeText = (value) => String(value ?? "").trim().toUpperCase();

function aircraftIdentity(flight) {
  const value = normalizeText(flight.aircraft?.msn) || normalizeText(flight.msn);
  return {
    msn: /^\d+$/.test(value) ? value.replace(/^0+(?=\d)/, "") : value,
    registration: normalizeText(flight.aircraft?.registration),
  };
}

// Also used by batch projections that have already loaded their flights.
// Keep aircraft/date coverage decisions identical to the database helper.
function selectUtilisationFlights(candidates) {
  const coverageByDate = new Map();
  for (const flight of candidates) {
    if (flight.category !== CATEGORIES.ACTUAL) continue;
    const { msn, registration } = aircraftIdentity(flight);
    if (!msn && !registration) throw new Error(`Actual flight ${flight._id} must identify its aircraft by MSN or registration.`);
    const key = dateKey(new Date(flight.date));
    if (!coverageByDate.has(key)) coverageByDate.set(key, { msns: new Set(), registrations: new Set(), registrationsWithoutMsn: new Set() });
    const coverage = coverageByDate.get(key);
    if (msn) coverage.msns.add(msn);
    if (registration) {
      coverage.registrations.add(registration);
      if (!msn) coverage.registrationsWithoutMsn.add(registration);
    }
  }
  return candidates.filter((flight) => {
    if (flight.category === CATEGORIES.ACTUAL) return true;
    if (flight.category && flight.category !== CATEGORIES.PLANNED) return false;
    const coverage = coverageByDate.get(dateKey(new Date(flight.date)));
    if (!coverage) return true;
    const { msn, registration } = aircraftIdentity(flight);
    if (msn && coverage.msns.has(msn)) return false;
    const registrations = msn ? coverage.registrationsWithoutMsn : coverage.registrations;
    return !registration || !registrations.has(registration);
  });
}

/** Return sorted, distinct UTC date keys for Actual rows belonging to this user.
 * These dates indicate actuals exist; they do not imply coverage of every aircraft.
 */
async function getActualDateKeys(userId) {
  requireUserId(userId);
  const dates = await Flight.distinct("date", {
    userId,
    category: CATEGORIES.ACTUAL,
    date: { $type: "date" },
  });
  return [...new Set(dates.map(dateKey))].sort();
}

/**
 * Return lean flights in chronological order, with optional Mongoose projection.
 * from/to are inclusive UTC calendar dates; omitted bounds are open-ended.
 *
 * Q16 confirmed: uploads may cover only some aircraft, but contain all flown
 * flights for each included aircraft/date. Actual replaces Planned per aircraft
 * AND date.
 * T4.2 assignment deletion must use the same scope, never a whole uploaded date.
 *
 * Prefer aircraft.msn (or the Master msn field); fall back to registration when
 * either side lacks an MSN. Conflicting known MSNs never match by registration.
 * Unassigned Planned rows remain Planned. Actual rows must identify an aircraft
 * so incomplete identities cannot silently cause double-counted utilisation.
 */
async function getUtilisationFlights({ userId, from, to, select } = {}) {
  requireUserId(userId);
  const filter = {
    userId,
    category: { $in: [CATEGORIES.PLANNED, CATEGORIES.ACTUAL] },
    date: windowFilter(from, to),
  };
  // Read identity independently of select: callers may request only BH/FH, or
  // explicitly exclude date/category/aircraft from the returned projection.
  const candidates = await Flight.find(filter)
    .select("_id date category msn aircraft.msn aircraft.registration")
    .lean();
  const ids = selectUtilisationFlights(candidates).map((flight) => flight._id);

  if (ids.length === 0) return [];
  const query = Flight.find({ ...filter, _id: { $in: ids } }).sort({ date: 1, flight: 1, _id: 1 });
  if (select != null) query.select(select);
  return query.lean();
}

module.exports = { getActualDateKeys, getUtilisationFlights, selectUtilisationFlights };
