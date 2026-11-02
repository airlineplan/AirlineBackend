// Case-insensitive exact flight-number matching that scales with the number of
// flight numbers. MongoDB evaluates every regex of a `$in: [/^X$/i, ...]` list
// against every scanned row, so large lists are matched in memory instead.

const REGEX_IN_LIMIT = 50;

const escapeRegExp = (value) => String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const toFlightRegex = (flightNumber) => new RegExp(`^${escapeRegExp(flightNumber)}$`, "i");

// Same as `regexes.some((regex) => regex.test(value))` for the exact-match
// regexes built by toFlightRegex.
const createFlightNumberMatcher = (flightNumbers) => {
  const keys = new Set([...flightNumbers].map((flightNumber) => String(flightNumber).toUpperCase()));
  return (value) => keys.has(String(value).toUpperCase());
};

// Equivalent to model.find({ ...filter, flight: { $in: flightNumbers.map(toFlightRegex) } }).
// `select` must include "flight". Like a $regex query, only string values match.
const findByFlightNumbers = async (model, filter, flightNumbers, select) => {
  const uniqueNumbers = [...new Set(flightNumbers)];
  if (uniqueNumbers.length === 0) return [];

  if (uniqueNumbers.length <= REGEX_IN_LIMIT) {
    return model.find({ ...filter, flight: { $in: uniqueNumbers.map(toFlightRegex) } })
      .select(select)
      .lean();
  }

  const matches = createFlightNumberMatcher(uniqueNumbers);
  const rows = await model.find(filter).select(select).lean();
  return rows.filter((row) => typeof row.flight === "string" && matches(row.flight));
};

module.exports = {
  REGEX_IN_LIMIT,
  createFlightNumberMatcher,
  escapeRegExp,
  findByFlightNumbers,
  toFlightRegex,
};
