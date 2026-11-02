// Helpers for bucketing large row sets by date without spreading arrays into
// Math.min/Math.max (which throws RangeError past ~100k arguments) or
// re-filtering every row once per reporting period.

const toTime = (value) => {
  if (value === null || value === undefined) return NaN;
  if (typeof value === "number") return value;
  return new Date(value).getTime();
};

// Same result as Math.min(...values) / Math.max(...values) for numbers and Dates,
// including NaN when any value is not a valid number, but safe for any length.
const minMaxTime = (values) => {
  let min = Infinity;
  let max = -Infinity;
  for (const value of values) {
    const time = Number(value);
    if (Number.isNaN(time)) return { min: NaN, max: NaN };
    if (time < min) min = time;
    if (time > max) max = time;
  }
  return { min, max };
};

// Indexes items by a millisecond key so that `between(start, end)` returns the
// same items as `items.filter((item) => key(item) >= start && key(item) <= end)`,
// in the same original order, in O(log n + k log k) instead of O(n).
// Items whose key is not a finite number never match, as with the filter.
const createDateRangeIndex = (items, getTime) => {
  const entries = [];
  items.forEach((item, position) => {
    const time = getTime(item);
    if (typeof time === "number" && Number.isFinite(time)) entries.push({ time, position, item });
  });
  entries.sort((a, b) => a.time - b.time || a.position - b.position);

  const lowerBound = (target) => {
    let low = 0;
    let high = entries.length;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (entries[mid].time < target) low = mid + 1;
      else high = mid;
    }
    return low;
  };

  const upperBound = (target) => {
    let low = 0;
    let high = entries.length;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (entries[mid].time <= target) low = mid + 1;
      else high = mid;
    }
    return low;
  };

  const between = (start, end) => {
    const startTime = toTime(start);
    const endTime = toTime(end);
    if (Number.isNaN(startTime) || Number.isNaN(endTime) || startTime > endTime) return [];
    const matches = entries.slice(lowerBound(startTime), upperBound(endTime));
    matches.sort((a, b) => a.position - b.position);
    return matches.map((entry) => entry.item);
  };

  return { between };
};

module.exports = {
  createDateRangeIndex,
  minMaxTime,
  toTime,
};
