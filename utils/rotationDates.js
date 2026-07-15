const DOW_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function parseRotationDow(dow) {
  return [...new Set(
    String(dow || "")
      .split("")
      .map(Number)
      .filter((day) => day >= 1 && day <= 7)
  )];
}

function scheduleDowForDate(date) {
  const jsDow = new Date(date).getUTCDay();
  return jsDow === 0 ? 7 : jsDow;
}

function dayNamesForDow(dow) {
  return parseRotationDow(dow).map((day) => DOW_NAMES[day === 7 ? 0 : day]);
}

function utcDateOnly(value) {
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [year, month, day] = value.split("-").map(Number);
    return new Date(Date.UTC(year, month - 1, day));
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return new Date(Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate()));
}

function occurrenceDates(fromValue, toValue, dow) {
  const fromDate = utcDateOnly(fromValue);
  const toDate = utcDateOnly(toValue);
  const allowedDays = new Set(parseRotationDow(dow));
  if (!fromDate || !toDate || fromDate > toDate || allowedDays.size === 0) return [];

  const dates = [];
  for (const cursor = new Date(fromDate); cursor <= toDate; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    if (allowedDays.has(scheduleDowForDate(cursor))) {
      dates.push(new Date(cursor));
    }
  }
  return dates;
}

module.exports = {
  dayNamesForDow,
  occurrenceDates,
  parseRotationDow,
  scheduleDowForDate,
  utcDateOnly,
};
