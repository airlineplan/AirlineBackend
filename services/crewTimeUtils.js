const moment = require("moment");

const DURATION_REGEX = /^(\d{1,3}):([0-5]\d)$/;
const CLOCK_REGEX = /^([01]\d|2[0-3]):([0-5]\d)$/;

const normalizeText = (value) => (
  value === null || value === undefined ? "" : String(value).trim()
);

const normalizeUpper = (value) => normalizeText(value).toUpperCase();

const normalizeKey = (value) => normalizeText(value).toLowerCase().replace(/[^a-z0-9]/g, "");

const getRowValue = (row, aliases = []) => {
  const wanted = new Set(aliases.map(normalizeKey));
  for (const key of Object.keys(row || {})) {
    if (wanted.has(normalizeKey(key))) {
      return row[key];
    }
  }
  return undefined;
};

const minutesToHHMM = (minutes = 0) => {
  const safeMinutes = Math.max(0, Math.round(Number(minutes) || 0));
  const hours = Math.floor(safeMinutes / 60);
  const mins = safeMinutes % 60;
  return `${String(hours).padStart(2, "0")}:${String(mins).padStart(2, "0")}`;
};

const parseDurationToMinutes = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) {
    if (value >= 0 && value < 1) return Math.round(value * 24 * 60);
    return Math.round(value);
  }

  const text = normalizeText(value);
  if (!text) return null;

  const decimal = Number(text);
  if (Number.isFinite(decimal) && text.includes(".")) {
    return Math.round(decimal * 24 * 60);
  }

  const match = text.match(DURATION_REGEX);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
};

const isValidClock = (value) => CLOCK_REGEX.test(normalizeText(value));

const clockToMinutes = (value) => {
  if (!isValidClock(value)) return null;
  const [hours, minutes] = normalizeText(value).split(":").map(Number);
  return hours * 60 + minutes;
};

const parseExcelDate = (value) => {
  if (!value && value !== 0) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return moment.utc(value).add(12, "hours").startOf("day").toDate();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const excelEpoch = Date.UTC(1899, 11, 30);
    return moment.utc(excelEpoch + value * 86400000).startOf("day").toDate();
  }

  const text = normalizeText(value);
  if (!text) return null;

  const parsed = moment.utc(text, [
    "YYYY-MM-DD",
    "DD-MM-YYYY",
    "D-MM-YYYY",
    "DD/MM/YYYY",
    "D/M/YYYY",
    "DD/MM/YY",
    "D/M/YY",
    "MM/DD/YYYY",
    "M/D/YYYY",
    "MM/DD/YY",
    "M/D/YY",
    "DD-MMM-YYYY",
    "D-MMM-YYYY",
    "DD-MMM-YY",
    "D-MMM-YY",
    "DD MMM YYYY",
    "D MMM YYYY",
    "DD MMM YY",
    "D MMM YY",
  ], true);

  if (parsed.isValid()) return parsed.startOf("day").toDate();

  const loose = moment.utc(text);
  return loose.isValid() ? loose.startOf("day").toDate() : null;
};

const parseClockTime = (value) => {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return `${String(value.getUTCHours()).padStart(2, "0")}:${String(value.getUTCMinutes()).padStart(2, "0")}`;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const totalMinutes = Math.round((value < 1 ? value * 24 * 60 : value) % (24 * 60));
    return minutesToHHMM(totalMinutes);
  }

  const text = normalizeText(value).replace(/\s*(AM|PM)\s*$/i, "");
  if (!text) return null;

  const hhmm = text.match(/^(\d{1,2}):([0-5]\d)(?::\d{2})?$/);
  if (hhmm) {
    return `${String(Number(hhmm[1])).padStart(2, "0")}:${hhmm[2]}`;
  }

  const parsed = moment.utc(normalizeText(value), ["h:mm A", "h A", "HH:mm", "HHmm"], true);
  if (!parsed.isValid()) return null;
  return parsed.format("HH:mm");
};

const combineDateAndClock = (dateValue, timeValue) => {
  const date = parseExcelDate(dateValue);
  const clock = parseClockTime(timeValue);
  if (!date || !clock) return null;
  const [hours, minutes] = clock.split(":").map(Number);
  const result = moment.utc(date).hour(hours).minute(minutes).second(0).millisecond(0);
  return result.toDate();
};

const parseDateTime = (value, fallbackDate) => {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const text = normalizeText(value);
  if (!text && fallbackDate) return parseExcelDate(fallbackDate);
  if (!text) return null;

  const parsed = moment.utc(text, [
    "YYYY-MM-DD HH:mm",
    "YYYY-MM-DDTHH:mm",
    "DD-MM-YYYY HH:mm",
    "D-MM-YYYY HH:mm",
    "DD/MM/YYYY HH:mm",
    "D/M/YYYY HH:mm",
    "DD-MMM-YYYY HH:mm",
    "D-MMM-YYYY HH:mm",
    "DD MMM YYYY HH:mm",
    "D MMM YYYY HH:mm",
  ], true);
  if (parsed.isValid()) return parsed.toDate();

  const loose = moment.utc(text);
  return loose.isValid() ? loose.toDate() : null;
};

const addMinutes = (date, minutes) => moment.utc(date).add(minutes, "minutes").toDate();

const diffMinutes = (start, end) => Math.max(0, Math.round(moment.utc(end).diff(moment.utc(start), "minutes", true)));

const endAfterStartWithOvernight = (start, end) => {
  if (!start || !end) return null;
  let result = moment.utc(end);
  while (result.isSameOrBefore(moment.utc(start))) {
    result = result.add(1, "day");
  }
  return result.toDate();
};

const dateKey = (date) => moment.utc(date).format("YYYY-MM-DD");

const monthKey = (date) => moment.utc(date).format("YYYY-MM");

const roundMoney = (value) => Math.round((Number(value) || 0) * 100) / 100;

module.exports = {
  CLOCK_REGEX,
  DURATION_REGEX,
  addMinutes,
  clockToMinutes,
  combineDateAndClock,
  dateKey,
  diffMinutes,
  endAfterStartWithOvernight,
  getRowValue,
  isValidClock,
  minutesToHHMM,
  monthKey,
  normalizeKey,
  normalizeText,
  normalizeUpper,
  parseClockTime,
  parseDateTime,
  parseDurationToMinutes,
  parseExcelDate,
  roundMoney,
};
