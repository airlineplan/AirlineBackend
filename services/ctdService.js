const Setting = require("../model/engineOperatingSettingSchema");
const Fleet = require("../model/fleet");
const { DEFAULT_CTD } = require("../utils/ctdValidation");
const key = (date) => new Date(date).toISOString().slice(0, 10);
const serial = (value) => String(value ?? "").trim().toUpperCase();

function resolveCtd(points, date, entry, exit) {
  const target = key(date);
  if ((entry && target < key(entry)) || (exit && target > key(exit))) return null;
  const result = { ...DEFAULT_CTD };
  for (const point of [...points].sort((a, b) => new Date(a.effectiveDate) - new Date(b.effectiveDate))) {
    if (key(point.effectiveDate) > target) break;
    result[point.metric] = point.value;
  }
  return result;
}

async function getCtdForEsnOnDate({ userId, esn, date }) {
  const [points, engine] = await Promise.all([
    Setting.find({ userId, esn: serial(esn), effectiveDate: { $lte: new Date(`${key(date)}T23:59:59.999Z`) } }).lean(),
    Fleet.findOne({ userId, category: "Engine", sn: serial(esn) }).lean(),
  ]);
  return engine ? resolveCtd(points, date, engine.entry, engine.exit) : null;
}

async function buildCtdTimeline({ userId, esns, from, to }) {
  const serials = esns.map(serial);
  const [points, engines] = await Promise.all([
    Setting.find({ userId, esn: { $in: serials }, effectiveDate: { $lte: new Date(`${key(to)}T23:59:59.999Z`) } }).sort({ effectiveDate: 1 }).lean(),
    Fleet.find({ userId, category: "Engine", sn: { $in: serials } }).lean(),
  ]);
  const result = new Map();
  for (const esn of serials) {
    const engine = engines.find((row) => serial(row.sn) === esn);
    const changes = points.filter((row) => row.esn === esn);
    const dates = new Map();
    for (let date = new Date(`${key(from)}T00:00:00Z`); date <= new Date(to); date.setUTCDate(date.getUTCDate() + 1)) {
      dates.set(key(date), engine ? resolveCtd(changes, date, engine.entry, engine.exit) : null);
    }
    result.set(esn, dates);
  }
  return result;
}
module.exports = { resolveCtd, getCtdForEsnOnDate, buildCtdTimeline };
