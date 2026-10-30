const Fleet = require("../model/fleet");
const Flight = require("../model/flight");
const Setting = require("../model/engineOperatingSettingSchema");
const Matrix = require("../model/severityMatrixSchema");
const { resolveCtd } = require("../services/ctdService");
const { validateCtdValue } = require("../utils/ctdValidation");
const moment = require("moment");
const key = (date) => moment.utc(date).format("YYYY-MM-DD");
async function masterRange(userId) {
  const rows = await Flight.aggregate([{ $match: { userId } }, { $group: { _id: null, min: { $min: "$date" }, max: { $max: "$date" } } }]);
  return rows[0] ? { min: key(rows[0].min), max: key(rows[0].max) } : { min: null, max: null };
}
exports.get = async (req, res) => {
  try {
    const userId = req.user.id;
    const dateRange = await masterRange(userId);
    const engines = await Fleet.find({ userId, category: "Engine" }).sort({ sno: 1 }).lean();
    const filtered = engines.filter((engine) => [["engineType", "type"], ["engineVariant", "variant"], ["esn", "sn"], ["titled", "titled"]]
      .every(([param, field]) => !req.query[param] || String(engine[field] || "").toUpperCase().includes(String(req.query[param]).toUpperCase())));
    const page = Math.max(1, Number(req.query.page) || 1), limit = Math.min(100, Math.max(1, Number(req.query.limit) || 20));
    const selected = filtered.slice((page - 1) * limit, page * limit);
    const start = moment.utc(req.query.from || dateRange.min);
    const end = req.query.to ? moment.utc(req.query.to) : moment.min(moment.utc(dateRange.max), start.clone().add(364, "days"));
    if ((req.query.from && !start.isValid()) || (req.query.to && !end.isValid())) return res.status(400).json({ message: "Invalid date window." });
    const dates = [];
    if (dateRange.min && dateRange.max) {
      start.startOf("day");
      const cursor = moment.max(start, moment.utc(dateRange.min));
      const last = moment.min(end, moment.utc(dateRange.max));
      if (last.diff(cursor, "days") > 730) return res.status(400).json({ message: "Request a window of at most 731 dates." });
      for (; cursor.isSameOrBefore(last, "day"); cursor.add(1, "day")) dates.push(key(cursor));
    }
    const [points, matrices] = await Promise.all([
      Setting.find({ userId, esn: { $in: selected.map((engine) => engine.sn) } }).sort({ effectiveDate: 1 }).lean(),
      Matrix.find({ userId }).select("columns.condition").lean(),
    ]);
    res.json({ dateRange, dates, total: filtered.length, page, limit,
      conditions: [...new Set(["Normal", ...matrices.flatMap((matrix) => matrix.columns.map((column) => column.condition))])],
      rows: selected.map((engine) => {
        const changePoints = points.filter((point) => point.esn === engine.sn);
        const values = { condition: {}, thrust: {}, derate: {} };
        dates.forEach((date) => {
          const ctd = resolveCtd(changePoints, date, engine.entry, engine.exit);
          Object.keys(values).forEach((metric) => { values[metric][date] = ctd?.[metric] ?? null; });
        });
        return { esn: engine.sn, type: engine.type, variant: engine.variant, titled: engine.titled, entry: engine.entry, exit: engine.exit, values, changePoints };
      }),
    });
  } catch (error) { res.status(500).json({ message: error.message }); }
};
exports.put = async (req, res) => {
  try {
    const userId = req.user.id;
    if (!Array.isArray(req.body.changes) || req.body.changes.length > 10000) throw new Error("changes must be an array with at most 10000 entries.");
    const range = await masterRange(userId);
    const engines = await Fleet.find({ userId, category: "Engine" }).lean();
    const changes = req.body.changes.map((change, index) => {
      const esn = String(change.esn || "").trim().toUpperCase();
      const engine = engines.find((row) => row.sn === esn);
      const date = moment.utc(change.date, "YYYY-MM-DD", true);
      if (!engine || !date.isValid() || !range.min || key(date) < range.min || key(date) > range.max
        || (engine.entry && date.isBefore(moment.utc(engine.entry), "day")) || (engine.exit && date.isAfter(moment.utc(engine.exit), "day"))) {
        throw new Error(`Row ${index + 1}: ESN or date is outside Fleet/Master dates.`);
      }
      return { esn, metric: change.metric, effectiveDate: date.toDate(), value: validateCtdValue(change.metric, change.value) };
    });
    for (const change of changes) {
      const previous = await Setting.findOne({ userId, esn: change.esn, metric: change.metric, effectiveDate: { $lt: change.effectiveDate } }).sort({ effectiveDate: -1 }).lean();
      const filter = { userId, esn: change.esn, metric: change.metric, effectiveDate: change.effectiveDate };
      const defaults = { condition: "Normal", thrust: 0, derate: 0 };
      // Like FX rates, each edit replaces the remainder of this metric's timeline.
      // Apply edits in submission order so a subsequent, intentional later edit survives.
      await Setting.deleteMany({ userId, esn: change.esn, metric: change.metric, effectiveDate: { $gte: change.effectiveDate } });
      if ((previous?.value ?? defaults[change.metric]) !== change.value) await Setting.updateOne(filter, { $set: { value: change.value } }, { upsert: true });
    }
    await require("../services/maintenanceReserveRecalculation").refreshMaintenanceReserves(userId, { esns: [...new Set(changes.map((change) => change.esn))] });
    res.json({ success: true, updated: changes.length });
  } catch (error) { res.status(400).json({ message: error.message }); }
};
