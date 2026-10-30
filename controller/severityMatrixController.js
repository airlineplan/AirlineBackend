const Matrix = require("../model/severityMatrixSchema");
const { defaultMatrix, validateMatrix } = require("../utils/severityMatrix");
const { refreshMaintenanceReserves } = require("../services/maintenanceReserveRecalculation");
const identity = (req) => ({ userId: req.user.id, mrAccId: String(req.params.mrAccId).trim().toUpperCase() });
exports.get = async (req, res) => {
  try { const filter = identity(req); res.json({ data: await Matrix.findOne(filter).lean() || { ...filter, ...defaultMatrix() } }); }
  catch (error) { res.status(500).json({ message: error.message }); }
};
exports.put = async (req, res) => {
  let matrix;
  try { matrix = validateMatrix(req.body); } catch (error) { return res.status(400).json({ message: error.message }); }
  try {
    const filter = identity(req);
    const data = await Matrix.findOneAndUpdate(filter, { $set: matrix }, { upsert: true, new: true });
    const schedule = await refreshMaintenanceReserves(filter.userId, { mrAccId: filter.mrAccId });
    res.json({ data, schedule });
  } catch (error) { res.status(500).json({ message: error.message }); }
};
