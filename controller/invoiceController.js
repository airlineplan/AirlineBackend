const fs = require("node:fs/promises");
const path = require("node:path");
const Draft = require("../model/invoiceDraft");
const Fleet = require("../model/fleet");
const CostConfig = require("../model/costConfigSchema");
const { extractInvoice } = require("../utils/invoiceExtractor");
const { applyInvoice } = require("../services/invoiceService");
exports.extract = async (req, res) => {
  if (!req.file) return res.status(400).json({ message: "Select a PDF invoice." });
  try {
    const userId = req.user.id;
    const [buffer, fleet, config] = await Promise.all([fs.readFile(req.file.path), Fleet.find({ userId }).lean(), CostConfig.findOne({ userId }).lean()]);
    const result = await extractInvoice(buffer, { fleet, settings: config?.leasedReserve || [] });
    const draft = await Draft.create({ userId, fileName: path.basename(req.file.originalname), storedFileKey: path.basename(req.file.path), candidates: result.candidates });
    res.json({ draftId: draft._id, ...result });
  } catch (error) { await fs.unlink(req.file.path).catch(() => {}); res.status(400).json({ message: error.message }); }
};
exports.apply = async (req, res) => {
  try { res.json(await applyInvoice({ userId: req.user.id, body: req.body })); }
  catch (error) { res.status(400).json({ message: error.message }); }
};
