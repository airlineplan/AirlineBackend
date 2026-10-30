const fs = require("node:fs/promises");
const xlsx = require("xlsx");
const Job = require("../model/actualsImportJob");
const { importActuals } = require("../services/actualsService");
exports.upload = async (req, res) => {
  if (!req.file) return res.status(400).json({ message: "Select an actuals workbook." });
  try {
    const workbook = xlsx.readFile(req.file.path, { cellDates: true });
    const rawRows = xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: true, defval: "" });
    if (!rawRows.length || rawRows.length > 20000) return res.status(400).json({ message: "The workbook must contain between 1 and 20000 flights." });
    if (rawRows.length > 500) {
      const job = await Job.create({ userId: req.user.id, fileName: req.file.originalname, rawRows });
      setImmediate(() => require("../workers/actualsWorker").processNextActualsJob().catch(console.error));
      return res.status(202).json({ jobId: job._id, status: job.status });
    }
    res.json(await importActuals({ userId: req.user.id, rawRows }));
  } catch (error) { res.status(400).json({ message: error.message }); }
  finally { await fs.unlink(req.file.path).catch(() => {}); }
};
exports.status = async (req, res) => {
  try {
    const job = await Job.findOne({ _id: req.params.jobId, userId: req.user.id }).select("status result error createdAt updatedAt").lean();
    if (!job) return res.status(404).json({ message: "Import job not found." });
    res.json(job);
  } catch (error) { res.status(400).json({ message: error.message }); }
};
