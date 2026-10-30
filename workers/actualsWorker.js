const crypto = require("node:crypto");
const Job = require("../model/actualsImportJob");
const { importActuals } = require("../services/actualsService");
let busy = false;
async function processNextActualsJob() {
  if (busy || Job.db.readyState !== 1) return;
  busy = true;
  let job;
  const lockToken = crypto.randomUUID();
  let heartbeat;
  try {
    job = await Job.findOneAndUpdate({ $or: [{ status: "queued" }, { status: "processing", lockUntil: { $lt: new Date() } }] },
      { $set: { status: "processing", lockToken, lockUntil: new Date(Date.now() + 180000) } }, { new: true, sort: { createdAt: 1 } }).lean();
    if (!job) return;
    heartbeat = setInterval(() => {
      Job.updateOne({ _id: job._id, lockToken }, { $set: { lockUntil: new Date(Date.now() + 180000) } }).catch(console.error);
    }, 30000);
    heartbeat.unref();
    const result = await importActuals({ userId: job.userId, rawRows: job.rawRows });
    await Job.updateOne({ _id: job._id, lockToken }, { $set: { status: "completed", result }, $unset: { rawRows: 1, lockUntil: 1, lockToken: 1 } });
  } catch (error) {
    if (job) await Job.updateOne({ _id: job._id, lockToken }, { $set: { status: "failed", error: error.message }, $unset: { rawRows: 1, lockUntil: 1, lockToken: 1 } });
    else console.error("Actuals worker:", error.message);
  } finally { clearInterval(heartbeat); busy = false; }
}
function startActualsWorker() {
  const timer = setInterval(() => { processNextActualsJob().catch(console.error); }, 15000);
  timer.unref();
  processNextActualsJob().catch(console.error);
  return () => clearInterval(timer);
}
module.exports = { processNextActualsJob, startActualsWorker };
