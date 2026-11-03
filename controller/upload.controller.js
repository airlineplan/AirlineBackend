const path = require("path");
const fs = require("fs");
const { Worker } = require("worker_threads");
const ImportJob = require("../model/ImportJob");

// Each import worker loads a whole workbook and opens its own MongoDB
// connection, so cap how many run at once and queue the rest.
const getMaxConcurrentImports = () => {
  const configured = Number(process.env.IMPORT_WORKER_CONCURRENCY);
  return Number.isInteger(configured) && configured > 0 ? configured : 2;
};

let runningImports = 0;
const pendingImports = [];

const runNextImport = () => {
  while (runningImports < getMaxConcurrentImports() && pendingImports.length > 0) {
    const startImport = pendingImports.shift();
    runningImports += 1;
    startImport(() => {
      runningImports -= 1;
      runNextImport();
    });
  }
};

const startImportWorker = (workerData) => (done) => {
  const workerPath = path.resolve(
    __dirname,
    "../workers/importWorker.js"
  );

  let worker;
  try {
    worker = new Worker(workerPath, { workerData });
  } catch (error) {
    console.error("❌ Worker Error:", error);
    ImportJob.findByIdAndUpdate(workerData.jobId, { status: "failed", error: error.message }).catch(() => {});
    fs.unlink(workerData.filePath, () => {});
    done();
    return;
  }

  worker.on("message", (msg) => {
    console.log("📩 Worker Message:", msg);
  });

  worker.on("error", async (err) => {
    console.error("❌ Worker Error:", err);

    await ImportJob.findByIdAndUpdate(workerData.jobId, {
      status: "failed",
      error: err.message,
    }).catch((updateError) => console.error("❌ Could not mark import failed:", updateError));
  });

  worker.on("exit", (code) => {
    console.log("🚪 Worker exited with code:", code);
    if (code !== 0) {
      // A worker that dies without an "error" event would otherwise leave the job "processing" forever.
      ImportJob.updateOne(
        { _id: workerData.jobId, status: "processing" },
        { status: "failed", error: `Import worker exited with code ${code}` }
      ).catch(() => {});
    }
    done();
  });
};

const importUser = async (req, res) => {
  try {
    console.log("📥 Import API called");

    const job = await ImportJob.create({
      userId: req.user.id,
      fileName: req.file.originalname,
      status: "processing",
    });

    console.log("📝 Job Created:", job._id);

    pendingImports.push(startImportWorker({
      filePath: req.file.path,
      userId: req.user.id,
      jobId: job._id.toString(),
    }));
    runNextImport();

    res.json({
      success: true,
      message: "Import started",
      jobId: job._id,
    });
  } catch (error) {
    console.error("❌ Import API Error:", error);
    res.status(500).json({ success: false, message: error.message });
  }
};

module.exports = {
  importUser,
};
