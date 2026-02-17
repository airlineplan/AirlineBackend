const path = require("path");
const { Worker } = require("worker_threads");
const ImportJob = require("../model/ImportJob");

const importUser = async (req, res) => {
  try {
    console.log("📥 Import API called");

    const job = await ImportJob.create({
      userId: req.user.id,
      fileName: req.file.originalname,
      status: "processing",
    });

    console.log("📝 Job Created:", job._id);

    const workerPath = path.resolve(
      __dirname,
      "../workers/importWorker.js"
    );

    const worker = new Worker(workerPath, {
      workerData: {
        filePath: req.file.path,
        userId: req.user.id,
        jobId: job._id.toString(),
      },
    });

    worker.on("message", (msg) => {
      console.log("📩 Worker Message:", msg);
    });

    worker.on("error", async (err) => {
      console.error("❌ Worker Error:", err);

      await ImportJob.findByIdAndUpdate(job._id, {
        status: "failed",
        error: err.message,
      });
    });

    worker.on("exit", (code) => {
      console.log("🚪 Worker exited with code:", code);
    });

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
