const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const net = require("node:net");
const mongoose = require("mongoose");
async function startMongoTestServer({ replicaSet = false } = {}) {
  const port = await new Promise((resolve, reject) => {
    const server = net.createServer(); server.on("error", reject);
    server.listen(0, "127.0.0.1", () => { const value = server.address().port; server.close(() => resolve(value)); });
  });
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "airline-maintenance-mongo-"));
  const args = ["--dbpath", directory, "--port", String(port), "--bind_ip", "127.0.0.1", "--quiet"];
  if (process.platform !== "win32") args.push("--nounixsocket");
  if (replicaSet) args.push("--replSet", "maintenancePlanningTest");
  const processHandle = spawn(fs.existsSync("/usr/bin/mongod") ? "/usr/bin/mongod" : "mongod", args, { stdio: "ignore", windowsHide: true });
  const close = async () => {
    await mongoose.disconnect();
    if (processHandle.pid && processHandle.exitCode === null && processHandle.signalCode === null) await new Promise(resolve => { processHandle.once("exit", resolve); processHandle.kill("SIGKILL"); });
    const target = fs.realpathSync(directory);
    assert.equal(path.dirname(target).toLowerCase(), fs.realpathSync(os.tmpdir()).toLowerCase());
    assert.ok(path.basename(target).startsWith("airline-maintenance-mongo-"));
    fs.rmSync(target, { recursive: true, force: true });
  };
  try {
    await Promise.race([mongoose.connect(`mongodb://127.0.0.1:${port}/maintenancePlanningTest?directConnection=true`, { serverSelectionTimeoutMS: 15000, autoCreate: false, autoIndex: false }),
      new Promise((resolve, reject) => { processHandle.once("error", reject); processHandle.once("exit", code => reject(new Error(`MongoDB exited (${code})`))); })]);
    if (replicaSet) {
      await mongoose.connection.db.admin().command({ replSetInitiate: { _id: "maintenancePlanningTest", members: [{ _id: 0, host: `127.0.0.1:${port}` }] } });
      const deadline = Date.now() + 15000;
      while (!(await mongoose.connection.db.admin().command({ hello: 1 })).isWritablePrimary) {
        if (Date.now() > deadline) throw new Error("MongoDB replica set did not elect a primary.");
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    const indexedModels = new Set(["FLIGHT", "Assignment", "EngineOperatingSetting", "SeverityMatrix", "MaintenanceReserveInvoice", "ActualsImportJob"]);
    for (const model of Object.values(mongoose.models)) {
      await model.createCollection();
      if (indexedModels.has(model.modelName)) await model.createIndexes();
    }
    return close;
  } catch (error) { await close(); throw error; }
}
module.exports = { startMongoTestServer };
