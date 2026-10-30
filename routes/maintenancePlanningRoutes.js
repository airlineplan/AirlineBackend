const { privateUpload } = require("../middlware/privateUpload");
module.exports = (router, { featureAccess }) => {
  const jsonParser = require("express").json({ limit: "2mb" });
  const ctd = require("../controller/engineOperatingController");
  const matrix = require("../controller/severityMatrixController");
  const actuals = require("../controller/actualsController");
  const invoices = require("../controller/invoiceController");
  router.get("/fleet/engine-operating-settings", ...featureAccess("fleet"), ctd.get);
  router.put("/fleet/engine-operating-settings", ...featureAccess("fleet", "edit"), jsonParser, ctd.put);
  router.get("/cost-config/severity-matrix/:mrAccId", ...featureAccess("cost"), matrix.get);
  router.put("/cost-config/severity-matrix/:mrAccId", ...featureAccess("cost", "edit"), jsonParser, matrix.put);
  router.post("/flights/upload-actuals", ...featureAccess("flgts", "edit"), privateUpload("actuals"), actuals.upload);
  router.get("/flights/actuals-imports/:jobId", ...featureAccess("flgts"), actuals.status);
  router.post("/maintenance-reserve/invoices/extract", ...featureAccess("cost", "edit"), privateUpload("invoice"), invoices.extract);
  router.post("/maintenance-reserve/invoices/apply", ...featureAccess("cost", "edit"), jsonParser, invoices.apply);
};
