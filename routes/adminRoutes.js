const express = require("express");
const adminController = require("../controller/adminController");
const verifyAdmin = require("../middlware/adminAuth");
const { requireRootAdminHost } = require("../middlware/adminAuth");

const router = express.Router();

router.use(requireRootAdminHost);
router.post("/login", adminController.adminLogin);
router.post("/internal/provisioning-events", (req, res, next) => {
  const expected = process.env.PROVISIONING_CALLBACK_SECRET;
  const supplied = req.headers["x-provisioning-callback-secret"];
  if (!expected || supplied !== expected) {
    return res.status(401).json({ error: "Invalid provisioning callback secret" });
  }
  return next();
}, adminController.provisioningEvent);
router.get("/features", verifyAdmin, adminController.listFeatures);
router.get("/tenants", verifyAdmin, adminController.listTenants);
router.post("/tenants", verifyAdmin, adminController.createTenant);
router.get("/tenants/:id", verifyAdmin, adminController.getTenant);
router.patch("/tenants/:id/config", verifyAdmin, adminController.updateTenantConfig);
router.post("/tenants/:id/deployments", verifyAdmin, adminController.createDeployment);
router.post("/tenants/:id/actions/:action", verifyAdmin, adminController.performTenantAction);
router.delete("/tenants/:id", verifyAdmin, adminController.deleteTenant);

module.exports = router;
