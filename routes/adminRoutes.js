const express = require("express");
const adminController = require("../controller/adminController");
const verifyAdmin = require("../middlware/adminAuth");
const { requireRootAdminHost } = require("../middlware/adminAuth");

const router = express.Router();

router.use(requireRootAdminHost);
router.post("/login", adminController.adminLogin);
router.get("/tenants", verifyAdmin, adminController.listTenants);
router.post("/tenants", verifyAdmin, adminController.createTenant);
router.get("/tenants/:id", verifyAdmin, adminController.getTenant);
router.post("/tenants/:id/retry", verifyAdmin, adminController.retryTenant);
router.post("/tenants/:id/deactivate", verifyAdmin, adminController.deactivateTenant);
router.delete("/tenants/:id", verifyAdmin, adminController.deleteTenant);

module.exports = router;
