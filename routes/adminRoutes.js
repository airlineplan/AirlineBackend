const express = require("express");
const adminController = require("../controller/adminController");
const verifyAdmin = require("../middlware/adminAuth");

const router = express.Router();

router.post("/login", adminController.adminLogin);
router.get("/tenants", verifyAdmin, adminController.listTenants);
router.post("/tenants", verifyAdmin, adminController.createTenant);
router.get("/tenants/:id", verifyAdmin, adminController.getTenant);
router.post("/tenants/:id/retry", verifyAdmin, adminController.retryTenant);
router.post("/tenants/:id/deactivate", verifyAdmin, adminController.deactivateTenant);

module.exports = router;
