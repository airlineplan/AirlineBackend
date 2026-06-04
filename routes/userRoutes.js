const express = require("express");
const user = express();
const multer = require("multer");
const path = require("path");
const verifyToken = require("../middlware/auth.js");
const { requireTenantAdmin } = require("../middlware/auth.js");
const { requireFeatureAccess, getTenantAdminFeatures } = require("../middlware/tenantFeatureAccess");
const userLogin = require("../controller/userLogin");
var bodyParser = require("body-parser");
const { verify } = require("crypto");
const { importUser } = require("../controller/upload.controller");

user.use(bodyParser.urlencoded({ extended: true }));
var jsonParser = bodyParser.json();

var storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "./public/uploads");
  },
  filename: (req, file, cb) => {
    cb(null, file.originalname);
  },
});
var upload = multer({ storage: storage });

const dataController = require("../controller/dataController");
const sectorController = require("../controller/sectorController");
const flightController = require("../controller/flightController");
const rotationController = require("../controller/rotationController");
const stationController = require("../controller/stationController");
const dashboardController = require("../controller/dashboardController");
const masterController = require("../controller/masterController");
const maintenanceController = require("../controller/maintenanceController");
const assignmentController = require("../controller/assignmentController");
const fleetController = require("../controller/fleetController");
const costController = require("../controller/costController");
const apuFuelController = require("../controller/apuFuelController");
const tenantUserController = require("../controller/tenantUserController");
const crewController = require("../controller/crewController");

// 🔥 UNCOMMENT AND IMPORT THE HELPER
const createConnections = require('../helper/createConnections');
const featureAccess = (featureId, requiredAccess = "read") => [verifyToken, requireFeatureAccess(featureId, requiredAccess)];

user.post(
  "/importUser",
  ...featureAccess("network", "edit"),
  upload.single("file"),
  importUser
);
user.post("/add-Data", jsonParser, ...featureAccess("network", "edit"), dataController.AddData);
user.get("/get-data", ...featureAccess("network"), dataController.getData);
user.get("/downloadFLGTs", ...featureAccess("flgts"), dataController.downloadExpenses);
user.get("/products/:id", ...featureAccess("network"), dataController.singleData);
user.delete("/delete", jsonParser, ...featureAccess("network", "edit"), dataController.deleteFlightsAndUpdateSectors);
user.delete("/delete-sector/:ids", ...featureAccess("sectors", "edit"), sectorController.deleteSectors);
user.put(
  "/update-data/:id",
  jsonParser,
  ...featureAccess("network", "edit"),
  dataController.updateData
);
user.get("/sectors", ...featureAccess("sectors"), sectorController.getSecors);
user.post("/add-sector", jsonParser, ...featureAccess("sectors", "edit"), sectorController.AddSectors);
user.put(
  "/update-sectore/:id",
  ...featureAccess("sectors", "edit"),
  jsonParser,
  sectorController.updateSector
);
user.get("/sectorsbyid/:id", ...featureAccess("sectors"), sectorController.singleSector);
user.post("/tenant/bootstrap-admin", jsonParser, tenantUserController.bootstrapTenantAdmin);
user.post("/user-signup", jsonParser, verifyToken, requireTenantAdmin, userLogin.createUser);
user.post("/user-login", jsonParser, userLogin.loginUser);
user.get("/auth/verify", verifyToken, (req, res) => {
  return res.status(200).json({
    valid: true,
    user: req.user,
  });
});
user.get("/tenant/users", verifyToken, requireTenantAdmin, tenantUserController.listTenantUsers);
user.post("/tenant/users", verifyToken, requireTenantAdmin, jsonParser, tenantUserController.createTenantUser);
user.patch("/tenant/users/:id/role", verifyToken, requireTenantAdmin, jsonParser, tenantUserController.updateTenantUserRole);
user.patch("/tenant/users/:id/access", verifyToken, requireTenantAdmin, jsonParser, tenantUserController.setTenantUserActive);
user.patch("/tenant/users/:id/page-access", verifyToken, requireTenantAdmin, jsonParser, tenantUserController.updateTenantUserPageAccess);
user.delete("/tenant/users/:id", verifyToken, requireTenantAdmin, tenantUserController.deleteTenantUser);
user.get("/tenant-admin/features", verifyToken, requireTenantAdmin, (req, res) => {
  return res.status(200).json({ features: getTenantAdminFeatures() });
});
user.post("/send-email", jsonParser, userLogin.sendEmail);
user.post("/send-contactEmail", jsonParser, userLogin.sendContactEmail);
user.post("/change-passowrd", jsonParser, userLogin.changePassword);
user.get("/flight", ...featureAccess("flgts"), flightController.getFlights);
user.post("/searchflights", ...featureAccess("flgts"), flightController.searchFlights);
user.post("/flightsWoRotations", ...featureAccess("rotations"), jsonParser, flightController.getFlightsWoRotations);
user.get("/listVariants", ...featureAccess(["flgts", "rotations"]), masterController.getVariants);
user.get("/listRotations", ...featureAccess("rotations"), rotationController.getRotations);
user.get("/dashboard", ...featureAccess("dashboard"), dashboardController.getDashboardData);

// 🔥 USE THE IMPORTED HELPER DIRECTLY FOR THIS ROUTE
user.get("/createConnections", ...featureAccess(["connections", "view", "dashboard"], "edit"), createConnections);
user.get("/getConnections", ...featureAccess("connections"), dataController.getConnections);
user.get("/dashboard/populateDropDowns", ...featureAccess(["dashboard", "cost", "revenue", "network"]), dashboardController.populateDashboardDropDowns);
user.get("/get-stationData", ...featureAccess(["stations", "network", "sectors", "poo"]), stationController.getStationsTableData);
user.get("/getNextRotationNumber", ...featureAccess("rotations", "edit"), rotationController.getNextRotationNumber);
user.get("/rotationbyid/:id", ...featureAccess("rotations"), rotationController.singleRotationDetail);
user.post("/updateRotationSummary", ...featureAccess("rotations", "edit"), jsonParser, rotationController.updateRotationSummary);
user.post("/addRotationDetailsFlgtChange", ...featureAccess("rotations", "edit"), jsonParser, rotationController.addRotationDetailsFlgtChange);
user.post("/saveStation", ...featureAccess("stations", "edit"), jsonParser, stationController.saveStation);
user.post("/deleteCompleteRotation/", ...featureAccess("rotations", "edit"), jsonParser, rotationController.deleteCompleteRotation);
user.post("/deletePrevInRotation/", ...featureAccess("rotations", "edit"), jsonParser, rotationController.deletePrevInRotation);
user.post("/list-page-data", ...featureAccess("list"), dataController.getListPageData);
user.get("/view-page-data", ...featureAccess("view"), dataController.getViewData);
user.get("/master-weeks", ...featureAccess(["view", "assignment", "revenue", "cost"]), masterController.getMasterWeeks);

// --- COST ENDPOINTS ---
user.get("/cost-config", ...featureAccess("cost"), costController.getCostConfig);
user.post("/cost-config", ...featureAccess("cost", "edit"), jsonParser, costController.saveCostConfig);
user.post("/cost-config/maintenance-reserve-schedule/generate", ...featureAccess("cost"), jsonParser, costController.generateMaintenanceReserveSchedulePreview);
user.post("/cost-page-data", ...featureAccess("cost"), jsonParser, costController.getCostPageData);
user.post("/cost-page-data/recalculate-and-save", ...featureAccess("cost", "edit"), jsonParser, costController.recalculateAndSaveCostPageData);

user.get("/apu-fuel-costs", ...featureAccess("cost"), apuFuelController.getApuFuelCosts);
user.post("/apu-fuel-costs", ...featureAccess("cost", "edit"), jsonParser, apuFuelController.bulkSaveApuFuelCosts);
user.post("/apu-fuel-costs/rebuild", ...featureAccess("cost", "edit"), jsonParser, apuFuelController.rebuildApuFuelCosts);

// Add this alongside your existing routes
user.get("/maintenance-dashboard", ...featureAccess("maintenance"), maintenanceController.getMaintenanceDashboard);

user.post(
  "/uploadAssignments",
  ...featureAccess("assignment", "edit"),
  upload.single("file"),
  assignmentController.uploadAssignments
);

// --- CREW ENDPOINTS ---
user.get("/crew/bootstrap", ...featureAccess("crew"), crewController.getCrewBootstrap);
user.get("/crew/options", ...featureAccess("crew"), crewController.getCrewOptions);
user.get("/crew/duty-settings", ...featureAccess("crew"), crewController.getDutySettings);
user.put("/crew/duty-settings", ...featureAccess("crew", "edit"), jsonParser, crewController.updateDutySettings);
user.get("/crew/positioning-settings", ...featureAccess("crew"), crewController.getPositioningSettings);
user.put("/crew/positioning-settings", ...featureAccess("crew", "edit"), jsonParser, crewController.updatePositioningSettings);
user.get("/crew/utilisation-targets", ...featureAccess("crew"), crewController.listUtilisationTargets);
user.post("/crew/utilisation-targets/bulk", ...featureAccess("crew", "edit"), jsonParser, crewController.bulkSaveUtilisationTargets);
user.delete("/crew/utilisation-targets/:id", ...featureAccess("crew", "edit"), crewController.deleteUtilisationTarget);
user.get("/crew/layover-rules", ...featureAccess("crew"), crewController.listLayoverRules);
user.post("/crew/layover-rules/bulk", ...featureAccess("crew", "edit"), jsonParser, crewController.bulkSaveLayoverRules);
user.delete("/crew/layover-rules/:id", ...featureAccess("crew", "edit"), crewController.deleteLayoverRule);
user.get("/crew/positioning-cost-rules", ...featureAccess("crew"), crewController.listPositioningCostRules);
user.post("/crew/positioning-cost-rules/bulk", ...featureAccess("crew", "edit"), jsonParser, crewController.bulkSavePositioningCostRules);
user.delete("/crew/positioning-cost-rules/:id", ...featureAccess("crew", "edit"), crewController.deletePositioningCostRule);
user.post("/crew/upload/members", ...featureAccess("crew", "edit"), upload.single("file"), crewController.uploadCrewInformation);
user.post("/crew/upload/flight-duties", ...featureAccess("crew", "edit"), upload.single("file"), crewController.uploadFlightDuties);
user.post("/crew/upload/other-duties", ...featureAccess("crew", "edit"), upload.single("file"), crewController.uploadOtherDuties);
user.post("/crew/update-plan", ...featureAccess("crew", "edit"), jsonParser, crewController.updatePlan);
user.get("/crew/calculation-runs/latest", ...featureAccess("crew"), crewController.getLatestRun);
user.get("/crew/diary", ...featureAccess("crew"), crewController.getCrewDiary);
user.get("/crew/kpis", ...featureAccess("crew"), crewController.getCrewKpis);

user.get(
  "/getWeeklyAssignments",
  ...featureAccess("assignment"),
  assignmentController.getWeeklyAssignments
);

user.get(
  "/fleet",
  ...featureAccess("fleet"),
  fleetController.getAllFleet
);

// Bulk save/update the fleet table
user.post(
  "/fleet/bulk-save",
  ...featureAccess("fleet", "edit"),
  jsonParser, // Parses the incoming JSON body from React
  fleetController.bulkUpsertFleet
);

// Delete a single asset row
user.delete(
  "/fleet/:id",
  ...featureAccess("fleet", "edit"),
  fleetController.deleteFleetAsset
);

// Add this alongside your other fleet routes
user.get(
  "/fleet/months",
  ...featureAccess("fleet"),
  fleetController.getFleetMonths
);

user.get(
  "/fleet/metrics",
  ...featureAccess("fleet"),
  fleetController.getFleetScheduleMetrics
);

user.get(
  "/maintenance/dashboard",
  ...featureAccess("maintenance"),
  maintenanceController.getMaintenanceDashboard
);

// 2. Get Reset Records for the Modal (with optional ?date= & ?msnEsn= filters)
user.get(
  "/maintenance/reset-records",
  ...featureAccess("maintenance"),
  maintenanceController.getResetRecords
);

// 3. Save/Update records from the Modal
user.post(
  "/maintenance/reset-records",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.bulkSaveResetRecords
);

user.delete(
  "/maintenance/reset-records/:id",
  ...featureAccess("maintenance", "edit"),
  maintenanceController.deleteResetRecord
);

// 4. Trigger Compute Button
user.post(
  "/maintenance/compute",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.computeMaintenanceLogic
);

// 5. Get Rotable Movements for the Modal
user.get(
  "/maintenance/rotables",
  ...featureAccess("maintenance"),
  maintenanceController.getRotables
);

// 6. Bulk Save/Update Rotable Movements
user.post(
  "/maintenance/rotables",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.bulkSaveRotables
);

user.delete(
  "/maintenance/rotables/:id",
  ...featureAccess("maintenance", "edit"),
  maintenanceController.deleteRotable
);

// 7. Get Target Maintenance Status
user.get(
  "/maintenance/targets",
  ...featureAccess("maintenance"),
  maintenanceController.getTargets
);

// 8. Bulk Save Target Maintenance Status
user.post(
  "/maintenance/targets",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.bulkSaveTargets
);

user.delete(
  "/maintenance/targets/:id",
  ...featureAccess("maintenance", "edit"),
  maintenanceController.deleteTarget
);

// 9. Get Utilisation Assumptions
user.get(
  "/maintenance/utilisation-assumptions",
  ...featureAccess("maintenance"),
  maintenanceController.getUtilisationAssumptions
);

// 10. Bulk Save Utilisation Assumptions
user.post(
  "/maintenance/utilisation-assumptions",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.bulkSaveUtilisationAssumptions
);

user.delete(
  "/maintenance/utilisation-assumptions/:id",
  ...featureAccess("maintenance", "edit"),
  maintenanceController.deleteUtilisationAssumption
);

user.get(
  "/maintenance/ground-days",
  ...featureAccess("maintenance"),
  maintenanceController.getGroundDays
);

// 11. Get Calendar Inputs
user.get(
  "/maintenance/calendar",
  ...featureAccess("maintenance"),
  maintenanceController.getCalendar
);

// 12. Bulk Save Calendar Inputs
user.post(
  "/maintenance/calendar",
  ...featureAccess("maintenance", "edit"),
  jsonParser,
  maintenanceController.bulkSaveCalendar
);

user.delete(
  "/maintenance/calendar/:id",
  ...featureAccess("maintenance", "edit"),
  maintenanceController.deleteCalendar
);

// ─── POO & Revenue ─────────────────────────────────────────────────
const pooController = require("../controller/pooController");

user.get("/poo", ...featureAccess("poo"), pooController.getPooData);
user.post("/poo/populate", ...featureAccess("poo", "edit"), jsonParser, pooController.populatePoo);
user.post("/poo/update", ...featureAccess("poo", "edit"), jsonParser, pooController.updatePooRecords);
user.post("/poo/transit", ...featureAccess("poo", "edit"), jsonParser, pooController.upsertTransit);
user.delete("/poo/transit/:odGroupKey", ...featureAccess("poo", "edit"), pooController.deleteTransit);
user.delete("/poo", ...featureAccess("poo", "edit"), jsonParser, pooController.deletePooRecords);
user.get("/revenue/config", ...featureAccess("revenue"), pooController.getRevenueConfig);
user.post("/revenue/config", ...featureAccess("revenue", "edit"), jsonParser, pooController.saveRevenueConfig);
user.get("/revenue-config", ...featureAccess("revenue"), pooController.getRevenueConfig);
user.post("/revenue-config", ...featureAccess("revenue", "edit"), jsonParser, pooController.saveRevenueConfig);
user.post("/revenue-config/reporting-currency", ...featureAccess(["revenue", "dashboard"], "edit"), jsonParser, pooController.saveReportingCurrency);
user.post("/revenue-config/fx-rates", ...featureAccess(["revenue", "dashboard"], "edit"), jsonParser, pooController.saveFxRates);
user.get("/revenue", ...featureAccess("revenue"), pooController.getRevenueData);
user.post("/revenue/backfill-master-fields-to-poo", ...featureAccess("revenue", "edit"), jsonParser, pooController.backfillMasterFieldsToPoo);

module.exports = user;
