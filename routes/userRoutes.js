const express = require("express");
const user = express();
const multer = require("multer");
const path = require("path");
const verifyToken = require("../middlware/auth.js");
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
const authController = require("../controller/authController");
const maintenanceController = require("../controller/maintenanceController");
const assignmentController = require("../controller/assignmentController");
const fleetController = require("../controller/fleetController");
const costController = require("../controller/costController");

// 🔥 UNCOMMENT AND IMPORT THE HELPER
const createConnections = require('../helper/createConnections');

user.post(
  "/importUser",
  upload.single("file"),
  verifyToken,
  importUser
);
user.post("/add-Data", jsonParser, verifyToken, dataController.AddData);
user.get("/get-data", verifyToken, dataController.getData);
user.get("/downloadFLGTs", verifyToken, dataController.downloadExpenses);
user.get("/products/:id", dataController.singleData);
user.delete("/delete", jsonParser, verifyToken, dataController.deleteFlightsAndUpdateSectors);
user.delete("/delete-sector/:ids", sectorController.deleteSectors);
user.put(
  "/update-data/:id",
  jsonParser,
  verifyToken,
  dataController.updateData
);
user.get("/sectors", verifyToken, sectorController.getSecors);
user.post("/add-sector", jsonParser, verifyToken, sectorController.AddSectors);
user.put(
  "/update-sectore/:id",
  verifyToken,
  jsonParser,
  sectorController.updateSector
);
user.get("/sectorsbyid/:id", sectorController.singleSector);
user.post("/admin-login", jsonParser, authController.AdminLogin);
user.post("/user-signup", jsonParser, userLogin.createUser);
user.post("/user-login", jsonParser, userLogin.loginUser);
user.post("/send-email", jsonParser, userLogin.sendEmail);
user.post("/send-contactEmail", jsonParser, userLogin.sendContactEmail);
user.post("/change-passowrd", jsonParser, userLogin.changePassword);
user.get("/flight", verifyToken, flightController.getFlights);
user.post("/searchflights", verifyToken, flightController.searchFlights);
user.post("/flightsWoRotations", verifyToken, jsonParser, flightController.getFlightsWoRotations);
user.get("/listVariants", verifyToken, masterController.getVariants);
user.get("/listRotations", verifyToken, rotationController.getRotations);
user.get("/dashboard", verifyToken, dashboardController.getDashboardData);

// 🔥 USE THE IMPORTED HELPER DIRECTLY FOR THIS ROUTE
user.get("/createConnections", verifyToken, createConnections);
user.get("/getConnections", verifyToken, dataController.getConnections);
user.get("/dashboard/populateDropDowns", verifyToken, dashboardController.populateDashboardDropDowns);
user.get("/get-stationData", verifyToken, stationController.getStationsTableData);
user.get("/getNextRotationNumber", verifyToken, rotationController.getNextRotationNumber);
user.get("/rotationbyid/:id", rotationController.singleRotationDetail);
user.post("/updateRotationSummary", verifyToken, jsonParser, rotationController.updateRotationSummary);
user.post("/addRotationDetailsFlgtChange", verifyToken, jsonParser, rotationController.addRotationDetailsFlgtChange);
user.post("/saveStation", verifyToken, jsonParser, stationController.saveStation);
user.post("/deleteCompleteRotation/", verifyToken, jsonParser, rotationController.deleteCompleteRotation);
user.post("/deletePrevInRotation/", verifyToken, jsonParser, rotationController.deletePrevInRotation);
user.post("/list-page-data", verifyToken, dataController.getListPageData);
user.get("/view-page-data", verifyToken, dataController.getViewData);
user.get("/master-weeks", verifyToken, masterController.getMasterWeeks);

// --- COST ENDPOINTS ---
user.get("/cost-config", verifyToken, costController.getCostConfig);
user.post("/cost-config", verifyToken, jsonParser, costController.saveCostConfig);
user.post("/cost-page-data", verifyToken, jsonParser, costController.getCostPageData);

// Add this alongside your existing routes
user.get("/maintenance-dashboard", verifyToken, maintenanceController.getMaintenanceDashboard);

user.post(
  "/uploadAssignments",
  verifyToken,
  upload.single("file"),
  assignmentController.uploadAssignments
);

user.get(
  "/getWeeklyAssignments",
  verifyToken,
  assignmentController.getWeeklyAssignments
);

user.get(
  "/fleet",
  verifyToken,
  fleetController.getAllFleet
);

// Bulk save/update the fleet table
user.post(
  "/fleet/bulk-save",
  verifyToken,
  jsonParser, // Parses the incoming JSON body from React
  fleetController.bulkUpsertFleet
);

// Delete a single asset row
user.delete(
  "/fleet/:id",
  verifyToken,
  fleetController.deleteFleetAsset
);

// Add this alongside your other fleet routes
user.get(
  "/fleet/months",
  verifyToken,
  fleetController.getFleetMonths
);

user.get(
  "/fleet/metrics",
  verifyToken,
  fleetController.getFleetScheduleMetrics
);

user.get(
  "/maintenance/dashboard",
  verifyToken,
  maintenanceController.getMaintenanceDashboard
);

// 2. Get Reset Records for the Modal (with optional ?date= & ?msnEsn= filters)
user.get(
  "/maintenance/reset-records",
  verifyToken,
  maintenanceController.getResetRecords
);

// 3. Save/Update records from the Modal
user.post(
  "/maintenance/reset-records",
  verifyToken,
  jsonParser,
  maintenanceController.bulkSaveResetRecords
);

// 4. Trigger Compute Button
user.post(
  "/maintenance/compute",
  verifyToken,
  jsonParser,
  maintenanceController.computeMaintenanceLogic
);

// 5. Get Rotable Movements for the Modal
user.get(
  "/maintenance/rotables",
  verifyToken,
  maintenanceController.getRotables
);

// 6. Bulk Save/Update Rotable Movements
user.post(
  "/maintenance/rotables",
  verifyToken,
  jsonParser,
  maintenanceController.bulkSaveRotables
);

// 7. Get Target Maintenance Status
user.get(
  "/maintenance/targets",
  verifyToken,
  maintenanceController.getTargets
);

// 8. Bulk Save Target Maintenance Status
user.post(
  "/maintenance/targets",
  verifyToken,
  jsonParser,
  maintenanceController.bulkSaveTargets
);

// 9. Get Calendar Inputs
user.get(
  "/maintenance/calendar",
  verifyToken,
  maintenanceController.getCalendar
);

// 10. Bulk Save Calendar Inputs
user.post(
  "/maintenance/calendar",
  verifyToken,
  jsonParser,
  maintenanceController.bulkSaveCalendar
);

// ─── POO & Revenue ─────────────────────────────────────────────────
const pooController = require("../controller/pooController");

user.get("/poo", verifyToken, pooController.getPooData);
user.post("/poo/populate", verifyToken, jsonParser, pooController.populatePoo);
user.post("/poo/update", verifyToken, jsonParser, pooController.updatePooRecords);
user.delete("/poo", verifyToken, jsonParser, pooController.deletePooRecords);
user.get("/revenue", verifyToken, pooController.getRevenueData);

module.exports = user;