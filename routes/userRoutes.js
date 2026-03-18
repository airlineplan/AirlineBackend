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

// Add this alongside your existing routes
user.get("/maintenance-dashboard", verifyToken, maintenanceController.getMaintenanceDashboard);

module.exports = user;