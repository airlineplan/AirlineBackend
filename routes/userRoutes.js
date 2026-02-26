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

const userController = require("../controller/userController");

// 🔥 UNCOMMENT AND IMPORT THE HELPER
const createConnections = require('../helper/createConnections');

user.post(
  "/importUser",
  upload.single("file"),
  verifyToken,
  importUser
);
user.post("/add-Data", jsonParser, verifyToken, userController.AddData);
user.get("/get-data", verifyToken, userController.getData);
user.get("/downloadFLGTs", verifyToken, userController.downloadExpenses);
user.get("/products/:id", userController.singleData);
user.delete("/delete", jsonParser, verifyToken, userController.deleteFlightsAndUpdateSectors);
user.delete("/delete-sector/:ids", userController.deleteSectors);
user.put(
  "/update-data/:id",
  jsonParser,
  verifyToken,
  userController.updateData
);
user.get("/sectors", verifyToken, userController.getSecors);
user.post("/add-sector", jsonParser, verifyToken, userController.AddSectors);
user.put(
  "/update-sectore/:id",
  verifyToken,
  jsonParser,
  userController.updateSector
);
user.get("/sectorsbyid/:id", userController.singleSector);
user.post("/admin-login", jsonParser, userController.AdminLogin);
user.post("/user-signup", jsonParser, userLogin.createUser);
user.post("/user-login", jsonParser, userLogin.loginUser);
user.post("/send-email", jsonParser, userLogin.sendEmail);
user.post("/send-contactEmail", jsonParser, userLogin.sendContactEmail);
user.post("/change-passowrd", jsonParser, userLogin.changePassword);
user.get("/flight", verifyToken, userController.getFlights);
user.post("/searchflights", verifyToken, userController.searchFlights);
user.post("/flightsWoRotations", verifyToken, jsonParser, userController.getFlightsWoRotations);
user.get("/listVariants", verifyToken, userController.getVariants);
user.get("/listRotations", verifyToken, userController.getRotations);
user.get("/dashboard", verifyToken, userController.getDashboardData);

// 🔥 USE THE IMPORTED HELPER DIRECTLY FOR THIS ROUTE
user.get("/createConnections", verifyToken, createConnections);

user.get("/dashboard/populateDropDowns", verifyToken, userController.populateDashboardDropDowns);
user.get("/get-stationData", verifyToken, userController.getStationsTableData);
user.get("/getNextRotationNumber", verifyToken, userController.getNextRotationNumber);
user.get("/rotationbyid/:id", userController.singleRotationDetail);
user.post("/updateRotationSummary", verifyToken, jsonParser, userController.updateRotationSummary);
user.post("/addRotationDetailsFlgtChange", verifyToken, jsonParser, userController.addRotationDetailsFlgtChange);
user.post("/saveStation", verifyToken, jsonParser, userController.saveStation);
user.post("/deleteCompleteRotation/", verifyToken, jsonParser, userController.deleteCompleteRotation);
user.post("/deletePrevInRotation/", verifyToken, jsonParser, userController.deletePrevInRotation);
user.post("/list-page-data", verifyToken, userController.getListPageData);
user.get("/view-page-data", verifyToken, userController.getViewData);

module.exports = user;