const express = require("express");
const { sendContactEmail } = require("../controller/contactController");

const router = express.Router();

router.post("/send-contactEmail", sendContactEmail);

module.exports = router;
