const express = require("express");
const app = express();
const cors = require("cors");
const userRoutes = require("./routes/userRoutes");
const exceljs = require("exceljs");
const path = require("path");
const PORT = 3000;
app.use(cors());

require("./config/db");

app.use("/", userRoutes);

// app.use(express.static("dist"));


// app.get("*", (req, res) => {
//   res.sendFile(path.resolve(__dirname, "dist", "index.html"));
// });

app.use(express.static(path.join(__dirname, "../Airlineplan/dist")));

// Catch-all route to serve the index.html from the frontend dist folder
app.get("*", (req, res) => {
  res.sendFile(path.resolve(__dirname, "../Airlineplan/dist", "index.html"));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`server started on ${PORT}`);
});
