const express = require("express");
const app = express();
const cors = require("cors");
const userRoutes = require("./routes/userRoutes");
const exceljs = require("exceljs");
const path = require("path");
const mongoose = require("mongoose");
const Fleet = require("./model/fleet");
const PORT = 3000;
app.use(cors());

require("./config/db");

mongoose.connection.once("open", async () => {
  try {
    // Keep Fleet indexes aligned with the schema so old global SN indexes do not block other users.
    await Fleet.syncIndexes();
    console.log("Fleet indexes synced successfully");
  } catch (error) {
    console.error("Failed to sync Fleet indexes:", error);
  }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use("/", userRoutes);

// app.use(express.static("dist"));


// app.get("*", (req, res) => {
//   res.sendFile(path.resolve(__dirname, "dist", "index.html"));
// });

app.use(express.static(path.join(__dirname, "../Airlineplan/dist")));

app.get("*", (req, res) => {
  res.sendFile(
    path.resolve(__dirname, "../Airlineplan/dist", "index.html")
  );
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`server started on ${PORT}`);
});
