const mongoose = require("mongoose");

// new developer - Himanshu link 
// const DB = "mongodb+srv://crazyphoton150hs:C00!buddy@cluster0.4mq6pjf.mongodb.net/?retryWrites=true&w=majority";
// const DB = "mongodb+srv://hhimanshu030:C00!buddy@cluster0.qondpde.mongodb.net/?retryWrites=true&w=majority";
// const DB = "mongodb://https://airlinebackend-zfsg.onrender.com/airlines"

// const DB = "mongodb://127.0.0.1/airlines";

// Client - Neelandri link
const DB = "mongodb+srv://neeladrinathsarangi:kBhaZHXuGOIUgt9y@cluster0.n0cx0yj.mongodb.net/?retryWrites=true&w=majority"

mongoose
  .connect(DB, {})
  .then(() => {
    console.log("connection sucussfull", DB);
  })
  .catch((err) => {
    console.log("no connection", err);
  });
