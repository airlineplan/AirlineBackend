const mongoose = require("mongoose");

const connectDatabase = async () => {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    throw new Error("MONGO_URI is required");
  }

  if (mongoose.connection.readyState === 1) {
    return mongoose.connection;
  }

  await mongoose.connect(uri, {
    dbName: process.env.MONGO_DB_NAME || undefined,
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 15000),
  });
  console.log("MongoDB connection established");
  return mongoose.connection;
};

const disconnectDatabase = async () => {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
};

module.exports = {
  connectDatabase,
  disconnectDatabase,
};
