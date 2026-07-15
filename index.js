require("dotenv").config();
require("./config/dns");

const { createApp } = require("./app");
const {
  connectDatabase,
  disconnectDatabase,
  formatMongoError,
} = require("./config/db");
const { connectRedis, disconnectRedis } = require("./config/redis");
const { getAppMode, validateRuntimeConfig } = require("./config/runtime");

const start = async () => {
  const mode = getAppMode();
  validateRuntimeConfig();
  await connectDatabase();

  if (mode === "tenant" && process.env.REDIS_URL) {
    await connectRedis();
  }

  const port = Number(process.env.PORT || 3000);
  const server = createApp().listen(port, "0.0.0.0", () => {
    const address = server.address();
    const listeningPort = typeof address === "object" && address ? address.port : port;
    console.log(`Server listening on port ${listeningPort} (${mode} mode)`);
  });

  const shutdown = async (signal) => {
    console.log(`Received ${signal}; shutting down`);
    server.close(async () => {
      await Promise.allSettled([disconnectRedis(), disconnectDatabase()]);
      process.exit(0);
    });
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  return server;
};

if (require.main === module) {
  start().catch((error) => {
    console.error("Application startup failed:", formatMongoError(error));
    process.exit(1);
  });
}

module.exports = {
  start,
};
