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

// A stray rejection outside a request (a background job, an event handler) must
// not take down every tenant request with it.
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason);
});

const start = async () => {
  const mode = getAppMode();
  validateRuntimeConfig();
  await connectDatabase();
  const stopActualsWorker = mode === "tenant" ? require("./workers/actualsWorker").startActualsWorker() : () => {};

  if (mode === "tenant" && process.env.REDIS_URL) {
    await connectRedis();
  }

  const port = Number(process.env.PORT || 3000);
  const server = createApp().listen(port, "0.0.0.0", () => {
    const address = server.address();
    const listeningPort = typeof address === "object" && address ? address.port : port;
    console.log(`Server listening on port ${listeningPort} (${mode} mode)`);
  });

  // Keep idle connections open longer than the load balancer's idle timeout (300s),
  // otherwise Node closes sockets the ALB still reuses and clients see sporadic 502s.
  const keepAliveTimeoutMs = Number(process.env.SERVER_KEEP_ALIVE_TIMEOUT_MS) || 310000;
  server.keepAliveTimeout = keepAliveTimeoutMs;
  server.headersTimeout = keepAliveTimeoutMs + 5000;

  const shutdown = async (signal) => {
    stopActualsWorker();
    console.log(`Received ${signal}; shutting down`);
    server.close(async () => {
      const { closeFlightQueue } = require("./helper/createConnections");
      await Promise.allSettled([
        closeFlightQueue(),
        disconnectRedis(),
        disconnectDatabase(),
      ]);
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
