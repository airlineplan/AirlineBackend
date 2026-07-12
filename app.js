const path = require("path");
const fs = require("fs");
const cors = require("cors");
const express = require("express");
const mongoose = require("mongoose");

const { getAppMode, getTenantRuntimeConfig } = require("./config/runtime");
const { getRedisClient } = require("./config/redis");
const { enforceTenantHost } = require("./middlware/tenantHost");

const createHealthPayload = async () => {
  const mode = getAppMode();
  const payload = {
    status: "ok",
    mode,
    appVersion: process.env.APP_VERSION || "development",
    mongo: mongoose.connection.readyState === 1 ? "connected" : "disconnected",
  };

  if (mode === "tenant") {
    const config = getTenantRuntimeConfig();
    const redis = getRedisClient();
    let redisStatus = redis ? "disconnected" : "not_configured";

    if (redis?.isReady) {
      try {
        await redis.ping();
        redisStatus = "connected";
      } catch {
        redisStatus = "unavailable";
      }
    }

    Object.assign(payload, {
      tenant: config.tenantId,
      domain: config.domain,
      redis: redisStatus,
    });
  }

  return payload;
};

const serveFrontend = (app) => {
  if (process.env.SERVE_FRONTEND === "false") return;
  const frontendDirectory =
    process.env.FRONTEND_DIST_PATH ||
    path.resolve(__dirname, "../Airlineplan/dist");

  if (!fs.existsSync(path.join(frontendDirectory, "index.html"))) return;

  app.use(express.static(frontendDirectory));
  app.get("*", (_req, res) => {
    res.sendFile(path.join(frontendDirectory, "index.html"));
  });
};

const createApp = () => {
  const app = express();
  const mode = getAppMode();

  app.set("trust proxy", 1);
  app.disable("x-powered-by");
  app.use(cors());
  app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || "10mb" }));
  app.use(express.urlencoded({ extended: true }));

  app.get("/healthz", (_req, res) => {
    res.status(200).json({
      status: "ok",
      mode,
      appVersion: process.env.APP_VERSION || "development",
    });
  });

  if (mode === "control-plane") {
    app.use("/admin", require("./routes/adminRoutes"));
    app.get("/api/health", async (_req, res) => {
      const payload = await createHealthPayload();
      res.status(payload.mongo === "connected" ? 200 : 503).json(payload);
    });
  } else {
    const config = getTenantRuntimeConfig();

    app.use(enforceTenantHost);
    app.use((req, _res, next) => {
      req.tenant = config;
      next();
    });

    app.get("/api/public-config", (_req, res) => {
      res.status(200).json({
        tenant: config.tenantId,
        slug: config.slug,
        companyName: config.companyName,
        domain: config.domain,
        appVersion: config.appVersion,
        features: config.features,
        branding: config.branding,
      });
    });

    app.get(["/api/health", "/health"], async (_req, res) => {
      const payload = await createHealthPayload();
      const healthy =
        payload.mongo === "connected" &&
        (!process.env.REDIS_URL || payload.redis === "connected");
      res.status(healthy ? 200 : 503).json(payload);
    });

    app.use("/", require("./routes/userRoutes"));
  }

  serveFrontend(app);

  app.use((error, _req, res, _next) => {
    console.error(error);
    res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Internal server error",
    });
  });

  return app;
};

module.exports = {
  createApp,
  createHealthPayload,
};
