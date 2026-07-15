require("./dns");

const dns = require("node:dns");
const mongoose = require("mongoose");

const DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 15000;
let connectionPromise;
let listenersRegistered = false;

const redactMongoUris = (value) =>
  String(value || "Unknown error").replace(
    /mongodb(?:\+srv)?:\/\/[^\s'"`]+/gi,
    "[redacted MongoDB URI]"
  );

const formatMongoError = (error) => {
  const code = error?.code ? ` [${error.code}]` : "";
  return `${redactMongoUris(error?.message || error)}${code}`;
};

const getServerSelectionTimeout = () => {
  const configured = Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS);
  return Number.isFinite(configured) && configured > 0
    ? configured
    : DEFAULT_SERVER_SELECTION_TIMEOUT_MS;
};

const isSrvUri = (uri) => /^mongodb\+srv:\/\//i.test(String(uri || "").trim());

const getSrvHostname = (uri) => {
  try {
    const parsed = new URL(uri);
    if (parsed.protocol !== "mongodb+srv:" || !parsed.hostname) {
      throw new Error("Not a MongoDB SRV URI");
    }
    return parsed.hostname;
  } catch {
    const error = new Error("MONGO_URI is not a valid mongodb+srv:// URI");
    error.code = "INVALID_MONGODB_SRV_URI";
    throw error;
  }
};

const diagnoseSrv = async (uri) => {
  if (!isSrvUri(uri)) return [];

  const hostname = getSrvHostname(uri);
  const srvRecord = `_mongodb._tcp.${hostname}`;

  try {
    const records = await dns.promises.resolveSrv(srvRecord);
    console.log(`MongoDB SRV lookup successful: ${records.length} hosts found`);
    return records;
  } catch (cause) {
    const dnsServers = dns.getServers();
    console.error(
      `MongoDB SRV lookup failed for ${hostname}; DNS servers in use: ${
        dnsServers.join(", ") || "none"
      } (${cause.code || cause.name})`
    );
    const error = new Error(
      `MongoDB SRV DNS resolution failed for ${hostname}. Check NODE_DNS_SERVERS and network DNS access.`
    );
    error.code = "MONGODB_SRV_DNS_FAILED";
    error.cause = cause;
    throw error;
  }
};

const isSrvDnsError = (error) => {
  const code = String(error?.code || "").toUpperCase();
  return (
    code === "MONGODB_SRV_DNS_FAILED" ||
    ["ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "ETIMEOUT", "ESERVFAIL"].includes(code) ||
    /querySrv|SRV DNS resolution failed/i.test(String(error?.message || ""))
  );
};

const isAuthenticationError = (error) =>
  Number(error?.code) === 18 ||
  /authentication failed|bad auth/i.test(String(error?.message || ""));

const registerConnectionListeners = () => {
  if (listenersRegistered) return;
  listenersRegistered = true;

  mongoose.connection.on("error", (error) => {
    console.error("MongoDB connection error:", formatMongoError(error));
  });
  mongoose.connection.on("disconnected", () => {
    console.warn("MongoDB disconnected");
  });
};

const validateStandardFallback = () => {
  const fallbackUri = String(process.env.MONGO_URI_STANDARD || "").trim();
  if (!fallbackUri) return null;
  if (!/^mongodb:\/\//i.test(fallbackUri)) {
    const error = new Error("MONGO_URI_STANDARD must begin with mongodb://");
    error.code = "INVALID_MONGODB_STANDARD_URI";
    throw error;
  }
  return fallbackUri;
};

const connectWithUri = async (uri) => {
  await diagnoseSrv(uri);
  return mongoose.connect(uri, {
    dbName: process.env.MONGO_DB_NAME || undefined,
    serverSelectionTimeoutMS: getServerSelectionTimeout(),
  });
};

const connectDatabase = async () => {
  const uri = String(process.env.MONGO_URI || "").trim();
  if (!uri) {
    throw new Error("MONGO_URI is required");
  }

  if (mongoose.connection.readyState === 1) {
    return mongoose.connection;
  }

  if (connectionPromise) return connectionPromise;
  registerConnectionListeners();

  connectionPromise = (async () => {
    try {
      await connectWithUri(uri);
    } catch (error) {
      const fallbackUri = isSrvUri(uri) && isSrvDnsError(error)
        ? validateStandardFallback()
        : null;

      if (!fallbackUri) {
        if (isAuthenticationError(error)) {
          const authError = new Error(
            "MongoDB authentication failed. Verify the configured database credentials and Atlas database user permissions."
          );
          authError.code = "MONGODB_AUTHENTICATION_FAILED";
          authError.cause = error;
          throw authError;
        }
        throw error;
      }

      console.warn(
        "MongoDB SRV resolution failed; using explicitly configured MONGO_URI_STANDARD fallback"
      );
      await mongoose.disconnect().catch(() => {});
      await connectWithUri(fallbackUri);
    }

    console.log("MongoDB connected successfully");
    return mongoose.connection;
  })();

  try {
    return await connectionPromise;
  } finally {
    connectionPromise = undefined;
  }
};

const disconnectDatabase = async () => {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
};

module.exports = {
  connectDatabase,
  diagnoseSrv,
  disconnectDatabase,
  formatMongoError,
  getSrvHostname,
  isSrvUri,
};
