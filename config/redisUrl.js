const PLACEHOLDER_HOSTS = new Set([
  "host",
  "hostname",
  "redis-host",
  "your-redis-host",
]);

const invalidRedisUrl = (message, cause) => {
  const error = new Error(message);
  error.code = "INVALID_RUNTIME_CONFIG";
  if (cause) error.cause = cause;
  return error;
};

const getRedisUrl = (env = process.env) => {
  const redisUrl = String(env.REDIS_URL || "").trim();
  if (!redisUrl) return null;

  let parsed;
  try {
    parsed = new URL(redisUrl);
  } catch (error) {
    throw invalidRedisUrl(
      "REDIS_URL must be a valid redis:// or rediss:// URL",
      error
    );
  }

  if (!["redis:", "rediss:"].includes(parsed.protocol)) {
    throw invalidRedisUrl("REDIS_URL must use the redis:// or rediss:// protocol");
  }

  const hostname = parsed.hostname.toLowerCase();
  if (!hostname || PLACEHOLDER_HOSTS.has(hostname)) {
    throw invalidRedisUrl(
      `REDIS_URL contains the placeholder hostname "${hostname || "(empty)"}"; ` +
        "set it to the actual Redis host (use 127.0.0.1 when Redis runs on this server)"
    );
  }

  return redisUrl;
};

module.exports = { getRedisUrl };
