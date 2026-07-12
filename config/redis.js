const { createClient } = require("redis");

let redisClient;

const getRedisClient = () => {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) return null;

  if (!redisClient) {
    redisClient = createClient({
      url: redisUrl,
      socket: {
        tls: redisUrl.startsWith("rediss://"),
        reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
      },
    });
    redisClient.on("error", (error) => {
      console.error("Redis connection error", error.message);
    });
  }

  return redisClient;
};

const connectRedis = async () => {
  const client = getRedisClient();
  if (client && !client.isOpen) {
    await client.connect();
  }
  return client;
};

const disconnectRedis = async () => {
  if (redisClient?.isOpen) {
    await redisClient.quit();
  }
};

module.exports = {
  connectRedis,
  disconnectRedis,
  getRedisClient,
};
