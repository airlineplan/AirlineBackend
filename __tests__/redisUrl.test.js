const assert = require("node:assert/strict");
const test = require("node:test");

const { getRedisUrl } = require("../config/redisUrl");

test("Redis URL accepts local and TLS endpoints", () => {
  assert.equal(
    getRedisUrl({ REDIS_URL: " redis://127.0.0.1:6379 " }),
    "redis://127.0.0.1:6379"
  );
  assert.equal(
    getRedisUrl({ REDIS_URL: "rediss://user:secret@cache.example.com:6380" }),
    "rediss://user:secret@cache.example.com:6380"
  );
});

test("Redis URL rejects placeholder hostnames", () => {
  assert.throws(
    () => getRedisUrl({ REDIS_URL: "redis://hostname:6379" }),
    /placeholder hostname "hostname"/
  );
});

test("Redis URL rejects invalid protocols and syntax", () => {
  assert.throws(
    () => getRedisUrl({ REDIS_URL: "http://127.0.0.1:6379" }),
    /redis:\/\/ or rediss:\/\//
  );
  assert.throws(
    () => getRedisUrl({ REDIS_URL: "not a url" }),
    /valid redis:\/\/ or rediss:\/\/ URL/
  );
});

test("Redis URL is optional outside required runtime modes", () => {
  assert.equal(getRedisUrl({}), null);
});
