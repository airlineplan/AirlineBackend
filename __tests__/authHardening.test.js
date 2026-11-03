const assert = require("node:assert/strict");
const test = require("node:test");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

process.env.APP_MODE = "tenant";
process.env.TENANT_DOMAIN = "tenant.example.com";
process.env.JWT_SECRET = "auth-hardening-test-secret";
process.env.RESEND_API_KEY = "test-resend-key";
process.env.SERVE_FRONTEND = "false";

const { createApp } = require("../app");
const User = require("../model/userSchema");
const Otp = require("../model/otp");
const Sector = require("../model/sectorSchema");
const { USER_TOKEN_AUDIENCE } = require("../middlware/auth");
const { startMongoTestServer } = require("./helpers/mongoTestServer");

const originalFetch = global.fetch;
const originalConsoleError = console.error;
let closeMongo;
let server;
let baseUrl;
const sentEmails = [];

const request = async (method, path, { body, token } = {}) => {
  const response = await originalFetch(`${baseUrl}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { "x-access-token": token } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : {} };
};

const createUser = async (email, role = "user") => User.create({
  email,
  password: await bcrypt.hash("Original#1", 4),
  role,
  pageAccessConfigured: true,
});

const tokenFor = (user) => jwt.sign(
  { id: user.id, email: user.email, role: user.role, aud: USER_TOKEN_AUDIENCE, tokenType: "tenant_user" },
  process.env.JWT_SECRET
);

const lastOtpSentTo = (email) => {
  const message = [...sentEmails].reverse().find((entry) => entry.to === email);
  return message?.text.match(/(\d+)\s*$/)?.[1];
};

test.before(async () => {
  closeMongo = await startMongoTestServer();
  await Otp.createIndexes();
  global.fetch = async (url, options) => {
    if (String(url).startsWith("https://api.resend.com")) {
      sentEmails.push(JSON.parse(options.body));
      return new Response(JSON.stringify({ id: "email_test" }), { status: 200 });
    }
    return originalFetch(url, options);
  };
  await new Promise((resolve) => {
    server = createApp().listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

test.after(async () => {
  global.fetch = originalFetch;
  await new Promise((resolve) => server.close(resolve));
  await closeMongo();
});

test.beforeEach(async () => {
  sentEmails.length = 0;
  console.error = () => {};
  await Promise.all([User.deleteMany({}), Otp.deleteMany({}), Sector.collection.deleteMany({})]);
});

test.afterEach(() => {
  console.error = originalConsoleError;
});

test("send-email gives the same answer for unknown emails and sends nothing", async () => {
  await createUser("known@example.com");

  const unknown = await request("POST", "/send-email", { body: { email: "nobody@example.com" } });
  const known = await request("POST", "/send-email", { body: { email: "known@example.com" } });

  assert.equal(unknown.status, 200);
  assert.deepEqual(unknown.body, known.body);
  assert.equal(unknown.body.statusText, "Success");
  assert.equal(sentEmails.length, 1);
  assert.equal(sentEmails[0].to, "known@example.com");
});

test("OTP is a random 6-digit code stored only as a hash, with a resend cooldown", async () => {
  await createUser("user@example.com");

  await request("POST", "/send-email", { body: { email: "user@example.com" } });
  await request("POST", "/send-email", { body: { email: "user@example.com" } });

  assert.equal(sentEmails.length, 1, "a second request inside the cooldown sends no email");
  const code = lastOtpSentTo("user@example.com");
  assert.match(code, /^\d{6}$/);

  const stored = await Otp.find({ email: "user@example.com" }).lean();
  assert.equal(stored.length, 1);
  assert.notEqual(stored[0].code, code);
  assert.match(stored[0].code, /^[0-9a-f]{64}$/);
});

test("OTP locks after 5 wrong guesses, even for the right code", async () => {
  await createUser("user@example.com");
  await request("POST", "/send-email", { body: { email: "user@example.com" } });
  const code = lastOtpSentTo("user@example.com");
  const wrong = code === "000000" ? "000001" : "000000";

  const guesses = await Promise.all(Array.from({ length: 8 }, () => request("POST", "/change-passowrd", {
    body: { email: "user@example.com", otpCode: wrong, password: "Hacked#1" },
  })));
  assert.ok(guesses.every((guess) => guess.body.statusText === "error"));

  const correct = await request("POST", "/change-passowrd", {
    body: { email: "user@example.com", otpCode: code, password: "Hacked#1" },
  });
  assert.equal(correct.body.statusText, "error");

  const user = await User.findOne({ email: "user@example.com" }).lean();
  assert.ok(await bcrypt.compare("Original#1", user.password), "password must be unchanged");
});

test("a correct OTP changes the password once and cannot be replayed", async () => {
  await createUser("user@example.com");
  await request("POST", "/send-email", { body: { email: "user@example.com" } });
  const code = lastOtpSentTo("user@example.com");

  const first = await request("POST", "/change-passowrd", {
    body: { email: "user@example.com", otpCode: code, password: "NewPass#1" },
  });
  assert.equal(first.body.statusText, "Success");

  const replay = await request("POST", "/change-passowrd", {
    body: { email: "user@example.com", otpCode: code, password: "Replay#1" },
  });
  assert.equal(replay.body.statusText, "error");

  const user = await User.findOne({ email: "user@example.com" }).lean();
  assert.ok(await bcrypt.compare("NewPass#1", user.password));
});

test("change-passowrd rejects a missing password instead of erroring", async () => {
  const response = await request("POST", "/change-passowrd", {
    body: { email: "user@example.com", otpCode: "123456" },
  });
  assert.equal(response.status, 400);
});

test("a malformed id in an async handler returns 400 and the server keeps serving", async () => {
  const admin = await createUser("admin@example.com", "tenant_admin");
  const token = tokenFor(admin);

  const response = await request("PATCH", "/tenant/users/not-an-object-id/role", {
    token,
    body: { role: "user" },
  });
  assert.equal(response.status, 400);

  const health = await request("GET", "/auth/verify", { token });
  assert.equal(health.status, 200);
});

test("a user cannot read another user's sector by id", async () => {
  const owner = await createUser("owner@example.com");
  const other = await createUser("other@example.com");
  const { insertedId } = await Sector.collection.insertOne({
    sector1: "AAA",
    sector2: "BBB",
    userId: owner.id,
  });

  const asOther = await request("GET", `/sectorsbyid/${insertedId}`, { token: tokenFor(other) });
  assert.equal(asOther.status, 404);

  const asOwner = await request("GET", `/sectorsbyid/${insertedId}`, { token: tokenFor(owner) });
  assert.equal(asOwner.status, 200);
  assert.equal(asOwner.body.sector1, "AAA");
});
