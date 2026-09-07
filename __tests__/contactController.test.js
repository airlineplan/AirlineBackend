const assert = require("node:assert/strict");
const http = require("node:http");
const test = require("node:test");

const { createApp } = require("../app");
const { validateContactRequest } = require("../controller/contactController");

const originalEnv = { ...process.env };
const originalFetch = global.fetch;
const originalConsoleError = console.error;

const postJson = (port, payload) =>
  new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const request = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/send-contactEmail",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (response) => {
        let responseBody = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          responseBody += chunk;
        });
        response.on("end", () => {
          resolve({
            status: response.statusCode,
            body: responseBody ? JSON.parse(responseBody) : {},
          });
        });
      }
    );
    request.on("error", reject);
    request.end(body);
  });

test.afterEach(() => {
  process.env = { ...originalEnv };
  global.fetch = originalFetch;
  console.error = originalConsoleError;
});

test("contact validation trims fields and rejects invalid addresses", () => {
  assert.deepEqual(
    validateContactRequest({
      name: "  Ada Lovelace  ",
      email: "  ADA@EXAMPLE.COM ",
      subject: " Help ",
      message: " Hello ",
    }),
    {
      contact: {
        name: "Ada Lovelace",
        email: "ada@example.com",
        subject: "Help",
        message: "Hello",
      },
    }
  );

  assert.deepEqual(validateContactRequest({ name: "Ada", email: "invalid", message: "Hi" }), {
    error: "Please enter a valid email address",
  });
});

test("contact endpoint is available in control-plane mode", async () => {
  process.env.APP_MODE = "control-plane";
  process.env.SERVE_FRONTEND = "false";
  process.env.RESEND_API_KEY = "re_test";
  process.env.CONTACT_EMAIL = "support@airlineplan.com";

  let providerPayload;
  global.fetch = async (_url, options) => {
    providerPayload = JSON.parse(options.body);
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ id: "email_contact_123" }),
    };
  };

  const server = createApp().listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));

  try {
    const response = await postJson(server.address().port, {
      name: "Ada",
      email: "ada@example.com",
      subject: "Need help",
      message: "Hello",
    });

    assert.equal(response.status, 200);
    assert.equal(response.body.message, "Message sent successfully");
    assert.equal(providerPayload.to, "support@airlineplan.com");
    assert.equal(providerPayload.reply_to, "ada@example.com");
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  }
});

test("contact endpoint returns an actionable 503 when delivery is unavailable", async () => {
  process.env.APP_MODE = "control-plane";
  process.env.SERVE_FRONTEND = "false";
  process.env.RESEND_API_KEY = "re_invalid";
  global.fetch = async () => ({
    ok: false,
    status: 401,
    text: async () => JSON.stringify({ message: "API key is invalid" }),
  });
  console.error = () => {};

  const server = createApp().listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));

  try {
    const response = await postJson(server.address().port, {
      name: "Ada",
      email: "ada@example.com",
      subject: "Need help",
      message: "Hello",
    });

    assert.equal(response.status, 503);
    assert.equal(response.body.code, "CONTACT_EMAIL_UNAVAILABLE");
    assert.match(response.body.message, /admin@airlineplan\.com/);
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  }
});
