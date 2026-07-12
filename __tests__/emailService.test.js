const assert = require("node:assert/strict");
const test = require("node:test");

const {
  sendOtpEmail,
  sendContactQueryEmail,
  sendResendEmail,
} = require("../services/emailService");

const originalEnv = { ...process.env };
const originalFetch = global.fetch;

test.afterEach(() => {
  process.env = { ...originalEnv };
  global.fetch = originalFetch;
});

test("sendOtpEmail posts the OTP email through Resend", async () => {
  process.env.RESEND_API_KEY = "re_test";
  process.env.RESEND_FROM_EMAIL = "Airlineplan <admin@mail.airlineplan.com>";

  let request;
  global.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ id: "email_123" }),
    };
  };

  const result = await sendOtpEmail({ to: "user@example.com", otp: 1234 });

  assert.deepEqual(result, { id: "email_123" });
  assert.equal(request.url, "https://api.resend.com/emails");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.headers.Authorization, "Bearer re_test");

  const payload = JSON.parse(request.options.body);
  assert.equal(payload.from, "Airlineplan <admin@mail.airlineplan.com>");
  assert.equal(payload.to, "user@example.com");
  assert.equal(payload.subject, "Airlineplan OTP");
  assert.match(payload.text, /1234/);
});

test("sendContactQueryEmail routes contact messages to the configured inbox", async () => {
  process.env.RESEND_API_KEY = "re_test";
  process.env.CONTACT_EMAIL = "support@airlineplan.com";

  let payload;
  global.fetch = async (_url, options) => {
    payload = JSON.parse(options.body);
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ id: "email_456" }),
    };
  };

  await sendContactQueryEmail({
    name: "Ada",
    email: "ada@example.com",
    subject: "Need help",
    message: "Hello",
  });

  assert.equal(payload.to, "support@airlineplan.com");
  assert.equal(payload.reply_to, "ada@example.com");
  assert.equal(payload.subject, "Need help");
  assert.match(payload.text, /Ada/);
  assert.match(payload.html, /New Airlineplan contact query/);
});

test("sendResendEmail throws when the API key is missing", async () => {
  delete process.env.RESEND_API_KEY;

  await assert.rejects(
    () => sendResendEmail({ to: "user@example.com", subject: "Hi", text: "Hello" }),
    /RESEND_API_KEY is not configured/
  );
});
