const RESEND_EMAILS_URL = "https://api.resend.com/emails";

const DEFAULT_FROM_EMAIL = "Airlineplan <admin@mail.airlineplan.com>";
const DEFAULT_CONTACT_EMAIL = "admin@airlineplan.com";

const getFromEmail = () => process.env.RESEND_FROM_EMAIL || DEFAULT_FROM_EMAIL;
const getContactEmail = () =>
  process.env.CONTACT_EMAIL || process.env.ADMIN_EMAIL || DEFAULT_CONTACT_EMAIL;

const escapeHtml = (value) =>
  String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const sendResendEmail = async ({ from, to, subject, text, html, replyTo }) => {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    throw new Error("RESEND_API_KEY is not configured");
  }

  const payload = {
    from: from || getFromEmail(),
    to,
    subject,
    text,
    html,
  };

  if (replyTo) {
    payload.reply_to = replyTo;
  }

  const response = await fetch(RESEND_EMAILS_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const responseText = await response.text();
  let responseBody = {};
  if (responseText) {
    try {
      responseBody = JSON.parse(responseText);
    } catch {
      responseBody = { message: responseText };
    }
  }

  if (!response.ok) {
    const details =
      responseBody?.message ||
      responseBody?.error?.message ||
      responseBody?.error ||
      "Resend email request failed";
    const error = new Error(details);
    error.statusCode = response.status;
    error.response = responseBody;
    throw error;
  }

  return responseBody;
};

const sendOtpEmail = ({ to, otp }) =>
  sendResendEmail({
    to,
    subject: "Airlineplan OTP",
    text: `Your OTP code is: ${otp}`,
    html: `
      <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
        <p>Your Airlineplan password reset OTP is:</p>
        <p style="font-size: 24px; font-weight: 700; letter-spacing: 4px;">${escapeHtml(otp)}</p>
        <p>This code expires in 5 minutes.</p>
      </div>
    `,
  });

const sendContactQueryEmail = ({ name, email, subject, message }) => {
  const safeSubject = String(subject || "").trim() || "Airlineplan contact query";
  const text = [
    `Name: ${name}`,
    `Email: ${email}`,
    `Subject: ${safeSubject}`,
    "",
    "Message:",
    message,
  ].join("\n");

  return sendResendEmail({
    to: getContactEmail(),
    subject: safeSubject,
    replyTo: email,
    text,
    html: `
      <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
        <h2 style="margin: 0 0 16px;">New Airlineplan contact query</h2>
        <p><strong>Name:</strong> ${escapeHtml(name)}</p>
        <p><strong>Email:</strong> ${escapeHtml(email)}</p>
        <p><strong>Subject:</strong> ${escapeHtml(safeSubject)}</p>
        <p><strong>Message:</strong></p>
        <p style="white-space: pre-wrap;">${escapeHtml(message)}</p>
      </div>
    `,
  });
};

module.exports = {
  sendResendEmail,
  sendOtpEmail,
  sendContactQueryEmail,
};
