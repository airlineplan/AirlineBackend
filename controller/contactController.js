const { sendContactQueryEmail } = require("../services/emailService");

const CONTACT_EMAIL = "admin@airlineplan.com";
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const FIELD_LIMITS = {
  name: 120,
  email: 254,
  subject: 200,
  message: 5000,
};

const normalizeField = (value) => String(value || "").trim();

const validateContactRequest = (body = {}) => {
  const contact = {
    name: normalizeField(body.name),
    email: normalizeField(body.email).toLowerCase(),
    subject: normalizeField(body.subject),
    message: normalizeField(body.message),
  };

  if (!contact.name || !contact.email || !contact.message) {
    return { error: "Name, email, and message are required" };
  }

  if (!EMAIL_PATTERN.test(contact.email)) {
    return { error: "Please enter a valid email address" };
  }

  const oversizedField = Object.entries(FIELD_LIMITS).find(
    ([field, limit]) => contact[field].length > limit
  );
  if (oversizedField) {
    const [field, limit] = oversizedField;
    return { error: `${field} must be ${limit} characters or fewer` };
  }

  return { contact };
};

const sendContactEmail = async (req, res) => {
  const { contact, error } = validateContactRequest(req.body);
  if (error) {
    return res.status(400).json({ message: error });
  }

  try {
    await sendContactQueryEmail(contact);
    return res.status(200).json({ message: "Message sent successfully" });
  } catch (deliveryError) {
    console.error("Contact email delivery failed:", {
      statusCode: deliveryError.statusCode || null,
      code: deliveryError.code || null,
      message: deliveryError.message,
    });
    return res.status(503).json({
      code: "CONTACT_EMAIL_UNAVAILABLE",
      message: `Our messaging service is temporarily unavailable. Please email ${CONTACT_EMAIL} directly.`,
    });
  }
};

module.exports = {
  sendContactEmail,
  validateContactRequest,
};
