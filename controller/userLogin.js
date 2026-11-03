const User = require("../model/userSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const CostConfig = require("../model/costConfigSchema");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const config = require("../config/config");
const Otp = require("../model/otp");
const { sendOtpEmail } = require("../services/emailService");
const { TENANT_ADMIN_ROLES, USER_TOKEN_AUDIENCE } = require("../middlware/auth");
const { getEffectivePageAccess, normalizePageAccessInput } = require("../config/pageAccess");

const MANAGED_ROLES = new Set(["tenant_admin", "user"]);

const normalizeEmail = (email) => String(email || "").trim().toLowerCase();
const normalizeRole = (role) => (TENANT_ADMIN_ROLES.has(role) ? "tenant_admin" : "user");

const buildUserPayload = (user) => ({
  id: user._id?.toString?.() || user.id,
  email: user.email,
  role: normalizeRole(user.role),
  firstName: user.firstName,
  lastName: user.lastName,
  pageAccess: getEffectivePageAccess(user),
  pageAccessConfigured: user.pageAccessConfigured === true,
});

exports.createUser = async (req, res) => {
  const { firstName, lastName, password } = req.body;
  const email = normalizeEmail(req.body?.email);
  const requestedRole = MANAGED_ROLES.has(req.body?.role) ? req.body.role : "user";

  if (!email || !password) {
    return res.status(400).json({ error: "Email and password are required" });
  }

  const isTenantAdmin = TENANT_ADMIN_ROLES.has(req.user?.role);
  const role = requestedRole;

  if (!isTenantAdmin) {
    return res.status(403).json({ error: "Only a tenant admin can create users for this instance" });
  }

  const existingUser = await User.findOne({ email });

  if (existingUser) {
    return res.status(400).json({ error: "User already exists" });
  }
  const salt = await bcrypt.genSalt(10);
  const hashedPassword = await bcrypt.hash(password, salt);
  let pageAccess;
  try {
    pageAccess = normalizePageAccessInput(req.body?.pageAccess);
  } catch (error) {
    return res.status(error.statusCode || 400).json({ error: error.message });
  }

  const user = new User({
    firstName,
    lastName,
    email,
    password: hashedPassword,
    role,
    pageAccess,
    pageAccessConfigured: true,
    createdBy: req.user?.id || null,
  });

  try {
    const savedUser = await user.save();
    const userId = savedUser._id.toString();
    await Promise.all([
      RevenueConfig.findOneAndUpdate(
        { userId },
        {
          $setOnInsert: {
            userId,
            reportingCurrency: "INR",
            currencyCodes: ["INR"],
            fxRates: [],
          },
        },
        { upsert: true, new: true }
      ),
      CostConfig.findOneAndUpdate(
        { userId },
        {
          $setOnInsert: {
            userId,
            reportingCurrency: "INR",
            fxRates: [],
          },
        },
        { upsert: true, new: true }
      ),
    ]);
    res.status(201).json({ user: buildUserPayload(savedUser) });
  } catch (err) {
    if (err.code === 11000) return res.status(400).json({ error: "User already exists" });
    res.status(400).json({ error: err.message || "User could not be created" });
  }
};

exports.loginUser = async (req, res) => {
  try {
    const { password } = req.body;
    const email = normalizeEmail(req.body?.email);

    const user = await User.findOne({ email });
    if (!user) {
      return res.status(400).json({ error: "Invalid credentials" });
    }
    if (user.isActive === false) {
      return res.status(403).json({ error: "This user has been deactivated" });
    }

    const isMatch = await bcrypt.compare(String(password || ""), user.password);
    if (!isMatch) {
      return res.status(400).json({ error: "Invalid credentials" });
    }

    const normalizedRole = normalizeRole(user.role);
    const payload = {
      id: user.id,
      email: user.email,
      role: normalizedRole,
      aud: USER_TOKEN_AUDIENCE,
      tokenType: "tenant_user",
    };

    if (!config.secret) {
      return res.status(500).json({ error: "JWT secret is not configured" });
    }

    user.role = normalizedRole;
    user.lastLoginAt = new Date();
    await user.save();

    const token = jwt.sign(payload, config.secret, { expiresIn: process.env.JWT_EXPIRES_IN || "7h" });
    res.json({ message: "Login successful", token, user: buildUserPayload(user) });
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};
const OTP_TTL_MS = 5 * 60 * 1000;
const OTP_RESEND_COOLDOWN_MS = 60 * 1000;
const OTP_MAX_ATTEMPTS = 5;
const OTP_SENT_MESSAGE = "If an account exists for this email, an OTP has been sent to it";

const hashOtp = (email, code) =>
  crypto.createHash("sha256").update(`${email}:${String(code ?? "").trim()}`).digest("hex");

const otpMatches = (storedHash, email, code) => {
  const expected = Buffer.from(String(storedHash || ""), "hex");
  const supplied = Buffer.from(hashOtp(email, code), "hex");
  return expected.length === supplied.length && crypto.timingSafeEqual(expected, supplied);
};

const otpError = (message) => ({ statusText: "error", message, error: message });

exports.changePassword = async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const otpCode = String(req.body?.otpCode ?? "").trim();
    const password = req.body?.password;

    if (!email || !otpCode || typeof password !== "string" || !password) {
      return res.status(400).json(otpError("Email, OTP and new password are required"));
    }

    // Only the most recently issued code is valid.
    const latest = await Otp.findOne({ email }).sort({ createdAt: -1 }).lean();
    if (!latest) {
      return res.status(200).json(otpError("Invalid OTP"));
    }
    if (latest.expireIn < Date.now()) {
      await Otp.deleteMany({ email });
      return res.status(200).json(otpError("Token Expire"));
    }

    // Spend an attempt before comparing, atomically, so parallel guesses
    // cannot exceed the limit.
    const attempt = await Otp.findOneAndUpdate(
      { _id: latest._id, attempts: { $lt: OTP_MAX_ATTEMPTS } },
      { $inc: { attempts: 1 } },
      { new: true }
    ).lean();
    if (!attempt) {
      await Otp.deleteMany({ email });
      return res.status(200).json(otpError("Too many incorrect attempts. Please request a new OTP"));
    }

    if (!otpMatches(attempt.code, email, otpCode)) {
      return res.status(200).json(otpError("Invalid OTP"));
    }

    // Consume the code before changing the password so it cannot be replayed.
    const consumed = await Otp.deleteOne({ _id: attempt._id });
    if (consumed.deletedCount !== 1) {
      return res.status(200).json(otpError("Invalid OTP"));
    }
    await Otp.deleteMany({ email });

    const user = await User.findOne({ email });
    if (!user) {
      return res.status(200).json(otpError("Invalid OTP"));
    }

    const salt = await bcrypt.genSalt(10);
    user.password = await bcrypt.hash(password, salt);
    await user.save();
    return res.status(200).json({ statusText: "Success", message: "Password Changed Successfully" });
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};

exports.sendEmail = async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    if (!email) {
      return res.status(400).json({ statusText: "error", message: "Email is required" });
    }

    // The same response whether or not the account exists, so this endpoint
    // cannot be used to discover registered emails.
    const responseType = { statusText: "Success", message: OTP_SENT_MESSAGE };

    const user = await User.findOne({ email }).select("_id").lean();
    if (!user) {
      return res.status(200).json(responseType);
    }

    const recent = await Otp.findOne({
      email,
      createdAt: { $gt: new Date(Date.now() - OTP_RESEND_COOLDOWN_MS) },
    }).lean();
    if (recent) {
      return res.status(200).json(responseType);
    }

    const otpcode = String(crypto.randomInt(0, 1000000)).padStart(6, "0");
    await Otp.deleteMany({ email });
    await Otp.create({
      email,
      code: hashOtp(email, otpcode),
      expireIn: Date.now() + OTP_TTL_MS,
    });

    await sendOtpEmail({ to: email, otp: otpcode });

    res.status(200).json(responseType);
  } catch (error) {
    console.error(error);
    res.status(500).json({ statusText: "error", message: "An error occurred" });
  }
};
