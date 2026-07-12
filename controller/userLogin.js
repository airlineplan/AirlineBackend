const User = require("../model/userSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const CostConfig = require("../model/costConfigSchema");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const config = require("../config/config");
const Otp = require("../model/otp");
const { sendOtpEmail, sendContactQueryEmail } = require("../services/emailService");
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

    const isMatch = await bcrypt.compare(password, user.password);
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

    jwt.sign(payload, config.secret, { expiresIn: process.env.JWT_EXPIRES_IN || "7h" }, (err, token) => {
      if (err) throw err;
      res.json({ message: "Login successful", token, user: buildUserPayload(user) });
    });
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};
exports.changePassword = async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const data = await Otp.findOne({
      email,
      code: req.body.otpCode,
    });

    const response = {};
    if (data) {
      let currentTime = new Date().getTime();
      let diff = data.expireIn - currentTime;
      if (diff < 0) {
        response.message = "Token Expire";
        response.statusText = "error";
      } else {
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(req.body.password, salt);

        const user = await User.findOne({ email });
        if (!user) {
          response.message = "User not found";
          response.statusText = "error";
        } else {
          user.password = hashedPassword;
          await user.save();
          response.message = "Password Changed Successfully";
          response.statusText = "Success";
        }
      }
    } else {
      response.message = "Invalid OTP";
      response.statusText = "error";
    }
    res.status(200).json(response);
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};

exports.sendEmail = async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const data = await User.findOne({ email });

    const responseType = {};

    if (data) {
      const otpcode = Math.floor(Math.random() * 9000) + 1000;
      const otpData = new Otp({
        email,
        code: otpcode,
        expireIn: new Date().getTime() + 300 * 1000,
      });

      await otpData.save();

      await sendOtpEmail({ to: email, otp: otpcode });

      responseType.statusText = "Success";
      responseType.message = "Please Check Your Email Id";
    } else {
      responseType.statusText = "error";
      responseType.message = "Email id not Exist";
    }

    res.status(200).json(responseType);
  } catch (error) {
    console.error(error);
    res.status(500).json({ statusText: "error", message: "An error occurred" });
  }
};

exports.sendContactEmail = async (req, res) => {
  try {
    const { name, email, subject, message } = req.body;

    if (!name || !email || !message) {
      return res.status(400).json({ message: "Name, email, and message are required" });
    }

    await sendContactQueryEmail({ name, email: normalizeEmail(email), subject, message });
    res.status(200).json({ message: 'Email sent successfully' });
  } catch (error) {
    console.error('Error sending email:', error);
    res.status(500).json({ message: 'Error sending email' });
  }

};
