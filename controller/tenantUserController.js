const bcrypt = require("bcryptjs");
const User = require("../model/userSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const CostConfig = require("../model/costConfigSchema");

const MANAGED_ROLES = new Set(["tenant_admin", "user"]);

const normalizeEmail = (email) => String(email || "").trim().toLowerCase();
const getBootstrapToken = (req) => {
  const header = req.headers.authorization || "";
  return header.startsWith("Bearer ") ? header.slice(7) : req.headers["x-tenant-bootstrap-token"];
};

const verifyBootstrapToken = (req) => {
  const expected = process.env.TENANT_BOOTSTRAP_SECRET || "";
  if (!expected) {
    const error = new Error("Tenant bootstrap auth is not configured");
    error.statusCode = 500;
    throw error;
  }

  if (getBootstrapToken(req) !== expected) {
    const error = new Error("Invalid tenant bootstrap token");
    error.statusCode = 401;
    throw error;
  }
};

const serializeUser = (user) => ({
  id: user._id?.toString?.() || user.id,
  firstName: user.firstName,
  lastName: user.lastName,
  email: user.email,
  role: user.role,
  isActive: user.isActive,
  createdAt: user.createdAt,
  updatedAt: user.updatedAt,
  lastLoginAt: user.lastLoginAt,
  createdBy: user.createdBy,
});

const ensureAnotherActiveTenantAdmin = async (userId) => {
  const admins = await User.countDocuments({
    _id: { $ne: userId },
    role: "tenant_admin",
    isActive: { $ne: false },
  });
  return admins > 0;
};

const seedUserDefaults = async (userId) => Promise.all([
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

const createUserRecord = async ({ firstName, lastName, email, password, role, createdBy = null }) => {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || !password) {
    const error = new Error("Email and password are required");
    error.statusCode = 400;
    throw error;
  }
  if (!MANAGED_ROLES.has(role)) {
    const error = new Error("Invalid user role");
    error.statusCode = 400;
    throw error;
  }

  const existingUser = await User.findOne({ email: normalizedEmail }).lean();
  if (existingUser) {
    const error = new Error("User already exists");
    error.statusCode = 409;
    throw error;
  }

  const salt = await bcrypt.genSalt(10);
  const user = await User.create({
    firstName,
    lastName,
    email: normalizedEmail,
    password: await bcrypt.hash(password, salt),
    role,
    createdBy,
  });

  await seedUserDefaults(user._id.toString());
  return user;
};

const bootstrapTenantAdmin = async (req, res) => {
  try {
    verifyBootstrapToken(req);

    const existingTenantAdmin = await User.findOne({ role: "tenant_admin" }).lean();
    if (existingTenantAdmin) {
      return res.status(409).json({ error: "Tenant admin has already been created for this instance" });
    }

    const user = await createUserRecord({
      firstName: req.body?.firstName,
      lastName: req.body?.lastName,
      email: req.body?.email,
      password: req.body?.password,
      role: "tenant_admin",
    });

    return res.status(201).json({ user: serializeUser(user) });
  } catch (error) {
    return res.status(error.statusCode || 500).json({ error: error.message || "Tenant admin could not be created" });
  }
};

const listTenantUsers = async (_req, res) => {
  const users = await User.find({})
    .select("-password")
    .sort({ role: 1, createdAt: -1 })
    .lean();

  return res.status(200).json({ users: users.map(serializeUser) });
};

const createTenantUser = async (req, res) => {
  try {
    const { firstName, lastName, email, password } = req.body || {};
    const role = MANAGED_ROLES.has(req.body?.role) ? req.body.role : "user";
    const user = await createUserRecord({
      firstName,
      lastName,
      email,
      password,
      role,
      createdBy: req.user.id,
    });

    return res.status(201).json({ user: serializeUser(user) });
  } catch (error) {
    if (error.code === 11000) return res.status(409).json({ error: "User already exists" });
    return res.status(error.statusCode || 500).json({ error: error.message || "User could not be created" });
  }
};

const updateTenantUserRole = async (req, res) => {
  const { id } = req.params;
  const role = req.body?.role;
  if (!MANAGED_ROLES.has(role)) return res.status(400).json({ error: "Invalid user role" });

  const user = await User.findById(id);
  if (!user) return res.status(404).json({ error: "User not found" });

  if (user.role === "tenant_admin" && role !== "tenant_admin") {
    const hasAnotherAdmin = await ensureAnotherActiveTenantAdmin(user._id);
    if (!hasAnotherAdmin) {
      return res.status(409).json({ error: "At least one active tenant admin is required" });
    }
  }

  user.role = role;
  await user.save();
  return res.status(200).json({ user: serializeUser(user) });
};

const setTenantUserActive = async (req, res) => {
  const { id } = req.params;
  const isActive = req.body?.isActive !== false;
  const user = await User.findById(id);
  if (!user) return res.status(404).json({ error: "User not found" });

  if (user.role === "tenant_admin" && isActive === false) {
    const hasAnotherAdmin = await ensureAnotherActiveTenantAdmin(user._id);
    if (!hasAnotherAdmin) {
      return res.status(409).json({ error: "At least one active tenant admin is required" });
    }
  }

  user.isActive = isActive;
  await user.save();
  return res.status(200).json({ user: serializeUser(user) });
};

const deleteTenantUser = async (req, res) => {
  const { id } = req.params;
  if (id === req.user.id) return res.status(409).json({ error: "Tenant admins cannot delete their own account" });

  const user = await User.findById(id);
  if (!user) return res.status(404).json({ error: "User not found" });

  if (user.role === "tenant_admin") {
    const hasAnotherAdmin = await ensureAnotherActiveTenantAdmin(user._id);
    if (!hasAnotherAdmin) {
      return res.status(409).json({ error: "At least one active tenant admin is required" });
    }
  }

  await User.deleteOne({ _id: user._id });
  return res.status(200).json({ message: "User deleted" });
};

module.exports = {
  bootstrapTenantAdmin,
  createTenantUser,
  deleteTenantUser,
  getBootstrapToken,
  listTenantUsers,
  setTenantUserActive,
  updateTenantUserRole,
  verifyBootstrapToken,
};
