const Tenant = require("../model/tenantSchema");
const { signAdminToken, verifyAdminCredentials } = require("../utils/adminAuth");
const { validateSubdomain } = require("../services/subdomainValidation");
const { startProvisioning } = require("../services/provisioning/tenantProvisioner");

const rootDomain = () => process.env.ROOT_DOMAIN || "airlineplan.com";

const sanitizeTenant = (tenant) => {
  const data = tenant.toObject ? tenant.toObject() : tenant;
  return {
    ...data,
    runtimeEnv: {
      ...data.runtimeEnv,
    },
  };
};

const adminLogin = async (req, res) => {
  try {
    const { email, password } = req.body || {};
    const ok = await verifyAdminCredentials({ email, password });
    if (!ok) return res.status(401).json({ error: "Invalid admin credentials" });

    return res.status(200).json({
      message: "Admin login successful",
      token: signAdminToken(email),
    });
  } catch (error) {
    return res.status(error.statusCode || 500).json({ error: error.message || "Admin login failed" });
  }
};

const listTenants = async (_req, res) => {
  const tenants = await Tenant.find({}).sort({ createdAt: -1 }).lean();
  return res.status(200).json({ tenants });
};

const getTenant = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).lean();
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });
  return res.status(200).json({ tenant });
};

const createTenant = async (req, res) => {
  try {
    const { tenantName, subdomain, adminEmail, instanceType = "t3.small", region } = req.body || {};
    const validation = validateSubdomain(subdomain);
    if (!validation.valid) return res.status(400).json({ error: validation.error });
    if (!tenantName || !String(tenantName).trim()) return res.status(400).json({ error: "Tenant name is required" });
    if (!adminEmail || !/.+@.+\..+/.test(String(adminEmail))) {
      return res.status(400).json({ error: "Valid admin email is required" });
    }
    if (!["t3.small", "t3.medium"].includes(instanceType)) {
      return res.status(400).json({ error: "Instance type must be t3.small or t3.medium" });
    }

    const fullDomain = `${validation.subdomain}.${rootDomain()}`;
    const duplicate = await Tenant.findOne({
      $or: [{ subdomain: validation.subdomain }, { fullDomain }],
    }).lean();
    if (duplicate) return res.status(409).json({ error: "Subdomain already exists" });

    const tenant = await Tenant.create({
      tenantName: String(tenantName).trim(),
      subdomain: validation.subdomain,
      fullDomain,
      adminEmail,
      status: "pending",
      aws: {
        region: region || process.env.AWS_REGION || "ap-south-1",
        instanceType,
      },
      runtimeEnv: {
        tenantDomain: fullDomain,
        tenantSubdomain: validation.subdomain,
        viteApiUrl: `https://${fullDomain}`,
      },
      logs: [
        {
          level: "info",
          message: "Tenant record created",
          meta: { requestedBy: req.admin?.email },
        },
      ],
    });

    startProvisioning(tenant._id.toString());
    return res.status(202).json({ tenant: sanitizeTenant(tenant) });
  } catch (error) {
    if (error.code === 11000) return res.status(409).json({ error: "Subdomain already exists" });
    return res.status(500).json({ error: error.message || "Failed to create tenant" });
  }
};

const retryTenant = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id);
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });
  if (!["failed", "pending"].includes(tenant.status)) {
    return res.status(409).json({ error: "Only pending or failed tenants can be retried" });
  }

  tenant.status = "pending";
  tenant.failureReason = "";
  tenant.logs.push({
    level: "info",
    message: "Provisioning retry requested",
    meta: { requestedBy: req.admin?.email },
  });
  await tenant.save();
  startProvisioning(tenant._id.toString());
  return res.status(202).json({ tenant: sanitizeTenant(tenant) });
};

const deactivateTenant = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id);
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });

  tenant.status = "deactivated";
  tenant.logs.push({
    level: "warning",
    message: "Tenant marked deactivated in control plane. Cloud resources are not automatically terminated.",
    meta: { requestedBy: req.admin?.email },
  });
  await tenant.save();
  return res.status(200).json({ tenant: sanitizeTenant(tenant) });
};

module.exports = {
  adminLogin,
  createTenant,
  deactivateTenant,
  getTenant,
  listTenants,
  retryTenant,
};
