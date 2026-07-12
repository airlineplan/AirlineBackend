const crypto = require("crypto");
const Tenant = require("../model/tenantSchema");
const ControlPlaneCounter = require("../model/controlPlaneCounterSchema");
const { FEATURE_CATALOG, normalizeFeatureMap } = require("../config/featureCatalog");
const { signAdminToken, verifyAdminCredentials } = require("../utils/adminAuth");
const { validateSubdomain } = require("../services/subdomainValidation");
const {
  createBootstrapSecret,
  deleteBootstrapSecret,
  maskSensitive,
  startPlatformExecution,
  updateTenantService,
} = require("../services/provisioning/platformClients");

const rootDomain = () => process.env.ROOT_DOMAIN || "airlineplan.com";
const DEFAULT_APP_VERSION = process.env.DEFAULT_APP_VERSION || "standard-2026.06.19";

const createTenantId = (slug) =>
  `tnt_${slug.replace(/-/g, "_")}_${crypto.randomBytes(3).toString("hex")}`;

const sanitizeTenant = (tenant) => {
  const data = tenant?.toObject ? tenant.toObject() : { ...tenant };
  if (data.provisioning) {
    delete data.provisioning.bootstrapSecretArn;
  }
  return maskSensitive(data);
};

const appendAudit = (tenant, type, message, actor, meta) => {
  tenant.auditEvents.push({
    type,
    message,
    actor,
    meta: maskSensitive(meta),
  });
};

const canRetryTenant = (status) => status === "FAILED";

const allocateAlbRulePriority = async () => {
  const minimum = Number(process.env.ALB_RULE_PRIORITY_START || 100);
  const counter = await ControlPlaneCounter.findOneAndUpdate(
    { key: "alb-rule-priority" },
    [
      {
        $set: {
          value: {
            $add: [{ $ifNull: ["$value", minimum - 1] }, 1],
          },
        },
      },
    ],
    { upsert: true, new: true }
  );
  return counter.value;
};

const startTenantWorkflow = async (tenant, options = {}) => {
  try {
    return await startPlatformExecution({ tenant, ...options });
  } catch (error) {
    if (options.markFailed !== false) {
      tenant.status = "FAILED";
      tenant.currentStep = "START_WORKFLOW";
      tenant.failure = {
        step: "START_WORKFLOW",
        message: error.message,
        code: error.code,
        occurredAt: new Date(),
      };
      appendAudit(
        tenant,
        "WORKFLOW_START_FAILED",
        "Platform workflow could not be started",
        "control-plane",
        { operation: options.operation, code: error.code }
      );
      await tenant.save();
    }
    throw error;
  }
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
    return res.status(error.statusCode || 500).json({
      error: error.message || "Admin login failed",
    });
  }
};

const listFeatures = (_req, res) =>
  res.status(200).json({
    features: FEATURE_CATALOG,
    defaultAppVersion: DEFAULT_APP_VERSION,
    defaultPlan: "enterprise-dedicated",
  });

const listTenants = async (_req, res) => {
  const tenants = await Tenant.find({}).sort({ createdAt: -1 }).lean();
  return res.status(200).json({ tenants: tenants.map(sanitizeTenant) });
};

const getTenant = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).lean();
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });
  return res.status(200).json({ tenant: sanitizeTenant(tenant) });
};

const createTenant = async (req, res) => {
  let bootstrapSecretArn;
  let tenant;

  try {
    const {
      companyName,
      tenantName,
      slug,
      subdomain,
      adminEmail,
      adminFirstName,
      adminLastName,
      adminPassword,
      plan = "enterprise-dedicated",
      appVersion = DEFAULT_APP_VERSION,
      imageTag,
      features,
      branding = {},
    } = req.body || {};

    const requestedSlug = slug || subdomain;
    const requestedCompany = companyName || tenantName;
    const validation = validateSubdomain(requestedSlug);
    if (!validation.valid) return res.status(400).json({ error: validation.error });
    if (!requestedCompany || !String(requestedCompany).trim()) {
      return res.status(400).json({ error: "Company name is required" });
    }
    if (!adminEmail || !/.+@.+\..+/.test(String(adminEmail))) {
      return res.status(400).json({ error: "Valid admin email is required" });
    }
    if (!adminPassword || String(adminPassword).length < 12) {
      return res.status(400).json({
        error: "Tenant admin temporary password must be at least 12 characters",
      });
    }

    const domain = `${validation.subdomain}.${rootDomain()}`;
    const duplicate = await Tenant.findOne({
      $or: [{ slug: validation.subdomain }, { domain }],
    }).lean();
    if (duplicate) return res.status(409).json({ error: "Tenant slug already exists" });

    const tenantId = createTenantId(validation.subdomain);
    const albRulePriority = await allocateAlbRulePriority();
    const desiredImageTag =
      imageTag ||
      `${process.env.TENANT_IMAGE_REPOSITORY || "airlineplan-tenant"}:${appVersion}`;

    bootstrapSecretArn = await createBootstrapSecret({
      tenantId,
      admin: {
        firstName: String(adminFirstName || "").trim(),
        lastName: String(adminLastName || "").trim(),
        email: String(adminEmail).trim().toLowerCase(),
        password: String(adminPassword),
      },
    });

    tenant = await Tenant.create({
      tenantId,
      slug: validation.subdomain,
      companyName: String(requestedCompany).trim(),
      domain,
      adminEmail,
      plan,
      branding: {
        companyName: String(branding.companyName || requestedCompany).trim(),
        logoUrl: String(branding.logoUrl || "").trim(),
        primaryColor: String(branding.primaryColor || "#0B3B75").trim(),
      },
      features: normalizeFeatureMap(features, { defaultEnabled: true }),
      status: "PENDING",
      currentStep: "PENDING",
      albRulePriority,
      deployment: {
        desiredAppVersion: appVersion,
        desiredImageTag,
      },
      resources: {
        awsRegion: process.env.AWS_REGION || "ap-south-1",
        terraformStateKey: `tenants/${validation.subdomain}/terraform.tfstate`,
      },
      provisioning: {
        bootstrapSecretArn,
        lastStartedAt: new Date(),
      },
      auditEvents: [
        {
          type: "TENANT_CREATED",
          message: "Tenant provisioning requested",
          actor: req.admin?.email,
          meta: { appVersion, plan },
        },
      ],
    });

    const tenantWithSecret = await Tenant.findById(tenant._id).select(
      "+provisioning.bootstrapSecretArn"
    );
    const executionArn = await startPlatformExecution({ tenant: tenantWithSecret });
    tenant.executionArn = executionArn;
    tenant.status = "PROVISIONING";
    tenant.currentStep = "TERRAFORM_APPLY";
    appendAudit(
      tenant,
      "PROVISIONING_STARTED",
      "Provisioning workflow started",
      req.admin?.email,
      { executionArn }
    );
    await tenant.save();

    return res.status(202).json({ tenant: sanitizeTenant(tenant) });
  } catch (error) {
    if (tenant) {
      tenant.status = "FAILED";
      tenant.currentStep = "START_WORKFLOW";
      tenant.failure = {
        step: "START_WORKFLOW",
        message: error.message,
        code: error.code,
        occurredAt: new Date(),
      };
      appendAudit(
        tenant,
        "PROVISIONING_FAILED",
        "Provisioning workflow could not be started",
        req.admin?.email,
        { code: error.code }
      );
      await tenant.save().catch(() => {});
    } else if (bootstrapSecretArn) {
      await deleteBootstrapSecret(bootstrapSecretArn).catch(() => {});
    }

    if (error.code === 11000) {
      return res.status(409).json({ error: "Tenant slug or ALB priority already exists" });
    }
    return res.status(500).json({ error: error.message || "Failed to create tenant" });
  }
};

const updateTenantConfig = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).select(
    "+provisioning.bootstrapSecretArn"
  );
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });
  if (["DELETING", "DELETED"].includes(tenant.status)) {
    return res.status(409).json({ error: "Deleted tenants cannot be updated" });
  }

  if (req.body?.features) {
    tenant.features = normalizeFeatureMap(
      { ...tenant.features, ...req.body.features },
      {
      defaultEnabled: false,
      }
    );
  }
  if (req.body?.branding) {
    tenant.branding = {
      companyName:
        req.body.branding.companyName ||
        tenant.branding?.companyName ||
        tenant.companyName,
      logoUrl: req.body.branding.logoUrl ?? tenant.branding?.logoUrl,
      primaryColor:
        req.body.branding.primaryColor ||
        tenant.branding?.primaryColor ||
        "#0B3B75",
    };
  }

  tenant.attempt += 1;
  tenant.status = "PROVISIONING";
  tenant.currentStep = "APPLYING_CONFIG";
  appendAudit(
    tenant,
    "CONFIG_UPDATED",
    "Tenant configuration update requested",
    req.admin?.email
  );
  await tenant.save();
  tenant.executionArn = await startTenantWorkflow(tenant, {
    operation: "CONFIGURE",
  });
  await tenant.save();
  return res.status(202).json({ tenant: sanitizeTenant(tenant) });
};

const createDeployment = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).select(
    "+provisioning.bootstrapSecretArn"
  );
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });

  const appVersion = String(req.body?.appVersion || "").trim();
  const imageTag = String(req.body?.imageTag || "").trim();
  if (!appVersion || !imageTag) {
    return res.status(400).json({ error: "appVersion and imageTag are required" });
  }

  tenant.deployment.previousImageTag = tenant.deployment.deployedImageTag;
  tenant.deployment.desiredAppVersion = appVersion;
  tenant.deployment.desiredImageTag = imageTag;
  tenant.attempt += 1;
  tenant.status = "PROVISIONING";
  tenant.currentStep = "DEPLOYING_VERSION";
  appendAudit(
    tenant,
    "DEPLOYMENT_REQUESTED",
    "Tenant deployment requested",
    req.admin?.email,
    { appVersion, imageTag }
  );
  await tenant.save();
  tenant.executionArn = await startTenantWorkflow(tenant, {
    operation: "DEPLOY",
  });
  await tenant.save();
  return res.status(202).json({ tenant: sanitizeTenant(tenant) });
};

const performTenantAction = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).select(
    "+provisioning.bootstrapSecretArn"
  );
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });

  const action = String(req.params.action || "").toLowerCase();
  const allowed = new Set(["retry", "rollback", "suspend", "resume", "restart", "export"]);
  if (!allowed.has(action)) {
    return res.status(404).json({ error: "Unknown tenant action" });
  }

  if (action === "retry" && !canRetryTenant(tenant.status)) {
    return res.status(409).json({ error: "Only failed tenants can be retried" });
  }
  if (action === "rollback" && !tenant.deployment?.previousImageTag) {
    return res.status(409).json({ error: "No previous image is available" });
  }

  if (action === "suspend") {
    await updateTenantService({ tenant, desiredCount: 0 });
    tenant.status = "SUSPENDED";
    tenant.currentStep = "SUSPENDED";
  } else if (action === "restart") {
    await updateTenantService({
      tenant,
      desiredCount: tenant.status === "SUSPENDED" ? 0 : 1,
      forceNewDeployment: true,
    });
  } else {
    tenant.attempt += 1;
    const operation = action.toUpperCase();
    if (action === "resume") {
      tenant.status = "HEALTH_CHECKING";
      tenant.currentStep = "RESUMING";
    } else if (action === "rollback") {
      tenant.status = "ROLLING_BACK";
      tenant.currentStep = "ROLLING_BACK";
      tenant.deployment.desiredImageTag = tenant.deployment.previousImageTag;
    } else if (action === "retry") {
      tenant.status = "PROVISIONING";
      tenant.currentStep = "RETRYING";
      tenant.failure = undefined;
    }
    tenant.executionArn = await startTenantWorkflow(tenant, {
      operation,
      markFailed: action !== "export",
      extra:
        action === "rollback"
          ? { targetImageTag: tenant.deployment.previousImageTag }
          : {},
    });
  }

  appendAudit(
    tenant,
    `TENANT_${action.toUpperCase()}`,
    `Tenant ${action} requested`,
    req.admin?.email
  );
  await tenant.save();
  return res.status(202).json({ tenant: sanitizeTenant(tenant) });
};

const deleteTenant = async (req, res) => {
  const tenant = await Tenant.findById(req.params.id).select(
    "+provisioning.bootstrapSecretArn"
  );
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });
  if (req.body?.confirmSlug !== tenant.slug) {
    return res.status(400).json({ error: "confirmSlug must match the tenant slug" });
  }
  if (typeof req.body?.retainBackups !== "boolean") {
    return res.status(400).json({ error: "retainBackups must be true or false" });
  }

  tenant.attempt += 1;
  tenant.status = "DELETING";
  tenant.currentStep = "DESTROYING_INFRASTRUCTURE";
  tenant.executionArn = await startTenantWorkflow(tenant, {
    operation: "DELETE",
    extra: { retainBackups: req.body.retainBackups },
  });
  appendAudit(
    tenant,
    "TENANT_DELETE",
    "Tenant deletion requested",
    req.admin?.email,
    { retainBackups: req.body.retainBackups }
  );
  await tenant.save();
  return res.status(202).json({ tenant: sanitizeTenant(tenant) });
};

const provisioningEvent = async (req, res) => {
  const tenant = await Tenant.findOne({ tenantId: req.body?.tenantId }).select(
    "+provisioning.bootstrapSecretArn"
  );
  if (!tenant) return res.status(404).json({ error: "Tenant not found" });

  const { status, step, executionArn, resources, failure, deployment, eventType } =
    req.body;
  if (status) tenant.status = status;
  if (step) tenant.currentStep = step;
  if (executionArn) tenant.executionArn = executionArn;
  if (resources) tenant.resources = { ...tenant.resources?.toObject?.(), ...resources };
  if (failure) {
    tenant.failure = {
      step: failure.step || step,
      message: failure.message || failure.Cause || failure.Error,
      code: failure.code || failure.Error,
      occurredAt: new Date(),
    };
  }
  if (deployment) {
    if (
      deployment.deployedImageTag &&
      deployment.deployedImageTag !== tenant.deployment?.deployedImageTag
    ) {
      tenant.deployment.history.push({
        appVersion: deployment.deployedAppVersion,
        imageTag: deployment.deployedImageTag,
        imageDigest: deployment.deployedImageDigest,
        deployedAt: new Date(),
      });
    }
    tenant.deployment = { ...tenant.deployment?.toObject?.(), ...deployment };
  }
  if (status === "ACTIVE" || status === "FAILED" || status === "DELETED") {
    tenant.provisioning.lastFinishedAt = new Date();
  }
  if (req.body?.clearBootstrapSecret === true) {
    tenant.provisioning.bootstrapSecretArn = undefined;
  }
  appendAudit(
    tenant,
    eventType || "WORKFLOW_EVENT",
    req.body?.message || `Workflow moved to ${status || step}`,
    "platform-workflow",
    { status, step }
  );
  await tenant.save();
  return res.status(200).json({ tenantId: tenant.tenantId, status: tenant.status });
};

module.exports = {
  adminLogin,
  createDeployment,
  createTenant,
  deleteTenant,
  getTenant,
  listFeatures,
  listTenants,
  performTenantAction,
  provisioningEvent,
  sanitizeTenant,
  updateTenantConfig,
  canRetryTenant,
};
