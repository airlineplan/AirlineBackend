const {
  CreateSecretCommand,
  DeleteSecretCommand,
  SecretsManagerClient,
} = require("@aws-sdk/client-secrets-manager");
const {
  SFNClient,
  StartExecutionCommand,
} = require("@aws-sdk/client-sfn");
const {
  ECSClient,
  UpdateServiceCommand,
} = require("@aws-sdk/client-ecs");

const region = () => process.env.AWS_REGION || "ap-south-1";

let secretsClient = new SecretsManagerClient({ region: region() });
let sfnClient = new SFNClient({ region: region() });
let ecsClient = new ECSClient({ region: region() });

const requireEnv = (name) => {
  const value = process.env[name];
  if (!value) {
    const error = new Error(`${name} is required`);
    error.code = "CONFIG_MISSING";
    throw error;
  }
  return value;
};

const maskSensitive = (value) => {
  if (Array.isArray(value)) return value.map(maskSensitive);
  if (!value || typeof value !== "object") return value;

  return Object.fromEntries(
    Object.entries(value).map(([key, child]) => {
      const sensitive = /password|secret|token|mongo.*uri|redis.*url|credential/i.test(key);
      return [key, sensitive ? "***" : maskSensitive(child)];
    })
  );
};

const createBootstrapSecret = async ({ tenantId, admin }) => {
  const expiresAt = new Date(
    Date.now() + Number(process.env.BOOTSTRAP_SECRET_TTL_HOURS || 24) * 60 * 60 * 1000
  );
  const name = `/airlineplan/bootstrap/${tenantId}/${Date.now()}`;
  const response = await secretsClient.send(
    new CreateSecretCommand({
      Name: name,
      Description: `Short-lived tenant administrator bootstrap secret for ${tenantId}`,
      KmsKeyId: process.env.BOOTSTRAP_SECRET_KMS_KEY_ARN || undefined,
      SecretString: JSON.stringify(admin),
      Tags: [
        { Key: "Application", Value: "airlineplan" },
        { Key: "TenantId", Value: tenantId },
        { Key: "Purpose", Value: "tenant-admin-bootstrap" },
        { Key: "ExpiresAt", Value: expiresAt.toISOString() },
      ],
    })
  );
  return response.ARN;
};

const deleteBootstrapSecret = async (secretArn) => {
  if (!secretArn) return;
  await secretsClient.send(
    new DeleteSecretCommand({
      SecretId: secretArn,
      ForceDeleteWithoutRecovery: true,
    })
  );
};

const executionName = ({ tenantId, operation, attempt }) =>
  `${tenantId}-${String(operation).toLowerCase()}-${attempt}`
    .replace(/[^A-Za-z0-9-_]/g, "-")
    .slice(0, 80);

const startPlatformExecution = async ({
  tenant,
  operation = "PROVISION",
  stateMachineArn,
  extra = {},
}) => {
  const machineArn =
    stateMachineArn ||
    (operation === "EXPORT"
      ? process.env.TENANT_EXPORT_STATE_MACHINE_ARN
      : process.env.TENANT_PROVISIONING_STATE_MACHINE_ARN);

  if (!machineArn) {
    throw new Error(
      operation === "EXPORT"
        ? "TENANT_EXPORT_STATE_MACHINE_ARN is required"
        : "TENANT_PROVISIONING_STATE_MACHINE_ARN is required"
    );
  }

  const input = {
    operation,
    tenantId: tenant.tenantId,
    slug: tenant.slug,
    companyName: tenant.companyName,
    domain: tenant.domain,
    adminEmail: tenant.adminEmail,
    plan: tenant.plan,
    features: tenant.features,
    branding: tenant.branding,
    appVersion: tenant.deployment?.desiredAppVersion,
    imageTag: tenant.deployment?.desiredImageTag,
    albRulePriority: tenant.albRulePriority,
    terraformStateKey: tenant.resources?.terraformStateKey,
    bootstrapSecretArn: tenant.provisioning?.bootstrapSecretArn,
    resources: tenant.resources,
    attempt: tenant.attempt,
    ...extra,
  };

  const response = await sfnClient.send(
    new StartExecutionCommand({
      stateMachineArn: machineArn,
      name: executionName({
        tenantId: tenant.tenantId,
        operation,
        attempt: tenant.attempt,
      }),
      input: JSON.stringify(input),
    })
  );
  return response.executionArn;
};

const updateTenantService = async ({
  tenant,
  desiredCount,
  forceNewDeployment = false,
}) => {
  const cluster =
    tenant.resources?.ecsClusterArn || process.env.TENANT_ECS_CLUSTER_ARN;
  const service =
    tenant.resources?.ecsServiceArn || tenant.resources?.ecsServiceName;
  if (!cluster || !service) {
    const error = new Error("Tenant ECS service has not been provisioned");
    error.code = "RESOURCE_MISSING";
    throw error;
  }

  return ecsClient.send(
    new UpdateServiceCommand({
      cluster,
      service,
      desiredCount,
      forceNewDeployment,
    })
  );
};

const setPlatformClientsForTests = (clients = {}) => {
  if (clients.secretsClient) secretsClient = clients.secretsClient;
  if (clients.sfnClient) sfnClient = clients.sfnClient;
  if (clients.ecsClient) ecsClient = clients.ecsClient;
};

module.exports = {
  createBootstrapSecret,
  deleteBootstrapSecret,
  maskSensitive,
  requireEnv,
  setPlatformClientsForTests,
  startPlatformExecution,
  updateTenantService,
};
