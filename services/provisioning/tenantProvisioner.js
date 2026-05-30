const crypto = require("crypto");
const dns = require("dns").promises;
const https = require("https");

const Tenant = require("../../model/tenantSchema");
const { createAtlasCluster, createGoDaddyRecord, launchTenantInstance, sleep } = require("./cloudClients");

const maskMongoUri = (uri = "") => uri.replace(/:\/\/([^:]+):([^@]+)@/, "://$1:***@");

const appendLog = async (tenantId, level, message, meta) => {
  await Tenant.findByIdAndUpdate(tenantId, {
    $push: {
      logs: {
        level,
        message,
        meta,
      },
    },
  });
};

const setStatus = async (tenantId, status, extra = {}) => {
  await Tenant.findByIdAndUpdate(tenantId, {
    $set: {
      status,
      ...extra,
    },
  });
};

const generateSecret = () => crypto.randomBytes(32).toString("base64url");

const checkHttps = (domain) =>
  new Promise((resolve) => {
    const req = https.get(
      {
        hostname: domain,
        path: "/health",
        timeout: Number(process.env.TENANT_HEALTH_TIMEOUT_MS || 10000),
        rejectUnauthorized: true,
      },
      (res) => {
        res.resume();
        resolve(res.statusCode >= 200 && res.statusCode < 500);
      }
    );
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });
    req.on("error", () => resolve(false));
  });

const bootstrapTenantAdmin = ({ domain, bootstrapSecret, initialAdmin }) =>
  new Promise((resolve, reject) => {
    const body = JSON.stringify({
      firstName: initialAdmin.firstName,
      lastName: initialAdmin.lastName,
      email: initialAdmin.email,
      password: initialAdmin.password,
    });

    const req = https.request(
      {
        hostname: domain,
        path: "/tenant/bootstrap-admin",
        method: "POST",
        timeout: Number(process.env.TENANT_BOOTSTRAP_TIMEOUT_MS || 15000),
        rejectUnauthorized: true,
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          "x-tenant-bootstrap-token": bootstrapSecret,
        },
      },
      (res) => {
        let responseBody = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          responseBody += chunk;
        });
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve();
            return;
          }

          const error = new Error(`Tenant admin bootstrap failed with status ${res.statusCode}`);
          error.response = responseBody;
          error.statusCode = res.statusCode;
          reject(error);
        });
      }
    );

    req.on("timeout", () => {
      req.destroy(new Error("Tenant admin bootstrap timed out"));
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });

const waitForDns = async ({ domain, expectedIp, tenantId }) => {
  const attempts = Number(process.env.DNS_POLL_ATTEMPTS || 60);
  const interval = Number(process.env.DNS_POLL_INTERVAL_MS || 30000);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const addresses = await dns.resolve4(domain);
      await appendLog(tenantId, "info", "Checking DNS propagation", { attempt, addresses, expectedIp });
      if (addresses.includes(expectedIp)) return true;
    } catch (error) {
      await appendLog(tenantId, "warning", "DNS is not ready yet", { attempt, error: error.message });
    }
    await sleep(interval);
  }

  return false;
};

const waitForHttps = async ({ domain, tenantId }) => {
  const attempts = Number(process.env.HTTPS_POLL_ATTEMPTS || 40);
  const interval = Number(process.env.HTTPS_POLL_INTERVAL_MS || 30000);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const ok = await checkHttps(domain);
    await appendLog(tenantId, ok ? "info" : "warning", "Checking HTTPS health", { attempt, ok });
    if (ok) return true;
    await sleep(interval);
  }

  return false;
};

const provisionTenant = async (tenantId, options = {}) => {
  const tenant = await Tenant.findById(tenantId);
  if (!tenant) return;

  const log = (level, message, meta) => appendLog(tenantId, level, message, meta);

  try {
    await setStatus(tenantId, "provisioning", {
      failureReason: "",
      lastProvisioningStartedAt: new Date(),
      lastProvisioningFinishedAt: null,
    });
    await log("info", "Tenant provisioning started");

    const databaseName = `airlineplan_${tenant.subdomain.replace(/-/g, "_")}`;
    const atlasUsername = `tenant_${tenant.subdomain.replace(/-/g, "_")}`.slice(0, 64);
    const atlasPassword = generateSecret();
    const tenantJwtSecret = generateSecret();
    const tenantBootstrapSecret = generateSecret();
    const clusterName = `airlineplan-${tenant.subdomain}`;

    const atlas = await createAtlasCluster({
      clusterName,
      databaseName,
      username: atlasUsername,
      password: atlasPassword,
      log,
    });

    await Tenant.findByIdAndUpdate(tenantId, {
      $set: {
        atlas: {
          projectId: atlas.projectId,
          clusterName: atlas.clusterName,
          clusterId: atlas.clusterId,
          databaseName: atlas.databaseName,
          username: atlas.username,
        },
        runtimeEnv: {
          tenantDomain: tenant.fullDomain,
          tenantSubdomain: tenant.subdomain,
          viteApiUrl: `https://${tenant.fullDomain}`,
          scheduleUploadLimit: process.env.SCHEDULE_UPLOAD_LIMIT || "",
          flightLimit: process.env.FLIGHT_LIMIT || "",
        },
      },
    });
    await log("info", "Atlas cluster is ready", { mongoUri: maskMongoUri(atlas.mongoUri) });

    const aws = await launchTenantInstance({
      tenant,
      mongoUri: atlas.mongoUri,
      tenantJwtSecret,
      tenantBootstrapSecret,
      log,
    });
    await Tenant.findByIdAndUpdate(tenantId, {
      $set: {
        "aws.instanceId": aws.instanceId,
        "aws.publicIp": aws.publicIp,
        "aws.region": aws.region,
        "dns.recordValue": aws.publicIp,
      },
    });

    await setStatus(tenantId, "dns_pending");
    await log("info", "Creating GoDaddy DNS record", { subdomain: tenant.subdomain, ipAddress: aws.publicIp });
    await createGoDaddyRecord({ subdomain: tenant.subdomain, ipAddress: aws.publicIp });

    const dnsReady = await waitForDns({ domain: tenant.fullDomain, expectedIp: aws.publicIp, tenantId });
    if (!dnsReady) throw new Error("DNS did not resolve to the tenant EC2 public IP in time");

    await Tenant.findByIdAndUpdate(tenantId, {
      $set: {
        "dns.lastCheckedAt": new Date(),
      },
    });
    await setStatus(tenantId, "ssl_pending");

    const httpsReady = await waitForHttps({ domain: tenant.fullDomain, tenantId });
    if (!httpsReady) throw new Error("HTTPS health check did not pass in time");

    if (options.initialAdmin?.email && options.initialAdmin?.password) {
      await log("info", "Creating initial tenant admin", { email: options.initialAdmin.email });
      await bootstrapTenantAdmin({
        domain: tenant.fullDomain,
        bootstrapSecret: tenantBootstrapSecret,
        initialAdmin: options.initialAdmin,
      });
    }

    await setStatus(tenantId, "active", {
      lastProvisioningFinishedAt: new Date(),
    });
    await log("info", "Tenant is active", { domain: `https://${tenant.fullDomain}` });
  } catch (error) {
    await setStatus(tenantId, "failed", {
      failureReason: error.message,
      lastProvisioningFinishedAt: new Date(),
    });
    await log("error", "Tenant provisioning failed", {
      message: error.message,
      code: error.code,
      statusCode: error.statusCode,
      response: error.response,
    });
  }
};

const startProvisioning = (tenantId, options = {}) => {
  setImmediate(() => {
    provisionTenant(tenantId, options).catch((error) => {
      appendLog(tenantId, "error", "Unhandled provisioning error", { message: error.message }).catch(() => {});
    });
  });
};

module.exports = {
  appendLog,
  provisionTenant,
  setStatus,
  startProvisioning,
};
