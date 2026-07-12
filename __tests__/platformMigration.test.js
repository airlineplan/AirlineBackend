const assert = require("node:assert/strict");
const test = require("node:test");

const { canRetryTenant } = require("../controller/adminController");
const {
  createDefaultFeatures,
  normalizeFeatureMap,
} = require("../config/featureCatalog");
const { migrations } = require("../services/migrations/runMigrations");
const {
  maskSensitive,
  setPlatformClientsForTests,
  startPlatformExecution,
} = require("../services/provisioning/platformClients");
const {
  requireFeatureAccess,
} = require("../middlware/tenantFeatureAccess");
const {
  enforceTenantHost,
  normalizeHostname,
} = require("../middlware/tenantHost");

const createResponse = () => ({
  statusCode: 200,
  body: null,
  status(code) {
    this.statusCode = code;
    return this;
  },
  json(body) {
    this.body = body;
    return this;
  },
});

test("feature maps are canonical and default-safe", () => {
  const defaults = createDefaultFeatures(true);
  const normalized = normalizeFeatureMap({
    network: false,
    view: true,
    unknown: true,
  });

  assert.equal(defaults.users, true);
  assert.equal(normalized.network, true);
  assert.equal(Object.hasOwn(normalized, "unknown"), false);
  assert.equal(Object.keys(normalized).length, Object.keys(defaults).length);
});

test("tenant entitlement is enforced before user page access", () => {
  const previous = process.env.TENANT_FEATURES_JSON;
  process.env.TENANT_FEATURES_JSON = JSON.stringify({
    ...createDefaultFeatures(true),
    revenue: false,
  });

  try {
    const response = createResponse();
    let nextCalled = false;
    requireFeatureAccess("revenue", "read")(
      { user: { role: "tenant_admin" } },
      response,
      () => {
        nextCalled = true;
      }
    );

    assert.equal(nextCalled, false);
    assert.equal(response.statusCode, 403);
    assert.match(response.body.error, /not enabled/);
  } finally {
    if (previous === undefined) delete process.env.TENANT_FEATURES_JSON;
    else process.env.TENANT_FEATURES_JSON = previous;
  }
});

test("hostname enforcement honors the configured forwarded host", () => {
  const previous = process.env.TENANT_DOMAIN;
  process.env.TENANT_DOMAIN = "star.airlineplan.com";

  try {
    const allowed = createResponse();
    let nextCalled = false;
    enforceTenantHost(
      {
        headers: {
          host: "10.0.1.20:3000",
          "x-forwarded-host": "star.airlineplan.com",
        },
      },
      allowed,
      () => {
        nextCalled = true;
      }
    );
    assert.equal(nextCalled, true);

    const denied = createResponse();
    enforceTenantHost(
      { headers: { host: "other.airlineplan.com" } },
      denied,
      () => {}
    );
    assert.equal(denied.statusCode, 403);
    assert.equal(normalizeHostname("STAR.AIRLINEPLAN.COM:443"), "star.airlineplan.com");
  } finally {
    if (previous === undefined) delete process.env.TENANT_DOMAIN;
    else process.env.TENANT_DOMAIN = previous;
  }
});

test("sensitive workflow metadata is recursively masked", () => {
  assert.deepEqual(
    maskSensitive({
      tenantId: "tnt_star",
      password: "plain",
      nested: {
        redisUrl: "rediss://secret",
        imageTag: "airlineplan:1",
      },
    }),
    {
      tenantId: "tnt_star",
      password: "***",
      nested: {
        redisUrl: "***",
        imageTag: "airlineplan:1",
      },
    }
  );
});

test("Step Functions input contains the bootstrap ARN but never an admin password", async () => {
  const previous = process.env.TENANT_PROVISIONING_STATE_MACHINE_ARN;
  process.env.TENANT_PROVISIONING_STATE_MACHINE_ARN =
    "arn:aws:states:ap-south-1:123456789012:stateMachine:tenant";
  let command;
  setPlatformClientsForTests({
    sfnClient: {
      async send(value) {
        command = value;
        return { executionArn: "arn:execution:test" };
      },
    },
  });

  try {
    const executionArn = await startPlatformExecution({
      tenant: {
        tenantId: "tnt_star",
        slug: "star",
        companyName: "Star",
        domain: "star.airlineplan.com",
        adminEmail: "admin@star.example",
        plan: "enterprise-dedicated",
        features: createDefaultFeatures(true),
        branding: {},
        albRulePriority: 101,
        attempt: 1,
        deployment: {
          desiredAppVersion: "standard-1",
          desiredImageTag: "airlineplan:standard-1",
        },
        resources: { terraformStateKey: "tenants/star/terraform.tfstate" },
        provisioning: {
          bootstrapSecretArn: "arn:aws:secretsmanager:::secret:bootstrap",
        },
      },
    });

    const input = JSON.parse(command.input.input);
    assert.equal(executionArn, "arn:execution:test");
    assert.equal(input.bootstrapSecretArn.includes("bootstrap"), true);
    assert.equal(JSON.stringify(input).includes("password"), false);
  } finally {
    if (previous === undefined) {
      delete process.env.TENANT_PROVISIONING_STATE_MACHINE_ARN;
    } else {
      process.env.TENANT_PROVISIONING_STATE_MACHINE_ARN = previous;
    }
  }
});

test("migration IDs are unique and retry is restricted to failed tenants", () => {
  const ids = migrations.map((migration) => migration.id);
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(canRetryTenant("FAILED"), true);
  assert.equal(canRetryTenant("ACTIVE"), false);
  assert.equal(canRetryTenant("PROVISIONING"), false);
});
