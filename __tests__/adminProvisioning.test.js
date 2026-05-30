const assert = require("node:assert/strict");
const test = require("node:test");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const Tenant = require("../model/tenantSchema");
const { validateSubdomain } = require("../services/subdomainValidation");
const { signAdminToken, verifyAdminCredentials, verifyAdminToken } = require("../utils/adminAuth");

test("subdomain validation accepts tenant slugs and blocks reserved/unsafe values", () => {
  assert.deepEqual(validateSubdomain("Star-Air").valid, true);
  assert.equal(validateSubdomain("Star-Air").subdomain, "star-air");

  assert.equal(validateSubdomain("admin").valid, false);
  assert.equal(validateSubdomain("www").valid, false);
  assert.equal(validateSubdomain("-star").valid, false);
  assert.equal(validateSubdomain("star_aviation").valid, false);
  assert.equal(validateSubdomain("star.aviation").valid, false);
});

test("admin auth uses env credentials and emits admin-scoped JWT", async () => {
  const previous = {
    ADMIN_EMAIL: process.env.ADMIN_EMAIL,
    ADMIN_PASSWORD_HASH: process.env.ADMIN_PASSWORD_HASH,
    ADMIN_JWT_SECRET: process.env.ADMIN_JWT_SECRET,
  };

  process.env.ADMIN_EMAIL = "admin@airlineplan.com";
  process.env.ADMIN_PASSWORD_HASH = await bcrypt.hash("correct-password", 4);
  process.env.ADMIN_JWT_SECRET = "test-admin-secret";

  try {
    assert.equal(await verifyAdminCredentials({ email: "admin@airlineplan.com", password: "correct-password" }), true);
    assert.equal(await verifyAdminCredentials({ email: "admin@airlineplan.com", password: "wrong" }), false);
    assert.equal(await verifyAdminCredentials({ email: "user@airlineplan.com", password: "correct-password" }), false);

    const token = signAdminToken("admin@airlineplan.com");
    const decoded = verifyAdminToken(token);
    assert.equal(decoded.role, "admin");
    assert.equal(decoded.aud, "airlineplan-admin");
    assert.equal(decoded.email, "admin@airlineplan.com");

    const userToken = jwt.sign({ role: "user" }, process.env.ADMIN_JWT_SECRET);
    assert.throws(() => verifyAdminToken(userToken), /Invalid admin token/);
  } finally {
    Object.entries(previous).forEach(([key, value]) => {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    });
  }
});

test("tenant model stores provisioning state and cloud identifiers", () => {
  const tenant = new Tenant({
    tenantName: "Star Aviation",
    subdomain: "star",
    fullDomain: "star.airlineplan.com",
    adminEmail: "ops@star.example",
    aws: {
      region: "ap-south-1",
      instanceType: "t3.small",
      instanceId: "i-123",
      publicIp: "203.0.113.10",
    },
    atlas: {
      projectId: "atlas-project",
      clusterName: "airlineplan-star",
      databaseName: "airlineplan_star",
    },
    logs: [{ level: "info", message: "created" }],
  });

  assert.equal(tenant.status, "pending");
  tenant.status = "dns_pending";
  assert.equal(tenant.aws.instanceType, "t3.small");
  assert.equal(tenant.atlas.clusterName, "airlineplan-star");
  assert.equal(tenant.logs[0].message, "created");
  assert.equal(tenant.validateSync(), undefined);

  tenant.status = "unknown";
  assert.match(tenant.validateSync().message, /`unknown` is not a valid enum value/);
});
