const assert = require("node:assert/strict");
const test = require("node:test");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const Tenant = require("../model/tenantSchema");
const User = require("../model/userSchema");
const { requireTenantAdmin } = require("../middlware/auth");
const { scopedUserQuery } = require("../controller/accessScope");
const { isFeatureAllowedForTenantAdmin, requireFeatureAccess, requireTenantFeatureAccess } = require("../middlware/tenantFeatureAccess");
const {
  createDefaultPageAccess,
  getEffectivePageAccess,
  normalizePageAccessInput,
} = require("../config/pageAccess");
const { verifyBootstrapToken } = require("../controller/tenantUserController");
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
    assert.equal(decoded.role, "super_admin");
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

test("tenant users have scoped roles and active access state", () => {
  const user = new User({
    firstName: "Ops",
    lastName: "Planner",
    email: "OPS@STAR.EXAMPLE",
    password: "hashed-password",
  });

  assert.equal(user.role, "user");
  assert.equal(user.isActive, true);
  assert.equal(user.validateSync(), undefined);

  user.role = "tenant_admin";
  assert.equal(user.validateSync(), undefined);

  user.role = "super_admin";
  assert.match(user.validateSync().message, /`super_admin` is not a valid enum value/);
});

test("tenant users validate page access maps", () => {
  const user = new User({
    email: "planner@star.example",
    password: "hashed-password",
    pageAccess: {
      network: "edit",
      stations: "read",
      sectors: "none",
    },
  });

  assert.equal(user.validateSync(), undefined);

  user.pageAccess.set("network", "manage");
  assert.match(user.validateSync().message, /`manage` is not a valid enum value/);

  user.pageAccess.set("network", "edit");
  user.pageAccess.set("unknown", "read");
  assert.match(user.validateSync().message, /Invalid page access feature/);
});

test("page access defaults preserve legacy users and grant edit by default", () => {
  const legacyUser = new User({
    email: "legacy@star.example",
    password: "hashed-password",
  });

  assert.equal(getEffectivePageAccess(legacyUser).network, "edit");
  assert.equal(getEffectivePageAccess({ pageAccess: {} }).network, "edit");
  assert.equal(getEffectivePageAccess({ pageAccess: createDefaultPageAccess() }).network, "edit");
  assert.equal(getEffectivePageAccess({ pageAccess: createDefaultPageAccess(), pageAccessConfigured: true }).network, "edit");
  assert.equal(normalizePageAccessInput({ network: "read" }).network, "read");
  assert.equal(normalizePageAccessInput({ network: "read" }).stations, "edit");
  assert.throws(() => normalizePageAccessInput({ network: "delete" }), /Invalid access level/);
  assert.throws(() => normalizePageAccessInput({ unknown: "read" }), /Unknown page access feature/);
});

test("tenant admin guard accepts current and legacy admin roles", () => {
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

  ["tenant_admin", "admin"].forEach((role) => {
    const res = createResponse();
    let nextCalled = false;
    requireTenantAdmin({ user: { role } }, res, () => {
      nextCalled = true;
    });

    assert.equal(nextCalled, true);
    assert.equal(res.statusCode, 200);
  });

  const res = createResponse();
  let nextCalled = false;
  requireTenantAdmin({ user: { role: "user" } }, res, () => {
    nextCalled = true;
  });

  assert.equal(nextCalled, false);
  assert.equal(res.statusCode, 403);
});

test("tenant admin data scope can see instance records while users stay self-scoped", () => {
  assert.deepEqual(
    scopedUserQuery({ user: { id: "regular-user", role: "user" } }, { isComplete: true }),
    { isComplete: true, userId: "regular-user" }
  );

  assert.deepEqual(
    scopedUserQuery({ user: { id: "tenant-admin", role: "tenant_admin" } }, { isComplete: true }),
    { isComplete: true }
  );
});

test("tenant admin feature access is driven by config flags", () => {
  assert.equal(isFeatureAllowedForTenantAdmin("users"), true);
  assert.equal(isFeatureAllowedForTenantAdmin("network"), true);

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

  const networkAllowed = createResponse();
  let networkNextCalled = false;
  requireTenantFeatureAccess("network")(
    { user: { role: "tenant_admin" } },
    networkAllowed,
    () => {
      networkNextCalled = true;
    }
  );

  assert.equal(networkNextCalled, true);
  assert.equal(networkAllowed.statusCode, 200);

  const allowed = createResponse();
  let allowedNextCalled = false;
  requireTenantFeatureAccess("users")(
    { user: { role: "tenant_admin" } },
    allowed,
    () => {
      allowedNextCalled = true;
    }
  );

  assert.equal(allowedNextCalled, true);
  assert.equal(allowed.statusCode, 200);
});

test("regular user page access enforces read and edit levels", () => {
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

  const readAllowed = createResponse();
  let readNextCalled = false;
  requireFeatureAccess("network", "read")(
    { user: { role: "user", pageAccess: { ...createDefaultPageAccess(), network: "read" } } },
    readAllowed,
    () => {
      readNextCalled = true;
    }
  );
  assert.equal(readNextCalled, true);
  assert.equal(readAllowed.statusCode, 200);

  const editBlocked = createResponse();
  let editNextCalled = false;
  requireFeatureAccess("network", "edit")(
    { user: { role: "user", pageAccess: { ...createDefaultPageAccess(), network: "read" } } },
    editBlocked,
    () => {
      editNextCalled = true;
    }
  );
  assert.equal(editNextCalled, false);
  assert.equal(editBlocked.statusCode, 403);
  assert.match(editBlocked.body.error, /read-only access/);

  const noAccess = createResponse();
  let noAccessNextCalled = false;
  requireFeatureAccess("stations", "read")(
    { user: { role: "user", pageAccess: { ...createDefaultPageAccess(), stations: "none" }, pageAccessConfigured: true } },
    noAccess,
    () => {
      noAccessNextCalled = true;
    }
  );
  assert.equal(noAccessNextCalled, false);
  assert.equal(noAccess.statusCode, 403);
});

test("tenant admin bootstrap requires the provisioning secret", () => {
  const previous = process.env.TENANT_BOOTSTRAP_SECRET;
  process.env.TENANT_BOOTSTRAP_SECRET = "tenant-bootstrap-test-secret";

  try {
    assert.doesNotThrow(() => verifyBootstrapToken({
      headers: { "x-tenant-bootstrap-token": "tenant-bootstrap-test-secret" },
    }));
    assert.doesNotThrow(() => verifyBootstrapToken({
      headers: { authorization: "Bearer tenant-bootstrap-test-secret" },
    }));
    assert.throws(
      () => verifyBootstrapToken({ headers: { "x-tenant-bootstrap-token": "wrong" } }),
      /Invalid tenant bootstrap token/
    );
  } finally {
    if (previous === undefined) delete process.env.TENANT_BOOTSTRAP_SECRET;
    else process.env.TENANT_BOOTSTRAP_SECRET = previous;
  }
});
