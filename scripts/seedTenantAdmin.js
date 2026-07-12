require("dotenv").config();

const {
  GetSecretValueCommand,
  SecretsManagerClient,
} = require("@aws-sdk/client-secrets-manager");
const { connectDatabase, disconnectDatabase } = require("../config/db");
const {
  createUserRecord,
} = require("../controller/tenantUserController");
const User = require("../model/userSchema");

const loadBootstrapAdmin = async () => {
  const secretArn = process.env.TENANT_BOOTSTRAP_SECRET_ARN;
  if (!secretArn) {
    throw new Error("TENANT_BOOTSTRAP_SECRET_ARN is required");
  }

  const client = new SecretsManagerClient({
    region: process.env.AWS_REGION || "ap-south-1",
  });
  const response = await client.send(
    new GetSecretValueCommand({ SecretId: secretArn })
  );
  if (!response.SecretString) {
    throw new Error("Tenant bootstrap secret is empty");
  }
  return JSON.parse(response.SecretString);
};

const main = async () => {
  await connectDatabase();
  const existing = await User.findOne({ role: "tenant_admin" }).lean();
  if (existing) {
    console.log("Tenant admin already exists; seed is idempotently complete");
    return;
  }

  const admin = await loadBootstrapAdmin();
  await createUserRecord({
    firstName: admin.firstName,
    lastName: admin.lastName,
    email: admin.email,
    password: admin.password,
    role: "tenant_admin",
  });
  console.log("Tenant admin created");
};

main()
  .then(disconnectDatabase)
  .catch(async (error) => {
    console.error("Tenant admin seed failed", error);
    await disconnectDatabase();
    process.exit(1);
  });
