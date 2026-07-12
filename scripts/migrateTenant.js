require("dotenv").config();

const { connectDatabase, disconnectDatabase } = require("../config/db");
const { registerModels } = require("../services/migrations/registerModels");
const { runMigrations } = require("../services/migrations/runMigrations");

const main = async () => {
  registerModels();
  await connectDatabase();
  const completed = await runMigrations();
  console.log(
    completed.length > 0
      ? `Applied migrations: ${completed.join(", ")}`
      : "No pending migrations"
  );
};

main()
  .then(disconnectDatabase)
  .catch(async (error) => {
    console.error("Tenant migration failed", error);
    await disconnectDatabase();
    process.exit(1);
  });
