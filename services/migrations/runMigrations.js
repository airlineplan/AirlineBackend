const mongoose = require("mongoose");
const SchemaMigration = require("../../model/schemaMigration");
const TenantSetting = require("../../model/tenantSettingSchema");
const { getTenantRuntimeConfig } = require("../../config/runtime");

const migrations = [
  {
    id: "001-sync-model-indexes",
    run: async () => {
      const modelNames = mongoose.modelNames().filter((name) => name !== "SchemaMigration");
      for (const modelName of modelNames) {
        await mongoose.model(modelName).syncIndexes();
      }
    },
  },
  {
    id: "002-default-tenant-settings",
    run: async () => {
      const config = getTenantRuntimeConfig();
      await TenantSetting.bulkWrite(
        [
          {
            updateOne: {
              filter: { key: "tenant" },
              update: {
                $setOnInsert: {
                  key: "tenant",
                  value: {
                    tenantId: config.tenantId,
                    slug: config.slug,
                    companyName: config.companyName,
                    domain: config.domain,
                  },
                },
              },
              upsert: true,
            },
          },
          {
            updateOne: {
              filter: { key: "features" },
              update: {
                $set: {
                  key: "features",
                  value: config.features,
                },
              },
              upsert: true,
            },
          },
          {
            updateOne: {
              filter: { key: "reportingCurrency" },
              update: {
                $setOnInsert: {
                  key: "reportingCurrency",
                  value: "INR",
                },
              },
              upsert: true,
            },
          },
        ],
        { ordered: true }
      );
    },
  },
];

const runMigrations = async () => {
  const applied = new Set(
    (await SchemaMigration.find({}, { migrationId: 1 }).lean()).map(
      (migration) => migration.migrationId
    )
  );

  const completed = [];
  for (const migration of migrations) {
    if (applied.has(migration.id)) continue;
    await migration.run();
    await SchemaMigration.create({
      migrationId: migration.id,
      appVersion: process.env.APP_VERSION || "development",
    });
    completed.push(migration.id);
  }

  return completed;
};

module.exports = {
  migrations,
  runMigrations,
};
