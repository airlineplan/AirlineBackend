const { MongoClient } = require("mongodb");
const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

// ============================================================
// CONFIGURATION
// ============================================================

// NEVER hard-code your MongoDB URI here.
// Set it through environment variable MONGO_URI.
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://neeladrinathsarangi:kBhaZHXuGOIUgt9y@cluster0.n0cx0yj.mongodb.net/?retryWrites=true&w=majority";

// IMPORTANT:
// Explicitly specify the database you want to clean.
// This protects you from accidentally cleaning the wrong DB.
const DB_NAME = process.env.MONGO_DB_NAME || "test";

// Collections that should NEVER be deleted.
const PRESERVE_COLLECTIONS = new Set([
  "users",
]);

// For additional collections:
//
// const PRESERVE_COLLECTIONS = new Set([
//   "users",
//   "someConfigurationCollection",
//   "anotherCollection",
// ]);

const CONFIRM_DELETE = process.env.CONFIRM_DELETE === "YES";


// ============================================================
// VALIDATION
// ============================================================

if (!MONGO_URI) {
  console.error("ERROR: MONGO_URI environment variable is missing.");
  process.exit(1);
}

if (!DB_NAME) {
  console.error("ERROR: MONGO_DB_NAME environment variable is missing.");
  process.exit(1);
}


// ============================================================
// BACKUP
// ============================================================

function createBackup() {
  const timestamp = new Date()
    .toISOString()
    .replace(/:/g, "-")
    .replace(/\./g, "-");

  const backupRoot = path.join(__dirname, "mongodb-backups");
  const backupDirectory = path.join(
    backupRoot,
    `${DB_NAME}-${timestamp}`
  );

  fs.mkdirSync(backupDirectory, {
    recursive: true,
  });

  console.log("\n==================================================");
  console.log("CREATING DATABASE BACKUP");
  console.log("==================================================");
  console.log(`Database : ${DB_NAME}`);
  console.log(`Backup   : ${backupDirectory}`);
  console.log("");

  try {
    execFileSync(
      "mongodump",
      [
        "--uri",
        MONGO_URI,
        "--db",
        DB_NAME,
        "--out",
        backupDirectory,
      ],
      {
        stdio: "inherit",
      }
    );
  } catch (error) {
    console.error("\n❌ BACKUP FAILED.");
    console.error("Database cleanup has been CANCELLED.");
    console.error("NO DATA WAS DELETED.");
    process.exit(1);
  }

  console.log("\n✅ BACKUP COMPLETED SUCCESSFULLY.");

  return backupDirectory;
}


// ============================================================
// DATABASE CLEANUP
// ============================================================

async function cleanupDatabase() {
  const client = new MongoClient(MONGO_URI);

  try {
    console.log("\nConnecting to MongoDB...");

    await client.connect();

    console.log("✅ Connected.");

    const db = client.db(DB_NAME);

    // --------------------------------------------------------
    // Discover every collection dynamically
    // --------------------------------------------------------

    const collectionInformation = await db
      .listCollections({}, { nameOnly: true })
      .toArray();

    const collectionNames = collectionInformation
      .map((collection) => collection.name)
      .filter((name) => !name.startsWith("system."))
      .sort();

    console.log("\n==================================================");
    console.log("COLLECTIONS FOUND");
    console.log("==================================================");

    for (const collectionName of collectionNames) {
      const count = await db
        .collection(collectionName)
        .countDocuments({});

      const action = PRESERVE_COLLECTIONS.has(collectionName)
        ? "KEEP"
        : "DELETE DATA";

      console.log(
        `${action.padEnd(12)} | ${collectionName.padEnd(35)} | ${count} documents`
      );
    }

    // --------------------------------------------------------
    // SAFETY STOP
    // --------------------------------------------------------

    

    // --------------------------------------------------------
    // Backup FIRST
    // --------------------------------------------------------


    // --------------------------------------------------------
    // Delete documents
    // --------------------------------------------------------

    console.log("\n==================================================");
    console.log("STARTING CLEANUP");
    console.log("==================================================");

    const results = [];

    for (const collectionName of collectionNames) {

      if (PRESERVE_COLLECTIONS.has(collectionName)) {

        const count = await db
          .collection(collectionName)
          .countDocuments({});

        console.log(
          `🛡️  PRESERVED: ${collectionName} (${count} documents)`
        );

        results.push({
          collection: collectionName,
          action: "PRESERVED",
          deleted: 0,
          remaining: count,
        });

        continue;
      }

      console.log(`Deleting data from: ${collectionName}`);

      const result = await db
        .collection(collectionName)
        .deleteMany({});

      console.log(
        `   ✅ Deleted ${result.deletedCount} documents`
      );

      results.push({
        collection: collectionName,
        action: "CLEARED",
        deleted: result.deletedCount,
        remaining: 0,
      });
    }

    // --------------------------------------------------------
    // Verification
    // --------------------------------------------------------

    console.log("\n==================================================");
    console.log("VERIFYING DATABASE");
    console.log("==================================================");

    for (const collectionName of collectionNames) {

      const remaining = await db
        .collection(collectionName)
        .countDocuments({});

      console.log(
        `${collectionName.padEnd(35)} : ${remaining}`
      );
    }

    // --------------------------------------------------------
    // Save deletion report
    // --------------------------------------------------------

    const report = {
      database: DB_NAME,
      completedAt: new Date().toISOString(),
      backupDirectory,
      preservedCollections: [...PRESERVE_COLLECTIONS],
      collections: results,
    };

    const reportFile = path.join(
      backupDirectory,
      "cleanup-report.json"
    );

    fs.writeFileSync(
      reportFile,
      JSON.stringify(report, null, 2)
    );

    console.log("\n==================================================");
    console.log("✅ DATABASE CLEANUP COMPLETED");
    console.log("==================================================");

    console.log(`Backup: ${backupDirectory}`);
    console.log(`Report: ${reportFile}`);

  } catch (error) {

    console.error("\n❌ ERROR:");
    console.error(error);

    process.exitCode = 1;

  } finally {

    await client.close();

    console.log("\nMongoDB connection closed.");
  }
}


// ============================================================
// START
// ============================================================

cleanupDatabase();

