require("dotenv").config();

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { pipeline } = require("stream/promises");
const { Readable } = require("stream");
const { PutObjectCommand, S3Client } = require("@aws-sdk/client-s3");
const { connectDatabase, disconnectDatabase } = require("../config/db");
const mongoose = require("mongoose");

const exportBucket = process.env.EXPORT_BUCKET;
const exportPrefix = process.env.EXPORT_PREFIX;

const requireExportConfig = () => {
  if (!exportBucket || !exportPrefix) {
    throw new Error("EXPORT_BUCKET and EXPORT_PREFIX are required");
  }
};

const collectionLines = async function* (collection) {
  const cursor = collection.find({});
  for await (const document of cursor) {
    yield `${JSON.stringify(document)}\n`;
  }
};

const uploadFile = async (client, filePath, key, contentType) => {
  const stats = await fs.promises.stat(filePath);
  await client.send(
    new PutObjectCommand({
      Bucket: exportBucket,
      Key: key,
      Body: fs.createReadStream(filePath),
      ContentLength: stats.size,
      ContentType: contentType,
    })
  );
};

const main = async () => {
  requireExportConfig();
  await connectDatabase();

  const s3 = new S3Client({ region: process.env.AWS_REGION || "ap-south-1" });
  const collections = await mongoose.connection.db.listCollections().toArray();
  const manifest = {
    tenantId: process.env.TENANT_ID,
    databaseName: mongoose.connection.db.databaseName,
    exportedAt: new Date().toISOString(),
    collections: [],
  };

  for (const metadata of collections) {
    const safeName = metadata.name.replace(/[^a-zA-Z0-9_-]/g, "_");
    const filePath = path.join("/tmp", `${safeName}.ndjson.gz`);
    await pipeline(
      Readable.from(collectionLines(mongoose.connection.db.collection(metadata.name))),
      zlib.createGzip(),
      fs.createWriteStream(filePath)
    );
    const key = `${exportPrefix}/${safeName}.ndjson.gz`;
    await uploadFile(s3, filePath, key, "application/x-ndjson");
    manifest.collections.push({ name: metadata.name, key });
    await fs.promises.unlink(filePath);
  }

  const manifestPath = "/tmp/manifest.json";
  await fs.promises.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  await uploadFile(
    s3,
    manifestPath,
    `${exportPrefix}/manifest.json`,
    "application/json"
  );
  await fs.promises.unlink(manifestPath);
  console.log(`Tenant export completed: s3://${exportBucket}/${exportPrefix}`);
};

main()
  .then(disconnectDatabase)
  .catch(async (error) => {
    console.error("Tenant export failed", error);
    await disconnectDatabase();
    process.exit(1);
  });
