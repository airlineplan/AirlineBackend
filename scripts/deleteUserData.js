const mongoose = require("mongoose");
require("dotenv").config();
const User = require("../model/userSchema");

// Database Connection
const DB = process.env.MONGO_URI || "mongodb+srv://neeladrinathsarangi:kBhaZHXuGOIUgt9y@cluster0.n0cx0yj.mongodb.net/?retryWrites=true&w=majority";

async function deleteUserData() {
  const email = process.argv[2];

  if (!email) {
    console.error("Please provide an email address as an argument.");
    console.error("Usage: node scripts/deleteUserData.js <email>");
    process.exit(1);
  }

  try {
    console.log(`Connecting to database...`);
    await mongoose.connect(DB);
    console.log("Connected successfully.");

    // 1. Find the user
    const user = await User.findOne({ email: email });

    if (!user) {
      console.error(`User with email ${email} not found.`);
      process.exit(1);
    }

    const userId = user._id.toString();
    console.log(`Found user: ${user.firstName || ''} ${user.lastName || ''} (${email})`);
    console.log(`User ID: ${userId}`);

    // 2. Get all collections
    const collections = await mongoose.connection.db.listCollections().toArray();
    const collectionNames = collections.map(col => col.name);

    console.log(`Scanning ${collectionNames.length} collections for data associated with userId: ${userId}...`);

    let totalDeleted = 0;
    const summary = [];

    for (const name of collectionNames) {
      // Skip the users collection for now, we'll delete the user last
      if (name === User.collection.name) continue;

      const collection = mongoose.connection.db.collection(name);
      
      // We try to delete by userId (as a string, which is how it's stored in most models)
      const result = await collection.deleteMany({ userId: userId });
      
      if (result.deletedCount > 0) {
        console.log(`- Deleted ${result.deletedCount} documents from collection: ${name}`);
        totalDeleted += result.deletedCount;
        summary.push({ collection: name, count: result.deletedCount });
      }
    }

    // 3. Delete the user document itself
    const userDeleteResult = await User.deleteOne({ _id: user._id });
    if (userDeleteResult.deletedCount > 0) {
      console.log(`- Deleted user document from collection: ${User.collection.name}`);
      summary.push({ collection: User.collection.name, count: 1 });
    }

    console.log("\nDeletion Summary:");
    console.table(summary);
    console.log(`Total documents deleted across all collections: ${totalDeleted + 1}`);
    console.log("\nOperation completed successfully.");

  } catch (error) {
    console.error("An error occurred during deletion:");
    console.error(error);
  } finally {
    await mongoose.connection.close();
    process.exit(0);
  }
}

deleteUserData();
