// controller/fleetController.js
const Fleet = require('../model/fleet'); // Adjust path to your fleet schema

// 1. GET: Fetch all fleet assets
exports.getAllFleet = async (req, res) => {
    try {
        const fleet = await Fleet.find().sort({ sno: 1 });
        res.status(200).json({ data: fleet });
    } catch (error) {
        console.error("🔥 Error fetching fleet:", error);
        res.status(500).json({ message: "Failed to fetch fleet data", error: error.message });
    }
};

// 2. POST (Bulk): Create or Update multiple assets at once
exports.bulkUpsertFleet = async (req, res) => {
    try {
        const { fleetData } = req.body;

        if (!fleetData || !Array.isArray(fleetData)) {
            return res.status(400).json({ message: "Invalid fleet data payload. Expected an array." });
        }

        const bulkOperations = fleetData.map((asset, index) => {
            // Clean up data before saving
            const updateData = { ...asset };

            // Auto-uppercase registration
            if (updateData.regn) updateData.regn = updateData.regn.trim().toUpperCase();

            // Ensure SN exists (required by schema)
            if (!updateData.sn) {
                throw new Error(`Asset at row ${index + 1} is missing a Serial Number (SN)`);
            }

            // Remove the temporary frontend 'id' (like Date.now()) so MongoDB can manage its own _id
            delete updateData.id;
            delete updateData._id;

            return {
                updateOne: {
                    // Match by unique Serial Number (SN)
                    filter: { sn: asset.sn.trim() },
                    update: { $set: updateData },
                    upsert: true // If it doesn't exist, create it. If it does, update it.
                }
            };
        });

        if (bulkOperations.length > 0) {
            await Fleet.bulkWrite(bulkOperations, { ordered: false });
        }

        res.status(200).json({ message: "Fleet data saved successfully!" });
    } catch (error) {
        console.error("🔥 Bulk Save Error:", error);
        res.status(500).json({ message: "Failed to save fleet data", error: error.message });
    }
};

// 3. DELETE: Remove a specific asset by its MongoDB _id
exports.deleteFleetAsset = async (req, res) => {
    try {
        const { id } = req.params;

        const deletedAsset = await Fleet.findByIdAndDelete(id);

        if (!deletedAsset) {
            return res.status(404).json({ message: "Asset not found" });
        }

        res.status(200).json({ message: "Asset deleted successfully" });
    } catch (error) {
        console.error("🔥 Delete Error:", error);
        res.status(500).json({ message: "Failed to delete asset", error: error.message });
    }
};