const Aircraft = require("../model/aircraftSchema.js");
const Utilisation = require("../model/utilisationSchema.js");
const MaintenanceStatus = require("../model/maintenanceStatusSchema.js");
const RotableMovement = require("../model/rotableMovementSchema.js");
const MaintenanceReset = require('../model/maintenanceReset');
const moment = require('moment'); // <-- Added missing moment import

/**
 * 1. GET: Fetch Main Dashboard Data
 */
exports.getMaintenanceDashboard = async (req, res) => {
    try {
        // Assuming verifyToken middleware attaches the user to req.user
        const userId = req.user?.userId || req.user?._id;

        // 1. Fetch Aircraft Owning
        const aircraft = await Aircraft.find({ userId }).lean();

        // 2. Fetch recent Utilisation
        const utilisation = await Utilisation.find({ userId })
            .sort({ date: -1 })
            .limit(10)
            .lean();

        // 3. Fetch Maintenance Status
        const status = await MaintenanceStatus.find({ userId }).lean();

        // 4. Fetch Rotable Movements
        const rotables = await RotableMovement.find({ userId })
            .sort({ date: -1 })
            .limit(10)
            .lean();

        res.status(200).json({
            success: true,
            data: {
                aircraft,
                utilisation,
                status,
                rotables
            }
        });
    } catch (error) {
        console.error("Error fetching maintenance dashboard:", error);
        res.status(500).json({ success: false, message: "Server Error" });
    }
};

/**
 * 2. GET: Fetch records for the "Set/Reset Maintenance status" Modal
 */
exports.getResetRecords = async (req, res) => {
    try {
        const { date, msnEsn } = req.query;
        const filter = {};

        // Apply filters if provided from the React frontend
        if (date) {
            // Match exact date (ignoring time)
            const startOfDay = moment(date).startOf('day').toDate();
            const endOfDay = moment(date).endOf('day').toDate();
            filter.date = { $gte: startOfDay, $lte: endOfDay };
        }

        if (msnEsn) {
            // Regex for partial matching in the search dropdown
            filter.msnEsn = { $regex: msnEsn, $options: 'i' };
        }

        const records = await MaintenanceReset.find(filter).sort({ date: -1, msnEsn: 1 }).lean();

        // Format data for the React frontend (map _id to id, format dates)
        const formattedRecords = records.map(record => ({
            id: record._id,
            date: moment(record.date).format("YYYY-MM-DD"),
            msnEsn: record.msnEsn || "",
            pn: record.pn || "",
            snBn: record.snBn || "",
            tsn: record.tsn || "",
            csn: record.csn || "",
            dsn: record.dsn || "",
            tso: record.tsoTsr || "",
            cso: record.csoCsr || "",
            dso: record.dsoDsr || "",
            tsr: record.tsRplmt || "",
            csr: record.csRplmt || "",
            dsr: record.dsRplmt || "",
            metric: record.timeMetric || "BH"
        }));

        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("🔥 Error fetching reset records:", error);
        res.status(500).json({ message: "Failed to fetch reset records", error: error.message });
    }
};

/**
 * 3. POST: Save/Update records from the "Set/Reset Maintenance status" Modal
 * Handles bulk upserts when the user clicks the "Update" button.
 */
exports.bulkSaveResetRecords = async (req, res) => {
    try {
        const { resetData } = req.body;

        if (!resetData || !Array.isArray(resetData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = resetData.map(record => {
            // Clean up empty string values to null for numeric fields
            const parseNum = (val) => (val === "" || val === undefined) ? null : Number(val);

            const updateFields = {
                date: new Date(record.date),
                msnEsn: record.msnEsn,
                pn: record.pn,
                snBn: record.snBn,
                tsn: parseNum(record.tsn),
                csn: parseNum(record.csn),
                dsn: parseNum(record.dsn),
                tsoTsr: parseNum(record.tso),
                csoCsr: parseNum(record.cso),
                dsoDsr: parseNum(record.dso),
                tsRplmt: parseNum(record.tsr),
                csRplmt: parseNum(record.csr),
                dsRplmt: parseNum(record.dsr),
                timeMetric: record.metric || "BH"
            };

            return {
                updateOne: {
                    // Match by Date, MSN, and SN to update existing records, otherwise insert new
                    filter: {
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        snBn: updateFields.snBn
                    },
                    update: { $set: updateFields },
                    upsert: true
                }
            };
        });

        if (bulkOperations.length > 0) {
            await MaintenanceReset.bulkWrite(bulkOperations, { ordered: false });
        }

        res.status(200).json({ success: true, message: "Maintenance reset records updated successfully!" });
    } catch (error) {
        console.error("🔥 Error saving reset records:", error);
        res.status(500).json({ message: "Failed to save records", error: error.message });
    }
};

/**
 * 4. POST: Trigger to compute maintenance logic (The green "Compute" button)
 */
exports.computeMaintenanceLogic = async (req, res) => {
    try {
        const { date } = req.body;
        // Logic to trigger background calculations based on Utilisation and Flights
        // ...

        res.status(200).json({ success: true, message: "Maintenance logic computation started." });
    } catch (error) {
        res.status(500).json({ message: "Failed to start computation", error: error.message });
    }
};