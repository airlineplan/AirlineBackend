const Aircraft = require("../model/aircraftSchema.js");
const Utilisation = require("../model/utilisation.js");
const MaintenanceStatus = require("../model/maintenanceStatusSchema.js");
const RotableMovement = require("../model/rotableMovementSchema.js");
const MaintenanceReset = require('../model/maintenanceReset');
const Fleet = require("../model/fleet.js");
const Assignment = require("../model/assignment.js");
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

        const bulkOperations = [];
        const utilisationOps = [];

        // Use a for...of loop to handle async Await calls
        for (const record of resetData) {
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
            };

            // 1. MaintenanceReset mapping (for the explicit reset date)
            bulkOperations.push({
                updateOne: {
                    filter: {
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        snBn: updateFields.snBn
                    },
                    update: { 
                        $set: {
                            ...updateFields,
                            timeMetric: record.metric || "BH"
                        } 
                    },
                    upsert: true
                }
            });

            // 2. Utilisation table mapping (Sync for the explicit reset date)
            utilisationOps.push({
                updateOne: {
                    filter: {
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        snBn: updateFields.snBn
                    },
                    update: { 
                        $set: {
                            ...updateFields,
                            setFlag: "Y",
                            remarks: "(end of day)"
                        } 
                    },
                    upsert: true
                }
            });

            // 3. Backfill Utilisation history to Fleet Entry Date
            const fleet = await Fleet.findOne({ sn: record.msnEsn });
            
            if (fleet && fleet.entry) {
                let currDate = moment(record.date).startOf('day');
                const fleetEntryDate = moment(fleet.entry).startOf('day');

                // Keep running totals that we will decrement
                let currentTsn = updateFields.tsn;
                let currentCsn = updateFields.csn;
                let currentDsn = updateFields.dsn;

                let currentTso = updateFields.tsoTsr;
                let currentCso = updateFields.csoCsr;
                let currentDso = updateFields.dsoDsr;

                let currentTsr = updateFields.tsRplmt;
                let currentCsr = updateFields.csRplmt;
                let currentDsr = updateFields.dsRplmt;

                // Loop backward until we reach the fleet entry date
                while (currDate.isAfter(fleetEntryDate)) {
                    // Fetch assignments that occurred ON currDate (the day we are subtracting usage FROM)
                    const assignments = await Assignment.find({
                        date: {
                            $gte: currDate.toDate(),
                            $lt: moment(currDate).endOf('day').toDate()
                        },
                        "aircraft.msn": Number(record.msnEsn)
                    });

                    let timeUsage = 0;
                    let cycleUsage = assignments.length; // Count of flight legs

                    if (record.metric === "FH") {
                        timeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.flightHours || 0), 0);
                    } else { // Default BH
                        timeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.blockHours || 0), 0);
                    }

                    // Decrement running totals based on the usage FOR that day
                    // Subtract timeUsage from hours, cycleUsage from cycles, 1 from days
                    if (currentTsn !== null) currentTsn = Number((currentTsn - timeUsage).toFixed(2));
                    if (currentCsn !== null) currentCsn -= cycleUsage;
                    if (currentDsn !== null) currentDsn -= 1;

                    if (currentTso !== null) currentTso = Number((currentTso - timeUsage).toFixed(2));
                    if (currentCso !== null) currentCso -= cycleUsage;
                    if (currentDso !== null) currentDso -= 1;

                    if (currentTsr !== null) currentTsr = Number((currentTsr - timeUsage).toFixed(2));
                    if (currentCsr !== null) currentCsr -= cycleUsage;
                    if (currentDsr !== null) currentDsr -= 1;

                    // Step back to the prior day
                    currDate.subtract(1, 'days');

                    // Push the backfilled record for the prior day
                    utilisationOps.push({
                        updateOne: {
                            filter: {
                                date: currDate.toDate(),
                                msnEsn: record.msnEsn,
                                snBn: record.snBn
                            },
                            update: {
                                $set: {
                                    date: currDate.toDate(),
                                    msnEsn: record.msnEsn,
                                    pn: record.pn,
                                    snBn: record.snBn,
                                    tsn: currentTsn,
                                    csn: currentCsn,
                                    dsn: currentDsn,
                                    tsoTsr: currentTso,
                                    csoCsr: currentCso,
                                    dsoDsr: currentDso,
                                    tsRplmt: currentTsr,
                                    csRplmt: currentCsr,
                                    dsRplmt: currentDsr
                                    // Notice: no setFlag or remarks for backfilled dates
                                },
                                $unset: { setFlag: "", remarks: "" }
                            },
                            upsert: true
                        }
                    });
                }
            }
        }

        if (bulkOperations.length > 0) {
            await Promise.all([
                MaintenanceReset.bulkWrite(bulkOperations, { ordered: false }),
                Utilisation.bulkWrite(utilisationOps, { ordered: false })
            ]);
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