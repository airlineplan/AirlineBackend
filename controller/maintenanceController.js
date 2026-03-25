const Aircraft = require("../model/aircraftSchema.js");
const Utilisation = require("../model/utilisation.js");
const MaintenanceStatus = require("../model/maintenanceStatusSchema.js");
const RotableMovement = require("../model/rotableMovementSchema.js");
const MaintenanceReset = require('../model/maintenanceReset');
const AircraftOnwing = require("../model/aircraftOnwing.js");
const Fleet = require("../model/fleet.js");
const Assignment = require("../model/assignment.js");
const Flight = require("../model/flight.js");
const moment = require('moment'); // <-- Added missing moment import

/**
 * 1. GET: Fetch Main Dashboard Data
 */
exports.getMaintenanceDashboard = async (req, res) => {
    try {
        // Assuming verifyToken middleware attaches the user to req.user
        const userId = req.user?.userId || req.user?._id;

        // 1. Fetch Aircraft Owning
        // const aircraft = await Aircraft.find({ userId }).lean();
        const aircraft = [];

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

        // 5. Build dynamically mapped Dashboard Status table Data
        let maintenanceData = [];
        const { date } = req.query;

        if (date) {
            const startOfDay = moment(date).startOf('day').toDate();
            const endOfDay = moment(date).endOf('day').toDate();

            const fleetAssets = await Fleet.find({}).lean();
            const utils = await Utilisation.find({
                date: { $gte: startOfDay, $lte: endOfDay }
            }).lean();

            // Fetch the most recent resets to accurately pull PN and SN/BN assignments
            const mResets = await MaintenanceReset.find({}).sort({ date: -1 }).lean();

            maintenanceData = fleetAssets.map(f => {
                const sn = f.sn; // MSN/ESN
                const util = utils.find(u => String(u.msnEsn) === String(sn));
                const latestReset = mResets.find(r => String(r.msnEsn) === String(sn));

                return {
                    id: f._id,
                    msn: sn,
                    pn: latestReset && latestReset.pn ? latestReset.pn : f.variant || "",
                    sn: latestReset && latestReset.snBn ? latestReset.snBn : "",
                    titled: f.titled || "",
                    tsn: util ? util.tsn : "",
                    csn: util ? util.csn : "",
                    dsn: util ? util.dsn : "",
                    tso: util ? util.tsoTsr : "",
                    cso: util ? util.csoCsr : "",
                    dso: util ? util.dsoDsr : "",
                    tsr: util ? util.tsRplmt : "",
                    csr: util ? util.csRplmt : "",
                    dsr: util ? util.dsRplmt : "",
                    allDisplay: ""
                };
            });
        }

        res.status(200).json({
            success: true,
            data: {
                maintenanceData, // The aggregated frontend status table
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

            // 3. Backfill Utilisation history to Master Table Start Date
            // First, find the starting date of the master table (Flight table)
            const firstFlight = await Flight.findOne().sort({ date: 1 }).lean();
            const masterStartDate = firstFlight && firstFlight.date ? moment(firstFlight.date).startOf('day') : false;

            // Optional: check Fleet for safety, but primary boundary is masterStartDate
            const fleet = await Fleet.findOne({ sn: record.msnEsn });

            // Proceed to backfill ONLY if we have a valid boundary date
            if (masterStartDate) {
                let currDate = moment(record.date).startOf('day');

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

                // Loop backward until we reach the master start date
                while (currDate.isAfter(masterStartDate)) {
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

            // 4. Forward Calculation to Fleet Exit Date or Master End Date
            const lastFlight = await Flight.findOne().sort({ date: -1 }).lean();
            const masterEndDate = lastFlight && lastFlight.date ? moment(lastFlight.date).endOf('day') : false;

            // Determine the true end boundary (fleet exit or master table end)
            let endBoundaryDate = masterEndDate;
            if (fleet && fleet.exit && moment(fleet.exit).isBefore(masterEndDate)) {
                endBoundaryDate = moment(fleet.exit).endOf('day');
            }

            if (endBoundaryDate) {
                // Reset tracking variables to the explicitly entered reset record values
                let currDate = moment(record.date).add(1, 'days').startOf('day');

                let currentTsn = updateFields.tsn;
                let currentCsn = updateFields.csn;
                let currentDsn = updateFields.dsn;

                let currentTso = updateFields.tsoTsr;
                let currentCso = updateFields.csoCsr;
                let currentDso = updateFields.dsoDsr;

                let currentTsr = updateFields.tsRplmt;
                let currentCsr = updateFields.csRplmt;
                let currentDsr = updateFields.dsRplmt;

                // Loop forward until the boundary
                while (currDate.isSameOrBefore(endBoundaryDate)) {
                    // Fetch assignments that occurred ON currDate (the day we are adding usage FOR)
                    const assignments = await Assignment.find({
                        date: {
                            $gte: currDate.toDate(),
                            $lt: moment(currDate).endOf('day').toDate()
                        },
                        "aircraft.msn": Number(record.msnEsn)
                    });

                    let timeUsage = 0;
                    let cycleUsage = assignments.length;

                    if (record.metric === "FH") {
                        timeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.flightHours || 0), 0);
                    } else {
                        timeUsage = assignments.reduce((sum, a) => sum + (a.metrics?.blockHours || 0), 0);
                    }

                    // Increment running totals based on usage
                    // Add timeUsage to hours, cycleUsage to cycles, 1 to days
                    if (currentTsn !== null) currentTsn = Number((currentTsn + timeUsage).toFixed(2));
                    if (currentCsn !== null) currentCsn += cycleUsage;
                    if (currentDsn !== null) currentDsn += 1;

                    if (currentTso !== null) currentTso = Number((currentTso + timeUsage).toFixed(2));
                    if (currentCso !== null) currentCso += cycleUsage;
                    if (currentDso !== null) currentDso += 1;

                    if (currentTsr !== null) currentTsr = Number((currentTsr + timeUsage).toFixed(2));
                    if (currentCsr !== null) currentCsr += cycleUsage;
                    if (currentDsr !== null) currentDsr += 1;

                    // Push the forward record for the current day
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
                                },
                                $unset: { setFlag: "", remarks: "" }
                            },
                            upsert: true
                        }
                    });

                    // Step forward to the next day
                    currDate.add(1, 'days');
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

/**
 * 5. GET: Fetch Major Rotable Movements for Modal
 */
exports.getRotables = async (req, res) => {
    try {
        const records = await RotableMovement.find({}).sort({ date: -1 }).lean();
        const formattedRecords = records.map(record => ({
            id: record._id,
            label: record.label || "",
            date: moment(record.date).format("YYYY-MM-DD"),
            pn: record.pn || "",
            msn: record.msn || "",
            acftRegn: record.acftReg || "",
            position: record.position || "",
            removedSN: record.removedSN || "",
            installedSN: record.installedSN || ""
        }));
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching rotable movements:", error);
        res.status(500).json({ message: "Failed to fetch rotable movements", error: error.message });
    }
};

/**
 * 6. POST: Bulk Save/Update Major Rotable Movements
 */
exports.bulkSaveRotables = async (req, res) => {
    try {
        const { rotablesData } = req.body;
        const userId = req.user?.userId || req.user?._id;

        if (!rotablesData || !Array.isArray(rotablesData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const onwingOps = [];

        for (const record of rotablesData) {
            bulkOperations.push({
                updateOne: {
                    filter: {
                        msn: record.msn,
                        pn: record.pn,
                        position: record.position,
                        date: new Date(record.date)
                    },
                    update: {
                        $set: {
                            label: record.label,
                            date: new Date(record.date),
                            pn: record.pn,
                            msn: record.msn,
                            acftReg: record.acftRegn,
                            position: record.position,
                            removedSN: record.removedSN,
                            installedSN: record.installedSN,
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });

            // Update AircraftOnwing if an Engine is assigned to Position #1 or #2
            if (record.position === "#1" || record.position === "#2") {
                const nextDay = new Date(record.date);
                nextDay.setDate(nextDay.getDate() + 1);

                const updateField = record.position === "#1" ? "pos1Esn" : "pos2Esn";

                // 1. Update all future chronological configurations for this MSN
                onwingOps.push({
                    updateMany: {
                        filter: { msn: record.msn, date: { $gte: nextDay } },
                        update: {
                            $set: {
                                [updateField]: record.installedSN
                            }
                        }
                    }
                });

                // 2. Explicitly log the new configuration timeline starting on nextDay
                onwingOps.push({
                    updateOne: {
                        filter: { msn: record.msn, date: nextDay },
                        update: {
                            $set: {
                                [updateField]: record.installedSN
                            }
                        },
                        upsert: true
                    }
                });
            }
        }

        if (bulkOperations.length > 0) {
            await RotableMovement.bulkWrite(bulkOperations);
        }

        if (onwingOps.length > 0) {
            await AircraftOnwing.bulkWrite(onwingOps, { ordered: false });
        }

        res.status(200).json({ success: true, message: "Rotable movements and Aircraft configurations updated successfully." });
    } catch (error) {
        console.error("Error saving rotables data:", error);
        res.status(500).json({ message: "Failed to update rotables data", error: error.message });
    }
};