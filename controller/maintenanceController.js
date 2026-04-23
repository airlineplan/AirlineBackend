const Aircraft = require("../model/aircraftSchema.js");
const Utilisation = require("../model/utilisation.js");
const MaintenanceStatus = require("../model/maintenanceStatusSchema.js");
const RotableMovement = require("../model/rotableMovementSchema.js");
const MaintenanceTarget = require("../model/maintenanceTargetSchema.js");
const MaintenanceReset = require('../model/maintenanceReset');
const AircraftOnwing = require("../model/aircraftOnwing.js");
const Fleet = require("../model/fleet.js");
const Assignment = require("../model/assignment.js");
const Flight = require("../model/flight.js");
const MaintenanceCalendar = require("../model/maintenanceCalendarSchema.js");
const moment = require('moment'); // <-- Added missing moment import

const getUserIdFromReq = (req) => req.user?.id || req.userId || req.user?.userId || req.user?._id;

const escapeRegex = (value = "") =>
    String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getFlightDateBounds = async ({ userId } = {}) => {
    const buildPipeline = (match = {}) => [
        { $match: { ...match, date: { $type: "date" } } },
        {
            $group: {
                _id: null,
                firstDate: { $min: "$date" },
                lastDate: { $max: "$date" }
            }
        }
    ];

    const userMatch = userId ? { userId: String(userId) } : {};
    const [userBounds] = await Flight.aggregate(buildPipeline(userMatch));
    return userBounds || null;
};

const getEffectiveUtilisationContext = async ({ userId, msnEsn, date }) => {
    const assetKey = String(msnEsn || "").trim();
    const lookupDate = date ? moment(date).endOf("day").toDate() : null;
    const ownershipFilter = {
        $or: [
            { pos1Esn: assetKey },
            { pos2Esn: assetKey },
            { apun: assetKey }
        ]
    };

    if (userId) {
        ownershipFilter.userId = String(userId);
    }

    if (lookupDate) {
        ownershipFilter.date = { $lte: lookupDate };
    }

    const owningAircraft = assetKey
        ? await AircraftOnwing.findOne(ownershipFilter).sort({ date: -1 }).lean()
        : null;

    const effectiveMsn = String(owningAircraft?.msn || assetKey).trim();
    const fleetFilter = effectiveMsn ? { sn: effectiveMsn } : {};
    if (userId) {
        fleetFilter.userId = String(userId);
    }

    const fleet = effectiveMsn ? await Fleet.findOne(fleetFilter).lean() : null;

    return {
        assetKey,
        effectiveMsn,
        fleet,
    };
};

const getAssignmentUsageForDate = async ({ userId, effectiveMsn, date, metric }) => {
    if (!effectiveMsn) {
        return { timeUsage: 0, cycleUsage: 0 };
    }

    const msnNumber = Number(effectiveMsn);
    if (!Number.isFinite(msnNumber)) {
        return { timeUsage: 0, cycleUsage: 0 };
    }

    const assignments = await Assignment.find({
        ...(userId ? { userId: String(userId) } : {}),
        date: {
            $gte: date.toDate(),
            $lt: moment(date).endOf("day").toDate()
        },
        "aircraft.msn": msnNumber
    });

    const timeUsage = assignments.reduce((sum, a) => {
        const usage = metric === "FH" ? (a.metrics?.flightHours || 0) : (a.metrics?.blockHours || 0);
        return sum + usage;
    }, 0);

    return {
        timeUsage,
        cycleUsage: assignments.length
    };
};

const getUtilisationWindow = ({ masterStartDate, masterEndDate, fleet }) => {
    let startBoundaryDate = masterStartDate ? moment(masterStartDate) : null;
    let endBoundaryDate = masterEndDate ? moment(masterEndDate) : null;

    if (fleet?.entry) {
        const fleetEntry = moment(fleet.entry).startOf("day");
        startBoundaryDate = startBoundaryDate ? moment.max(startBoundaryDate, fleetEntry) : fleetEntry;
    }

    if (fleet?.exit) {
        const fleetExit = moment(fleet.exit).endOf("day");
        endBoundaryDate = endBoundaryDate ? moment.min(endBoundaryDate, fleetExit) : fleetExit;
    }

    return { startBoundaryDate, endBoundaryDate };
};

/**
 * 1. GET: Fetch Main Dashboard Data
 */
exports.getMaintenanceDashboard = async (req, res) => {
    try {
        // Assuming verifyToken middleware attaches the user to req.user
        const userId = getUserIdFromReq(req);

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

        // 5. Build maintenance dashboard rows from the reset/status model
        let maintenanceData = [];
        const { date, msnEsn } = req.query;

        if (date) {
            const endOfDay = moment(date).endOf('day').toDate();
            const utilFilter = {
                date: { $lte: endOfDay }
            };
            if (userId) {
                utilFilter.userId = String(userId);
            }
            if (msnEsn) {
                utilFilter.msnEsn = { $regex: `^${escapeRegex(msnEsn.trim())}$`, $options: "i" };
            }

            const resetFilter = {
                date: { $lte: endOfDay }
            };
            if (msnEsn) {
                resetFilter.msnEsn = { $regex: `^${escapeRegex(msnEsn.trim())}$`, $options: "i" };
            }
            if (userId) {
                resetFilter.userId = String(userId);
            }

            const [utils, resetRecords] = await Promise.all([
                Utilisation.find(utilFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
                MaintenanceReset.find(resetFilter).sort({ date: -1, updatedAt: -1, createdAt: -1 }).lean(),
            ]);

            const utilByKey = new Map();
            utils.forEach(record => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                if (!utilByKey.has(key)) {
                    utilByKey.set(key, record);
                }
            });

            const latestResetByKey = new Map();
            resetRecords.forEach(record => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                if (!latestResetByKey.has(key)) {
                    latestResetByKey.set(key, record);
                }
            });

            const selectedDate = moment(date).format("YYYY-MM-DD");
            const rows = Array.from(latestResetByKey.values()).map((record) => {
                const key = [
                    String(record.msnEsn || "").trim().toUpperCase(),
                    String(record.pn || "").trim().toUpperCase(),
                    String(record.snBn || "").trim().toUpperCase()
                ].join("|");
                const util = utilByKey.get(key);
                const savedResetDate = moment(record.date).format("YYYY-MM-DD");

                return {
                    id: record._id,
                    msn: record.msnEsn || "",
                    msnEsn: record.msnEsn || "",
                    pn: record.pn || "",
                    sn: record.snBn || "",
                    snBn: record.snBn || "",
                    titled: "",
                    date: savedResetDate,
                    savedResetDate,
                    asOnDate: selectedDate,
                    resetDate: savedResetDate,
                    timeMetric: record.timeMetric || "BH",
                    tsn: util?.tsn ?? record.tsn ?? "",
                    csn: util?.csn ?? record.csn ?? "",
                    dsn: util?.dsn ?? record.dsn ?? "",
                    tso: util?.tsoTsr ?? record.tsoTsr ?? "",
                    cso: util?.csoCsr ?? record.csoCsr ?? "",
                    dso: util?.dsoDsr ?? record.dsoDsr ?? "",
                    tsr: util?.tsRplmt ?? record.tsRplmt ?? "",
                    csr: util?.csRplmt ?? record.csRplmt ?? "",
                    dsr: util?.dsRplmt ?? record.dsRplmt ?? "",
                    allDisplay: ""
                };
            });

            maintenanceData = rows;
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
        const userId = getUserIdFromReq(req);
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
        if (userId) {
            filter.userId = String(userId);
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
        const userId = getUserIdFromReq(req);
        const resetData = Array.isArray(req.body) ? req.body : req.body?.resetData;

        if (!resetData || !Array.isArray(resetData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const utilisationOps = [];

        // Use a for...of loop to handle async Await calls
        for (const record of resetData) {
            // Clean up empty string values to null for numeric fields
            const parseNum = (val) => (val === "" || val === undefined) ? null : Number(val);
            const msnEsn = String(record.msnEsn || "").trim();
            const pn = String(record.pn || "").trim();
            const snBn = String(record.snBn || "").trim();
            const utilizationContext = await getEffectiveUtilisationContext({
                userId,
                msnEsn,
                date: record.date
            });
            const effectiveMsn = utilizationContext.effectiveMsn || msnEsn;

            const updateFields = {
                date: new Date(record.date),
                msnEsn,
                pn,
                snBn,
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
                        userId: String(userId),
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        pn: updateFields.pn,
                        snBn: updateFields.snBn
                    },
                    update: {
                        $set: {
                            ...updateFields,
                            userId: String(userId),
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
                        userId: String(userId),
                        date: updateFields.date,
                        msnEsn: updateFields.msnEsn,
                        pn: updateFields.pn,
                        snBn: updateFields.snBn
                    },
                    update: {
                        $set: {
                            ...updateFields,
                            userId: String(userId),
                            timeMetric: record.metric || "BH",
                            setFlag: "Y",
                            remarks: "(end of day)"
                        }
                    },
                    upsert: true
                }
            });

            // 3. Backfill Utilisation history to Master Table Start Date
            // First, find the starting date of the master table (Flight table)
            const flightBounds = await getFlightDateBounds({ userId });
            const masterStartDate = flightBounds && flightBounds.firstDate ? moment(flightBounds.firstDate).startOf('day') : false;

            // Optional: check Fleet for safety, but primary boundary is masterStartDate
            const fleet = utilizationContext.fleet || await Fleet.findOne({
                ...(userId ? { userId: String(userId) } : {}),
                sn: effectiveMsn
            }).lean();
            const utilisationWindow = getUtilisationWindow({
                masterStartDate,
                masterEndDate: false,
                fleet
            });
            const startBoundaryDate = utilisationWindow.startBoundaryDate;

            // Proceed to backfill ONLY if we have a valid boundary date
            if (startBoundaryDate) {
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
                while (currDate.isAfter(startBoundaryDate)) {
                    const { timeUsage, cycleUsage } = await getAssignmentUsageForDate({
                        userId,
                        effectiveMsn,
                        date: currDate,
                        metric: record.metric || "BH"
                    });

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
                                userId: String(userId),
                                date: currDate.toDate(),
                                msnEsn,
                                pn,
                                snBn
                            },
                            update: {
                                $set: {
                                    userId: String(userId),
                                    date: currDate.toDate(),
                                    msnEsn,
                                    pn,
                                    snBn,
                                    tsn: currentTsn,
                                    csn: currentCsn,
                                    dsn: currentDsn,
                                    tsoTsr: currentTso,
                                    csoCsr: currentCso,
                                    dsoDsr: currentDso,
                                    tsRplmt: currentTsr,
                                    csRplmt: currentCsr,
                                    dsRplmt: currentDsr,
                                    timeMetric: record.metric || "BH"
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
            const masterEndDate = flightBounds && flightBounds.lastDate ? moment(flightBounds.lastDate).endOf('day') : false;

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
                    const { timeUsage, cycleUsage } = await getAssignmentUsageForDate({
                        userId,
                        effectiveMsn,
                        date: currDate,
                        metric: record.metric || "BH"
                    });

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
                                userId: String(userId),
                                date: currDate.toDate(),
                                msnEsn,
                                pn,
                                snBn
                            },
                            update: {
                                $set: {
                                    userId: String(userId),
                                    date: currDate.toDate(),
                                    msnEsn,
                                    pn,
                                    snBn,
                                    tsn: currentTsn,
                                    csn: currentCsn,
                                    dsn: currentDsn,
                                    tsoTsr: currentTso,
                                    csoCsr: currentCso,
                                    dsoDsr: currentDso,
                                    tsRplmt: currentTsr,
                                    csRplmt: currentCsr,
                                    dsRplmt: currentDsr,
                                    timeMetric: record.metric || "BH"
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
/**
 * 4. POST: Trigger to compute maintenance logic (The green "Compute" button)
 * This function performs a full recalculation for all assets that have Maintenance Reset records.
 * It propagates metrics forwards and backwards from these reset points.
 */
exports.computeMaintenanceLogic = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        // 1. Determine Global Boundaries
        const flightBounds = await getFlightDateBounds({ userId });

        if (!flightBounds?.firstDate || !flightBounds?.lastDate) {
            return res.status(200).json({ success: true, message: "No flights found to compute." });
        }

        const masterStartDate = moment(flightBounds.firstDate).startOf('day');
        const masterEndDate = moment(flightBounds.lastDate).endOf('day');

        // Fetch calendar limits
        const allCalendars = await MaintenanceCalendar.find({ userId: String(userId) }).lean();

        // 2. Identify all Assets and Parts needing recalculation
        const resetGroups = await MaintenanceReset.aggregate([
            { $match: { userId: String(userId) } },
            { $group: { _id: { msnEsn: "$msnEsn", pn: "$pn", snBn: "$snBn" } } }
        ]);

        const totalOps = [];

        for (const group of resetGroups) {
            const { msnEsn, pn, snBn } = group._id;
            const utilizationContext = await getEffectiveUtilisationContext({
                userId,
                msnEsn,
                date: null
            });
            const effectiveMsn = utilizationContext.effectiveMsn || msnEsn;
            
            // Get all resets for this specific part/asset, sorted chronologically
            const resets = await MaintenanceReset.find({
                userId: String(userId),
                msnEsn,
                pn,
                snBn
            }).sort({ date: 1 }).lean();
            if (resets.length === 0) continue;

            const fleet = utilizationContext.fleet || await Fleet.findOne({
                ...(userId ? { userId: String(userId) } : {}),
                sn: effectiveMsn
            }).lean();
            const utilisationWindow = getUtilisationWindow({
                masterStartDate,
                masterEndDate,
                fleet
            });
            const startBoundaryDate = utilisationWindow.startBoundaryDate;
            const endBoundaryDate = utilisationWindow.endBoundaryDate;
            
            // Determine the true end boundary (fleet exit or master table end)
            if (!endBoundaryDate) {
                endBoundaryDate = masterEndDate;
            }

            // --- RECALCULATION LOGIC ---
            // We iterate through all resets and calculate their respective segments.
            for (let i = 0; i < resets.length; i++) {
                const currentReset = resets[i];
                const nextReset = resets[i + 1];

                // If this is the FIRST reset, backfill to masterStartDate
                if (i === 0 && startBoundaryDate) {
                    let currDate = moment(currentReset.date).startOf('day');
                    let currentTsn = currentReset.tsn;
                    let currentCsn = currentReset.csn;
                    let currentDsn = currentReset.dsn;
                    let currentTso = currentReset.tsoTsr;
                    let currentCso = currentReset.csoCsr;
                    let currentDso = currentReset.dsoDsr;
                    let currentTsr = currentReset.tsRplmt;
                    let currentCsr = currentReset.csRplmt;
                    let currentDsr = currentReset.dsRplmt;

                    while (currDate.isAfter(startBoundaryDate)) {
                        const assignments = await Assignment.find({
                            userId: String(userId),
                            date: {
                                $gte: currDate.toDate(),
                                $lt: moment(currDate).endOf('day').toDate()
                            },
                            "aircraft.msn": Number(effectiveMsn)
                        });

                        const timeUsage = assignments.reduce((sum, a) => 
                            sum + (currentReset.timeMetric === "FH" ? (a.metrics?.flightHours || 0) : (a.metrics?.blockHours || 0)), 0);
                        const cycleUsage = assignments.length;

                        if (currentTsn !== null) currentTsn = Number((currentTsn - timeUsage).toFixed(2));
                        if (currentCsn !== null) currentCsn -= cycleUsage;
                        if (currentDsn !== null) currentDsn -= 1;
                        if (currentTso !== null) currentTso = Number((currentTso - timeUsage).toFixed(2));
                        if (currentCso !== null) currentCso -= cycleUsage;
                        if (currentDso !== null) currentDso -= 1;
                        if (currentTsr !== null) currentTsr = Number((currentTsr - timeUsage).toFixed(2));
                        if (currentCsr !== null) currentCsr -= cycleUsage;
                        if (currentDsr !== null) currentDsr -= 1;

                        currDate.subtract(1, 'days');

                        totalOps.push({
                            updateOne: {
                                filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                                update: {
                                    $set: {
                                        userId: String(userId),
                                        date: currDate.toDate(),
                                        msnEsn, pn, snBn,
                                        tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                        tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                        tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                        timeMetric: currentReset.timeMetric
                                    },
                                    $unset: { setFlag: "", remarks: "" }
                                },
                                upsert: true
                            }
                        });
                    }
                }

                // --- SAVE THE RESET POINT ITSELF ---
                totalOps.push({
                    updateOne: {
                        filter: { userId: String(userId), date: new Date(currentReset.date), msnEsn, pn, snBn },
                        update: {
                            $set: {
                                userId: String(userId),
                                date: new Date(currentReset.date),
                                msnEsn, pn, snBn,
                                tsn: currentReset.tsn, csn: currentReset.csn, dsn: currentReset.dsn,
                                tsoTsr: currentReset.tsoTsr, csoCsr: currentReset.csoCsr, dsoDsr: currentReset.dsoDsr,
                                tsRplmt: currentReset.tsRplmt, csRplmt: currentReset.csRplmt, dsRplmt: currentReset.dsRplmt,
                                timeMetric: currentReset.timeMetric, setFlag: "Y", remarks: "(reset point)"
                            }
                        },
                        upsert: true
                    }
                });

                // --- FORWARD CALCULATION ---
                // Until the next reset date OR the global end boundary
                const segmentEnd = nextReset ? moment(nextReset.date).subtract(1, 'day') : endBoundaryDate;
                let currDate = moment(currentReset.date).add(1, 'days').startOf('day');

                let currentTsn = currentReset.tsn;
                let currentCsn = currentReset.csn;
                let currentDsn = currentReset.dsn;
                let currentTso = currentReset.tsoTsr;
                let currentCso = currentReset.csoCsr;
                let currentDso = currentReset.dsoDsr;
                let currentTsr = currentReset.tsRplmt;
                let currentCsr = currentReset.csRplmt;
                let currentDsr = currentReset.dsRplmt;

                const assetCalendars = allCalendars.filter(c => String(c.calMsn) === String(msnEsn) && String(c.snBn) === String(snBn));
                let inMaintenanceUntil = null;

                while (currDate.isSameOrBefore(segmentEnd)) {
                    if (inMaintenanceUntil && currDate.isSameOrBefore(inMaintenanceUntil)) {
                        // Aircraft in maintenance down-time
                        // Remove assignment for this day
                        await Assignment.updateMany({
                            userId: String(userId),
                            date: {
                                $gte: currDate.toDate(),
                                $lt: moment(currDate).endOf('day').toDate()
                            },
                            "aircraft.msn": Number(effectiveMsn)
                        }, {
                            $unset: { "aircraft.msn": "", "aircraft.registration": "" },
                            $set: { removedReason: "GROUND_DAY_CONFLICT" }
                        });

                        // NO time or cycle usage, but DSN/DSO/DSR increment
                        if (currentDsn !== null) currentDsn += 1;
                        if (currentDso !== null) currentDso += 1;
                        if (currentDsr !== null) currentDsr += 1;

                        totalOps.push({
                            updateOne: {
                                filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                                update: {
                                    $set: {
                                        userId: String(userId),
                                        date: currDate.toDate(),
                                        msnEsn, pn, snBn,
                                        tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                        tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                        tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                        timeMetric: currentReset.timeMetric,
                                        remarks: "Maintenance Downtime"
                                    },
                                    $unset: { setFlag: "" }
                                },
                                upsert: true
                            }
                        });
                        currDate.add(1, 'days');
                        continue;
                    }

                    const assignments = await Assignment.find({
                        userId: String(userId),
                        date: {
                            $gte: currDate.toDate(),
                            $lt: moment(currDate).endOf('day').toDate()
                        },
                        "aircraft.msn": Number(effectiveMsn)
                    });

                    const timeUsage = assignments.reduce((sum, a) => 
                        sum + (currentReset.timeMetric === "FH" ? (a.metrics?.flightHours || 0) : (a.metrics?.blockHours || 0)), 0);
                    const cycleUsage = assignments.length;

                    const projectedTsn = currentTsn !== null ? Number((currentTsn + timeUsage).toFixed(2)) : null;
                    const projectedCsn = currentCsn !== null ? currentCsn + cycleUsage : null;
                    const projectedDsn = currentDsn !== null ? currentDsn + 1 : null;

                    const projectedTso = currentTso !== null ? Number((currentTso + timeUsage).toFixed(2)) : null;
                    const projectedCso = currentCso !== null ? currentCso + cycleUsage : null;
                    const projectedDso = currentDso !== null ? currentDso + 1 : null;

                    const projectedTsr = currentTsr !== null ? Number((currentTsr + timeUsage).toFixed(2)) : null;
                    const projectedCsr = currentCsr !== null ? currentCsr + cycleUsage : null;
                    const projectedDsr = currentDsr !== null ? currentDsr + 1 : null;

                    let triggerHit = false;
                    let downDaysToApply = 0;

                    for (const cal of assetCalendars) {
                        if (
                            (cal.eTsn && projectedTsn !== null && projectedTsn >= cal.eTsn) ||
                            (cal.eCsn && projectedCsn !== null && projectedCsn >= cal.eCsn) ||
                            (cal.eDsn && projectedDsn !== null && projectedDsn >= cal.eDsn) ||
                            (cal.eTso && projectedTso !== null && projectedTso >= cal.eTso) ||
                            (cal.eCso && projectedCso !== null && projectedCso >= cal.eCso) ||
                            (cal.eDso && projectedDso !== null && projectedDso >= cal.eDso) ||
                            (cal.eTsr && projectedTsr !== null && projectedTsr >= cal.eTsr) ||
                            (cal.eCsr && projectedCsr !== null && projectedCsr >= cal.eCsr) ||
                            (cal.eDsr && projectedDsr !== null && projectedDsr >= cal.eDsr)
                        ) {
                            triggerHit = true;
                            downDaysToApply = Math.max(downDaysToApply, cal.downDays || 0);
                        }
                    }

                    if (triggerHit) {
                        // Assignment is removed
                        await Assignment.updateMany({
                            userId: String(userId),
                            date: {
                                $gte: currDate.toDate(),
                                $lt: moment(currDate).endOf('day').toDate()
                            },
                            "aircraft.msn": Number(effectiveMsn)
                        }, {
                            $unset: { "aircraft.msn": "", "aircraft.registration": "" },
                            $set: { removedReason: "GROUND_DAY_CONFLICT" }
                        });

                        if (downDaysToApply > 0) {
                            inMaintenanceUntil = moment(currDate).add(downDaysToApply - 1, 'days');
                        }

                        // Apply the NO UTILISATION logic for today (only days increment)
                        if (currentDsn !== null) currentDsn += 1;
                        if (currentDso !== null) currentDso += 1;
                        if (currentDsr !== null) currentDsr += 1;

                        totalOps.push({
                            updateOne: {
                                filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                                update: {
                                    $set: {
                                        userId: String(userId),
                                        date: currDate.toDate(),
                                        msnEsn, pn, snBn,
                                        tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                        tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                        tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                        timeMetric: currentReset.timeMetric,
                                        remarks: "Maintenance Check Triggered"
                                    },
                                    $unset: { setFlag: "" }
                                },
                                upsert: true
                            }
                        });

                        currDate.add(1, 'days');
                        continue;
                    }

                    // Not in maintenance, apply normal projected values
                    currentTsn = projectedTsn;
                    currentCsn = projectedCsn;
                    currentDsn = projectedDsn;

                    currentTso = projectedTso;
                    currentCso = projectedCso;
                    currentDso = projectedDso;

                    currentTsr = projectedTsr;
                    currentCsr = projectedCsr;
                    currentDsr = projectedDsr;

                        totalOps.push({
                            updateOne: {
                                filter: { userId: String(userId), date: currDate.toDate(), msnEsn, pn, snBn },
                                update: {
                                    $set: {
                                        userId: String(userId),
                                        date: currDate.toDate(),
                                        msnEsn, pn, snBn,
                                        tsn: currentTsn, csn: currentCsn, dsn: currentDsn,
                                    tsoTsr: currentTso, csoCsr: currentCso, dsoDsr: currentDso,
                                    tsRplmt: currentTsr, csRplmt: currentCsr, dsRplmt: currentDsr,
                                    timeMetric: currentReset.timeMetric
                                },
                                $unset: { setFlag: "", remarks: "" }
                            },
                            upsert: true
                        }
                    });

                    currDate.add(1, 'days');
                }
            }
        }

        if (totalOps.length > 0) {
            // Apply updates in chunks to avoid overwhelming the driver if there are many MSNs
            const chunkSize = 1000;
            for (let i = 0; i < totalOps.length; i += chunkSize) {
                const chunk = totalOps.slice(i, i + chunkSize);
                await Utilisation.bulkWrite(chunk, { ordered: false });
            }
        }

        res.status(200).json({ success: true, message: `Maintenance logic computed for ${resetGroups.length} assets.` });
    } catch (error) {
        console.error("🔥 Error computing maintenance logic:", error);
        res.status(500).json({ message: "Failed to recalculate maintenance logic", error: error.message });
    }
};

/**
 * 5. GET: Fetch Major Rotable Movements for Modal
 */
exports.getRotables = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const records = await RotableMovement.find({ userId: String(userId) }).sort({ date: -1 }).lean();
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
        const userId = getUserIdFromReq(req);

        if (!rotablesData || !Array.isArray(rotablesData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];
        const onwingOps = [];

        for (const record of rotablesData) {
            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        msn: record.msn,
                        pn: record.pn,
                        position: record.position,
                        date: record.date ? new Date(record.date) : new Date()
                    },
                    update: {
                        $set: {
                            label: record.label,
                            date: record.date ? new Date(record.date) : new Date(),
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
            if ((record.position === "#1" || record.position === "#2") && record.date) {
                const nextDay = new Date(record.date);
                if (!isNaN(nextDay.getTime())) {
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

/**
 * 7. GET: Fetch Target Maintenance Status Records
 */
exports.getTargets = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const { date } = req.query;
        if (date) {
            const selectedDate = moment(date, moment.ISO_8601, true);
            const flightBounds = await getFlightDateBounds({ userId });
            const outsideRange = !selectedDate.isValid() || !flightBounds?.firstDate || !flightBounds?.lastDate ||
                selectedDate.isBefore(moment(flightBounds.firstDate).startOf("day")) ||
                selectedDate.isAfter(moment(flightBounds.lastDate).endOf("day"));

            if (outsideRange) {
                return res.status(200).json({ success: true, data: [] });
            }
        }

        const records = await MaintenanceTarget.find({ userId: String(userId) }).sort({ date: -1 }).lean();
        const formattedRecords = records.map(record => ({
            id: record._id,
            label: record.label || "",
            msnEsn: record.msnEsn || "",
            pn: record.pn || "",
            snBn: record.snBn || "",
            category: record.category || "",
            date: moment(record.date).format("DD MMM YY"),
            tsn: record.tsn || "",
            csn: record.csn || "",
            dsn: record.dsn || "",
            tso: record.tso || "",
            cso: record.cso || "",
            dso: record.dso || "",
            tsRplmt: record.tsRplmt || "",
            csRplmt: record.csRplmt || "",
            dsRplmt: record.dsRplmt || ""
        }));
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching target maintenance data:", error);
        res.status(500).json({ message: "Failed to fetch targets", error: error.message });
    }
};

/**
 * 8. POST: Bulk Save/Update Target Maintenance Status
 */
exports.bulkSaveTargets = async (req, res) => {
    try {
        const { targetData } = req.body;
        const userId = getUserIdFromReq(req);

        if (!targetData || !Array.isArray(targetData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];

        for (const record of targetData) {
            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        msnEsn: record.msnEsn,
                        pn: record.pn,
                        snBn: record.snBn
                    },
                    update: {
                        $set: {
                            label: record.label,
                            category: record.category,
                            date: record.date ? new Date(record.date) : new Date(),
                            tsn: record.tsn,
                            csn: record.csn,
                            dsn: record.dsn,
                            tso: record.tso,
                            cso: record.cso,
                            dso: record.dso,
                            tsRplmt: record.tsRplmt,
                            csRplmt: record.csRplmt,
                            dsRplmt: record.dsRplmt,
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await MaintenanceTarget.bulkWrite(bulkOperations);
        }

        res.status(200).json({ success: true, message: "Target maintenance status updated successfully." });
    } catch (error) {
        console.error("Error saving target maintenance data:", error);
        res.status(500).json({ message: "Failed to update target maintenance status", error: error.message });
    }
};

/**
 * 9. GET: Fetch Calendar Inputs
 */
exports.getCalendar = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        const records = await MaintenanceCalendar.find({ userId: String(userId) }).lean();
        const formattedRecords = records.map(record => ({
            id: record._id,
            calLabel: record.calLabel || "",
            lineBase: record.lineBase || "",
            calMsn: record.calMsn || "",
            schEvent: record.schEvent || "",
            calPn: record.calPn || "",
            snBn: record.snBn || "",
            eTsn: record.eTsn || "",
            eCsn: record.eCsn || "",
            eDsn: record.eDsn || "",
            eTso: record.eTso || "",
            eCso: record.eCso || "",
            eDso: record.eDso || "",
            eTsr: record.eTsr || "",
            eCsr: record.eCsr || "",
            eDsr: record.eDsr || "",
            downDays: record.downDays || 0,
            avgDownda: record.avgDownda || 0
        }));
        res.status(200).json({ success: true, data: formattedRecords });
    } catch (error) {
        console.error("Error fetching calendar data:", error);
        res.status(500).json({ message: "Failed to fetch calendar data", error: error.message });
    }
};

/**
 * 10. POST: Bulk Save/Update Calendar Inputs
 */
exports.bulkSaveCalendar = async (req, res) => {
    try {
        const { calendarData } = req.body;
        const userId = getUserIdFromReq(req);

        if (!calendarData || !Array.isArray(calendarData)) {
            return res.status(400).json({ message: "Invalid payload. Expected an array of records." });
        }

        const bulkOperations = [];

        for (const record of calendarData) {
            const parseNum = (val) => (val === "" || val === undefined) ? null : Number(val);

            bulkOperations.push({
                updateOne: {
                    filter: {
                        userId: String(userId),
                        calMsn: record.calMsn,
                        calPn: record.calPn,
                        snBn: record.snBn
                    },
                    update: {
                        $set: {
                            calLabel: record.calLabel,
                            lineBase: record.lineBase,
                            schEvent: record.schEvent,
                            calMsn: record.calMsn,
                            calPn: record.calPn,
                            snBn: record.snBn,
                            eTsn: parseNum(record.eTsn),
                            eCsn: parseNum(record.eCsn),
                            eDsn: parseNum(record.eDsn),
                            eTso: parseNum(record.eTso),
                            eCso: parseNum(record.eCso),
                            eDso: parseNum(record.eDso),
                            eTsr: parseNum(record.eTsr),
                            eCsr: parseNum(record.eCsr),
                            eDsr: parseNum(record.eDsr),
                            downDays: parseNum(record.downDays),
                            avgDownda: parseNum(record.avgDownda),
                            userId: userId
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await MaintenanceCalendar.bulkWrite(bulkOperations);
        }

        res.status(200).json({ success: true, message: "Calendar inputs updated successfully." });
    } catch (error) {
        console.error("Error saving calendar data:", error);
        res.status(500).json({ message: "Failed to update calendar inputs", error: error.message });
    }
};
