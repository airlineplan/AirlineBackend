const Fleet = require('../model/fleet');
const Flight = require('../model/flight'); // Add this to query flight dates
const Assignment = require('../model/assignment'); // Adjust path if needed
const GroundDay = require('../model/groundDay');
const AircraftOnwing = require('../model/aircraftOnwing');
const moment = require('moment');


exports.getFleetScheduleMetrics = async (req, res) => {
    try {
        const { month } = req.query;
        if (!month) return res.status(400).json({ message: "Month is required" });

        const startDt = moment(month, "MMMM YYYY").startOf('month').toDate();
        // Look back further for Onwing records to get active configurations prior to the month start
        const endDt = moment(month, "MMMM YYYY").endOf('month').toDate();

        // 1. Fetch data
        const [groundDays, assignments, flights, onwings] = await Promise.all([
            GroundDay.find({ date: { $gte: startDt, $lte: endDt } }),
            Assignment.find({ date: { $gte: startDt, $lte: endDt }, isValid: true }),
            Flight.find({ date: { $gte: startDt, $lte: endDt } }),
            AircraftOnwing.find({ date: { $lte: endDt } }).sort({ date: 1 }) // Sorted chronologically
        ]);

        const flightMap = {};
        flights.forEach(f => {
            if (!f.date || !f.flight) return;
            const dateKey = moment(f.date).format("YYYY-MM-DD");
            const key = `${dateKey}_${f.flight.trim()}`;
            if (!flightMap[key]) flightMap[key] = [];
            flightMap[key].push(f);
        });

        const metricsMap = {};
        const groundMap = {}; // Helper to easily find grounded MSNs: groundMap['DD MMM YY']['4120'] = "C-check"

        // 2. Process Ground Days for AIRCRAFT
        groundDays.forEach(gd => {
            const msn = gd.msn;
            if (!msn) return;
            const dateStr = moment(gd.date).format("DD MMM YY");

            // Populate helper map
            if (!groundMap[dateStr]) groundMap[dateStr] = {};
            groundMap[dateStr][msn] = gd.event || "Maintenance";

            // Add to main metrics map for the Aircraft row
            if (!metricsMap[msn]) metricsMap[msn] = {};
            metricsMap[msn][dateStr] = {
                status: "maintenance",
                label: gd.event || "Maintenance"
            };
        });

        // 3. Process Assignments for AIRCRAFT
        assignments.forEach(assign => {
            const msn = assign.aircraft?.msn;
            if (!msn) return;

            const dateStr = moment(assign.date).format("DD MMM YY");
            const dateKey = moment(assign.date).format("YYYY-MM-DD");
            const fKey = `${dateKey}_${assign.flightNumber.trim()}`;

            if (!metricsMap[msn]) metricsMap[msn] = {};

            // Only add assignment if there isn't already a maintenance event for this day
            if (!metricsMap[msn][dateStr] || metricsMap[msn][dateStr].status !== "maintenance") {
                if (!metricsMap[msn][dateStr]) {
                    metricsMap[msn][dateStr] = { status: "aircraft-assigned", label: "", bh: 0, fh: 0, dep: 0 };
                }

                const matchedFlights = flightMap[fKey] || [];
                matchedFlights.forEach(f => {
                    metricsMap[msn][dateStr].bh += (f.bh || 0);
                    metricsMap[msn][dateStr].fh += (f.fh || 0);
                    metricsMap[msn][dateStr].dep += 1;
                });
            }
        });

        Object.keys(metricsMap).forEach(msn => {
            Object.keys(metricsMap[msn]).forEach(dateStr => {
                const data = metricsMap[msn][dateStr];
                if (data.status === "aircraft-assigned") {
                    data.label = data.bh > 0 ? data.bh.toFixed(2) : "0";
                }
            });
        });

        // 4. NEW: INHERIT GROUND DAYS FOR ENGINES & APUs
        const daysInMonth = moment(month, "MMMM YYYY").daysInMonth();
        const monthStart = moment(month, "MMMM YYYY").startOf('month');

        for (let i = 0; i < daysInMonth; i++) {
            const currentDay = moment(monthStart).add(i, 'days');
            const dDisp = currentDay.format("DD MMM YY");

            // Find the active configuration for this specific day
            const latestOnwingPerMsn = {};
            onwings.forEach(ow => {
                if (moment(ow.date).isSameOrBefore(currentDay)) {
                    latestOnwingPerMsn[ow.msn] = ow; // Overwrites until we get the latest valid record for this day
                }
            });

            // If an Aircraft (MSN) is grounded today, ground its attached Engines and APU
            Object.keys(latestOnwingPerMsn).forEach(msn => {
                if (groundMap[dDisp] && groundMap[dDisp][msn]) {
                    const eventLabel = groundMap[dDisp][msn];
                    const config = latestOnwingPerMsn[msn];

                    // Gather all attached assets
                    const attachedAssets = [config.pos1Esn, config.pos2Esn, config.apun].filter(Boolean);

                    // Assign the maintenance event to the Engines/APU in the metrics map
                    attachedAssets.forEach(assetSn => {
                        const snKey = String(assetSn).trim();
                        if (!metricsMap[snKey]) metricsMap[snKey] = {};

                        metricsMap[snKey][dDisp] = {
                            status: "maintenance",
                            label: eventLabel
                        };
                    });
                }
            });
        }

        res.status(200).json({ data: metricsMap });
    } catch (error) {
        console.error("🔥 Error fetching fleet metrics:", error);
        res.status(500).json({ message: "Failed to fetch metrics", error: error.message });
    }
};



exports.getFleetMonths = async (req, res) => {
    try {
        // Assuming your verifyToken middleware sets req.userId or req.user.id
        const userId = req.userId || req.user?.id;

        if (!userId) {
            return res.status(400).json({ message: "User ID missing from token" });
        }

        // Aggregate unique year-month combinations from the FLIGHT table
        const distinctDates = await Flight.aggregate([
            { $match: { userId: String(userId), date: { $exists: true, $ne: null } } },
            {
                $group: {
                    _id: {
                        year: { $year: "$date" },
                        month: { $month: "$date" }
                    }
                }
            },
            { $sort: { "_id.year": 1, "_id.month": 1 } }
        ]);

        const monthNames = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ];

        // Format to "Month Year" (e.g., "October 2025")
        const formattedMonths = distinctDates.map(
            d => `${monthNames[d._id.month - 1]} ${d._id.year}`
        );

        res.status(200).json({ months: formattedMonths });
    } catch (error) {
        console.error("🔥 Error fetching fleet months:", error);
        res.status(500).json({ message: "Failed to fetch months", error: error.message });
    }
};

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

        // --------------------------------------------------------
        // STEP 1: Save data to the main Fleet table
        // --------------------------------------------------------
        const fleetBulkOps = fleetData.map((asset, index) => {
            const updateData = { ...asset };

            // Auto-uppercase registration
            if (updateData.regn) updateData.regn = updateData.regn.trim().toUpperCase();

            // Ensure SN exists
            if (!updateData.sn) {
                throw new Error(`Asset at row ${index + 1} is missing a Serial Number (SN)`);
            }

            delete updateData.id;
            delete updateData._id;

            return {
                updateOne: {
                    filter: { sn: asset.sn.trim() },
                    update: { $set: updateData },
                    upsert: true
                }
            };
        });

        if (fleetBulkOps.length > 0) {
            await Fleet.bulkWrite(fleetBulkOps, { ordered: false });
        }

        // --------------------------------------------------------
        // STEP 2: Auto-populate AircraftOnwing Table
        // --------------------------------------------------------
        const aircraftMap = {};

        // First pass: Find all Aircraft and create base configurations mapped by their Registration
        fleetData.forEach(asset => {
            if (asset.category === 'Aircraft' && asset.regn && asset.sn) {
                const regnKey = asset.regn.trim().toUpperCase();
                aircraftMap[regnKey] = {
                    msn: asset.sn.trim(),
                    // Use the fleet entry date. If missing, default to current date.
                    date: asset.entry ? new Date(asset.entry) : new Date(),
                    pos1Esn: null,
                    pos2Esn: null,
                    apun: null
                };
            }
        });

        // Second pass: Find Engines and APUs, and attach them to the correct Aircraft configuration
        fleetData.forEach(asset => {
            if (!asset.titled || !asset.sn) return;

            // e.g., "VT-DKU #1"
            const titleStr = asset.titled.trim().toUpperCase();

            if (asset.category === 'Engine') {
                if (titleStr.endsWith('#1')) {
                    // Extract "VT-DKU" from "VT-DKU #1"
                    const regn = titleStr.replace('#1', '').trim();
                    if (aircraftMap[regn]) aircraftMap[regn].pos1Esn = asset.sn.trim();
                }
                else if (titleStr.endsWith('#2')) {
                    // Extract "VT-DKU" from "VT-DKU #2"
                    const regn = titleStr.replace('#2', '').trim();
                    if (aircraftMap[regn]) aircraftMap[regn].pos2Esn = asset.sn.trim();
                }
            }
            else if (asset.category === 'APU') {
                // For APU, the title usually directly matches the Aircraft Registration (e.g., "VT-DKU")
                if (aircraftMap[titleStr]) aircraftMap[titleStr].apun = asset.sn.trim();
            }
        });

        // Build Bulk Operations for AircraftOnwing
        const onwingOps = [];
        Object.values(aircraftMap).forEach(config => {
            // Only create an Onwing record if at least one component (Engine or APU) is attached
            if (config.pos1Esn || config.pos2Esn || config.apun) {
                onwingOps.push({
                    updateOne: {
                        // Match by MSN and Date so we update existing configs for that specific date instead of duplicating
                        filter: { msn: config.msn, date: config.date },
                        update: {
                            $set: {
                                pos1Esn: config.pos1Esn,
                                pos2Esn: config.pos2Esn,
                                apun: config.apun
                            }
                        },
                        upsert: true
                    }
                });
            }
        });

        // Execute Bulk Write for Onwing Data
        if (onwingOps.length > 0) {
            await AircraftOnwing.bulkWrite(onwingOps, { ordered: false });
        }

        res.status(200).json({ message: "Fleet data and Onwing configurations saved successfully!" });
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