const Fleet = require('../model/fleet');
const Flight = require('../model/flight'); // Add this to query flight dates
const Assignment = require('../model/assignment'); // Adjust path if needed
const GroundDay = require('../model/groundDay');
const AircraftOnwing = require('../model/aircraftOnwing');
const moment = require('moment');

const getUserIdFromReq = (req) => req.user?.id || req.userId || req.user?.userId || req.user?._id;
const normalizeSnKey = (value) => {
    if (value === null || value === undefined) return "";
    const raw = String(value).trim().toUpperCase();
    if (!raw) return "";
    const digitsOnly = raw.replace(/\D/g, "");
    return digitsOnly || raw;
};
const createAssignedMetricEntry = (metric = {}) => ({
    status: "aircraft-assigned",
    label: Number(metric.bh) > 0 ? Number(metric.bh).toFixed(2) : "0",
    bh: Number(metric.bh) || 0,
    fh: Number(metric.fh) || 0,
    dep: Number(metric.dep) || 0
});

exports.getFleetScheduleMetrics = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });

        const { month } = req.query;
        if (!month) return res.status(400).json({ message: "Month is required" });

        const startDt = moment.utc(month, "MMMM YYYY").startOf('month').toDate();
        const endDt = moment.utc(month, "MMMM YYYY").endOf('month').toDate();

        const [groundDays, assignments, flights, onwings] = await Promise.all([
            GroundDay.find({ userId, date: { $gte: startDt, $lte: endDt } })
                .select("msn date event")
                .lean(),
            Assignment.find({ userId, date: { $gte: startDt, $lte: endDt }, isValid: true })
                .select("date flightNumber aircraft.msn")
                .lean(),
            Flight.find({ userId, date: { $gte: startDt, $lte: endDt } })
                .select("date flight bh fh")
                .lean(),
            AircraftOnwing.find({ userId, date: { $lte: endDt } })
                .select("msn date pos1Esn pos2Esn apun")
                .sort({ date: 1, msn: 1, _id: 1 })
                .lean()
        ]);

        const flightMap = new Map();
        flights.forEach((f) => {
            if (!f.date || !f.flight) return;
            const dateKey = moment.utc(f.date).format("YYYY-MM-DD");
            const key = `${dateKey}_${String(f.flight).trim().toUpperCase()}`;
            if (!flightMap.has(key)) flightMap.set(key, []);
            flightMap.get(key).push(f);
        });

        const metricsMap = {};
        const groundMap = {};

        groundDays.forEach((gd) => {
            const snKey = normalizeSnKey(gd.msn);
            if (!snKey) return;
            const dateStr = moment.utc(gd.date).format("DD MMM YY");

            if (!groundMap[dateStr]) groundMap[dateStr] = {};
            groundMap[dateStr][snKey] = gd.event || "Maintenance";

            if (!metricsMap[snKey]) metricsMap[snKey] = {};
            metricsMap[snKey][dateStr] = {
                status: "maintenance",
                label: gd.event || "Maintenance",
                bh: 0,
                fh: 0,
                dep: 0,
                event: gd.event || "Maintenance"
            };
        });

        assignments.forEach((assign) => {
            const snKey = normalizeSnKey(assign.aircraft?.msn);
            if (!snKey) return;

            const dateStr = moment.utc(assign.date).format("DD MMM YY");
            const dateKey = moment.utc(assign.date).format("YYYY-MM-DD");
            const fKey = `${dateKey}_${String(assign.flightNumber || "").trim().toUpperCase()}`;

            if (!metricsMap[snKey]) metricsMap[snKey] = {};

            if (!metricsMap[snKey][dateStr] || metricsMap[snKey][dateStr].status !== "maintenance") {
                if (!metricsMap[snKey][dateStr]) {
                    metricsMap[snKey][dateStr] = createAssignedMetricEntry();
                }

                const matchedFlights = flightMap.get(fKey) || [];
                matchedFlights.forEach((f) => {
                    metricsMap[snKey][dateStr].bh += Number(f.bh) || 0;
                    metricsMap[snKey][dateStr].fh += Number(f.fh) || 0;
                    metricsMap[snKey][dateStr].dep += 1;
                });
            }
        });

        Object.keys(metricsMap).forEach((snKey) => {
            Object.keys(metricsMap[snKey]).forEach((dateStr) => {
                const data = metricsMap[snKey][dateStr];
                if (data.status === "aircraft-assigned") {
                    data.label = data.bh > 0 ? data.bh.toFixed(2) : "0";
                }
            });
        });

        const daysInMonth = moment.utc(month, "MMMM YYYY").daysInMonth();
        const monthStart = moment.utc(month, "MMMM YYYY").startOf('month');
        const activeOnwingByMsn = {};
        let onwingIdx = 0;

        for (let i = 0; i < daysInMonth; i++) {
            const currentDay = moment(monthStart).add(i, "days");
            const currentDayEnd = moment(currentDay).endOf("day");
            const dDisp = currentDay.format("DD MMM YY");

            while (
                onwingIdx < onwings.length &&
                moment.utc(onwings[onwingIdx].date).isSameOrBefore(currentDayEnd)
            ) {
                const ow = onwings[onwingIdx];
                const acftSn = normalizeSnKey(ow.msn);
                if (acftSn) activeOnwingByMsn[acftSn] = ow;
                onwingIdx += 1;
            }

            const componentOwnerMap = {};
            Object.entries(activeOnwingByMsn).forEach(([acftSn, config]) => {
                [config.pos1Esn, config.pos2Esn, config.apun].forEach((assetSn) => {
                    const componentSn = normalizeSnKey(assetSn);
                    if (componentSn) componentOwnerMap[componentSn] = acftSn;
                });
            });

            Object.entries(componentOwnerMap).forEach(([componentSn, ownerAcftSn]) => {
                if (!metricsMap[componentSn]) metricsMap[componentSn] = {};

                const existingComponentMetric = metricsMap[componentSn][dDisp];
                if (existingComponentMetric?.status === "maintenance") return;

                const aircraftGroundEvent = groundMap[dDisp]?.[ownerAcftSn];
                if (aircraftGroundEvent) {
                    metricsMap[componentSn][dDisp] = {
                        status: "maintenance",
                        label: aircraftGroundEvent || "Maintenance",
                        bh: 0,
                        fh: 0,
                        dep: 0,
                        event: aircraftGroundEvent
                    };
                    return;
                }

                const aircraftMetric = metricsMap[ownerAcftSn]?.[dDisp];
                if (aircraftMetric?.status === "aircraft-assigned") {
                    metricsMap[componentSn][dDisp] = createAssignedMetricEntry(aircraftMetric);
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
        const userId = getUserIdFromReq(req);

        if (!userId) {
            return res.status(400).json({ message: "User ID missing from token" });
        }

        const dateBounds = await Flight.aggregate([
            { $match: { userId: String(userId), date: { $exists: true, $ne: null } } },
            {
                $group: {
                    _id: null,
                    minDate: { $min: "$date" },
                    maxDate: { $max: "$date" }
                }
            }
        ]);

        if (!dateBounds.length || !dateBounds[0].minDate || !dateBounds[0].maxDate) {
            return res.status(200).json({ months: [] });
        }

        const monthNames = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ];

        const startMonth = moment.utc(dateBounds[0].minDate).startOf("month");
        const endMonth = moment.utc(dateBounds[0].maxDate).startOf("month");
        const formattedMonths = [];
        const cursor = moment.utc(startMonth);

        while (cursor.isSameOrBefore(endMonth, "month")) {
            formattedMonths.push(`${monthNames[cursor.month()]} ${cursor.year()}`);
            cursor.add(1, "month");
        }

        res.status(200).json({ months: formattedMonths });
    } catch (error) {
        console.error("🔥 Error fetching fleet months:", error);
        res.status(500).json({ message: "Failed to fetch months", error: error.message });
    }
};

// 1. GET: Fetch all fleet assets
exports.getAllFleet = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });

        const fleet = await Fleet.find({ userId }).sort({ sno: 1 });
        res.status(200).json({ data: fleet });
    } catch (error) {
        console.error("🔥 Error fetching fleet:", error);
        res.status(500).json({ message: "Failed to fetch fleet data", error: error.message });
    }
};

// 2. POST (Bulk): Create or Update multiple assets at once
exports.bulkUpsertFleet = async (req, res) => {
    try {
        const userId = getUserIdFromReq(req);
        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });

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
            updateData.userId = userId;

            // 👇 ADD THESE TWO LINES: Convert empty strings to null so Mongoose doesn't crash
            if (updateData.entry === "") updateData.entry = null;
            if (updateData.exit === "") updateData.exit = null;
            // 👆 ------------------------------------------------------------------------

            // Ensure SN exists
            if (!updateData.sn) {
                throw new Error(`Asset at row ${index + 1} is missing a Serial Number (SN)`);
            }

            delete updateData.id;
            delete updateData._id;

            return {
                updateOne: {
                    filter: { userId, sn: asset.sn.trim() },
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
                    userId,
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
                        filter: { userId, msn: config.msn, date: config.date },
                        update: {
                            $set: {
                                userId,
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
        const userId = getUserIdFromReq(req);
        if (!userId) return res.status(401).json({ message: "Unauthorized user context missing" });

        const { id } = req.params;

        const deletedAsset = await Fleet.findOneAndDelete({ _id: id, userId });

        if (!deletedAsset) {
            return res.status(404).json({ message: "Asset not found" });
        }

        res.status(200).json({ message: "Asset deleted successfully" });
    } catch (error) {
        console.error("🔥 Delete Error:", error);
        res.status(500).json({ message: "Failed to delete asset", error: error.message });
    }
};
