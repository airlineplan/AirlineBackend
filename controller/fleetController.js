const Fleet = require('../model/fleet');
const Flight = require('../model/flight'); // Add this to query flight dates
const Assignment = require('../model/assignment'); // Adjust path if needed
const GroundDay = require('../model/groundDay');
const moment = require('moment');


exports.getFleetScheduleMetrics = async (req, res) => {
    try {
        const { month } = req.query; // e.g., "October 2025"
        if (!month) return res.status(400).json({ message: "Month is required" });

        const startDt = moment(month, "MMMM YYYY").startOf('month').toDate();
        const endDt = moment(month, "MMMM YYYY").endOf('month').toDate();

        // 1. Fetch data for the specified month
        const [groundDays, assignments, flights] = await Promise.all([
            GroundDay.find({ date: { $gte: startDt, $lte: endDt } }),
            Assignment.find({ date: { $gte: startDt, $lte: endDt }, isValid: true }),
            Flight.find({ date: { $gte: startDt, $lte: endDt } })
        ]);

        // 2. Map FLIGHT master table by "YYYY-MM-DD_FlightNumber" for O(1) lookups
        const flightMap = {};
        flights.forEach(f => {
            if (!f.date || !f.flight) return;
            const dateKey = moment(f.date).format("YYYY-MM-DD");
            const key = `${dateKey}_${f.flight.trim()}`;
            if (!flightMap[key]) flightMap[key] = [];
            flightMap[key].push(f);
        });

        const metricsMap = {}; // Format: { "msn123": { "14 Oct 25": { status, label, bh, fh, dep } } }

        // 3. Process Assignments & Aggregate Metrics
        assignments.forEach(assign => {
            const msn = assign.aircraft?.msn;
            if (!msn) return;

            const dateStr = moment(assign.date).format("DD MMM YY");
            const dateKey = moment(assign.date).format("YYYY-MM-DD");
            const fKey = `${dateKey}_${assign.flightNumber.trim()}`;

            if (!metricsMap[msn]) metricsMap[msn] = {};

            if (!metricsMap[msn][dateStr]) {
                metricsMap[msn][dateStr] = { status: "aircraft-assigned", label: "", bh: 0, fh: 0, dep: 0 };
            }

            // Find matching flights in Master Table and sum BH, FH, Departures
            const matchedFlights = flightMap[fKey] || [];
            matchedFlights.forEach(f => {
                metricsMap[msn][dateStr].bh += (f.bh || 0);
                metricsMap[msn][dateStr].fh += (f.fh || 0);
                metricsMap[msn][dateStr].dep += 1;
            });
        });

        // 4. Finalize Assignment Labels (Sum of Block Hours)
        Object.keys(metricsMap).forEach(msn => {
            Object.keys(metricsMap[msn]).forEach(dateStr => {
                const data = metricsMap[msn][dateStr];
                if (data.status === "aircraft-assigned") {
                    data.label = data.bh > 0 ? data.bh.toFixed(2) : "0";
                }
            });
        });

        // 5. Add Ground Days (Overrides Assignments if there's a conflict)
        groundDays.forEach(gd => {
            const msn = gd.msn;
            if (!msn) return;
            const dateStr = moment(gd.date).format("DD MMM YY");

            if (!metricsMap[msn]) metricsMap[msn] = {};
            metricsMap[msn][dateStr] = {
                status: "maintenance",
                label: gd.event || "C-check"
            };
        });

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