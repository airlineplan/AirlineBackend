const xlsx = require('xlsx');
const Assignment = require('../model/assignment');
const Flight = require('../model/flight');
const moment = require('moment');
const { buildAssignmentSyncPlan } = require('../utils/assignmentSync');

// 🛠️ FIX 1: Enforce UTC to prevent dates drifting by 1 day based on server timezone
const parseExcelDate = (value) => {
    if (!value) return null;
    if (value instanceof Date && !isNaN(value)) {
        return moment.utc(moment(value).format('YYYY-MM-DD')).toDate();
    }
    if (!isNaN(value)) {
        const excelEpoch = Date.UTC(1899, 11, 30);
        return moment.utc(excelEpoch + value * 86400000).toDate();
    }
    const formats = ["DD-MM-YYYY", "DD/MM/YYYY", "YYYY-MM-DD", "MM/DD/YYYY", "DD-MMM-YY", "D MMM YY"];
    const m = moment.utc(value, formats, true);
    return m.isValid() ? m.startOf('day').toDate() : null;
};

// Helper to find Excel columns even if they have weird spaces or casing
const getExcelValue = (row, possibleKeys) => {
    for (const key of Object.keys(row)) {
        const cleanKey = key.toLowerCase().replace(/[^a-z0-9]/g, '');
        if (possibleKeys.includes(cleanKey)) return row[key];
    }
    return null;
};

exports.uploadAssignments = async (req, res) => {
    try {
        const userId = req.user?.id;

        if (!userId) {
            return res.status(401).json({ message: "Unauthorized user context missing" });
        }

        if (!req.file) {
            return res.status(400).json({ message: "No file uploaded" });
        }

        const workbook = xlsx.readFile(req.file.path, { cellDates: true });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rawData = xlsx.utils.sheet_to_json(sheet, { raw: false });

        const validRows = [];

        for (let i = 0; i < rawData.length; i++) {
            const row = rawData[i];

            // 🛡️ FIX 1: Robust Header Extraction
            const dateStr = getExcelValue(row, ['date']);
            const flightNum = getExcelValue(row, ['flightnumber', 'flight', 'flightno', 'flight number', 'flight no', 'flight #', 'flight#', 'Flight #', 'Flight # ']);
            const acft = getExcelValue(row, ['acft', 'registration', 'aircraft']);

            const parsedDate = parseExcelDate(dateStr);

            if (!parsedDate || !flightNum || !acft) {
                continue;
            }

            const flight = String(flightNum).trim().toUpperCase();
            const cleanAcft = String(acft).trim().toUpperCase();
            const dateKey = moment.utc(parsedDate).format("YYYY-MM-DD");

            validRows.push({
                assignDate: parsedDate,
                dateKey,
                flight,
                acft: cleanAcft
            });
        }

        if (validRows.length === 0) {
            return res.status(400).json({ message: "No valid rows found. Check your Excel column names." });
        }
        const { assignmentBulkOps, flightBulkOps, diagnostics } = await buildAssignmentSyncPlan({
            userId,
            rows: validRows,
        });

        const dbPromises = [];
        if (assignmentBulkOps.length > 0) dbPromises.push(Assignment.bulkWrite(assignmentBulkOps, { ordered: false }));
        if (flightBulkOps.length > 0) dbPromises.push(Flight.bulkWrite(flightBulkOps, { ordered: false }));

        await Promise.all(dbPromises);

        res.status(200).json({
            message: "Upload and Flight Sync complete",
            diagnostics,
        });

    } catch (error) {
        console.error("🔥 Upload Error:", error);
        res.status(500).json({ message: "Failed to process assignments", error: error.message });
    }
};

exports.getWeeklyAssignments = async (req, res) => {
    try {
        const userId = req.user?.id;
        if (!userId) {
            return res.status(401).json({ message: "Unauthorized user context missing" });
        }

        const { weekEnding } = req.query;
        if (!weekEnding) {
            return res.status(400).json({ message: "weekEnding parameter is required" });
        }
        // 🛠️ FIX 5: Enforce UTC bounds on query to align with standard Mongoose DB queries
        const endDate = moment.utc(weekEnding).endOf('day').toDate();
        const startDate = moment.utc(weekEnding).subtract(6, 'days').startOf('day').toDate();

        const assignments = await Assignment.find({
            userId,
            date: { $gte: startDate, $lte: endDate }
        })
            .sort({ rotationNumber: 1, flightNumber: 1, date: 1 })
            .lean();

        res.status(200).json({ data: assignments });
    } catch (error) {
        console.error("🔥 Fetch Error:", error);
        res.status(500).json({ message: "Failed to fetch assignments" });
    }
}
