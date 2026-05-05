const xlsx = require('xlsx');
const Assignment = require('../model/assignment');
const Flight = require('../model/flight');
const moment = require('moment');
const { buildAssignmentSyncPlan } = require('../utils/assignmentSync');

const hasValidationRejections = (diagnostics) => Boolean(
    diagnostics?.rejections &&
    Object.values(diagnostics.rejections).some((count) => Number(count) > 0)
);

const buildUploadMessage = (diagnostics) => {
    const missingFleet = diagnostics?.rejections?.missingFromFleetDB || 0;
    const preEntryDates = diagnostics?.rejections?.preEntryDates || 0;
    const postExitDates = diagnostics?.rejections?.postExitDates || 0;
    const flightNotFound = diagnostics?.rejections?.flightNotFound || 0;
    const variantMismatches = diagnostics?.rejections?.variantMismatches || 0;
    const groundConflicts = diagnostics?.rejections?.groundConflicts || 0;
    const overlaps = diagnostics?.rejections?.acftOverlaps || 0;
    const rejectedRows = Array.isArray(diagnostics?.rejectedRows) ? diagnostics.rejectedRows : [];

    if (!missingFleet && !preEntryDates && !postExitDates && !flightNotFound && !variantMismatches && !groundConflicts && !overlaps) {
        return "Assignments uploaded successfully!";
    }

    const parts = [];

    if (missingFleet) {
        parts.push(
            `${missingFleet} row(s) used ACFT values that did not match a fleet registration for this user. Use a registration like VT-AAB, not an aircraft type.`
        );
    }
    if (preEntryDates) {
        parts.push(`${preEntryDates} row(s) were before the aircraft entry date.`);
    }
    if (postExitDates) {
        parts.push(`${postExitDates} row(s) were after the aircraft exit date.`);
    }
    if (flightNotFound) {
        parts.push(`${flightNotFound} row(s) referenced flights that were not found in the master schedule.`);
    }
    if (variantMismatches) {
        parts.push(`${variantMismatches} row(s) failed aircraft-variant validation.`);
    }
    if (groundConflicts) {
        parts.push(`${groundConflicts} row(s) conflicted with ground-day records.`);
    }
    if (overlaps) {
        parts.push(`${overlaps} row(s) overlapped another assignment for the same aircraft.`);
    }

    if (rejectedRows.length > 0) {
        const sample = rejectedRows[0];
        const sampleLabel = [sample.date, sample.flight, sample.acft].filter(Boolean).join(" / ");
        if (sampleLabel) {
            parts.push(`Example rejected row: ${sampleLabel}.`);
        }
    }

    return `Assignments uploaded with warnings. ${parts.join(" ")}`.trim();
};

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
    const formats = [
        "DD-MM-YYYY",
        "D-MM-YYYY",
        "DD/MM/YYYY",
        "D/MM/YYYY",
        "YYYY-MM-DD",
        "MM/DD/YYYY",
        "M/DD/YYYY",
        "M/D/YYYY",
        "DD-MMM-YYYY",
        "D-MMM-YYYY",
        "DD-MMM-YY",
        "D-MMM-YY",
        "DD MMM YYYY",
        "D MMM YYYY",
        "DD MMM YY",
        "D MMM YY",
    ];
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

        const hasRejections = hasValidationRejections(diagnostics);
        const message = buildUploadMessage(diagnostics);

        if (hasRejections) {
            return res.status(422).json({
                success: false,
                message,
                diagnostics,
            });
        }

        const dbPromises = [];
        if (assignmentBulkOps.length > 0) dbPromises.push(Assignment.bulkWrite(assignmentBulkOps, { ordered: false }));
        if (flightBulkOps.length > 0) dbPromises.push(Flight.bulkWrite(flightBulkOps, { ordered: false }));

        await Promise.all(dbPromises);

        res.status(200).json({
            message,
            success: true,
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
            date: { $gte: startDate, $lte: endDate },
            isValid: true,
        })
            .sort({ rotationNumber: 1, flightNumber: 1, date: 1 })
            .lean();

        if (assignments.length === 0) {
            return res.status(200).json({ data: [] });
        }

        const flightRegexArray = [...new Set(assignments
            .map((assignment) => String(assignment.flightNumber || "").trim().toUpperCase())
            .filter(Boolean))]
            .map((flight) => new RegExp(`^${flight}$`, "i"));

        const flights = flightRegexArray.length > 0
            ? await Flight.find({
                userId,
                date: { $gte: startDate, $lte: endDate },
                flight: { $in: flightRegexArray },
            })
                .select("date flight")
                .lean()
            : [];

        const activeFlightKeys = new Set(flights.map((flight) => (
            `${moment.utc(flight.date).format("YYYY-MM-DD")}_${String(flight.flight || "").trim().toUpperCase()}`
        )));

        const currentAssignments = assignments.filter((assignment) => {
            const key = `${moment.utc(assignment.date).format("YYYY-MM-DD")}_${String(assignment.flightNumber || "").trim().toUpperCase()}`;
            return activeFlightKeys.has(key);
        });

        res.status(200).json({ data: currentAssignments });
    } catch (error) {
        console.error("🔥 Fetch Error:", error);
        res.status(500).json({ message: "Failed to fetch assignments" });
    }
}

exports.__testables__ = {
    parseExcelDate,
    getExcelValue,
    hasValidationRejections,
    buildUploadMessage,
};
