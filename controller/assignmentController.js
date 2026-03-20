const xlsx = require('xlsx');
const Assignment = require('../model/assignment');
const Flight = require('../model/flight');
const Fleet = require('../model/fleet');
const GroundDay = require('../model/groundDay'); // 👉 IMPORT THE NEW MODEL
const moment = require('moment');

const parseExcelDate = (value) => {
    if (!value) return null;
    if (value instanceof Date && !isNaN(value)) {
        return moment(value).startOf('day').toDate();
    }
    if (!isNaN(value)) {
        const excelEpoch = new Date(1899, 11, 30);
        return moment(new Date(excelEpoch.getTime() + value * 86400000)).startOf('day').toDate();
    }
    const formats = ["DD-MM-YYYY", "DD/MM/YYYY", "YYYY-MM-DD", "MM/DD/YYYY", "DD-MMM-YY", "D MMM YY"];
    const m = moment(value, formats, true);
    return m.isValid() ? m.startOf('day').toDate() : null;
};

exports.uploadAssignments = async (req, res) => {
    try {
        console.time("⚡ UploadProcessing");

        if (!req.file) {
            return res.status(400).json({ message: "No file uploaded" });
        }

        const workbook = xlsx.readFile(req.file.path, { cellDates: true });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rawData = xlsx.utils.sheet_to_json(sheet, { raw: false });

        const validRows = [];
        const flightSet = new Set();
        const dateSet = new Set();
        const acftSet = new Set();

        let skipped = 0;

        // 🔥 PASS 1: CLEAN + NORMALIZE
        for (let i = 0; i < rawData.length; i++) {
            const row = rawData[i];

            const dateStr = row['Date'] || row['date'] || row['DATE'] || row['date '];
            const flightNum = row['Flight Number'] || row['flight number'] || row['flight #'] || row['flight#'] || row['Flight #'] || row['flight'] || row['flightno'];
            const acft = row['ACFT'] || row['acft'] || row['registration'];

            const parsedDate = parseExcelDate(dateStr);

            if (!parsedDate || !flightNum || !acft) {
                skipped++;
                continue;
            }

            const flight = flightNum.toString().trim().toUpperCase();
            const cleanAcft = acft.toString().trim().toUpperCase();
            const dateKey = moment(parsedDate).format("YYYY-MM-DD");

            validRows.push({
                assignDate: parsedDate,
                dateKey,
                flight,
                acft: cleanAcft
            });

            flightSet.add(flight);
            dateSet.add(parsedDate.getTime());
            acftSet.add(cleanAcft);
        }

        if (validRows.length === 0) {
            return res.status(400).json({ message: "No valid rows found" });
        }

        const minDate = new Date(Math.min(...dateSet));
        const maxDate = new Date(Math.max(...dateSet));
        const flightArray = [...flightSet];
        const acftArray = [...acftSet];

        // 🔥 FETCH FLIGHTS, FLEET, AND GROUND DAYS IN PARALLEL
        const [flights, fleetData, groundDays] = await Promise.all([
            Flight.find({
                date: { $gte: minDate, $lte: maxDate },
                flight: { $in: flightArray }
            }).select('flight date rotationNumber').lean(),

            Fleet.find({
                regn: { $in: acftArray }
            }).select('sn regn entry exit').lean(), // Added 'sn' to select

            GroundDay.find({
                date: { $gte: minDate, $lte: maxDate }
            }).select('msn date event').lean()
        ]);

        // 🔥 BUILD FAST IN-MEMORY MAPS
        const flightMap = new Map();
        for (const f of flights) {
            const key = `${moment(f.date).format("YYYY-MM-DD")}_${f.flight.toUpperCase()}`;
            flightMap.set(key, f);
        }

        const fleetMap = new Map();
        for (const asset of fleetData) {
            fleetMap.set(asset.regn.toUpperCase(), asset);
        }

        const groundDayMap = new Map();
        for (const gd of groundDays) {
            // Key format: YYYY-MM-DD_MSN
            const key = `${moment(gd.date).format("YYYY-MM-DD")}_${gd.msn}`;
            groundDayMap.set(key, gd);
        }

        // 🔥 BUILD BULK OPS WITH ALL VALIDATIONS
        const bulkOperations = [];
        let notFoundCount = 0;
        let outsideDatesCount = 0;
        let groundConflictCount = 0;

        for (const row of validRows) {
            const flightKey = `${row.dateKey}_${row.flight}`;
            const flightRecord = flightMap.get(flightKey);
            const fleetRecord = fleetMap.get(row.acft);

            const errors = [];
            let isValid = true;
            let removedReason = null;
            let assignedAcft = row.acft; // Assume it works initially

            // 1. Flight Validation
            if (!flightRecord) {
                notFoundCount++;
                isValid = false;
                errors.push("Flight not found in master schedule");
            }

            // 2. Fleet Validations
            if (fleetRecord) {
                const assignMom = moment(row.assignDate);
                const entryMom = fleetRecord.entry ? parseExcelDate(fleetRecord.entry) : null;
                const exitMom = fleetRecord.exit ? parseExcelDate(fleetRecord.exit) : null;
                const msn = fleetRecord.sn; // Extract MSN from fleet data

                // Rule A: Outside Fleet Dates
                if (entryMom && assignMom.isBefore(moment(entryMom).startOf('day'))) {
                    isValid = false;
                    assignedAcft = null;
                    removedReason = "OUTSIDE_FLEET_DATES";
                    errors.push(`Date precedes fleet entry (${moment(entryMom).format('DD-MMM-YY')})`);
                    outsideDatesCount++;
                }

                if (exitMom && assignMom.isAfter(moment(exitMom).endOf('day'))) {
                    isValid = false;
                    assignedAcft = null;
                    removedReason = "OUTSIDE_FLEET_DATES";
                    errors.push(`Date succeeds fleet exit (${moment(exitMom).format('DD-MMM-YY')})`);
                    outsideDatesCount++;
                }

                // Rule B: Ground Day Conflict (Scheduled Maintenance)
                const groundKey = `${row.dateKey}_${msn}`;
                const groundRecord = groundDayMap.get(groundKey);

                if (groundRecord) {
                    isValid = false;
                    assignedAcft = null;
                    removedReason = "GROUND_DAY_CONFLICT";
                    const eventName = groundRecord.event ? ` (${groundRecord.event})` : '';
                    errors.push(`Aircraft ${msn} is on ground${eventName} for this date`);
                    groundConflictCount++;
                }
            }

            // Get Rotation Number
            let rotationNum = null;
            if (flightRecord?.rotationNumber) {
                rotationNum = parseInt(flightRecord.rotationNumber.replace(/\D/g, ''), 10) || null;
            }

            // Create Operation
            bulkOperations.push({
                updateOne: {
                    filter: {
                        date: row.assignDate,
                        flightNumber: row.flight
                    },
                    update: {
                        $set: {
                            date: row.assignDate,
                            flightNumber: row.flight,
                            'aircraft.registration': assignedAcft, // Null if validation fails
                            rotationNumber: rotationNum,
                            isValid: isValid,
                            validationErrors: errors,
                            removedReason: removedReason
                        }
                    },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await Assignment.bulkWrite(bulkOperations, { ordered: false });
        }

        console.timeEnd("⚡ UploadProcessing");

        res.status(200).json({
            message: "Upload complete with all validations",
            total: rawData.length,
            valid: validRows.length,
            skipped,
            flightNotFound: notFoundCount,
            fleetDateViolations: outsideDatesCount,
            groundConflicts: groundConflictCount // Added to the response payload!
        });

    } catch (error) {
        console.error("🔥 Upload Error:", error);
        res.status(500).json({ message: "Failed to process assignments", error: error.message });
    }
};


exports.getWeeklyAssignments = async (req, res) => {
    try {
        const { weekEnding } = req.query;
        if (!weekEnding) {
            return res.status(400).json({ message: "weekEnding parameter is required" });
        }
        const endDate = moment(weekEnding).endOf('day').toDate();
        const startDate = moment(weekEnding).subtract(6, 'days').startOf('day').toDate();
        const assignments = await Assignment.find({ date: { $gte: startDate, $lte: endDate } })
            .sort({ rotationNumber: 1, flightNumber: 1, date: 1 })
            .lean();
        res.status(200).json({ data: assignments });
    } catch (error) {
        console.error("🔥 Fetch Error:", error);
        res.status(500).json({ message: "Failed to fetch assignments" });
    }
}