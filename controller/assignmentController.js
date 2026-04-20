const xlsx = require('xlsx');
const Assignment = require('../model/assignment');
const Flight = require('../model/flight');
const Fleet = require('../model/fleet');
const GroundDay = require('../model/groundDay');
const moment = require('moment');

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

const normalizeSnForCompare = (value) => {
    if (value === null || value === undefined) return '';
    const raw = String(value).trim().toUpperCase();
    if (!raw) return '';
    const digitsOnly = raw.replace(/\D/g, '');
    return digitsOnly || raw;
};

const parseLegNumberFromFlight = (flightRecord) => {
    if (!flightRecord) return null;

    if (flightRecord.legNumber !== undefined && flightRecord.legNumber !== null) {
        const leg = parseInt(String(flightRecord.legNumber).replace(/\D/g, ''), 10);
        if (!Number.isNaN(leg)) return leg;
    }

    if (flightRecord.addedByRotation) {
        const addedBy = String(flightRecord.addedByRotation);
        const parts = addedBy.split('-');
        if (parts.length >= 2) {
            const leg = parseInt(String(parts[1]).replace(/\D/g, ''), 10);
            if (!Number.isNaN(leg)) return leg;
        }
    }

    return null;
};

const pickFleetRecordForDate = (fleetRecordsForRegn, assignDate) => {
    if (!Array.isArray(fleetRecordsForRegn) || fleetRecordsForRegn.length === 0) {
        return null;
    }

    const assignMom = moment.utc(assignDate).startOf('day');
    for (const record of fleetRecordsForRegn) {
        const entryMom = record.entry ? moment.utc(record.entry).startOf('day') : null;
        const exitMom = record.exit ? moment.utc(record.exit).endOf('day') : null;
        const isBeforeEntry = entryMom && assignMom.isBefore(entryMom);
        const isAfterExit = exitMom && assignMom.isAfter(exitMom);
        if (!isBeforeEntry && !isAfterExit) return record;
    }

    return fleetRecordsForRegn[0];
};

exports.uploadAssignments = async (req, res) => {
    try {
        console.time("⚡ UploadProcessing");
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
        const flightSet = new Set();
        const dateSet = new Set();
        const acftSet = new Set();

        let skipped = 0;

        for (let i = 0; i < rawData.length; i++) {
            const row = rawData[i];

            // 🛡️ FIX 1: Robust Header Extraction
            const dateStr = getExcelValue(row, ['date']);
            const flightNum = getExcelValue(row, ['flightnumber', 'flight', 'flightno', 'flight number', 'flight no', 'flight #', 'flight#', 'Flight #', 'Flight # ']);
            const acft = getExcelValue(row, ['acft', 'registration', 'aircraft']);

            const parsedDate = parseExcelDate(dateStr);

            if (!parsedDate || !flightNum || !acft) {
                skipped++;
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

            flightSet.add(flight);
            dateSet.add(parsedDate.getTime());
            acftSet.add(cleanAcft);
        }

        if (validRows.length === 0) {
            return res.status(400).json({ message: "No valid rows found. Check your Excel column names." });
        }

        const minDate = new Date(Math.min(...dateSet));
        const maxDate = new Date(Math.max(...dateSet));

        // 🛡️ FIX 2: Case-Insensitive regex for MongoDB lookups
        const flightRegexArray = [...flightSet].map(f => new RegExp(`^${f}$`, 'i'));
        const acftRegexArray = [...acftSet].map(a => new RegExp(`^${a}$`, 'i'));

        const [flights, fleetData, groundDays] = await Promise.all([
            Flight.find({
                userId,
                date: { $gte: minDate, $lte: maxDate },
                flight: { $in: flightRegexArray }
            }).select('_id flight date rotationNumber addedByRotation legNumber').lean(),

            Fleet.find({
                userId,
                category: "Aircraft",
                regn: { $in: acftRegexArray }
            }).select('sn regn entry exit').lean(),

            GroundDay.find({
                userId,
                date: { $gte: minDate, $lte: maxDate }
            }).select('msn date event').lean()
        ]);

        const flightMap = new Map();
        for (const f of flights) {
            if (!f.flight) continue;
            const key = `${moment.utc(f.date).format("YYYY-MM-DD")}_${String(f.flight).trim().toUpperCase()}`;
            flightMap.set(key, f);
        }

        const fleetMap = new Map();
        for (const asset of fleetData) {
            if (!asset.regn) continue;
            const regnKey = String(asset.regn).trim().toUpperCase();
            if (!fleetMap.has(regnKey)) fleetMap.set(regnKey, []);
            fleetMap.get(regnKey).push(asset);
        }

        const groundDayMap = new Map();
        for (const gd of groundDays) {
            if (!gd.msn) continue;
            const normalizedSn = normalizeSnForCompare(gd.msn);
            if (!normalizedSn) continue;
            const key = `${moment.utc(gd.date).format("YYYY-MM-DD")}_${normalizedSn}`;
            groundDayMap.set(key, gd);
        }

        const processedRowsByFlightKey = new Map();
        let duplicateComboCount = 0;

        // Diagnostic Counters
        let notFoundCount = 0;
        let missingFleetDBCount = 0;
        let preEntryCount = 0;
        let postExitCount = 0;
        let groundConflictCount = 0;
        let successfulAcftLinks = 0;

        for (const row of validRows) {
            const flightKey = `${row.dateKey}_${row.flight}`;
            const flightRecord = flightMap.get(flightKey);
            const fleetRecordsForRegn = fleetMap.get(row.acft) || [];
            const fleetRecord = pickFleetRecordForDate(fleetRecordsForRegn, row.assignDate);

            const errors = [];
            let isValid = true;
            let removedReason = null;
            let assignedAcft = row.acft;

            if (!flightRecord) {
                notFoundCount++;
                isValid = false;
                errors.push("Flight not found in master schedule");
            }

            // 🛡️ THE VALIDATION GAUNTLET
            if (!fleetRecord) {
                isValid = false;
                assignedAcft = null;
                missingFleetDBCount++;
                errors.push(`Aircraft ${row.acft} not found in Fleet master`);
            } else {
                const assignMom = moment.utc(row.assignDate);
                // Safe date comparisons
                const entryMom = fleetRecord.entry ? moment.utc(fleetRecord.entry).startOf('day') : null;
                const exitMom = fleetRecord.exit ? moment.utc(fleetRecord.exit).endOf('day') : null;
                const msn = normalizeSnForCompare(fleetRecord.sn);

                if (entryMom && assignMom.isBefore(entryMom)) {
                    isValid = false;
                    assignedAcft = null;
                    removedReason = "OUTSIDE_FLEET_DATES";
                    preEntryCount++;
                    errors.push(`Date precedes fleet entry`);
                }
                else if (exitMom && assignMom.isAfter(exitMom)) {
                    isValid = false;
                    assignedAcft = null;
                    removedReason = "OUTSIDE_FLEET_DATES";
                    postExitCount++;
                    errors.push(`Date succeeds fleet exit`);
                }
                else {
                    const groundKey = `${row.dateKey}_${msn}`;
                    const groundRecord = groundDayMap.get(groundKey);

                    if (groundRecord) {
                        isValid = false;
                        assignedAcft = null;
                        removedReason = "GROUND_DAY_CONFLICT";
                        groundConflictCount++;
                        errors.push(`Aircraft ${msn} is on ground for this date`);
                    }
                }
            }

            if (assignedAcft) successfulAcftLinks++;

            let rotationNum = null;
            if (flightRecord?.rotationNumber) {
                rotationNum = parseInt(String(flightRecord.rotationNumber).replace(/\D/g, ''), 10) || null;
            }
            const legNumber = parseLegNumberFromFlight(flightRecord);

            // 🛡️ FIX 3: Safe MSN Parsing (Strips letters if SN is "MSN-1234")
            let msnVal = null;
            if (fleetRecord && assignedAcft && fleetRecord.sn) {
                const strippedSn = String(fleetRecord.sn).replace(/\D/g, ''); // Keeps only numbers
                if (strippedSn.length > 0) msnVal = Number(strippedSn);
            }

            if (processedRowsByFlightKey.has(flightKey)) {
                duplicateComboCount++;
                continue;
            }

            processedRowsByFlightKey.set(flightKey, {
                row,
                flightRecord,
                assignedAcft,
                msnVal,
                rotationNum,
                legNumber,
                isValid,
                errors,
                removedReason
            });
        }

        const assignmentBulkOps = [];
        const flightBulkOps = [];
        for (const item of processedRowsByFlightKey.values()) {
            const {
                row,
                flightRecord,
                assignedAcft,
                msnVal,
                rotationNum,
                legNumber,
                isValid,
                errors,
                removedReason
            } = item;

            assignmentBulkOps.push({
                updateOne: {
                    filter: { userId, date: row.assignDate, flightNumber: row.flight },
                    update: {
                        $set: {
                            userId,
                            date: row.assignDate,
                            flightNumber: row.flight,
                            'aircraft.registration': assignedAcft,
                            'aircraft.msn': msnVal,
                            rotationNumber: rotationNum,
                            legNumber,
                            isValid: isValid,
                            validationErrors: errors,
                            removedReason: removedReason
                        }
                    },
                    upsert: true
                }
            });

            if (flightRecord && flightRecord._id) {
                flightBulkOps.push({
                    updateOne: {
                        filter: { _id: flightRecord._id, userId },
                        update: {
                            $set: {
                                'aircraft.registration': assignedAcft,
                                'aircraft.msn': msnVal
                            }
                        }
                    }
                });
            }
        }

        const dbPromises = [];
        if (assignmentBulkOps.length > 0) dbPromises.push(Assignment.bulkWrite(assignmentBulkOps, { ordered: false }));
        if (flightBulkOps.length > 0) dbPromises.push(Flight.bulkWrite(flightBulkOps, { ordered: false }));

        await Promise.all(dbPromises);
        console.timeEnd("⚡ UploadProcessing");

        // 🛡️ DIAGNOSTIC LOGGING: Watch your terminal!
        console.log("=== UPLOAD DIAGNOSTICS ===");
        console.log(`Total Rows Processed: ${validRows.length}`);
        console.log(`✅ Successfully Assigned ACFT: ${successfulAcftLinks}`);
        console.log(`❌ Failed: Missing from Fleet DB: ${missingFleetDBCount}`);
        console.log(`❌ Failed: Pre-Entry Date: ${preEntryCount}`);
        console.log(`❌ Failed: Post-Exit Date: ${postExitCount}`);
        console.log(`❌ Failed: Ground Day Conflict: ${groundConflictCount}`);
        console.log("==========================");

        res.status(200).json({
            message: "Upload and Flight Sync complete",
                diagnostics: {
                    totalValidRows: validRows.length,
                    uniqueFlightsProcessed: processedRowsByFlightKey.size,
                    duplicateDateFlightCombosSkipped: duplicateComboCount,
                    successfullyAssigned: successfulAcftLinks,
                    rejections: {
                        missingFromFleetDB: missingFleetDBCount,
                    preEntryDates: preEntryCount,
                    postExitDates: postExitCount,
                    groundConflicts: groundConflictCount
                }
            }
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
