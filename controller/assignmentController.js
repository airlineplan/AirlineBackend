// controllers/assignmentController.js
const xlsx = require('xlsx');
const Assignment = require('../model/assignment');
const Flight = require('../model/flight');
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

    const formats = ["DD-MM-YYYY", "DD/MM/YYYY", "YYYY-MM-DD", "MM/DD/YYYY", "DD-MMM-YY"];
    const m = moment(value, formats, true);

    return m.isValid() ? m.startOf('day').toDate() : null;
};

exports.uploadAssignments = async (req, res) => {
    try {
        console.time("⚡ UploadProcessing");

        if (!req.file) {
            console.log("❌ No file uploaded");
            return res.status(400).json({ message: "No file uploaded" });
        }

        const workbook = xlsx.readFile(req.file.path, { cellDates: true });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rawData = xlsx.utils.sheet_to_json(sheet, { raw: false });

        console.log(`📦 Raw rows: ${rawData.length}`);

        const validRows = [];
        const flightSet = new Set();
        const dateSet = new Set();

        let skipped = 0;

        // 🔥 PASS 1: CLEAN + NORMALIZE
        for (let i = 0; i < rawData.length; i++) {
            const row = rawData[i];

            const dateStr =
                row['Date'] ||
                row['date'] ||
                row['DATE'] ||
                row['date '];
            const flightNum =
                row['Flight Number'] ||
                row['flight number'] ||
                row['flight #'] ||   // 🔥 ADD THIS
                row['flight#'] ||    // 🔥 ADD THIS (extra safety)
                row['Flight #'] ||   // 🔥 ADD THIS
                row['flight'] ||
                row['flightno'];
            const acft = row['ACFT'] || row['acft'] || row['registration'];

            const parsedDate = parseExcelDate(dateStr);

            if (!parsedDate || !flightNum || !acft) {
                skipped++;
                continue;
            }

            const flight = flightNum.toString().trim().toUpperCase();
            const dateKey = moment(parsedDate).format("YYYY-MM-DD");

            validRows.push({
                assignDate: parsedDate,
                dateKey,
                flight,
                acft: acft.toString().trim().toUpperCase()
            });

            flightSet.add(flight);
            dateSet.add(parsedDate.getTime());
        }

        console.log(`✅ Valid rows: ${validRows.length}`);
        console.log(`⚠️ Skipped rows: ${skipped}`);

        if (validRows.length === 0) {
            return res.status(400).json({ message: "No valid rows found" });
        }

        const minDate = new Date(Math.min(...dateSet));
        const maxDate = new Date(Math.max(...dateSet));
        const flightArray = [...flightSet];

        console.log(`📅 Range: ${moment(minDate).format("YYYY-MM-DD")} → ${moment(maxDate).format("YYYY-MM-DD")}`);
        console.log(`✈️ Flights count: ${flightArray.length}`);

        // 🔥 FETCH ONLY REQUIRED FLIGHTS
        const flights = await Flight.find({
            date: { $gte: minDate, $lte: maxDate },
            flight: { $in: flightArray }
        })
            .select('flight date rotationNumber')
            .lean();

        console.log(`📦 Flights fetched: ${flights.length}`);

        // 🔥 MAP
        const flightMap = new Map();
        for (const f of flights) {
            const key = `${moment(f.date).format("YYYY-MM-DD")}_${f.flight.toUpperCase()}`;
            flightMap.set(key, f);
        }

        console.log(`⚡ FlightMap size: ${flightMap.size}`);

        // 🔥 BUILD BULK OPS
        const bulkOperations = [];
        let notFoundCount = 0;

        for (const row of validRows) {
            const key = `${row.dateKey}_${row.flight}`;
            const flightRecord = flightMap.get(key);

            if (!flightRecord) notFoundCount++;

            let rotationNum = null;
            if (flightRecord?.rotationNumber) {
                rotationNum = parseInt(flightRecord.rotationNumber.replace(/\D/g, ''), 10) || null;
            }

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
                            'aircraft.registration': row.acft,
                            rotationNumber: rotationNum,
                            isValid: !!flightRecord,
                            validationErrors: flightRecord ? [] : ["Flight not found"]
                        }
                    },
                    upsert: true
                }
            });
        }

        console.log(`❌ Flights not found: ${notFoundCount}`);
        console.log(`🚀 Bulk ops: ${bulkOperations.length}`);

        if (bulkOperations.length > 0) {
            const result = await Assignment.bulkWrite(bulkOperations, { ordered: false });
            console.log(`✅ Inserted: ${result.upsertedCount}, Modified: ${result.modifiedCount}`);
        }

        console.timeEnd("⚡ UploadProcessing");

        res.status(200).json({
            message: "Upload complete",
            total: rawData.length,
            valid: validRows.length,
            skipped,
            notFound: notFoundCount
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

        // Adding .lean() here makes the API response significantly faster
        const assignments = await Assignment.find({
            date: { $gte: startDate, $lte: endDate }
        })
            .sort({ rotationNumber: 1, flightNumber: 1, date: 1 })
            .lean();

        res.status(200).json({ data: assignments });

    } catch (error) {
        console.error("🔥 Fetch Error:", error);
        res.status(500).json({ message: "Failed to fetch assignments" });
    }
};