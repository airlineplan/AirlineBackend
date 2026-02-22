require("dotenv").config();

const { workerData, parentPort } = require("worker_threads");
const mongoose = require("mongoose");
const xlsx = require("xlsx");
const fs = require("fs");

const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const ImportJob = require("../model/ImportJob");

const CHUNK_SIZE = 1000;

(async () => {
    try {
        console.log("🚀 Worker Started");
        console.log("Worker Data:", workerData);

        await mongoose.connect(process.env.MONGO_URI);
        console.log("✅ Mongo Connected (Worker)");

        const { filePath, userId, jobId } = workerData;

        if (!filePath) throw new Error("File path missing");

        console.log("📂 Reading Excel file:", filePath);

        const workbook = xlsx.readFile(filePath);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        
        // Reading the rows from Excel
        const rows = xlsx.utils.sheet_to_json(sheet);

        console.log("📊 Total rows found:", rows.length);

        await ImportJob.findByIdAndUpdate(jobId, {
            totalRows: rows.length,
        });

        for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
            const chunk = rows.slice(i, i + CHUNK_SIZE);

            console.log(`🔄 Processing chunk ${i} - ${i + chunk.length}`);

            try {
                const dataBulk = [];
                const sectorBulk = [];
                const dataDocsForFlights = [];

                for (const row of chunk) {
                    const dataId = new mongoose.Types.ObjectId();
                    const processed = processExcelRow(row);

                    if (!validateRow(processed)) continue;

                    // 1. Prepare Data Document
                    const dataDoc = {
                        _id: dataId,
                        ...processed,
                        userId,
                        isScheduled: true,
                        domINTL: processed.domINTL?.toLowerCase() || "",
                    };

                    dataBulk.push({
                        insertOne: { document: dataDoc }
                    });

                    // 2. Prepare Sector Document (Now including all metrics)
                    sectorBulk.push({
                        insertOne: {
                            document: {
                                sector1: processed.depStn,
                                sector2: processed.arrStn,
                                variant: processed.variant,
                                bt: processed.bt,
                                sta: processed.sta,
                                dow: processed.dow,
                                flight: processed.flight,
                                std: processed.std,
                                networkId: dataId,
                                userId,
                                isScheduled: true,
                                // 🔥 NEW: Added metrics and tags to Sector
                                gcd: processed.gcd,
                                paxCapacity: processed.paxCapacity,
                                CargoCapT: processed.CargoCapT,
                                paxLF: processed.paxLF,
                                cargoLF: processed.cargoLF,
                                fromDt: processed.effFromDt,
                                toDt: processed.effToDt,
                                domINTL: processed.domINTL?.toLowerCase() || "",
                                userTag1: processed.userTag1,
                                userTag2: processed.userTag2,
                                remarks1: processed.remarks1,
                                remarks2: processed.remarks2,
                            }
                        }
                    });

                    dataDocsForFlights.push(dataDoc);
                }

                if (dataBulk.length) {
                    await Data.bulkWrite(dataBulk, { ordered: false });
                    await Sector.bulkWrite(sectorBulk, { ordered: false });

                    // 🔥 GENERATE FLIGHTS MANUALLY
                    await generateFlightsBulk(dataDocsForFlights);
                }

                await ImportJob.findByIdAndUpdate(jobId, {
                    $inc: {
                        processedRows: chunk.length,
                        successRows: dataBulk.length,
                    },
                });

                console.log(`✅ Chunk completed`);

            } catch (error) {
                console.error("❌ Chunk Error:", error.message);
            }
        }

        await ImportJob.findByIdAndUpdate(jobId, {
            status: "completed",
        });

        console.log("🎉 Import Completed");

        fs.unlinkSync(workerData.filePath);

        process.exit(0);
    } catch (error) {
        console.error("🔥 Worker Fatal Error:", error);

        if (workerData.jobId) {
            await ImportJob.findByIdAndUpdate(workerData.jobId, {
                status: "failed",
                error: error.message,
            });
        }

        process.exit(1);
    }
})();

//--- helpers -----------

function parseExcelTime(excelTime) {
    if (excelTime === undefined || excelTime === null) return "";

    if (typeof excelTime === "string") {
        return excelTime.trim();
    }

    if (typeof excelTime === "number") {
        const totalSeconds = Math.round(excelTime * 86400); 
        const hours = Math.floor(totalSeconds / 3600) % 24;
        const minutes = Math.floor((totalSeconds % 3600) / 60);

        const formattedHours = String(hours).padStart(2, "0");
        const formattedMinutes = String(minutes).padStart(2, "0");

        return `${formattedHours}:${formattedMinutes}`;
    }

    return String(excelTime);
}

function parseExcelDate(excelValue) {
    if (!excelValue) return null;
    if (typeof excelValue === "string") return new Date(excelValue);
    
    if (typeof excelValue === "number") {
        const date = new Date(Math.round((excelValue - 25569) * 86400 * 1000));
        date.setMinutes(date.getMinutes() + date.getTimezoneOffset());
        return date;
    }
    return new Date(excelValue);
}

// Fixed formatDecimal to return 0 instead of undefined for empty cells
function formatDecimal(value) {
    if (typeof value === "number") {
        return parseFloat(value.toFixed(2));
    }
    if (typeof value === "string" && !isNaN(value) && value.trim() !== "") {
        return parseFloat(parseFloat(value).toFixed(2));
    }
    return value || 0; 
}

function validateRow(row) {
    return (
        row.flight &&
        /^[a-zA-Z0-9]{1,8}$/.test(row.flight) &&
        /^[a-zA-Z0-9]{1,4}$/.test(row.depStn) &&
        /^[a-zA-Z0-9]{1,4}$/.test(row.arrStn)
    );
}

function processExcelRow(row) {
    return {
        flight: row["Flight #"],
        depStn: row["Dep Stn"],
        std: parseExcelTime(row["STD (LT)"]),
        bt: parseExcelTime(row["BT"]),
        sta: parseExcelTime(row["STA(LT)"]),
        arrStn: row["Arr Stn"],
        variant: row["Variant"],
        effFromDt: parseExcelDate(row["Eff from Dt"]),
        effToDt: parseExcelDate(row["Eff to Dt"]),
        dow: String(row["DoW"] || ""),
        domINTL: row["Dom / INTL"],
        // 🔥 NEW: Extracting all the missing columns
        userTag1: row["User Tag 1"] || "",
        userTag2: row["User Tag 2"] || "",
        remarks1: row["Remarks 1"] || "",
        remarks2: row["Remarks 2"] || "",
        gcd: formatDecimal(row["GCD"]),
        paxCapacity: formatDecimal(row["Pax Capacity"]),
        CargoCapT: formatDecimal(row["Cargo Cap T"]),
        paxLF: formatDecimal(row["Pax SF%"]),
        cargoLF: formatDecimal(row["Cargo LF%"])
    };
}

//---------- flight generation helper ----------------------

async function generateFlightsBulk(dataDocs) {
    const flightBulk = [];

    for (const doc of dataDocs) {
        const startDate = new Date(doc.effFromDt);
        const endDate = new Date(doc.effToDt);
        const allowedDays = String(doc.dow).split("").map(Number);

        let currentDate = new Date(startDate);

        // Map values for math calculations to prevent NaNs
        const paxCapacity = doc.paxCapacity || 0;
        const CargoCapT = doc.CargoCapT || 0;
        const gcd = doc.gcd || 0;
        const paxLF = doc.paxLF || 0;
        const cargoLF = doc.cargoLF || 0;

        while (currentDate <= endDate) {
            const currentDay = currentDate.getDay() !== 0 ? currentDate.getDay() : 7;

            if (allowedDays.includes(currentDay)) {
                flightBulk.push({
                    insertOne: {
                        document: {
                            date: new Date(currentDate),
                            flight: doc.flight,
                            depStn: doc.depStn,
                            std: doc.std,
                            bt: doc.bt,
                            sta: doc.sta,
                            arrStn: doc.arrStn,
                            sector: `${doc.depStn}-${doc.arrStn}`,
                            variant: doc.variant,
                            domIntl: doc.domINTL,
                            userId: doc.userId,
                            networkId: doc._id,
                            effFromDt: doc.effFromDt,
                            effToDt: doc.effToDt,
                            dow: doc.dow,
                            // 🔥 NEW: Push the metrics and tags all the way to FLIGHT DB
                            userTag1: doc.userTag1,
                            userTag2: doc.userTag2,
                            remarks1: doc.remarks1,
                            remarks2: doc.remarks2,
                            seats: paxCapacity,
                            CargoCapT: CargoCapT,
                            dist: gcd,
                            pax: paxCapacity * (paxLF / 100),
                            CargoT: CargoCapT * (cargoLF / 100),
                            ask: paxCapacity * gcd,
                            rsk: paxCapacity * (paxLF / 100) * gcd,
                            cargoAtk: CargoCapT * gcd,
                            cargoRtk: CargoCapT * (cargoLF / 100) * gcd,
                            isComplete: true
                        }
                    }
                });
            }
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    if (flightBulk.length) {
        await mongoose.model("FLIGHT").bulkWrite(flightBulk, {
            ordered: false
        });
    }

    console.log(`✈ Flights generated: ${flightBulk.length}`);
}