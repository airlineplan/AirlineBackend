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
                                isScheduled: true
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

// 🔥 NEW: Helper to format values to exactly 2 decimal places
function formatDecimal(value) {
    if (typeof value === "number") {
        return parseFloat(value.toFixed(2));
    }
    // Handle cases where the number was parsed as a string
    if (typeof value === "string" && !isNaN(value) && value.trim() !== "") {
        return parseFloat(parseFloat(value).toFixed(2));
    }
    return value;
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
        // Wrap the decimal fields in our format helper
        std: formatDecimal(row["STD (LT)"]),
        bt: formatDecimal(row["BT"]),
        sta: formatDecimal(row["STA(LT)"]),
        arrStn: row["Arr Stn"],
        variant: row["Variant"],
        effFromDt: row["Eff from Dt"],
        effToDt: row["Eff to Dt"],
        dow: row["DoW"],
        domINTL: row["Dom / INTL"],
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