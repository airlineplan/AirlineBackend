require("dotenv").config();

const { workerData, parentPort } = require("worker_threads");
const mongoose = require("mongoose");
const xlsx = require("xlsx");
const fs = require("fs");

const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const ImportJob = require("../model/ImportJob");
const Station = require("../model/stationSchema");

const CHUNK_SIZE = 1000;

(async () => {
    try {
        console.log("🚀 Worker Started");
        await mongoose.connect(process.env.MONGO_URI);
        console.log("✅ Mongo Connected (Worker)");

        const { filePath, userId, jobId } = workerData;
        if (!filePath) throw new Error("File path missing");

        const workbook = xlsx.readFile(filePath);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rows = xlsx.utils.sheet_to_json(sheet);

        await ImportJob.findByIdAndUpdate(jobId, { totalRows: rows.length });

        for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
            const chunk = rows.slice(i, i + CHUNK_SIZE);
            console.log(`🔄 Processing chunk ${i} - ${i + chunk.length}`);

            try {
                const dataBulk = [];
                const sectorBulk = [];
                const dataDocsForFlights = [];

                // 🛫 1. OPTIMIZED: Fetch all taxi times for this chunk in ONE query
                const uniqueStations = [...new Set(chunk.flatMap(row => [row["Dep Stn"], row["Arr Stn"]]).filter(Boolean))];
                const stationsDB = await Station.find({ stationName: { $in: uniqueStations }, userId });

                const taxiTimes = {};
                stationsDB.forEach(stn => {
                    taxiTimes[stn.stationName] = {
                        out: timeStrToMinutes(stn.avgTaxiOutTime || "00:00"),
                        in: timeStrToMinutes(stn.avgTaxiInTime || "00:00")
                    };
                });

                for (const row of chunk) {
                    const dataId = new mongoose.Types.ObjectId();
                    const processed = processExcelRow(row);

                    if (!validateRow(processed)) continue;

                    // 🧮 2. Calculate BH and FH in memory
                    const btMins = timeStrToMinutes(processed.bt);
                    const taxiOutMins = taxiTimes[processed.depStn] ? taxiTimes[processed.depStn].out : 0;
                    const taxiInMins = taxiTimes[processed.arrStn] ? taxiTimes[processed.arrStn].in : 0;

                    let fhMins = btMins - taxiOutMins - taxiInMins;
                    if (fhMins < 0) fhMins = 0;

                    const bhDecimal = btMins / 60;
                    const fhDecimal = fhMins / 60;

                    const dataDoc = {
                        _id: dataId,
                        ...processed,
                        userId,
                        isScheduled: true,
                        domINTL: processed.domINTL?.toLowerCase() || "",
                        // Pass computed values to the flight generator
                        bh: bhDecimal,
                        fh: fhDecimal
                    };

                    dataBulk.push({ insertOne: { document: dataDoc } });

                    sectorBulk.push({
                        insertOne: {
                            document: {
                                sector1: processed.depStn,
                                sector2: processed.arrStn,
                                variant: processed.variant,
                                acftType: processed.acftType,
                                bt: processed.bt,
                                sta: processed.sta,
                                dow: processed.dow,
                                flight: processed.flight,
                                std: processed.std,
                                networkId: dataId,
                                userId,
                                isScheduled: true,
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
                                bh: bhDecimal,
                                fh: fhDecimal
                            }
                        }
                    });

                    dataDocsForFlights.push(dataDoc);
                }

                if (dataBulk.length) {
                    await Data.bulkWrite(dataBulk, { ordered: false });
                    await Sector.bulkWrite(sectorBulk, { ordered: false });

                    // 3. Update Station Frequencies 
                    await updateStationsBulk(dataDocsForFlights, userId);

                    // 4. Generate Flights
                    await generateFlightsBulk(dataDocsForFlights);
                }

                await ImportJob.findByIdAndUpdate(jobId, {
                    $inc: { processedRows: chunk.length, successRows: dataBulk.length },
                });

            } catch (error) {
                console.error("❌ Chunk Error:", error.message);
            }
        }

        await ImportJob.findByIdAndUpdate(jobId, { status: "completed" });
        fs.unlinkSync(workerData.filePath);
        process.exit(0);

    } catch (error) {
        if (workerData.jobId) await ImportJob.findByIdAndUpdate(workerData.jobId, { status: "failed", error: error.message });
        process.exit(1);
    }
})();

// --- HELPERS ---

function timeStrToMinutes(timeStr) {
    if (!timeStr) return 0;
    const parts = String(timeStr).split(':');
    if (parts.length === 2) {
        const hours = parseInt(parts[0], 10) || 0;
        const minutes = parseInt(parts[1], 10) || 0;
        return (hours * 60) + minutes;
    }
    return parseFloat(timeStr) * 60 || 0;
}

function parseExcelTime(excelTime) {
    if (excelTime === undefined || excelTime === null) return "";
    if (typeof excelTime === "string") return excelTime.trim();
    if (typeof excelTime === "number") {
        const totalSeconds = Math.round(excelTime * 86400);
        const hours = Math.floor(totalSeconds / 3600) % 24;
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
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

function formatDecimal(value) {
    if (typeof value === "number") return parseFloat(value.toFixed(2));
    if (typeof value === "string" && !isNaN(value) && value.trim() !== "") return parseFloat(parseFloat(value).toFixed(2));
    return value || 0;
}

function validateRow(row) {
    return (row.flight && /^[a-zA-Z0-9]{1,8}$/.test(row.flight) && /^[a-zA-Z0-9]{1,4}$/.test(row.depStn) && /^[a-zA-Z0-9]{1,4}$/.test(row.arrStn));
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
        acftType: row["ACFT Type"] || row["Variant"] || "",
        effFromDt: parseExcelDate(row["Eff from Dt"]),
        effToDt: parseExcelDate(row["Eff to Dt"]),
        dow: String(row["DoW"] || ""),
        domINTL: row["Dom / INTL"],
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

async function updateStationsBulk(dataDocs, userId) {
    const stationCountMap = {};

    for (const doc of dataDocs) {
        if (!doc.depStn || !doc.arrStn) continue;
        stationCountMap[doc.depStn] = (stationCountMap[doc.depStn] || 0) + 1;
        stationCountMap[doc.arrStn] = (stationCountMap[doc.arrStn] || 0) + 1;
    }

    const bulkOps = Object.keys(stationCountMap).map(stationName => ({
        updateOne: {
            filter: { stationName, userId },
            update: {
                $inc: { freq: stationCountMap[stationName] },
                $setOnInsert: {
                    avgTaxiOutTime: "00:00",
                    avgTaxiInTime: "00:00",
                    stdtz: "UTC+0:00",
                    dsttz: "UTC+0:00",
                    ddMinCT: "1:30",
                    ddMaxCT: "7:00",
                    dInMinCT: "2:00",
                    dInMaxCT: "7:00",
                    inDMinCT: "2:00",
                    inDMaxCT: "7:00",
                    inInMinDT: "2:00",
                    inInMaxDT: "7:00",
                    nextDSTStart: "",
                    nextDSTEnd: ""
                }
            },
            upsert: true
        }
    }));

    if (bulkOps.length) {
        await mongoose.model("Station").bulkWrite(bulkOps, { ordered: false });
        console.log("🏢 Stations updated successfully");
    }
}

async function generateFlightsBulk(dataDocs) {
    const flightBulk = [];
    
    // Array to easily map JS getDay() to readable string if you prefer "Mon", "Tue", etc.
    // If your airline users prefer numbers (1=Mon, 7=Sun), you can use String(currentDay) instead.
    const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]; 

    for (const doc of dataDocs) {
        const startDate = new Date(doc.effFromDt);
        const endDate = new Date(doc.effToDt);
        const allowedDays = String(doc.dow).split("").map(Number);
        let currentDate = new Date(startDate);

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
                            day: dayNames[currentDate.getDay()], // 👈 ADDED THIS LINE
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
                            acftType: doc.acftType,
                            bh: doc.bh,
                            fh: doc.fh,
                            isComplete: true
                        }
                    }
                });
            }
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    if (flightBulk.length) {
        await mongoose.model("FLIGHT").bulkWrite(flightBulk, { ordered: false });
    }
    console.log(`✈ Flights generated: ${flightBulk.length}`);
}