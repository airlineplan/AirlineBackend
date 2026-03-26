const PooTable = require('../model/pooTable');
const Flight = require('../model/flight');
const Sector = require('../model/sectorSchema');
const moment = require('moment');

// ─── 1. GET POO DATA ────────────────────────────────────────────────
// Fetch existing POO table records with optional filters
exports.getPooData = async (req, res) => {
    try {
        const { poo, date, od, flightNumber, sector, variant, identifier } = req.query;
        const filter = {};

        if (poo) filter.poo = poo.toUpperCase();
        if (date) {
            const d = moment(date).startOf('day').toDate();
            const dEnd = moment(date).endOf('day').toDate();
            filter.date = { $gte: d, $lte: dEnd };
        }
        if (od) filter.od = od.toUpperCase();
        if (flightNumber) filter.flightNumber = flightNumber;
        if (sector) filter.sector = sector.toUpperCase();
        if (variant) filter.variant = variant;
        if (identifier) filter.identifier = identifier;

        const records = await PooTable.find(filter).sort({ sNo: 1 });
        res.status(200).json({ data: records });
    } catch (error) {
        console.error("🔥 Error fetching POO data:", error);
        res.status(500).json({ message: "Failed to fetch POO data", error: error.message });
    }
};


// ─── 2. POPULATE POO ────────────────────────────────────────────────
// Main business logic: read flights for a POO + date and build OD records
exports.populatePoo = async (req, res) => {
    try {
        const { poo, date } = req.body;

        if (!poo || !date) {
            return res.status(400).json({ message: "POO station and date are required" });
        }

        const pooUpper = poo.toUpperCase();
        const dayStart = moment(date).startOf('day').toDate();
        const dayEnd = moment(date).endOf('day').toDate();

        // 1. Find all flights departing from POO on that date
        const flights = await Flight.find({
            depStn: pooUpper,
            date: { $gte: dayStart, $lte: dayEnd }
        });

        if (!flights.length) {
            return res.status(200).json({ data: [], message: "No flights found for this POO and date" });
        }

        // 2. Fetch all sectors for GCD lookup
        const allSectors = await Sector.find({});
        const sectorGcdMap = {};
        allSectors.forEach(s => {
            const key = `${s.sector1}-${s.sector2}`;
            sectorGcdMap[key] = {
                gcd: parseFloat(s.gcd) || 0,
                paxCapacity: parseFloat(s.paxCapacity) || 0,
                cargoCapT: parseFloat(s.CargoCapT) || 0,
            };
        });

        // Helper: get sector info
        const getSectorInfo = (dep, arr) => {
            const key = `${dep}-${arr}`;
            return sectorGcdMap[key] || { gcd: 0, paxCapacity: 0, cargoCapT: 0 };
        };

        const records = [];
        let sNo = 1;

        // 3. Process each flight from POO
        for (const flight of flights) {
            const sectorKey = `${flight.depStn}-${flight.arrStn}`;
            const sectorInfo = getSectorInfo(flight.depStn, flight.arrStn);

            // ── NON-STOP RECORD ──
            // POO → direct destination, 0 stops
            records.push({
                sNo: sNo++,
                al: (flight.flight || '').substring(0, 2),
                poo: pooUpper,
                od: `${flight.depStn}-${flight.arrStn}`,
                odDI: (flight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                stops: 0,
                identifier: 'Non-Stop',
                sector: sectorKey,
                legDI: (flight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                date: flight.date,
                day: flight.day,
                flightNumber: flight.flight,
                variant: flight.variant,
                maxPax: flight.seats || sectorInfo.paxCapacity,
                maxCargoT: flight.CargoCapT || sectorInfo.cargoCapT,
                pax: flight.pax || 0,
                cargoT: flight.CargoT || 0,
                sectorGcd: flight.dist || sectorInfo.gcd,
                odViaGcd: flight.dist || sectorInfo.gcd, // same as sector GCD for non-stop
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                prorateRatioL1: 1, // Non-stop: full prorate
            });

            // ── CONNECTING RECORDS ──
            // Find flights departing from this flight's arrival station on the same day
            const connectingFlights = await Flight.find({
                depStn: flight.arrStn,
                date: { $gte: dayStart, $lte: dayEnd },
                flight: { $ne: flight.flight } // different flight number
            });

            for (const connFlight of connectingFlights) {
                const connSectorInfo = getSectorInfo(connFlight.depStn, connFlight.arrStn);
                const legGcd = flight.dist || sectorInfo.gcd;
                const connGcd = connFlight.dist || connSectorInfo.gcd;
                const totalOdGcd = legGcd + connGcd;

                records.push({
                    sNo: sNo++,
                    al: (flight.flight || '').substring(0, 2),
                    poo: pooUpper,
                    od: `${pooUpper}-${connFlight.arrStn}`,
                    odDI: (connFlight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                    stops: 1,
                    identifier: 'Connecting',
                    sector: sectorKey,
                    legDI: (flight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                    date: flight.date,
                    day: flight.day,
                    flightNumber: `${flight.flight}, ${connFlight.flight}`,
                    variant: flight.variant,
                    maxPax: flight.seats || sectorInfo.paxCapacity,
                    maxCargoT: flight.CargoCapT || sectorInfo.cargoCapT,
                    pax: 0, // User fills in connecting pax
                    cargoT: 0,
                    sectorGcd: legGcd,
                    odViaGcd: totalOdGcd,
                    legFare: 0,
                    legRate: 0,
                    odFare: 0,
                    odRate: 0,
                    prorateRatioL1: totalOdGcd > 0 ? parseFloat((legGcd / totalOdGcd).toFixed(4)) : 0,
                });
            }

            // ── TRANSIT RECORDS ──
            // Flights with the same flight number continuing from arrival station
            const transitFlights = await Flight.find({
                depStn: flight.arrStn,
                date: { $gte: dayStart, $lte: dayEnd },
                flight: flight.flight // same flight number = transit
            });

            for (const transitFlight of transitFlights) {
                const transitSectorInfo = getSectorInfo(transitFlight.depStn, transitFlight.arrStn);
                const legGcd = flight.dist || sectorInfo.gcd;
                const transitGcd = transitFlight.dist || transitSectorInfo.gcd;
                const totalOdGcd = legGcd + transitGcd;

                records.push({
                    sNo: sNo++,
                    al: (flight.flight || '').substring(0, 2),
                    poo: pooUpper,
                    od: `${pooUpper}-${transitFlight.arrStn}`,
                    odDI: (transitFlight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                    stops: 1,
                    identifier: 'Transit',
                    sector: sectorKey,
                    legDI: (flight.domIntl || '').toLowerCase() === 'intl' ? 'INTL' : 'Dom',
                    date: flight.date,
                    day: flight.day,
                    flightNumber: `${flight.flight}`,
                    variant: flight.variant,
                    maxPax: flight.seats || sectorInfo.paxCapacity,
                    maxCargoT: flight.CargoCapT || sectorInfo.cargoCapT,
                    pax: 0,
                    cargoT: 0,
                    sectorGcd: legGcd,
                    odViaGcd: totalOdGcd,
                    legFare: 0,
                    legRate: 0,
                    odFare: 0,
                    odRate: 0,
                    prorateRatioL1: totalOdGcd > 0 ? parseFloat((legGcd / totalOdGcd).toFixed(4)) : 0,
                });
            }
        }

        // 4. Upsert records into pooTables collection
        const bulkOps = records.map(rec => ({
            updateOne: {
                filter: {
                    poo: rec.poo,
                    date: rec.date,
                    od: rec.od,
                    sector: rec.sector,
                    flightNumber: rec.flightNumber,
                    identifier: rec.identifier
                },
                update: { $set: rec },
                upsert: true
            }
        }));

        if (bulkOps.length > 0) {
            await PooTable.bulkWrite(bulkOps, { ordered: false });
        }

        // 5. Re-fetch to return the saved records (with _id)
        const savedRecords = await PooTable.find({
            poo: pooUpper,
            date: { $gte: dayStart, $lte: dayEnd }
        }).sort({ sNo: 1 });

        res.status(200).json({
            data: savedRecords,
            message: `Populated ${records.length} POO records`
        });

    } catch (error) {
        console.error("🔥 Error populating POO:", error);
        res.status(500).json({ message: "Failed to populate POO", error: error.message });
    }
};


// ─── 3. UPDATE POO RECORDS (BULK) ───────────────────────────────────
// Save edited records and compute revenue fields
exports.updatePooRecords = async (req, res) => {
    try {
        const { records } = req.body;

        if (!records || !Array.isArray(records) || records.length === 0) {
            return res.status(400).json({ message: "No records provided" });
        }

        const bulkOps = records.map(rec => {
            // Compute revenue from editable fields
            const pax = parseFloat(rec.pax) || 0;
            const cargoT = parseFloat(rec.cargoT) || 0;
            const legFare = parseFloat(rec.legFare) || 0;
            const legRate = parseFloat(rec.legRate) || 0;
            const odFare = parseFloat(rec.odFare) || 0;
            const odRate = parseFloat(rec.odRate) || 0;
            const pooCcyToRccy = parseFloat(rec.pooCcyToRccy) || 1;
            const prorateRatioL1 = parseFloat(rec.prorateRatioL1) || 0;

            // Leg Revenue (Local Currency)
            const legPaxRev = pax * legFare;
            const legCargoRev = cargoT * legRate;
            const legTotalRev = legPaxRev + legCargoRev;

            // OD Revenue (Local Currency)
            const odPaxRev = pax * odFare;
            const odCargoRev = cargoT * odRate;
            const odTotalRev = odPaxRev + odCargoRev;

            // RCCY (Reporting Currency) conversions
            const rccyLegPaxRev = legPaxRev * pooCcyToRccy;
            const rccyLegCargoRev = legCargoRev * pooCcyToRccy;
            const rccyLegTotalRev = rccyLegPaxRev + rccyLegCargoRev;

            const rccyOdPaxRev = odPaxRev * pooCcyToRccy;
            const rccyOdCargoRev = odCargoRev * pooCcyToRccy;
            const rccyOdTotalRev = rccyOdPaxRev + rccyOdCargoRev;

            // Total RCCY
            const rccyPax = rccyLegPaxRev + rccyOdPaxRev;
            const rccyCargo = rccyLegCargoRev + rccyOdCargoRev;
            const rccyTotalRev = rccyPax + rccyCargo;

            const updateData = {
                ...rec,
                pax, cargoT, legFare, legRate, odFare, odRate,
                pooCcyToRccy, prorateRatioL1,
                legPaxRev, legCargoRev, legTotalRev,
                odPaxRev, odCargoRev, odTotalRev,
                rccyLegPaxRev, rccyLegCargoRev, rccyLegTotalRev,
                rccyOdPaxRev, rccyOdCargoRev, rccyOdTotalRev,
                rccyPax, rccyCargo, rccyTotalRev
            };

            // Remove _id from $set
            delete updateData._id;
            delete updateData.__v;

            if (rec._id) {
                return {
                    updateOne: {
                        filter: { _id: rec._id },
                        update: { $set: updateData },
                        upsert: false
                    }
                };
            } else {
                return {
                    updateOne: {
                        filter: {
                            poo: rec.poo,
                            date: rec.date,
                            od: rec.od,
                            sector: rec.sector,
                            flightNumber: rec.flightNumber,
                            identifier: rec.identifier
                        },
                        update: { $set: updateData },
                        upsert: true
                    }
                };
            }
        });

        if (bulkOps.length > 0) {
            await PooTable.bulkWrite(bulkOps, { ordered: false });
        }

        res.status(200).json({ message: `${bulkOps.length} records updated successfully` });
    } catch (error) {
        console.error("🔥 Error updating POO records:", error);
        res.status(500).json({ message: "Failed to update POO records", error: error.message });
    }
};


// ─── 4. GET REVENUE DATA (AGGREGATED) ───────────────────────────────
// Revenue page: aggregate POO data with grouping + periodicity
exports.getRevenueData = async (req, res) => {
    try {
        const {
            fromDate, toDate,
            poo, od, sector, flightNumber, variant, identifier,
            groupBy = 'poo',       // poo | od | sector | flightNumber | stops | identifier
            periodicity = 'monthly' // daily | weekly | monthly
        } = req.query;

        // Build match filter
        const match = {};
        if (fromDate && toDate) {
            match.date = {
                $gte: moment(fromDate).startOf('day').toDate(),
                $lte: moment(toDate).endOf('day').toDate()
            };
        } else if (fromDate) {
            match.date = { $gte: moment(fromDate).startOf('day').toDate() };
        } else if (toDate) {
            match.date = { $lte: moment(toDate).endOf('day').toDate() };
        }

        if (poo) match.poo = { $in: poo.split(',').map(s => s.trim().toUpperCase()) };
        if (od) match.od = { $in: od.split(',').map(s => s.trim().toUpperCase()) };
        if (sector) match.sector = { $in: sector.split(',').map(s => s.trim().toUpperCase()) };
        if (flightNumber) match.flightNumber = { $in: flightNumber.split(',').map(s => s.trim()) };
        if (variant) match.variant = { $in: variant.split(',').map(s => s.trim()) };
        if (identifier) match.identifier = { $in: identifier.split(',').map(s => s.trim()) };

        // Build period expression for grouping
        let periodExpr;
        switch (periodicity.toLowerCase()) {
            case 'daily':
                periodExpr = { $dateToString: { format: "%Y-%m-%d", date: "$date" } };
                break;
            case 'weekly':
                periodExpr = { $dateToString: { format: "%Y-W%V", date: "$date" } };
                break;
            case 'monthly':
            default:
                periodExpr = { $dateToString: { format: "%Y-%m", date: "$date" } };
                break;
        }

        // Build group key
        const groupField = `$${groupBy}`;

        const pipeline = [
            { $match: match },
            {
                $group: {
                    _id: {
                        groupKey: groupField,
                        period: periodExpr
                    },
                    totalPax: { $sum: "$pax" },
                    totalCargoT: { $sum: "$cargoT" },
                    totalLegPaxRev: { $sum: "$rccyLegPaxRev" },
                    totalLegCargoRev: { $sum: "$rccyLegCargoRev" },
                    totalLegRev: { $sum: "$rccyLegTotalRev" },
                    totalOdPaxRev: { $sum: "$rccyOdPaxRev" },
                    totalOdCargoRev: { $sum: "$rccyOdCargoRev" },
                    totalOdRev: { $sum: "$rccyOdTotalRev" },
                    totalPaxRev: { $sum: "$rccyPax" },
                    totalCargoRev: { $sum: "$rccyCargo" },
                    totalRev: { $sum: "$rccyTotalRev" },
                    count: { $sum: 1 }
                }
            },
            { $sort: { "_id.groupKey": 1, "_id.period": 1 } }
        ];

        const results = await PooTable.aggregate(pipeline);

        // Pivot: transform into { groupKey: { period1: metrics, period2: metrics, ... } }
        const pivoted = {};
        const allPeriods = new Set();

        results.forEach(r => {
            const key = r._id.groupKey || 'Unknown';
            const period = r._id.period;
            allPeriods.add(period);

            if (!pivoted[key]) pivoted[key] = {};
            pivoted[key][period] = {
                pax: r.totalPax,
                cargoT: r.totalCargoT,
                legPaxRev: r.totalLegPaxRev,
                legCargoRev: r.totalLegCargoRev,
                legRev: r.totalLegRev,
                odPaxRev: r.totalOdPaxRev,
                odCargoRev: r.totalOdCargoRev,
                odRev: r.totalOdRev,
                paxRev: r.totalPaxRev,
                cargoRev: r.totalCargoRev,
                totalRev: r.totalRev,
                count: r.count
            };
        });

        res.status(200).json({
            data: pivoted,
            periods: Array.from(allPeriods).sort(),
            groupBy,
            periodicity
        });

    } catch (error) {
        console.error("🔥 Error fetching revenue data:", error);
        res.status(500).json({ message: "Failed to fetch revenue data", error: error.message });
    }
};


// ─── 5. DELETE POO RECORDS ──────────────────────────────────────────
exports.deletePooRecords = async (req, res) => {
    try {
        const { ids } = req.body;

        if (!ids || !Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ message: "No record IDs provided" });
        }

        const result = await PooTable.deleteMany({ _id: { $in: ids } });
        res.status(200).json({
            message: `${result.deletedCount} records deleted successfully`
        });
    } catch (error) {
        console.error("🔥 Error deleting POO records:", error);
        res.status(500).json({ message: "Failed to delete records", error: error.message });
    }
};
