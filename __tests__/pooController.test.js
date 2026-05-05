const test = require("node:test");
const assert = require("node:assert/strict");

const pooController = require("../controller/pooController");
const PooTable = require("../model/pooTable");
const Flight = require("../model/flight");

const {
    calculateProrateRatio,
    calculateLegShare,
    recalculateRevenue,
    recalculateRevenueForPooRow,
    getCarriedForwardFxRate,
    convertLocalToReporting,
    normalizeRevenueRowsForReporting,
    buildBlankAwareClause,
    backfillMasterFieldsToPooRows,
    buildSelectedDateRange,
    buildRevenueAggregateResponse,
    buildEditableResponse,
    buildFlightSnapshot,
    buildLegRows,
    buildSystemConnectionRows,
    buildExplicitConnectionEdges,
    buildStationConnectionRuleMap,
    buildStationRuleConnectionEdges,
    applyTrafficUpdates,
    applyUpdatesForDate,
    assignSerialNumbers,
} = pooController.__testables__;

function makeConnectionSnapshot(overrides = {}) {
    return {
        userId: "user-1",
        flightId: "f1",
        al: "Own",
        depStn: "DEL",
        arrStn: "BOM",
        sector: "DEL-BOM",
        odDI: "Dom",
        legDI: "Dom",
        date: new Date("2026-03-04T00:00:00.000Z"),
        day: "Wed",
        flightNumber: "A 100",
        variant: "",
        std: "09:00",
        sta: "11:30",
        maxPax: 153,
        maxCargoT: 0.6,
        sourcePaxTotal: 153,
        sourceCargoTotal: 0.6,
        sourceSeats: 180,
        sourceCargoCapT: 1.2,
        sourcePaxLF: 85,
        sourceCargoLF: 50,
        sectorGcd: 1200,
        ...overrides,
    };
}

function makeStateRow(overrides = {}) {
    return {
        _id: overrides._id,
        trafficType: overrides.trafficType || "leg",
        flightId: overrides.flightId || "f1",
        connectedFlightId: overrides.connectedFlightId || null,
        poo: overrides.poo || "DEL",
        od: overrides.od || "DEL-BOM",
        odOrigin: overrides.odOrigin || "DEL",
        odDestination: overrides.odDestination || "BOM",
        odDI: overrides.odDI || "Dom",
        sector: overrides.sector || "DEL-BOM",
        legDI: overrides.legDI || "Dom",
        identifier: overrides.identifier || "Leg",
        rowKey: overrides.rowKey || overrides._id,
        odGroupKey: overrides.odGroupKey || `leg::${overrides.flightId || "f1"}`,
        flightNumber: overrides.flightNumber || "A 100",
        connectedFlightNumber: overrides.connectedFlightNumber || null,
        flightList: overrides.flightList || [overrides.flightNumber || "A 100"],
        timeInclLayover: overrides.timeInclLayover || "00:00",
        pax: overrides.pax ?? 0,
        cargoT: overrides.cargoT ?? 0,
        maxPax: overrides.maxPax ?? 100,
        maxCargoT: overrides.maxCargoT ?? 10,
        stops: overrides.stops ?? 0,
        sourceSeats: overrides.sourceSeats ?? 100,
        sourceCargoCapT: overrides.sourceCargoCapT ?? 10,
        sourcePaxTotal: overrides.sourcePaxTotal ?? 0,
        sourceCargoTotal: overrides.sourceCargoTotal ?? 0,
        sourcePaxLF: overrides.sourcePaxLF ?? 0,
        sourceCargoLF: overrides.sourceCargoLF ?? 0,
        odViaGcd: overrides.odViaGcd ?? 1200,
        sectorGcd: overrides.sectorGcd ?? 1200,
        totalGcd: overrides.totalGcd ?? overrides.odViaGcd ?? 1200,
        legFare: overrides.legFare ?? 0,
        legRate: overrides.legRate ?? 0,
        odFare: overrides.odFare ?? 0,
        odRate: overrides.odRate ?? 0,
        fareProrateRatioL1L2: overrides.fareProrateRatioL1L2 ?? 0,
        rateProrateRatioL1L2: overrides.rateProrateRatioL1L2 ?? 0,
        pooCcyToRccy: overrides.pooCcyToRccy ?? 1,
        applySSPricing: overrides.applySSPricing ?? false,
        ...overrides,
    };
}

test("buildFlightSnapshot carries a userId fallback into generated POO rows", () => {
    const snapshot = buildFlightSnapshot(
        {
            _id: "flight-1",
            depStn: "del",
            arrStn: "bom",
            domIntl: "dom",
            date: new Date("2026-03-01T00:00:00.000Z"),
            day: "Sun",
            flight: "A 100",
            std: "09:00",
            sta: "11:30",
            seats: 180,
            CargoCapT: 1.2,
            pax: 153,
            CargoT: 0.6,
            dist: 1200,
        },
        new Map(),
        "user-1"
    );

    const { rows } = buildLegRows({
        snapshot,
        existingRowsByKey: new Map(),
        existingRecords: [],
        currencyContextByPoo: {},
    });

    assert.equal(snapshot.userId, "user-1");
    assert.equal(snapshot.maxPax, 153);
    assert.equal(rows.length, 2);
    assert.deepEqual(rows.map((row) => row.poo).sort(), ["BOM", "DEL"]);
    assert.ok(rows.every((row) => row.userId === "user-1"));
});

test("buildFlightSnapshot falls back to seats times pax load factor when flight pax is missing", () => {
    const snapshot = buildFlightSnapshot(
        {
            _id: "flight-fallback",
            depStn: "DEL",
            arrStn: "BOM",
            domIntl: "dom",
            date: new Date("2026-03-01T00:00:00.000Z"),
            day: "Sun",
            flight: "A 201",
            std: "09:00",
            sta: "11:30",
            seats: 200,
            paxLF: 75,
            CargoCapT: 0.75,
            CargoT: 0.6,
            dist: 1200,
        },
        new Map(),
        "user-1"
    );

    assert.equal(snapshot.maxPax, 150);
    assert.equal(snapshot.sourcePaxTotal, 150);
    assert.equal(snapshot.sourcePaxLF, 75);
});

test("selected POO date range uses the exact selected date", () => {
    const range = buildSelectedDateRange("2026-03-04");

    assert.equal(range.$gte.toISOString(), "2026-03-04T00:00:00.000Z");
    assert.equal(range.$lte.toISOString(), "2026-03-04T23:59:59.999Z");
});

test("backfills POO master fields including updated variant", async () => {
    const originalPooFind = PooTable.find;
    const originalFlightFind = Flight.find;
    const originalBulkWrite = PooTable.bulkWrite;
    const opsSeen = [];

    const pooRows = [
        {
            _id: "poo-1",
            date: new Date("2026-03-04T12:00:00.000Z"),
            sector: "DEL-BOM",
            flightNumber: "A100",
            depStn: "",
            arrStn: "",
            variant: "OLD",
            userTag1: "",
            userTag2: "",
        },
    ];
    const flights = [
        {
            date: new Date("2026-03-04T00:00:00.000Z"),
            sector: "DEL-BOM",
            flight: "A100",
            depStn: "DEL",
            arrStn: "BOM",
            variant: "A320",
            userTag1: "Label A",
            userTag2: "Group 1",
        },
    ];
    const chain = (rows) => ({
        select: () => ({
            lean: async () => rows,
        }),
    });

    PooTable.find = () => chain(pooRows);
    Flight.find = () => chain(flights);
    PooTable.bulkWrite = async (ops) => {
        opsSeen.push(...ops);
        return { modifiedCount: ops.length };
    };

    try {
        const result = await backfillMasterFieldsToPooRows("user-1");
        const set = opsSeen[0].updateOne.update.$set;

        assert.equal(result.matchedRows, 1);
        assert.equal(set.depStn, "DEL");
        assert.equal(set.arrStn, "BOM");
        assert.equal(set.variant, "A320");
        assert.equal(set.userTag1, "Label A");
        assert.equal(set.userTag2, "Group 1");
    } finally {
        PooTable.find = originalPooFind;
        Flight.find = originalFlightFind;
        PooTable.bulkWrite = originalBulkWrite;
    }
});

test("assigns leg POO rows in departure-then-arrival order for each flight", () => {
    const snapshot = buildFlightSnapshot(
        {
            _id: "flight-order",
            depStn: "DEL",
            arrStn: "BOM",
            domIntl: "dom",
            date: new Date("2026-03-01T00:00:00.000Z"),
            day: "Sun",
            flight: "A 200",
            std: "09:00",
            sta: "11:30",
            seats: 180,
            CargoCapT: 1.2,
            pax: 153,
            CargoT: 0.6,
            dist: 1200,
        },
        new Map(),
        "user-1"
    );

    const { rows } = buildLegRows({
        snapshot,
        existingRowsByKey: new Map(),
        existingRecords: [],
        currencyContextByPoo: {},
    });

    const ordered = assignSerialNumbers([rows[1], rows[0]]);

    assert.deepEqual(
        ordered.map((row) => row.poo),
        ["DEL", "BOM"]
    );
    assert.deepEqual(
        ordered.map((row) => row.pax),
        [77, 76]
    );
});

test("capacity increase preserves existing leg split and distributes added traffic", () => {
    const snapshot = makeConnectionSnapshot({
        sourcePaxTotal: 160,
        maxPax: 160,
        sourceCargoTotal: 0.8,
        maxCargoT: 0.8,
    });
    const existingRows = [
        makeStateRow({
            rowKey: "system|leg|DEL|f1|none|leg::f1",
            flightId: "f1",
            poo: "DEL",
            pax: 65,
            cargoT: 0.2,
            maxPax: 153,
            maxCargoT: 0.6,
            sourcePaxTotal: 153,
            sourceCargoTotal: 0.6,
        }),
        makeStateRow({
            rowKey: "system|leg|BOM|f1|none|leg::f1",
            flightId: "f1",
            poo: "BOM",
            pax: 88,
            cargoT: 0.4,
            maxPax: 153,
            maxCargoT: 0.6,
            sourcePaxTotal: 153,
            sourceCargoTotal: 0.6,
        }),
    ];
    const existingRowsByKey = new Map(existingRows.map((row) => [row.rowKey, row]));

    const { rows, forceReset } = buildLegRows({
        snapshot,
        existingRowsByKey,
        existingRecords: existingRows,
        currencyContextByPoo: {},
    });

    assert.equal(forceReset, false);
    assert.deepEqual(rows.map((row) => row.pax), [69, 91]);
    assert.deepEqual(rows.map((row) => row.cargoT), [0.3, 0.5]);
});

test("capacity decrease resets leg traffic to the new equal allocation", () => {
    const snapshot = makeConnectionSnapshot({
        sourcePaxTotal: 120,
        maxPax: 120,
        sourceCargoTotal: 0.4,
        maxCargoT: 0.4,
    });
    const existingRows = [
        makeStateRow({
            rowKey: "system|leg|DEL|f1|none|leg::f1",
            flightId: "f1",
            poo: "DEL",
            pax: 65,
            cargoT: 0.2,
            maxPax: 153,
            maxCargoT: 0.6,
            sourcePaxTotal: 153,
            sourceCargoTotal: 0.6,
        }),
        makeStateRow({
            rowKey: "system|leg|BOM|f1|none|leg::f1",
            flightId: "f1",
            poo: "BOM",
            pax: 88,
            cargoT: 0.4,
            maxPax: 153,
            maxCargoT: 0.6,
            sourcePaxTotal: 153,
            sourceCargoTotal: 0.6,
        }),
    ];
    const existingRowsByKey = new Map(existingRows.map((row) => [row.rowKey, row]));

    const { rows, forceReset } = buildLegRows({
        snapshot,
        existingRowsByKey,
        existingRecords: existingRows,
        currencyContextByPoo: {},
    });

    assert.equal(forceReset, true);
    assert.deepEqual(rows.map((row) => row.pax), [60, 60]);
    assert.deepEqual(rows.map((row) => row.cargoT), [0.2, 0.2]);
});

test("connection rows are generated for both OD endpoint POO values", () => {
    const firstSnapshot = makeConnectionSnapshot();
    const secondSnapshot = makeConnectionSnapshot({
        flightId: "f2",
        depStn: "BOM",
        arrStn: "DXB",
        sector: "BOM-DXB",
        odDI: "Intl",
        legDI: "Intl",
        flightNumber: "A 102",
        maxPax: 243,
        maxCargoT: 7.7,
        sourcePaxTotal: 243,
        sourceCargoTotal: 7.7,
        sourceSeats: 296,
        sourceCargoCapT: 8.5,
        sourcePaxLF: 89,
        sourceCargoLF: 91,
        sectorGcd: 1900,
    });

    const rows = ["DEL", "DXB"].flatMap((pagePoo) =>
        buildSystemConnectionRows({
            pagePoo,
            firstSnapshot,
            secondSnapshot,
            existingRowsByKey: new Map(),
            shouldReset: false,
            pageCurrencyContext: {},
        })
    );

    assert.equal(rows.length, 4);
    assert.deepEqual(rows.map((row) => row.poo).sort(), ["DEL", "DEL", "DXB", "DXB"]);
    assert.ok(rows.every((row) => row.userId === "user-1"));
    assert.ok(rows.every((row) => row.od === "DEL-DXB"));
    assert.ok(rows.every((row) => row.odOrigin === "DEL"));
    assert.ok(rows.every((row) => row.odDestination === "DXB"));
    assert.ok(rows.every((row) => row.odDI === "Intl"));
    assert.ok(rows.every((row) => row.stops === 1));
    assert.ok(rows.every((row) => row.maxPax === 153));
    assert.ok(rows.every((row) => row.maxCargoT === 0.6));
    assert.deepEqual(
        rows.filter((row) => row.trafficType === "behind").map((row) => row.sector),
        ["DEL-BOM", "DEL-BOM"]
    );
    assert.deepEqual(
        rows.filter((row) => row.trafficType === "beyond").map((row) => row.sector),
        ["BOM-DXB", "BOM-DXB"]
    );
});

test("station rules generate same-day Master flight connections", () => {
    const firstSnapshot = makeConnectionSnapshot({
        flightId: "a100",
        depStn: "DEL",
        arrStn: "BOM",
        sector: "DEL-BOM",
        flightNumber: "A100",
        std: "09:00",
        sta: "11:30",
        legDI: "Dom",
        odDI: "Dom",
    });
    const domSecond = makeConnectionSnapshot({
        flightId: "a101",
        depStn: "BOM",
        arrStn: "HYD",
        sector: "BOM-HYD",
        flightNumber: "A101",
        std: "14:30",
        sta: "16:10",
        legDI: "Dom",
        odDI: "Dom",
    });
    const intlSecond = makeConnectionSnapshot({
        flightId: "a102",
        depStn: "BOM",
        arrStn: "DXB",
        sector: "BOM-DXB",
        flightNumber: "A102",
        std: "15:00",
        sta: "17:00",
        legDI: "Intl",
        odDI: "Intl",
    });
    const tooEarly = makeConnectionSnapshot({
        flightId: "a103",
        depStn: "BOM",
        arrStn: "GOI",
        sector: "BOM-GOI",
        flightNumber: "A103",
        std: "12:00",
        sta: "13:00",
        legDI: "Dom",
        odDI: "Dom",
    });

    const stationRules = buildStationConnectionRuleMap([{
        stationName: "BOM",
        ddMinCT: "01:30",
        ddMaxCT: "07:00",
        dInMinCT: "01:30",
        dInMaxCT: "07:00",
        inDMinCT: "02:00",
        inDMaxCT: "07:00",
        inInMinDT: "02:00",
        inInMaxDT: "07:00",
    }]);
    const snapshots = new Map([
        [firstSnapshot.flightId, firstSnapshot],
        [domSecond.flightId, domSecond],
        [intlSecond.flightId, intlSecond],
        [tooEarly.flightId, tooEarly],
    ]);

    const edges = buildStationRuleConnectionEdges(snapshots, stationRules);

    assert.deepEqual(
        edges.map((edge) => `${edge.flightID}->${edge.beyondOD}`).sort(),
        ["a100->a101", "a100->a102"]
    );
});

test("timeInclLayover uses BT for leg rows and BT-plus-gap for OD rows", () => {
    const firstSnapshot = makeConnectionSnapshot({
        std: "09:00",
        sta: "11:30",
        bt: "02:30",
    });
    const secondSnapshot = makeConnectionSnapshot({
        flightId: "f2",
        depStn: "BOM",
        arrStn: "HYD",
        sector: "BOM-HYD",
        odDI: "Dom",
        legDI: "Dom",
        flightNumber: "A 101",
        std: "14:30",
        sta: "16:10",
        bt: "01:40",
        maxPax: 128,
        maxCargoT: 1.4,
        sourcePaxTotal: 128,
        sourceCargoTotal: 1.4,
        sourceSeats: 144,
        sourceCargoCapT: 1.4,
        sourcePaxLF: 89,
        sourceCargoLF: 100,
        sectorGcd: 600,
    });

    const legRows = buildLegRows({
        snapshot: firstSnapshot,
        existingRowsByKey: new Map(),
        existingRecords: [],
        currencyContextByPoo: {},
    }).rows;

    const odRows = buildSystemConnectionRows({
        pagePoo: "DEL",
        firstSnapshot,
        secondSnapshot,
        existingRowsByKey: new Map(),
        shouldReset: false,
        pageCurrencyContext: {},
    });

    assert.ok(legRows.every((row) => row.timeInclLayover === "02:30"));
    assert.deepEqual(
        odRows.map((row) => row.timeInclLayover),
        ["07:10", "07:10"]
    );
});

test("builds raw POO rows from the March fixture explicit behind and beyond references", () => {
    const fixtureFlights = [
        [1, "2026-03-01", "Sun", "A 100", "DEL", "09:00", "11:30", "BOM", "Dom", 1200, 180, 1.2, 153, 0.6, null, null, null, null],
        [2, "2026-03-02", "Mon", "A 100", "DEL", "09:00", "11:30", "BOM", "Dom", 1200, 180, 1.2, 153, 0.6, null, 13, null, null],
        [3, "2026-03-04", "Wed", "A 100", "DEL", "09:00", "11:30", "BOM", "Dom", 1200, 180, 1.2, 153, 0.6, 6, 15, null, null],
        [4, "2026-03-06", "Fri", "A 100", "DEL", "09:00", "11:30", "BOM", "Dom", 1200, 180, 1.2, 153, 0.6, 8, 17, null, null],
        [5, "2026-03-03", "Tue", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, 11, null, null, null],
        [6, "2026-03-04", "Wed", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, null, null, 3, null],
        [7, "2026-03-05", "Thu", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, null, null, null, null],
        [8, "2026-03-06", "Fri", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, null, null, 4, null],
        [9, "2026-03-09", "Mon", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, null, null, null, null],
        [10, "2026-03-10", "Tue", "A 101", "BOM", "14:30", "16:10", "HYD", "Dom", 600, 144, 1.4, 128, 1.4, null, null, null, null],
        [11, "2026-03-04", "Wed", "A 200", "HYD", "18:00", "23:15", "BKK", "Intl", 2400, 189, 1.3, 142, 0.4, null, null, 5, null],
        [12, "2026-03-07", "Sat", "A 200", "HYD", "18:00", "23:15", "BKK", "Intl", 2400, 189, 1.3, 142, 0.4, null, null, null, null],
        [13, "2026-03-02", "Mon", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, 2, null],
        [14, "2026-03-03", "Tue", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [15, "2026-03-04", "Wed", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, 3, null],
        [16, "2026-03-05", "Thu", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [17, "2026-03-06", "Fri", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, 4, null],
        [18, "2026-03-07", "Sat", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [19, "2026-03-08", "Sun", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [20, "2026-03-09", "Mon", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [21, "2026-03-10", "Tue", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [22, "2026-03-11", "Wed", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
        [23, "2026-03-12", "Thu", "A 102", "BOM", "15:00", "17:00", "DXB", "Intl", 1900, 296, 8.5, 243, 7.7, null, null, null, null],
    ].map(([serial, date, day, flight, depStn, std, sta, arrStn, domIntl, dist, seats, CargoCapT, pax, CargoT, beyond1, beyond2, behind1, behind2]) => ({
        _id: `flight-${serial}`,
        userId: "user-1",
        sourceSerialNo: serial,
        date: new Date(`${date}T00:00:00.000Z`),
        day,
        flight,
        depStn,
        std,
        bt: "00:00",
        sta,
        arrStn,
        domIntl,
        dist,
        seats,
        CargoCapT,
        pax,
        CargoT,
        beyond1,
        beyond2,
        behind1,
        behind2,
    }));

    const flightsById = new Map(fixtureFlights.map((flight) => [flight._id, flight]));
    const snapshots = new Map(
        fixtureFlights.map((flight) => [flight._id, buildFlightSnapshot(flight, new Map(), "user-1")])
    );

    const legRows = fixtureFlights.flatMap((flight) => buildLegRows({
        snapshot: snapshots.get(flight._id),
        existingRowsByKey: new Map(),
        existingRecords: [],
        currencyContextByPoo: {},
    }).rows);

    const connectionRows = buildExplicitConnectionEdges(flightsById).flatMap((edge) => {
        const firstSnapshot = snapshots.get(edge.flightID);
        const secondSnapshot = snapshots.get(edge.beyondOD);
        return [firstSnapshot.depStn, secondSnapshot.arrStn].flatMap((pagePoo) =>
            buildSystemConnectionRows({
                pagePoo,
                firstSnapshot,
                secondSnapshot,
                existingRowsByKey: new Map(),
                shouldReset: false,
                pageCurrencyContext: {},
            })
        );
    });

    const rows = assignSerialNumbers([...legRows, ...connectionRows]);
    const rowFor = (criteria) => rows.find((row) =>
        Object.entries(criteria).every(([key, value]) => row[key] === value)
    );
    const rowsFor = (criteria) => rows.filter((row) =>
        Object.entries(criteria).every(([key, value]) => row[key] === value)
    );

    assert.equal(rowsFor({ trafficType: "leg" }).length, 46);
    assert.equal(rowFor({ trafficType: "leg", poo: "DEL", od: "DEL-BOM", flightNumber: "A 100" }).pax, 77);
    assert.equal(rowFor({ trafficType: "leg", poo: "BOM", od: "DEL-BOM", flightNumber: "A 100" }).pax, 76);
    assert.equal(rowFor({ trafficType: "leg", poo: "BOM", od: "BOM-HYD", flightNumber: "A 101" }).cargoT, 0.7);
    assert.equal(rowFor({ trafficType: "leg", poo: "BOM", od: "BOM-DXB", flightNumber: "A 102" }).cargoT, 3.8);
    assert.equal(rowFor({ trafficType: "leg", poo: "DXB", od: "BOM-DXB", flightNumber: "A 102" }).cargoT, 3.9);

    assert.equal(rowsFor({ od: "DEL-HYD", trafficType: "behind" }).length, 4);
    assert.equal(rowsFor({ od: "DEL-HYD", trafficType: "beyond" }).length, 4);
    assert.equal(rowsFor({ od: "BOM-BKK", trafficType: "behind" }).length, 2);
    assert.equal(rowsFor({ od: "BOM-BKK", trafficType: "beyond" }).length, 2);
    assert.equal(rowsFor({ od: "DEL-DXB", trafficType: "behind" }).length, 6);
    assert.equal(rowsFor({ od: "DEL-DXB", trafficType: "beyond" }).length, 6);

    const delDxbBehind = rowFor({ od: "DEL-DXB", trafficType: "behind", poo: "DEL", flightNumber: "A 100" });
    const delDxbBeyond = rowFor({ od: "DEL-DXB", trafficType: "beyond", poo: "DXB", flightNumber: "A 102" });
    assert.equal(delDxbBehind.sector, "DEL-BOM");
    assert.equal(delDxbBeyond.sector, "BOM-DXB");
    assert.equal(delDxbBeyond.odDI, "Intl");
    assert.equal(delDxbBeyond.legDI, "Intl");
    assert.equal(delDxbBeyond.maxPax, 153);
    assert.equal(delDxbBeyond.maxCargoT, 0.6);
    assert.equal(delDxbBeyond.pax, 0);
    assert.equal(delDxbBeyond.cargoT, 0);
});

test("calculates proration from the field-specific ratio before GCD fallback", () => {
    const row = {
        stops: 1,
        sectorGcd: 50,
        odViaGcd: 100,
        fareProrateRatioL1L2: 0.75,
        rateProrateRatioL1L2: 0,
        trafficType: "behind",
    };

    assert.equal(calculateProrateRatio(row, "fareProrateRatioL1L2"), 0.75);
    assert.equal(calculateProrateRatio(row, "rateProrateRatioL1L2"), 0.5);
});

test("keeps OD revenue local and uses leg revenue only for final RCCY when applySSPricing is enabled", () => {
    const row = recalculateRevenue({
        stops: 0,
        pax: 10,
        cargoT: 5,
        legFare: 1.5,
        legRate: 2,
        odFare: 9,
        odRate: 8,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: true,
    });

    assert.equal(row.odPaxRev, 90);
    assert.equal(row.odCargoRev, 40);
    assert.equal(row.odTotalRev, 130);
    assert.equal(row.rccyLegTotalRev, 25);
    assert.equal(row.fnlRccyTotalRev, 25);
});

test("uses OD RCCY as final revenue when applySSPricing is disabled", () => {
    const row = recalculateRevenue({
        stops: 0,
        pax: 48,
        cargoT: 0.2,
        legFare: 100,
        legRate: 10,
        odFare: 3000,
        odRate: 50,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: false,
    });

    assert.equal(row.odPaxRev, 144000);
    assert.equal(row.odCargoRev, 10);
    assert.equal(row.fnlRccyTotalRev, 144010);
});

test("direct leg revenue calculates leg, cargo, and final INR totals", () => {
    const row = recalculateRevenueForPooRow({
        date: new Date("2026-03-04T00:00:00.000Z"),
        stops: 0,
        trafficType: "leg",
        identifier: "Leg",
        pax: 48,
        cargoT: 0.2,
        legFare: 3000,
        legRate: 50,
        odFare: 3000,
        odRate: 50,
        pooCcy: "INR",
        applySSPricing: false,
    }, { reportingCurrency: "INR", fxRates: [] });

    assert.equal(row.legPaxRev, 144000);
    assert.equal(row.legCargoRev, 10);
    assert.equal(row.fnlRccyPaxRev, 144000);
    assert.equal(row.fnlRccyCargoRev, 10);
    assert.equal(row.fnlRccyTotalRev, 144010);
});

test("reporting rows repair stale final revenue from saved OD inputs", () => {
    const [row] = normalizeRevenueRowsForReporting([
        {
            date: new Date("2026-05-02T00:00:00.000Z"),
            trafficType: "leg",
            stops: 0,
            pax: 76,
            cargoT: 0.4,
            odFare: 5000,
            odRate: 35000,
            legFare: 5000,
            legRate: 35000,
            odPaxRev: 380000,
            odCargoRev: 14000,
            odTotalRev: 394000,
            fnlRccyPaxRev: 0,
            fnlRccyCargoRev: 0,
            fnlRccyTotalRev: 0,
            pooCcy: "INR",
            reportingCurrency: "INR",
            pooCcyToRccy: 1,
            applySSPricing: false,
        },
    ], { reportingCurrency: "INR", fxRates: [] });

    assert.equal(row.fnlRccyPaxRev, 380000);
    assert.equal(row.fnlRccyCargoRev, 14000);
    assert.equal(row.fnlRccyTotalRev, 394000);
});

test("reporting rows recalculate stale non-zero final revenue with current FX config", () => {
    const [row] = normalizeRevenueRowsForReporting([
        {
            date: new Date("2026-05-02T00:00:00.000Z"),
            trafficType: "leg",
            stops: 0,
            pax: 76,
            cargoT: 0.4,
            odFare: 5000,
            odRate: 35000,
            legFare: 5000,
            legRate: 35000,
            odPaxRev: 380000,
            odCargoRev: 14000,
            odTotalRev: 394000,
            fnlRccyPaxRev: 1,
            fnlRccyCargoRev: 1,
            fnlRccyTotalRev: 2,
            pooCcy: "USD",
            reportingCurrency: "INR",
            pooCcyToRccy: 1,
            applySSPricing: false,
        },
    ], {
        reportingCurrency: "INR",
        fxRates: [{ pair: "USD/INR", dateKey: "2026-05-01", rate: 83 }],
    });

    assert.equal(row.pooCcyToRccy, 83);
    assert.equal(row.fnlRccyPaxRev, 31540000);
    assert.equal(row.fnlRccyCargoRev, 1162000);
    assert.equal(row.fnlRccyTotalRev, 32702000);
});

test("multiplies local OD revenue by FX into final reporting currency", () => {
    const row = recalculateRevenue({
        stops: 0,
        pax: 10,
        cargoT: 0,
        legFare: 0,
        legRate: 0,
        odFare: 100,
        odRate: 0,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcy: "USD",
        reportingCurrency: "INR",
        pooCcyToRccy: 83,
        applySSPricing: false,
    });

    assert.equal(row.odPaxRev, 1000);
    assert.equal(row.fnlRccyPaxRev, 83000);
});

test("config-aware revenue helper uses carried-forward LOCAL/REPORTING FX", () => {
    const config = {
        reportingCurrency: "INR",
        fxRates: [
            { pair: "USD/INR", dateKey: "2026-03-01", rate: 83 },
            { pair: "USD/INR", dateKey: "2026-03-10", rate: 84 },
        ],
    };

    const row = recalculateRevenueForPooRow({
        date: new Date("2026-03-04T00:00:00.000Z"),
        pax: 10,
        cargoT: 0,
        legFare: 100,
        legRate: 0,
        odFare: 100,
        odRate: 0,
        pooCcy: "usd",
        applySSPricing: false,
    }, config);

    assert.equal(getCarriedForwardFxRate(config.fxRates, "USD/INR", "2026-03-05"), 83);
    assert.equal(getCarriedForwardFxRate(config.fxRates, "USD/INR", "2026-03-12"), 84);
    assert.equal(convertLocalToReporting(100, "USD", "INR", "2026-03-04", config.fxRates), 8300);
    assert.equal(row.pooCcyToRccy, 83);
    assert.equal(row.fnlRccyPaxRev, 83000);
});

test("default one-stop proration uses each row sector GCD when no explicit ratio is entered", () => {
    const behind = recalculateRevenue({
        stops: 1,
        trafficType: "behind",
        pax: 10,
        cargoT: 1,
        sectorGcd: 1200,
        odViaGcd: 1800,
        odFare: 900,
        odRate: 90,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: false,
    });
    const beyond = recalculateRevenue({
        ...behind,
        trafficType: "beyond",
        sectorGcd: 600,
        legFare: 0,
        legRate: 0,
    });

    assert.equal(behind.legFare, 600);
    assert.equal(behind.legRate, 60);
    assert.equal(beyond.legFare, 300);
    assert.equal(beyond.legRate, 30);
    assert.equal(behind.odPaxRev, 9000);
    assert.equal(beyond.odPaxRev, 9000);
});

test("missing one-stop GCD defaults leg proration to 50/50", () => {
    const row = recalculateRevenue({
        stops: 1,
        trafficType: "behind",
        pax: 5,
        cargoT: 1,
        sectorGcd: 0,
        odViaGcd: 0,
        odFare: 1000,
        odRate: 100,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: false,
    });

    assert.equal(calculateLegShare(row, "fareProrateRatioL1L2"), 0.5);
    assert.equal(row.legFare, 500);
    assert.equal(row.legRate, 50);
});

test("blank-aware revenue clauses match missing, null, and empty values", () => {
    assert.deepEqual(
        buildBlankAwareClause("userTag2", "__BLANK__"),
        { $or: [{ userTag2: { $exists: false } }, { userTag2: null }, { userTag2: "" }] }
    );
    assert.deepEqual(
        buildBlankAwareClause("depStn", "DEL,__BLANK__", (value) => String(value).trim().toUpperCase()),
        {
            $or: [
                { depStn: { $in: ["DEL"] } },
                { depStn: { $exists: false } },
                { depStn: null },
                { depStn: "" },
            ],
        }
    );
});

test("OD revenue aggregation dedupes one-stop behind and beyond rows", () => {
    const rows = [
        makeStateRow({
            _id: "del-hyd-behind",
            trafficType: "behind",
            poo: "DEL",
            od: "DEL-HYD",
            odGroupKey: "system::DEL-HYD::f1::f2",
            date: new Date("2026-03-04T00:00:00.000Z"),
            pax: 11,
            cargoT: 0.2,
            legPaxRev: 57200,
            legCargoRev: 8267,
            legTotalRev: 65467,
            odPaxRev: 85800,
            odCargoRev: 12400,
            odTotalRev: 98200,
            rccyLegPaxRev: 572,
            rccyLegCargoRev: 83,
            rccyLegTotalRev: 655,
            rccyOdPaxRev: 858,
            rccyOdCargoRev: 124,
            rccyOdTotalRev: 982,
            fnlRccyPaxRev: 858,
            fnlRccyCargoRev: 124,
            fnlRccyTotalRev: 982,
        }),
        makeStateRow({
            _id: "del-hyd-beyond",
            trafficType: "beyond",
            poo: "DEL",
            od: "DEL-HYD",
            odGroupKey: "system::DEL-HYD::f1::f2",
            date: new Date("2026-03-04T00:00:00.000Z"),
            pax: 11,
            cargoT: 0.2,
            legPaxRev: 28600,
            legCargoRev: 4133,
            legTotalRev: 32733,
            odPaxRev: 85800,
            odCargoRev: 12400,
            odTotalRev: 98200,
            rccyLegPaxRev: 286,
            rccyLegCargoRev: 41,
            rccyLegTotalRev: 327,
            rccyOdPaxRev: 858,
            rccyOdCargoRev: 124,
            rccyOdTotalRev: 982,
            fnlRccyPaxRev: 858,
            fnlRccyCargoRev: 124,
            fnlRccyTotalRev: 982,
        }),
    ];

    const aggregate = buildRevenueAggregateResponse(rows, ["poo"], "daily");

    assert.equal(aggregate.data.DEL["2026-03-04"].pax, 11);
    assert.equal(aggregate.data.DEL["2026-03-04"].cargoT, 0.2);
    assert.equal(aggregate.data.DEL["2026-03-04"].odRev, 98200);
    assert.equal(aggregate.data.DEL["2026-03-04"].totalRev, 982);
    assert.equal(aggregate.data.DEL["2026-03-04"].count, 1);
});

test("sector revenue aggregation keeps both one-stop leg rows", () => {
    const rows = [
        makeStateRow({
            _id: "del-hyd-behind",
            trafficType: "behind",
            poo: "DEL",
            sector: "DEL-BOM",
            od: "DEL-HYD",
            odGroupKey: "system::DEL-HYD::f1::f2",
            date: new Date("2026-03-04T00:00:00.000Z"),
            pax: 11,
            cargoT: 0.2,
            legPaxRev: 57200,
            legCargoRev: 8267,
            legTotalRev: 65467,
            rccyLegPaxRev: 572,
            rccyLegCargoRev: 83,
            rccyLegTotalRev: 655,
            fnlRccyTotalRev: 982,
        }),
        makeStateRow({
            _id: "del-hyd-beyond",
            trafficType: "beyond",
            poo: "DEL",
            sector: "BOM-HYD",
            od: "DEL-HYD",
            odGroupKey: "system::DEL-HYD::f1::f2",
            date: new Date("2026-03-04T00:00:00.000Z"),
            pax: 11,
            cargoT: 0.2,
            legPaxRev: 28600,
            legCargoRev: 4133,
            legTotalRev: 32733,
            rccyLegPaxRev: 286,
            rccyLegCargoRev: 41,
            rccyLegTotalRev: 327,
            fnlRccyTotalRev: 982,
        }),
    ];

    const aggregate = buildRevenueAggregateResponse(rows, ["sector"], "daily");

    assert.equal(aggregate.data["DEL-BOM"]["2026-03-04"].totalRev, 655);
    assert.equal(aggregate.data["BOM-HYD"]["2026-03-04"].totalRev, 327);
    assert.equal(aggregate.data["DEL-BOM"]["2026-03-04"].count, 1);
    assert.equal(aggregate.data["BOM-HYD"]["2026-03-04"].count, 1);
});

test("buildEditableResponse strips legacy revenue fields from the payload", () => {
    const rows = buildEditableResponse([
        {
            _id: "row-1",
            sNo: 1,
            identifier: "Leg",
            trafficType: "leg",
            poo: "DEL",
            od: "DEL-BOM",
            sector: "DEL-BOM",
            flightNumber: "A100",
            flightId: "flight-1",
            variant: "V1",
            prorateRatioL1: 0.6,
            rccyPax: 1,
            rccyCargo: 2,
            rccyTotalRev: 3,
        },
    ]);

    assert.equal(rows[0].displayType, "Leg");
    assert.equal(rows[0].prorateRatioL1, undefined);
    assert.equal(rows[0].rccyPax, undefined);
    assert.equal(rows[0].rccyCargo, undefined);
    assert.equal(rows[0].rccyTotalRev, undefined);
});

test("rebalances the paired leg when a direct leg is edited", () => {
    const finalRows = applyTrafficUpdates(
        [
            {
                _id: "leg-del",
                trafficType: "leg",
                flightId: "flight-1",
                poo: "DEL",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                odDI: "Dom",
                sector: "DEL-BOM",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|DEL|flight-1|none|leg::flight-1",
                odGroupKey: "leg::flight-1",
                pax: 50,
                cargoT: 0,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 50,
                sourceCargoLF: 0,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "leg-bom",
                trafficType: "leg",
                flightId: "flight-1",
                poo: "BOM",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                odDI: "Dom",
                sector: "DEL-BOM",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|BOM|flight-1|none|leg::flight-1",
                odGroupKey: "leg::flight-1",
                pax: 50,
                cargoT: 0,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 50,
                sourceCargoLF: 0,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
        ],
        [{ _id: "leg-del", pax: 55 }]
    );

    const delRow = finalRows.find((row) => row._id === "leg-del");
    const bomRow = finalRows.find((row) => row._id === "leg-bom");

    assert.equal(delRow.pax, 55);
    assert.equal(bomRow.pax, 45);
});

test("moves a direct leg pax and cargo delta into the alternative POO leg row", () => {
    const finalRows = applyTrafficUpdates(
        [
            makeStateRow({
                _id: "a100-del",
                trafficType: "leg",
                flightId: "A100-2026-03-04",
                poo: "DEL",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                sector: "DEL-BOM",
                flightNumber: "A 100",
                rowKey: "system|leg|DEL|A100-2026-03-04|none|leg::A100-2026-03-04",
                odGroupKey: "leg::A100-2026-03-04",
                pax: 77,
                cargoT: 0.3,
                maxPax: 153,
                maxCargoT: 0.6,
            }),
            makeStateRow({
                _id: "a100-bom",
                trafficType: "leg",
                flightId: "A100-2026-03-04",
                poo: "BOM",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                sector: "DEL-BOM",
                flightNumber: "A 100",
                rowKey: "system|leg|BOM|A100-2026-03-04|none|leg::A100-2026-03-04",
                odGroupKey: "leg::A100-2026-03-04",
                pax: 76,
                cargoT: 0.3,
                maxPax: 153,
                maxCargoT: 0.6,
            }),
        ],
        [{ _id: "a100-del", pax: 65, cargoT: 0.1 }]
    );

    const byId = new Map(finalRows.map((row) => [row._id, row]));

    assert.equal(byId.get("a100-del").pax, 65);
    assert.equal(byId.get("a100-del").cargoT, 0.1);
    assert.equal(byId.get("a100-bom").pax, 88);
    assert.equal(byId.get("a100-bom").cargoT, 0.5);
});

test("rebalance connection traffic across both legs in a two-leg OD", () => {
    const finalRows = applyTrafficUpdates(
        [
            {
                _id: "del-leg",
                trafficType: "leg",
                flightId: "f1",
                poo: "DEL",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                odDI: "Dom",
                sector: "DEL-BOM",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|DEL|f1|none|leg::f1",
                odGroupKey: "leg::f1",
                pax: 20,
                cargoT: 2,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 20,
                sourceCargoLF: 20,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "bom-leg",
                trafficType: "leg",
                flightId: "f1",
                poo: "BOM",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                odDI: "Dom",
                sector: "DEL-BOM",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|BOM|f1|none|leg::f1",
                odGroupKey: "leg::f1",
                pax: 20,
                cargoT: 2,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 20,
                sourceCargoLF: 20,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "conn-first",
                trafficType: "behind",
                flightId: "f1",
                connectedFlightId: "f2",
                poo: "DEL",
                od: "DEL-MAA",
                odOrigin: "DEL",
                odDestination: "MAA",
                odDI: "Dom",
                sector: "DEL-BOM",
                legDI: "Dom",
                identifier: "Behind",
                rowKey: "system|behind|DEL|f1|f2|system::DEL-MAA::f1::f2",
                odGroupKey: "system::DEL-MAA::f1::f2",
                pax: 0,
                cargoT: 0,
                maxPax: 20,
                maxCargoT: 10,
                stops: 1,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 20,
                sourceCargoLF: 20,
                odViaGcd: 2400,
                sectorGcd: 1200,
                totalGcd: 2400,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "conn-second",
                trafficType: "beyond",
                flightId: "f2",
                connectedFlightId: "f1",
                poo: "DEL",
                od: "DEL-MAA",
                odOrigin: "DEL",
                odDestination: "MAA",
                odDI: "Dom",
                sector: "BOM-MAA",
                legDI: "Dom",
                identifier: "Beyond",
                rowKey: "system|beyond|DEL|f2|f1|system::DEL-MAA::f1::f2",
                odGroupKey: "system::DEL-MAA::f1::f2",
                pax: 0,
                cargoT: 0,
                maxPax: 20,
                maxCargoT: 10,
                stops: 1,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 20,
                sourceCargoLF: 20,
                odViaGcd: 2400,
                sectorGcd: 1200,
                totalGcd: 2400,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "maa-leg",
                trafficType: "leg",
                flightId: "f2",
                poo: "MAA",
                od: "BOM-MAA",
                odOrigin: "BOM",
                odDestination: "MAA",
                odDI: "Dom",
                sector: "BOM-MAA",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|MAA|f2|none|leg::f2",
                odGroupKey: "leg::f2",
                pax: 30,
                cargoT: 3,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 30,
                sourceCargoLF: 30,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
            {
                _id: "bom-leg-f2",
                trafficType: "leg",
                flightId: "f2",
                poo: "BOM",
                od: "BOM-MAA",
                odOrigin: "BOM",
                odDestination: "MAA",
                odDI: "Dom",
                sector: "BOM-MAA",
                legDI: "Dom",
                identifier: "Leg",
                rowKey: "system|leg|BOM|f2|none|leg::f2",
                odGroupKey: "leg::f2",
                pax: 30,
                cargoT: 3,
                maxPax: 100,
                maxCargoT: 10,
                stops: 0,
                sourceSeats: 100,
                sourceCargoCapT: 10,
                sourcePaxLF: 30,
                sourceCargoLF: 30,
                odViaGcd: 1200,
                sectorGcd: 1200,
                totalGcd: 1200,
                legFare: 0,
                legRate: 0,
                odFare: 0,
                odRate: 0,
                fareProrateRatioL1L2: 0,
                rateProrateRatioL1L2: 0,
                pooCcyToRccy: 1,
                applySSPricing: false,
            },
        ],
        [{ _id: "conn-first", pax: 5, cargoT: 1 }]
    );

    assert.equal(finalRows.find((row) => row._id === "conn-first").pax, 5);
    assert.equal(finalRows.find((row) => row._id === "conn-second").pax, 5);
    assert.equal(finalRows.find((row) => row._id === "del-leg").pax, 15);
    assert.equal(finalRows.find((row) => row._id === "maa-leg").pax, 25);
});

test("connection edits mirror only rows for the same endpoint POO", () => {
    const firstSnapshot = makeConnectionSnapshot();
    const secondSnapshot = makeConnectionSnapshot({
        flightId: "f2",
        depStn: "BOM",
        arrStn: "DXB",
        sector: "BOM-DXB",
        odDI: "Intl",
        legDI: "Intl",
        flightNumber: "A 102",
        maxPax: 243,
        maxCargoT: 7.7,
        sourcePaxTotal: 243,
        sourceCargoTotal: 7.7,
        sourceSeats: 296,
        sourceCargoCapT: 8.5,
        sectorGcd: 1900,
    });

    const connectionRows = ["DEL", "DXB"]
        .flatMap((pagePoo) =>
            buildSystemConnectionRows({
                pagePoo,
                firstSnapshot,
                secondSnapshot,
                existingRowsByKey: new Map(),
                shouldReset: false,
                pageCurrencyContext: {},
            })
        )
        .map((row) => makeStateRow({
            ...row,
            _id: `${row.poo.toLowerCase()}-${row.trafficType}`,
        }));

    const finalRows = applyTrafficUpdates(
        [
            makeStateRow({
                _id: "leg-del",
                flightId: "f1",
                poo: "DEL",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                sector: "DEL-BOM",
                pax: 20,
                cargoT: 2,
            }),
            makeStateRow({
                _id: "leg-bom-f1",
                flightId: "f1",
                poo: "BOM",
                od: "DEL-BOM",
                odOrigin: "DEL",
                odDestination: "BOM",
                sector: "DEL-BOM",
                pax: 20,
                cargoT: 2,
            }),
            makeStateRow({
                _id: "leg-bom-f2",
                flightId: "f2",
                poo: "BOM",
                od: "BOM-DXB",
                odOrigin: "BOM",
                odDestination: "DXB",
                odDI: "Intl",
                sector: "BOM-DXB",
                legDI: "Intl",
                pax: 30,
                cargoT: 3,
            }),
            makeStateRow({
                _id: "leg-dxb",
                flightId: "f2",
                poo: "DXB",
                od: "BOM-DXB",
                odOrigin: "BOM",
                odDestination: "DXB",
                odDI: "Intl",
                sector: "BOM-DXB",
                legDI: "Intl",
                pax: 30,
                cargoT: 3,
            }),
            ...connectionRows,
        ],
        [{ _id: "del-behind", pax: 5, cargoT: 0.1 }]
    );

    const byId = new Map(finalRows.map((row) => [row._id, row]));

    assert.equal(byId.get("del-behind").pax, 5);
    assert.equal(byId.get("del-beyond").pax, 5);
    assert.equal(byId.get("dxb-behind").pax, 0);
    assert.equal(byId.get("dxb-beyond").pax, 0);
    assert.equal(byId.get("leg-del").pax, 15);
    assert.equal(byId.get("leg-dxb").pax, 25);
});

test("rejects an edit when the balancing bucket does not have enough capacity", () => {
    assert.throws(() =>
        applyTrafficUpdates(
            [
                {
                    _id: "leg-del",
                    trafficType: "leg",
                    flightId: "flight-1",
                    poo: "DEL",
                    od: "DEL-BOM",
                    odOrigin: "DEL",
                    odDestination: "BOM",
                    odDI: "Dom",
                    sector: "DEL-BOM",
                    legDI: "Dom",
                    identifier: "Leg",
                    rowKey: "system|leg|DEL|flight-1|none|leg::flight-1",
                    odGroupKey: "leg::flight-1",
                    pax: 50,
                    cargoT: 0,
                    maxPax: 100,
                    maxCargoT: 10,
                    stops: 0,
                    sourceSeats: 100,
                    sourceCargoCapT: 10,
                    sourcePaxLF: 50,
                    sourceCargoLF: 0,
                    odViaGcd: 1200,
                    sectorGcd: 1200,
                    totalGcd: 1200,
                    legFare: 0,
                    legRate: 0,
                    odFare: 0,
                    odRate: 0,
                    fareProrateRatioL1L2: 0,
                    rateProrateRatioL1L2: 0,
                    pooCcyToRccy: 1,
                    applySSPricing: false,
                },
                {
                    _id: "leg-bom",
                    trafficType: "leg",
                    flightId: "flight-1",
                    poo: "BOM",
                    od: "DEL-BOM",
                    odOrigin: "DEL",
                    odDestination: "BOM",
                    odDI: "Dom",
                    sector: "DEL-BOM",
                    legDI: "Dom",
                    identifier: "Leg",
                    rowKey: "system|leg|BOM|flight-1|none|leg::flight-1",
                    odGroupKey: "leg::flight-1",
                    pax: 0,
                    cargoT: 0,
                    maxPax: 100,
                    maxCargoT: 10,
                    stops: 0,
                    sourceSeats: 100,
                    sourceCargoCapT: 10,
                    sourcePaxLF: 0,
                    sourceCargoLF: 0,
                    odViaGcd: 1200,
                    sectorGcd: 1200,
                    totalGcd: 1200,
                    legFare: 0,
                    legRate: 0,
                    odFare: 0,
                    odRate: 0,
                    fareProrateRatioL1L2: 0,
                    rateProrateRatioL1L2: 0,
                    pooCcyToRccy: 1,
                    applySSPricing: false,
                },
            ],
            [{ _id: "leg-del", pax: 60 }]
        )
    );
});

test("applies the 4 Mar DEL POO example across leg, behind, and beyond rows", () => {
    const baseRow = (overrides) => ({
        _id: overrides._id,
        trafficType: overrides.trafficType,
        flightId: overrides.flightId,
        connectedFlightId: overrides.connectedFlightId || null,
        poo: overrides.poo,
        od: overrides.od,
        odOrigin: overrides.odOrigin,
        odDestination: overrides.odDestination,
        odDI: overrides.odDI || "Dom",
        sector: overrides.sector,
        legDI: overrides.legDI || "Dom",
        identifier: overrides.identifier,
        rowKey: overrides.rowKey || overrides._id,
        odGroupKey: overrides.odGroupKey,
        flightNumber: overrides.flightNumber,
        connectedFlightNumber: overrides.connectedFlightNumber || null,
        flightList: overrides.flightList || [overrides.flightNumber].filter(Boolean),
        timeInclLayover: overrides.timeInclLayover || "00:00",
        pax: overrides.pax,
        cargoT: overrides.cargoT,
        maxPax: overrides.maxPax,
        maxCargoT: overrides.maxCargoT,
        stops: overrides.stops || 0,
        sourceSeats: overrides.sourceSeats || overrides.maxPax,
        sourceCargoCapT: overrides.sourceCargoCapT || overrides.maxCargoT,
        sourcePaxTotal: overrides.sourcePaxTotal || 0,
        sourceCargoTotal: overrides.sourceCargoTotal || 0,
        sourcePaxLF: overrides.sourcePaxLF || 0,
        sourceCargoLF: overrides.sourceCargoLF || 0,
        odViaGcd: overrides.odViaGcd,
        sectorGcd: overrides.sectorGcd,
        totalGcd: overrides.totalGcd || overrides.odViaGcd,
        legFare: 0,
        legRate: 0,
        odFare: 0,
        odRate: 0,
        fareProrateRatioL1L2: 0,
        rateProrateRatioL1L2: 0,
        pooCcyToRccy: 1,
        applySSPricing: false,
    });

    const rows = [
        baseRow({
            _id: "a100-del",
            trafficType: "leg",
            flightId: "A100-2026-03-04",
            poo: "DEL",
            od: "DEL-BOM",
            odOrigin: "DEL",
            odDestination: "BOM",
            sector: "DEL-BOM",
            identifier: "Leg",
            odGroupKey: "leg::A100-2026-03-04",
            flightNumber: "A 100",
            pax: 77,
            cargoT: 0.3,
            maxPax: 153,
            maxCargoT: 0.6,
            odViaGcd: 1200,
            sectorGcd: 1200,
        }),
        baseRow({
            _id: "a100-bom",
            trafficType: "leg",
            flightId: "A100-2026-03-04",
            poo: "BOM",
            od: "DEL-BOM",
            odOrigin: "DEL",
            odDestination: "BOM",
            sector: "DEL-BOM",
            identifier: "Leg",
            odGroupKey: "leg::A100-2026-03-04",
            flightNumber: "A 100",
            pax: 76,
            cargoT: 0.3,
            maxPax: 153,
            maxCargoT: 0.6,
            odViaGcd: 1200,
            sectorGcd: 1200,
        }),
        baseRow({
            _id: "a101-bom",
            trafficType: "leg",
            flightId: "A101-2026-03-04",
            poo: "BOM",
            od: "BOM-HYD",
            odOrigin: "BOM",
            odDestination: "HYD",
            sector: "BOM-HYD",
            identifier: "Leg",
            odGroupKey: "leg::A101-2026-03-04",
            flightNumber: "A 101",
            pax: 64,
            cargoT: 0.7,
            maxPax: 128,
            maxCargoT: 1.4,
            odViaGcd: 600,
            sectorGcd: 600,
        }),
        baseRow({
            _id: "a101-hyd",
            trafficType: "leg",
            flightId: "A101-2026-03-04",
            poo: "HYD",
            od: "BOM-HYD",
            odOrigin: "BOM",
            odDestination: "HYD",
            sector: "BOM-HYD",
            identifier: "Leg",
            odGroupKey: "leg::A101-2026-03-04",
            flightNumber: "A 101",
            pax: 64,
            cargoT: 0.7,
            maxPax: 128,
            maxCargoT: 1.4,
            odViaGcd: 600,
            sectorGcd: 600,
        }),
        baseRow({
            _id: "a102-bom",
            trafficType: "leg",
            flightId: "A102-2026-03-04",
            poo: "BOM",
            od: "BOM-DXB",
            odOrigin: "BOM",
            odDestination: "DXB",
            odDI: "Intl",
            legDI: "Intl",
            sector: "BOM-DXB",
            identifier: "Leg",
            odGroupKey: "leg::A102-2026-03-04",
            flightNumber: "A 102",
            pax: 121,
            cargoT: 3.85,
            maxPax: 243,
            maxCargoT: 7.7,
            odViaGcd: 1900,
            sectorGcd: 1900,
        }),
        baseRow({
            _id: "a102-dxb",
            trafficType: "leg",
            flightId: "A102-2026-03-04",
            poo: "DXB",
            od: "BOM-DXB",
            odOrigin: "BOM",
            odDestination: "DXB",
            odDI: "Intl",
            legDI: "Intl",
            sector: "BOM-DXB",
            identifier: "Leg",
            odGroupKey: "leg::A102-2026-03-04",
            flightNumber: "A 102",
            pax: 122,
            cargoT: 3.85,
            maxPax: 243,
            maxCargoT: 7.7,
            odViaGcd: 1900,
            sectorGcd: 1900,
        }),
        baseRow({
            _id: "del-hyd-behind",
            trafficType: "behind",
            flightId: "A100-2026-03-04",
            connectedFlightId: "A101-2026-03-04",
            poo: "DEL",
            od: "DEL-HYD",
            odOrigin: "DEL",
            odDestination: "HYD",
            sector: "DEL-BOM",
            identifier: "Behind",
            odGroupKey: "system::DEL-HYD::A100-2026-03-04::A101-2026-03-04",
            flightNumber: "A 100",
            connectedFlightNumber: "A 101",
            flightList: ["A 100", "A 101"],
            timeInclLayover: "07:10",
            pax: 0,
            cargoT: 0,
            maxPax: 128,
            maxCargoT: 0.6,
            stops: 1,
            odViaGcd: 1800,
            sectorGcd: 1200,
            totalGcd: 1800,
        }),
        baseRow({
            _id: "del-hyd-beyond",
            trafficType: "beyond",
            flightId: "A101-2026-03-04",
            connectedFlightId: "A100-2026-03-04",
            poo: "DEL",
            od: "DEL-HYD",
            odOrigin: "DEL",
            odDestination: "HYD",
            sector: "BOM-HYD",
            identifier: "Beyond",
            odGroupKey: "system::DEL-HYD::A100-2026-03-04::A101-2026-03-04",
            flightNumber: "A 101",
            connectedFlightNumber: "A 100",
            flightList: ["A 100", "A 101"],
            timeInclLayover: "07:10",
            pax: 0,
            cargoT: 0,
            maxPax: 128,
            maxCargoT: 0.6,
            stops: 1,
            odViaGcd: 1800,
            sectorGcd: 600,
            totalGcd: 1800,
        }),
        baseRow({
            _id: "del-dxb-behind",
            trafficType: "behind",
            flightId: "A100-2026-03-04",
            connectedFlightId: "A102-2026-03-04",
            poo: "DEL",
            od: "DEL-DXB",
            odOrigin: "DEL",
            odDestination: "DXB",
            odDI: "Intl",
            sector: "DEL-BOM",
            identifier: "Behind",
            odGroupKey: "system::DEL-DXB::A100-2026-03-04::A102-2026-03-04",
            flightNumber: "A 100",
            connectedFlightNumber: "A 102",
            flightList: ["A 100", "A 102"],
            timeInclLayover: "09:30",
            pax: 0,
            cargoT: 0,
            maxPax: 153,
            maxCargoT: 0.6,
            stops: 1,
            odViaGcd: 3100,
            sectorGcd: 1200,
            totalGcd: 3100,
        }),
        baseRow({
            _id: "del-dxb-beyond",
            trafficType: "beyond",
            flightId: "A102-2026-03-04",
            connectedFlightId: "A100-2026-03-04",
            poo: "DEL",
            od: "DEL-DXB",
            odOrigin: "DEL",
            odDestination: "DXB",
            odDI: "Intl",
            legDI: "Intl",
            sector: "BOM-DXB",
            identifier: "Beyond",
            odGroupKey: "system::DEL-DXB::A100-2026-03-04::A102-2026-03-04",
            flightNumber: "A 102",
            connectedFlightNumber: "A 100",
            flightList: ["A 100", "A 102"],
            timeInclLayover: "09:30",
            pax: 0,
            cargoT: 0,
            maxPax: 153,
            maxCargoT: 0.6,
            stops: 1,
            odViaGcd: 3100,
            sectorGcd: 1900,
            totalGcd: 3100,
        }),
    ];

    const finalRows = applyTrafficUpdates(rows, [
        { _id: "a100-del", pax: 65, cargoT: 0.1, odFare: 3000, odRate: 45 },
        { _id: "del-hyd-behind", pax: 5, cargoT: 0, odFare: 5000, odRate: 60 },
        { _id: "del-dxb-behind", pax: 12, cargoT: 0.1 },
    ]);

    const byId = new Map(finalRows.map((row) => [row._id, row]));

    assert.equal(byId.get("a100-del").pax, 48);
    assert.equal(byId.get("a100-del").cargoT, 0);
    assert.equal(byId.get("a100-del").odFare, 3000);
    assert.equal(byId.get("a100-del").odRate, 45);

    assert.equal(byId.get("a100-bom").pax, 88);
    assert.equal(byId.get("a100-bom").cargoT, 0.5);
    assert.equal(byId.get("a101-hyd").pax, 59);
    assert.equal(byId.get("a102-dxb").pax, 110);
    assert.equal(byId.get("a102-dxb").cargoT, 3.75);

    assert.equal(byId.get("del-hyd-behind").pax, 5);
    assert.equal(byId.get("del-hyd-beyond").pax, 5);
    assert.equal(byId.get("del-hyd-behind").timeInclLayover, "07:10");
    assert.equal(byId.get("del-hyd-beyond").timeInclLayover, "07:10");
    assert.equal(byId.get("del-dxb-behind").pax, 12);
    assert.equal(byId.get("del-dxb-beyond").pax, 12);
    assert.equal(byId.get("del-dxb-behind").timeInclLayover, "09:30");
    assert.equal(byId.get("del-dxb-beyond").timeInclLayover, "09:30");
    assert.equal(byId.get("a101-bom").pax, 64);
    assert.equal(byId.get("a102-bom").pax, 121);
});

test("loads connected flight leg buckets before applying an OD edit", async () => {
    const rows = [
        makeStateRow({
            _id: "a100-del",
            trafficType: "leg",
            flightId: "f1",
            poo: "DEL",
            od: "DEL-BOM",
            odOrigin: "DEL",
            odDestination: "BOM",
            sector: "DEL-BOM",
            pax: 65,
            cargoT: 0.1,
            maxPax: 153,
            maxCargoT: 0.6,
        }),
        makeStateRow({
            _id: "a100-bom",
            trafficType: "leg",
            flightId: "f1",
            poo: "BOM",
            od: "DEL-BOM",
            odOrigin: "DEL",
            odDestination: "BOM",
            sector: "DEL-BOM",
            pax: 88,
            cargoT: 0.5,
            maxPax: 153,
            maxCargoT: 0.6,
        }),
        makeStateRow({
            _id: "a101-bom",
            trafficType: "leg",
            flightId: "f2",
            poo: "BOM",
            od: "BOM-HYD",
            odOrigin: "BOM",
            odDestination: "HYD",
            sector: "BOM-HYD",
            pax: 64,
            cargoT: 0.7,
            maxPax: 128,
            maxCargoT: 1.4,
        }),
        makeStateRow({
            _id: "a101-hyd",
            trafficType: "leg",
            flightId: "f2",
            poo: "HYD",
            od: "BOM-HYD",
            odOrigin: "BOM",
            odDestination: "HYD",
            sector: "BOM-HYD",
            pax: 64,
            cargoT: 0.7,
            maxPax: 128,
            maxCargoT: 1.4,
        }),
        makeStateRow({
            _id: "del-hyd-behind",
            trafficType: "behind",
            flightId: "f1",
            connectedFlightId: "f2",
            poo: "DEL",
            od: "DEL-HYD",
            odOrigin: "DEL",
            odDestination: "HYD",
            sector: "DEL-BOM",
            identifier: "Behind",
            odGroupKey: "system::DEL-HYD::f1::f2",
            pax: 0,
            cargoT: 0,
            maxPax: 128,
            maxCargoT: 0.6,
            stops: 1,
        }),
        makeStateRow({
            _id: "del-hyd-beyond",
            trafficType: "beyond",
            flightId: "f2",
            connectedFlightId: "f1",
            poo: "DEL",
            od: "DEL-HYD",
            odOrigin: "DEL",
            odDestination: "HYD",
            sector: "BOM-HYD",
            identifier: "Beyond",
            odGroupKey: "system::DEL-HYD::f1::f2",
            pax: 0,
            cargoT: 0,
            maxPax: 128,
            maxCargoT: 0.6,
            stops: 1,
        }),
    ];

    const originalFind = PooTable.find;
    const originalBulkWrite = PooTable.bulkWrite;
    const originalDeleteMany = PooTable.deleteMany;
    const findQueries = [];
    let persistedRows = [];

    PooTable.find = async (query) => {
        findQueries.push(query);
        if (findQueries.length === 1) {
            return [rows.find((row) => row._id === "del-hyd-behind")];
        }
        if (findQueries.length === 2) {
            return rows;
        }
        return persistedRows;
    };
    PooTable.bulkWrite = async (ops) => {
        persistedRows = ops.map((op) => ({
            _id: op.updateOne.filter._id,
            ...op.updateOne.update.$set,
        }));
    };
    PooTable.deleteMany = async () => ({ deletedCount: 0 });

    try {
        const updatedRows = await applyUpdatesForDate({
            userId: "user-1",
            updates: [{ _id: "del-hyd-behind", pax: 5, cargoT: 0 }],
        });

        const workingRowsQuery = findQueries[1];
        const flightScope = workingRowsQuery.$or.find((clause) => clause.flightId)?.flightId.$in;
        assert.deepEqual(flightScope.sort(), ["f1", "f2"]);

        const byId = new Map(updatedRows.map((row) => [row._id, row]));
        assert.equal(byId.get("a100-del").pax, 60);
        assert.equal(byId.get("a101-hyd").pax, 59);
        assert.equal(byId.get("del-hyd-behind").pax, 5);
        assert.equal(byId.get("del-hyd-beyond").pax, 5);
    } finally {
        PooTable.find = originalFind;
        PooTable.bulkWrite = originalBulkWrite;
        PooTable.deleteMany = originalDeleteMany;
    }
});
