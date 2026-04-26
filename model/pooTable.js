const mongoose = require('mongoose');

const pooTableSchema = new mongoose.Schema({
    userId: { type: String, required: true, index: true },

    // Identifiers and Routing
    sNo: { type: Number, required: true },
    rowKey: { type: String, trim: true, required: true },
    flightId: { type: String, trim: true, required: true, index: true },
    connectedFlightId: { type: String, trim: true, default: null },
    connectedFlightNumber: { type: String, trim: true, default: null },
    ownerFlightId: { type: String, trim: true, default: null },
    connectionKey: { type: String, trim: true, default: null, index: true },
    odGroupKey: { type: String, trim: true, default: null, index: true },
    trafficType: { type: String, trim: true, required: true, index: true },
    source: { type: String, trim: true, default: "system" },
    isUserDefined: { type: Boolean, default: false },
    al: { type: String, trim: true }, // Airline Code
    poo: { type: String, trim: true }, // Point of Origin
    od: { type: String, trim: true }, // Origin-Destination
    odOrigin: { type: String, trim: true, default: null },
    odDestination: { type: String, trim: true, default: null },
    odDI: { type: String, trim: true }, // OD Domestic/International
    stops: { type: Number, default: 0 },
    identifier: { type: String, trim: true },
    sector: { type: String, trim: true },
    legDI: { type: String, trim: true }, // Leg Domestic/International

    // Flight Details
    date: { type: Date },
    day: { type: String, trim: true }, // e.g., 'Mon', 'Tue' or day number
    flightNumber: { type: String, trim: true },
    variant: { type: String, trim: true },
    std: { type: String, trim: true, default: null },
    sta: { type: String, trim: true, default: null },
    connectedStd: { type: String, trim: true, default: null },
    connectedSta: { type: String, trim: true, default: null },
    flightList: { type: [String], default: [] },
    timeInclLayover: { type: String, trim: true, default: null },

    // Capacity and Traffic (Pax/Cargo)
    maxPax: { type: Number, default: 0 },
    maxCargoT: { type: Number, default: 0 },
    pax: { type: Number, default: 0 },
    cargoT: { type: Number, default: 0 },
    sourcePaxTotal: { type: Number, default: 0 },
    sourceCargoTotal: { type: Number, default: 0 },
    sourceSeats: { type: Number, default: 0 },
    sourceCargoCapT: { type: Number, default: 0 },
    sourcePaxLF: { type: Number, default: 0 },
    sourceCargoLF: { type: Number, default: 0 },

    // Distances
    sectorGcd: { type: Number, default: 0 }, // Great Circle Distance
    odViaGcd: { type: Number, default: 0 },
    totalGcd: { type: Number, default: 0 },

    // Fares, Rates, and Proration
    legFare: { type: Number, default: 0 },
    legRate: { type: Number, default: 0 },
    odFare: { type: Number, default: 0 },
    odRate: { type: Number, default: 0 },
    fareProrateRatioL1L2: { type: Number, default: 0 },
    rateProrateRatioL1L2: { type: Number, default: 0 },
    applySSPricing: { type: Boolean, default: false },
    interline: { type: String, trim: true, default: "" },
    codeshare: { type: String, trim: true, default: "" },

    // Leg Revenue (Local Currency)
    legPaxRev: { type: Number, default: 0 },
    legCargoRev: { type: Number, default: 0 },
    legTotalRev: { type: Number, default: 0 },

    // OD Revenue (Local Currency)
    odPaxRev: { type: Number, default: 0 },
    odCargoRev: { type: Number, default: 0 },
    odTotalRev: { type: Number, default: 0 },

    // Currencies & Exchange Rates
    pooCcy: { type: String, trim: true }, // POO Currency Code
    pooCcyToRccy: { type: Number, default: 1 }, // Exchange rate

    // Leg Revenue (Reporting Currency - RCCY)
    rccyLegPaxRev: { type: Number, default: 0 },
    rccyLegCargoRev: { type: Number, default: 0 },
    rccyLegTotalRev: { type: Number, default: 0 },

    // OD Revenue (Reporting Currency - RCCY)
    rccyOdPaxRev: { type: Number, default: 0 },
    rccyOdCargoRev: { type: Number, default: 0 },
    rccyOdTotalRev: { type: Number, default: 0 },

    // Final Revenue (Reporting Currency - RCCY)
    fnlRccyPaxRev: { type: Number, default: 0 },
    fnlRccyCargoRev: { type: Number, default: 0 },
    fnlRccyTotalRev: { type: Number, default: 0 },
    reportingCurrency: { type: String, trim: true, default: "" },
    stationCurrencySource: { type: String, trim: true, default: "manual" },
    reportingCurrencySource: { type: String, trim: true, default: "manual" }
}, {
    timestamps: true, // Automatically adds createdAt and updatedAt fields
    collection: 'pooTables'
});

pooTableSchema.index(
    { userId: 1, rowKey: 1 },
    { unique: true }
);

pooTableSchema.index(
    { userId: 1, poo: 1, date: 1, trafficType: 1, sNo: 1 }
);

pooTableSchema.index(
    { userId: 1, date: 1, odGroupKey: 1, poo: 1 }
);

const PooTable = mongoose.model('PooTable', pooTableSchema);

module.exports = PooTable;
