const mongoose = require('mongoose');

const pooTableSchema = new mongoose.Schema({
    // Identifiers and Routing
    sNo: { type: Number, required: true },
    al: { type: String, trim: true }, // Airline Code
    poo: { type: String, trim: true }, // Point of Origin
    od: { type: String, trim: true }, // Origin-Destination
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

    // Capacity and Traffic (Pax/Cargo)
    maxPax: { type: Number, default: 0 },
    maxCargoT: { type: Number, default: 0 },
    pax: { type: Number, default: 0 },
    cargoT: { type: Number, default: 0 },

    // Distances
    sectorGcd: { type: Number, default: 0 }, // Great Circle Distance
    odViaGcd: { type: Number, default: 0 },

    // Fares, Rates, and Proration
    legFare: { type: Number, default: 0 },
    legRate: { type: Number, default: 0 },
    odFare: { type: Number, default: 0 },
    odRate: { type: Number, default: 0 },
    prorateRatioL1: { type: Number, default: 0 },

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

    // Total Revenue (Reporting Currency - RCCY)
    rccyPax: { type: Number, default: 0 },
    rccyCargo: { type: Number, default: 0 },
    rccyTotalRev: { type: Number, default: 0 }
}, {
    timestamps: true, // Automatically adds createdAt and updatedAt fields
    collection: 'pooTables'
});

const PooTable = mongoose.model('PooTable', pooTableSchema);

module.exports = PooTable;