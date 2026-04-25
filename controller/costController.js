const CostConfig = require("../model/costConfigSchema");
const Flights = require("../model/flight");
const Utilisation = require("../model/utilisation");
const MaintenanceReserve = require("../model/maintenanceReserveSchema");
const {
  normalizeCostConfig,
  computeFlightCostsBatch,
  flattenFuelConsumRows,
  flattenFuelConsumIndexRows,
  flattenPlfEffectRows,
  flattenFuelPriceRows,
  normalizeAllocationTable,
  normalizeApuUsage,
  normalizeOtherMx,
  normalizeTransitMx,
  hydrateSchMxEvents,
  groupFuelConsumRows,
  groupFuelConsumIndexRows,
  groupPlfEffectRows,
  groupFuelPriceRows,
} = require("../utils/costLogic");
const moment = require("moment");

const buildExactDayDates = (schMxEvents = []) => {
  const exactDates = new Map();
  const toExactDay = (value) => {
    const parsed = moment.utc(value);
    return parsed.isValid() ? parsed.startOf("day").toDate() : null;
  };

  schMxEvents.forEach((row) => {
    const eventDate = toExactDay(row?.date);
    if (eventDate) exactDates.set(eventDate.toISOString(), eventDate);
    const drawdownDate = toExactDay(row?.drawdownDate || row?.mrDrawdownDate);
    if (drawdownDate) exactDates.set(drawdownDate.toISOString(), drawdownDate);
  });

  return Array.from(exactDates.values());
};

const hydrateSchMxEventsForUser = async (userId, schMxEvents = []) => {
  if (!Array.isArray(schMxEvents) || schMxEvents.length === 0) {
    return [];
  }

  const exactDayDates = buildExactDayDates(schMxEvents);
  if (exactDayDates.length === 0) {
    return hydrateSchMxEvents(schMxEvents);
  }

  const [utilisationRows, maintenanceReserveRows] = await Promise.all([
    Utilisation.find({ userId, date: { $in: exactDayDates } }).lean(),
    MaintenanceReserve.find({ userId, date: { $in: exactDayDates } }).lean(),
  ]);

  return hydrateSchMxEvents(schMxEvents, {
    utilisationRows,
    maintenanceReserveRows,
  });
};

// Save or Update user's Cost Configuration
exports.saveCostConfig = async (req, res) => {
  try {
    const userId = req.user.id;
    const configData = req.body;
    const hydratedSchMxEvents = await hydrateSchMxEventsForUser(
      userId,
      Array.isArray(configData.schMxEvents) ? configData.schMxEvents : []
    );

    const nextConfig = {
      ...configData,
      allocationTable: normalizeAllocationTable(configData.allocationTable || configData.costAllocation || []),
      fuelConsum: flattenFuelConsumRows(configData.fuelConsum || []),
      fuelConsumIndex: flattenFuelConsumIndexRows(configData.fuelConsumIndex || []),
      plfEffect: flattenPlfEffectRows(configData.plfEffect || []),
      ccyFuel: flattenFuelPriceRows(configData.ccyFuel || []),
      apuUsage: normalizeApuUsage(configData.apuUsage || []),
      otherMx: normalizeOtherMx(configData.otherMx || []),
      transitMx: normalizeTransitMx(configData.transitMx || []),
      schMxEvents: hydratedSchMxEvents,
    };

    const updatedConfig = await CostConfig.findOneAndUpdate(
      { userId },
      { $set: nextConfig },
      { new: true, upsert: true }
    );

    res.status(200).json({ success: true, data: updatedConfig, message: "Cost settings saved successfully." });
  } catch (error) {
    console.error("Error saving cost config:", error);
    res.status(500).json({ success: false, message: "Failed to save cost settings." });
  }
};

// Retrieve user's Cost Configuration
exports.getCostConfig = async (req, res) => {
  try {
    const userId = req.user.id;
    let config = await CostConfig.findOne({ userId }).lean();
    if (!config) {
      // Return empty sets if not found
      config = {
        allocationTable: [],
        fuelConsum: [], fuelConsumIndex: [], apuUsage: [], plfEffect: [], ccyFuel: [],
        leasedReserve: [], schMxEvents: [], transitMx: [], otherMx: [], rotableChanges: [],
        navEnr: [], navTerm: [], airportLanding: [], airportDom: [], airportIntl: [], airportAvsec: [], otherDoc: []
      };
    } else {
      config.fuelConsum = groupFuelConsumRows(config.fuelConsum || []);
      config.fuelConsumIndex = groupFuelConsumIndexRows(config.fuelConsumIndex || []);
      config.plfEffect = groupPlfEffectRows(config.plfEffect || []);
      config.ccyFuel = groupFuelPriceRows(config.ccyFuel || []);
      config.allocationTable = normalizeAllocationTable(config.allocationTable || config.costAllocation || []);
      config.apuUsage = normalizeApuUsage(config.apuUsage || []);
      config.otherMx = normalizeOtherMx(config.otherMx || []);
      config.transitMx = normalizeTransitMx(config.transitMx || []);
      config.schMxEvents = await hydrateSchMxEventsForUser(userId, config.schMxEvents || []);
    }
    res.status(200).json({ success: true, data: config });
  } catch (error) {
    console.error("Error getting cost config:", error);
    res.status(500).json({ success: false, message: "Failed to load cost settings." });
  }
};

// Get Dashboard Data for CostPage
exports.getCostPageData = async (req, res) => {
  try {
    const { label, from, to, sector, variant, sn, flight, userTag1, userTag2 } = req.body;
    const userId = req.user.id;

    // 1. Build Query for flights
    const matchQuery = { userId };
    if (label && label.value !== "both") {
      matchQuery.domIntl = label.value.toLowerCase();
    }
    
    const applyArrayFilter = (field, filterArray) => {
      if (filterArray?.length) matchQuery[field] = { $in: filterArray.map(f => f.value) };
    };

    applyArrayFilter("depStn", from);
    applyArrayFilter("arrStn", to);
    applyArrayFilter("sector", sector);
    applyArrayFilter("variant", variant);
    applyArrayFilter("aircraft.msn", sn);
    applyArrayFilter("flight", flight);
    applyArrayFilter("userTag1", userTag1);
    applyArrayFilter("userTag2", userTag2);

    // 2. Fetch Flights
    const flights = await Flights.find(matchQuery).lean();

    // 3. Fetch Cost Config 
    const costConfig = normalizeCostConfig(await CostConfig.findOne({ userId }).lean() || {});

    // 4. Compute Costs
    const enrichedFlights = computeFlightCostsBatch(flights, costConfig);

    res.status(200).json({ flights: enrichedFlights });

  } catch (error) {
    console.error("Error computing cost page data:", error);
    res.status(500).json({ message: "Internal Server Error" });
  }
};
