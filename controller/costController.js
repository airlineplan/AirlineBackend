const CostConfig = require("../model/costConfigSchema");
const RevenueConfig = require("../model/revenueConfigSchema");
const Flights = require("../model/flight");
const Utilisation = require("../model/utilisation");
const MaintenanceReserve = require("../model/maintenanceReserveSchema");
const Fleet = require("../model/fleet");
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
  normalizeNavMtowTiers,
  serializeNavigationCostRows,
  hydrateSchMxEvents,
  groupFuelConsumRows,
  groupFuelConsumIndexRows,
  groupPlfEffectRows,
  groupFuelPriceRows,
  getFlightSnContext,
} = require("../utils/costLogic");
const { buildMaintenanceReserveContext } = require("../utils/maintenanceReserveContext");
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

const normalizeNavigationTablesForStorage = (configData = {}) => {
  const navMtowTiers = normalizeNavMtowTiers(configData.navMtowTiers);
  return {
    navMtowTiers,
    navEnr: serializeNavigationCostRows(configData.navEnr || [], "sector", navMtowTiers),
    navTerm: serializeNavigationCostRows(configData.navTerm || [], "arrStn", navMtowTiers),
  };
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
    const navigationTables = normalizeNavigationTablesForStorage(configData);

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
      ...navigationTables,
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
        navMtowTiers: [73000, 77000, 78000, 79000],
        navEnr: [], navTerm: [], airportLanding: [], airportDom: [], airportIntl: [], airportAvsec: [], airportOther: [], otherDoc: []
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
      config.navMtowTiers = normalizeNavMtowTiers(config.navMtowTiers);
      config.navEnr = serializeNavigationCostRows(config.navEnr || [], "sector", config.navMtowTiers);
      config.navTerm = serializeNavigationCostRows(config.navTerm || [], "arrStn", config.navMtowTiers);
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
    applyArrayFilter("flight", flight);
    applyArrayFilter("userTag1", userTag1);
    applyArrayFilter("userTag2", userTag2);

    // 2. Fetch Flights
    const flights = await Flights.find(matchQuery).lean();

    // 3. Fetch Cost Config 
    const [rawCostConfig, rawRevenueConfig, fleetRows, mrContext] = await Promise.all([
      CostConfig.findOne({ userId }).lean(),
      RevenueConfig.findOne({ userId }).lean(),
      Fleet.find({ userId }).lean(),
      buildMaintenanceReserveContext(userId, flights),
    ]);
    const costConfig = normalizeCostConfig({
      ...(rawCostConfig || {}),
      reportingCurrency: rawRevenueConfig?.reportingCurrency || rawCostConfig?.reportingCurrency,
      fxRates: rawRevenueConfig?.fxRates || rawCostConfig?.fxRates,
      fleet: fleetRows,
    });

    // 4. Compute Costs
    let enrichedFlights = computeFlightCostsBatch(flights, {
      ...costConfig,
      ...mrContext,
      fleet: fleetRows,
      debugCosts: req.query?.debug === "true" || req.body?.debug === true || costConfig.debugCosts === true,
    });

    if (sn?.length) {
      const selectedSn = new Set(sn.map((item) => String(item.value ?? item).trim().toUpperCase()).filter(Boolean));
      enrichedFlights = enrichedFlights.filter((flight) => {
        const context = getFlightSnContext(flight, mrContext.aircraftOnwing || []);
        return context.snList.some((value) => selectedSn.has(value));
      });
    }

    res.status(200).json({ flights: enrichedFlights });

  } catch (error) {
    console.error("Error computing cost page data:", error);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.recalculateAndSaveCostPageData = async (req, res) => {
  try {
    const userId = req.user.id;
    const matchQuery = { userId };
    const { label, from, to, sector, variant, flight, userTag1, userTag2 } = req.body || {};

    if (label && label.value !== "both") matchQuery.domIntl = label.value.toLowerCase();
    const applyArrayFilter = (field, filterArray) => {
      if (filterArray?.length) matchQuery[field] = { $in: filterArray.map((item) => item.value) };
    };

    applyArrayFilter("depStn", from);
    applyArrayFilter("arrStn", to);
    applyArrayFilter("sector", sector);
    applyArrayFilter("variant", variant);
    applyArrayFilter("flight", flight);
    applyArrayFilter("userTag1", userTag1);
    applyArrayFilter("userTag2", userTag2);

    const flights = await Flights.find(matchQuery).lean();
    const [rawCostConfig, rawRevenueConfig, fleetRows, mrContext] = await Promise.all([
      CostConfig.findOne({ userId }).lean(),
      RevenueConfig.findOne({ userId }).lean(),
      Fleet.find({ userId }).lean(),
      buildMaintenanceReserveContext(userId, flights),
    ]);
    const costConfig = normalizeCostConfig({
      ...(rawCostConfig || {}),
      reportingCurrency: rawRevenueConfig?.reportingCurrency || rawCostConfig?.reportingCurrency,
      fxRates: rawRevenueConfig?.fxRates || rawCostConfig?.fxRates,
      fleet: fleetRows,
    });
    const enrichedFlights = computeFlightCostsBatch(flights, { ...costConfig, ...mrContext, fleet: fleetRows });

    const costColumns = [
      "engineFuelConsumptionKg", "engineFuelConsumption", "engineFuelKg", "engineFuelLitres",
      "engineFuelCost", "engineFuelCostCCY", "engineFuelCostRCCY",
      "apuFuelConsumptionKg", "apuFuelKg", "apuFuelLitres", "apuFuelCostDirect", "apuFuelCostAllocated",
      "apuFuelCost", "apuFuelCostCCY", "apuFuelCostRCCY",
      "maintenanceReserveContribution", "maintenanceReserveContributionCCY", "maintenanceReserveContributionRCCY",
      "mrContribution", "mrContributionCCY", "mrContributionRCCY",
      "mrMonthly", "mrMonthlyCCY", "mrMonthlyRCCY",
      "qualifyingSchMxEvents", "qualifyingSchMxEventsCCY", "qualifyingSchMxEventsRCCY",
      "transitMaintenance", "transitMaintenanceCCY", "transitMaintenanceRCCY",
      "otherMaintenance", "otherMaintenanceCCY", "otherMaintenanceRCCY",
      "otherMxExpenses", "otherMxExpensesCCY", "otherMxExpensesRCCY",
      "rotableChanges", "rotableChangesCCY", "rotableChangesRCCY",
      "navigation", "navigationCCY", "navigationRCCY", "navEnr", "navTrml",
      "airport", "airportCCY", "airportRCCY", "aptLandingCost", "aptHandlingCost", "aptAvsecCost", "aptOtherCost",
      "otherDoc", "otherDocCCY", "otherDocRCCY", "otherDoc1", "otherDoc2", "otherDoc3",
      "crewAllowances", "crewAllowancesCCY", "crewAllowancesRCCY",
      "layoverCost", "layoverCostCCY", "layoverCostRCCY",
      "crewPositioningCost", "crewPositioningCostCCY", "crewPositioningCostRCCY",
      "totalCost", "totalCostCCY", "totalCostRCCY", "ftEffective", "mtowUsed",
    ];

    await Promise.all(enrichedFlights.map((row) => {
      const $set = {};
      costColumns.forEach((key) => {
        if (row[key] !== undefined) $set[key] = row[key];
      });
      return Flights.updateOne({ _id: row._id, userId }, { $set });
    }));

    res.status(200).json({ success: true, updated: enrichedFlights.length });
  } catch (error) {
    console.error("Error recalculating and saving cost page data:", error);
    res.status(500).json({ success: false, message: "Internal Server Error" });
  }
};
