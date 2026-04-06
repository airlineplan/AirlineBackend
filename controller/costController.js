const CostConfig = require("../model/costConfigSchema");
const Flights = require("../model/flight");
const { computeFlightCosts } = require("../utils/costLogic");

// Save or Update user's Cost Configuration
exports.saveCostConfig = async (req, res) => {
  try {
    const userId = req.user.id;
    const configData = req.body;

    const updatedConfig = await CostConfig.findOneAndUpdate(
      { userId },
      { $set: configData },
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
        fuelConsum: [], apuUsage: [], plfEffect: [], ccyFuel: [],
        leasedReserve: [], schMxEvents: [], transitMx: [], otherMx: [], rotableChanges: [],
        navEnr: [], navTerm: [], airportLanding: [], airportDom: [], airportIntl: [], airportAvsec: [], otherDoc: []
      };
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
    const { label, from, to, sector, variant, flight, poo, userTag1, userTag2 } = req.body;
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
    // Note: If POO maps to something specific in your flight schema, map it here. Or ignore if not present.

    // 2. Fetch Flights
    const flights = await Flights.find(matchQuery).lean();

    // 3. Fetch Cost Config 
    const costConfig = await CostConfig.findOne({ userId }).lean() || {};

    // 4. Compute Costs
    const enrichedFlights = flights.map(flgt => {
      return computeFlightCosts(flgt, costConfig);
    });

    res.status(200).json({ flights: enrichedFlights });

  } catch (error) {
    console.error("Error computing cost page data:", error);
    res.status(500).json({ message: "Internal Server Error" });
  }
};
