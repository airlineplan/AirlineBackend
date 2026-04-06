/**
 * Computes dynamic cost parameters per flight utilizing the user's specific cost Config tables.
 * Falls back to 0 if an explicit configuration matrix rule doesn't match the flight params.
 * @param {Object} flgt Flight document object
 * @param {Object} config User's CostConfig document representing the matrices
 * @returns {Object} Flight object with appended numeric cost properties mapped to frontend
 */
exports.computeFlightCosts = (flgt, config) => {
  // We return a new object to avoid mutating the database document reference
  const flight = { ...flgt };

  // Init all expected metrics to $0.00
  flight.engineFuel = 0;
  flight.apuFuel = 0;
  flight.mrContribution = 0;
  flight.majorSchMx = 0;
  flight.transitMx = 0;
  flight.otherMx = 0;
  flight.rotableChanges = 0;
  flight.navigation = 0;
  flight.airport = 0;
  flight.crewAllowances = 0; // Out of scope for current screenshot, placeholder
  flight.crewOverlay = 0;    // placeholder
  flight.crewPositioning = 0; // placeholder
  flight.otherDoc = 0;

  // Destructure config gracefully
  const {
    fuelConsum = [], apuUsage = [],
    transitMx = [], otherMx = [], rotableChanges = [],
    navEnr = [], navTerm = [],
    airportLanding = [], airportAvsec = [], airportDom = [], airportIntl = [],
    otherDoc = []
  } = config;

  // 1. ENGINE FUEL: (Rough proxy logic based on "Fuel Consum" tables matching Sector/GCD)
  // Simplified logic: Find matching sector in fuelConsum matrix
  const fuelRule = fuelConsum.find(r => r.type === flight.sector || r.type === flight.dist);
  if (fuelRule) {
    // Determine which ACFT matches or take average
    const rate = Number(fuelRule.acft1) || Number(fuelRule.acft2) || 1000;
    flight.engineFuel = rate; 
  }

  // 2. APU FUEL
  const apuRule = apuUsage.find(r => r.arrStn === flight.arrStn && r.variant === flight.variant);
  if (apuRule) {
    flight.apuFuel = Number(apuRule.consumption) || 0;
  }

  // 3. TRANSIT MAINTENANCE
  const transitRule = transitMx.find(r => r.stn === flight.arrStn && r.var === flight.variant);
  if (transitRule) {
    flight.transitMx = Number(transitRule.costDep) || 0;
  }

  // 4. OTHER MAINTENANCE
  const otherMxRule = otherMx.find(r => r.var === flight.variant);
  if (otherMxRule) {
    const costPerBH = Number(otherMxRule.costBh) || 0;
    const costPerDep = Number(otherMxRule.costDep) || 0;
    // Map Flight Block Hours (BH) or default
    const bh = Number(flight.bh) || 0;
    flight.otherMx = (costPerBH * bh) + costPerDep;
  }

  // 5. NAVIGATION
  // Combines ENR + Terminal costs
  let navTotal = 0;
  const enrRule = navEnr.find(r => r.sector === flight.sector);
  if (enrRule) navTotal += (Number(enrRule.m1) || 0);

  const termRule = navTerm.find(r => r.stn === flight.arrStn);
  if (termRule) navTotal += (Number(termRule.m1) || 0);

  flight.navigation = navTotal;


  // 6. AIRPORT
  // Combines Landing + AvSec + Dom/Intl Handling
  let airTotal = 0;
  const landRule = airportLanding.find(r => r.stn === flight.arrStn);
  if (landRule) airTotal += (Number(landRule.m1) || 0);

  const avsecRule = airportAvsec.find(r => r.stn === flight.depStn);
  if (avsecRule) airTotal += (Number(avsecRule.v1) || 0);

  const handlingArr = flight.domIntl === "intl" ? airportIntl : airportDom;
  const handRule = handlingArr.find(r => r.stn === flight.arrStn);
  if (handRule) airTotal += (Number(handRule.v1) || 0);

  flight.airport = airTotal;


  // 7. OTHER DOC
  // Combines matches where variant and sector/arrStn align
  const docRule = otherDoc.find(r => 
    (r.sec === flight.sector || r.arr === flight.arrStn || r.dep === flight.depStn) && 
    r.var === flight.variant
  );
  if (docRule) {
    flight.otherDoc = Number(docRule.cost) || 0;
  }

  return flight;
};
