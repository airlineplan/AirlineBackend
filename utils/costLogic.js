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

  const toNumber = (value) => {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Number.isFinite(value) ? value : 0;
    const parsed = Number(String(value).replace(/,/g, ""));
    return Number.isFinite(parsed) ? parsed : 0;
  };

  const firstNumeric = (row, keys = []) => {
    for (const key of keys) {
      const value = toNumber(row?.[key]);
      if (value !== 0) return value;
    }
    return 0;
  };

  // Init all expected metrics to $0.00
  flight.engineFuel = 0;
  flight.engineFuelConsumption = 0;
  flight.engineFuelCost = 0;
  flight.apuFuel = 0;
  flight.mrContribution = 0;
  flight.majorSchMx = 0;
  flight.transitMx = 0;
  flight.transitMaintenance = 0;
  flight.otherMx = 0;
  flight.otherMaintenance1 = 0;
  flight.otherMaintenance2 = 0;
  flight.otherMaintenance3 = 0;
  flight.rotableChanges = 0;
  flight.navigation = 0;
  flight.navEnr = 0;
  flight.navTrml = 0;
  flight.airport = 0;
  flight.aptLandingCost = 0;
  flight.aptHandlingCost = 0;
  flight.aptOtherCost = 0;
  flight.crewAllowances = 0;
  flight.layoverCost = 0;
  flight.crewPositioningCost = 0;
  flight.crewOverlay = 0;
  flight.crewPositioning = 0;
  flight.otherDoc1 = 0;
  flight.otherDoc2 = 0;
  flight.otherDoc3 = 0;
  flight.otherDoc = 0;

  // Destructure config gracefully
  const {
    fuelConsum = [], apuUsage = [],
    leasedReserve = [], transitMx = [], otherMx = [], rotableChanges = [],
    navEnr = [], navTerm = [],
    airportLanding = [], airportAvsec = [], airportDom = [], airportIntl = [],
    otherDoc = []
  } = config;

  // 1. ENGINE FUEL: (Rough proxy logic based on "Fuel Consum" tables matching Sector/GCD)
  // Simplified logic: Find matching sector in fuelConsum matrix
  const fuelRule = fuelConsum.find(r => r.type === flight.sector || r.type === flight.dist);
  if (fuelRule) {
    // Determine which ACFT matches or take average
    const rate = firstNumeric(fuelRule, ["acft1", "acft2", "m1", "m2"]) || 1000;
    flight.engineFuelConsumption = rate;
    flight.engineFuelCost = rate;
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
    flight.transitMaintenance = toNumber(transitRule.costDep);
    flight.transitMx = flight.transitMaintenance;
  }

  // 4. OTHER MAINTENANCE
  const otherMxRule = otherMx.find(r => r.var === flight.variant);
  if (otherMxRule) {
    const costPerBH = Number(otherMxRule.costBh) || 0;
    const costPerDep = Number(otherMxRule.costDep) || 0;
    // Map Flight Block Hours (BH) or default
    const bh = Number(flight.bh) || 0;
    flight.otherMaintenance1 = costPerBH * bh;
    flight.otherMaintenance2 = costPerDep;
    flight.otherMaintenance3 = 0;
    flight.otherMx = flight.otherMaintenance1 + flight.otherMaintenance2 + flight.otherMaintenance3;
  }

  // 4b. MR CONTRIBUTION
  const acftReg = flight.aircraft?.registration || flight.acftType || "";
  const msn = flight.aircraft?.msn != null ? String(flight.aircraft.msn) : "";
  const mrRule = leasedReserve.find((r) => {
    const regMatch = r.acftReg && String(r.acftReg).trim() === String(acftReg).trim();
    const msnMatch = r.sn && String(r.sn).trim() === msn;
    return regMatch || msnMatch;
  });
  if (mrRule) {
    flight.mrContribution = toNumber(mrRule.contribution || mrRule.setRate || mrRule.rate || mrRule.setBalance);
  }

  // 5. NAVIGATION
  // Combines ENR + Terminal costs
  let navTotal = 0;
  const enrRule = navEnr.find(r => r.sector === flight.sector);
  if (enrRule) {
    flight.navEnr = toNumber(enrRule.m1 || enrRule.m2);
    navTotal += flight.navEnr;
  }

  const termRule = navTerm.find(r => r.stn === flight.arrStn);
  if (termRule) {
    flight.navTrml = toNumber(termRule.m1 || termRule.m2);
    navTotal += flight.navTrml;
  }

  flight.navigation = navTotal;


  // 6. AIRPORT
  // Combines Landing + AvSec + Dom/Intl Handling
  let airTotal = 0;
  const landRule = airportLanding.find(r => r.stn === flight.arrStn);
  if (landRule) {
    flight.aptLandingCost = toNumber(landRule.m1 || landRule.m2);
    airTotal += flight.aptLandingCost;
  }

  const avsecRule = airportAvsec.find(r => r.stn === flight.depStn);
  if (avsecRule) {
    flight.aptOtherCost = toNumber(avsecRule.v1 || avsecRule.v2);
    airTotal += flight.aptOtherCost;
  }

  const handlingArr = flight.domIntl === "intl" ? airportIntl : airportDom;
  const handRule = handlingArr.find(r => r.stn === flight.arrStn);
  if (handRule) {
    flight.aptHandlingCost = toNumber(handRule.v1 || handRule.v2);
    airTotal += flight.aptHandlingCost;
  }

  flight.airport = airTotal;


  // 7. OTHER DOC
  // Combines matches where variant and sector/arrStn align
  const docSectorRule = otherDoc.find(r => r.sec === flight.sector && r.var === flight.variant);
  const docArrRule = otherDoc.find(r => r.arr === flight.arrStn && r.var === flight.variant);
  const docDepRule = otherDoc.find(r => r.dep === flight.depStn && r.var === flight.variant);
  const docRules = [...new Set([docSectorRule, docArrRule, docDepRule].filter(Boolean))];

  if (docSectorRule) flight.otherDoc1 = toNumber(docSectorRule.cost);
  if (docArrRule) flight.otherDoc2 = toNumber(docArrRule.cost);
  if (docDepRule) flight.otherDoc3 = toNumber(docDepRule.cost);

  if (docRules.length) {
    flight.otherDoc = docRules.reduce((sum, rule) => sum + toNumber(rule.cost), 0);
  }

  return flight;
};
