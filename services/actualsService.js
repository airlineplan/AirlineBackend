const crypto = require("node:crypto");
const mongoose = require("mongoose");
const moment = require("moment");
const Flight = require("../model/flight");
const Fleet = require("../model/fleet");
const Data = require("../model/dataSchema");
const Sector = require("../model/sectorSchema");
const Assignment = require("../model/assignment");
const { calculateBH_FH, timeStrToMinutes } = require("../utils/calculateFlightHours");
const { parseActualRows } = require("../utils/actualsFormat");
const { refreshMaintenanceReserves, recalculateActualCosts } = require("./maintenanceReserveRecalculation");
const key = (date) => moment.utc(date).format("YYYY-MM-DD");
const norm = (value) => String(value ?? "").trim().toUpperCase();

async function importActuals({ userId, rawRows, actualUploadId = crypto.randomUUID() }) {
  const parsed = parseActualRows(rawRows);
  const invalidGroups = new Set(parsed.filter((row) => row.reasons.length && row.date).map((row) => `${key(row.date)}|${row.acft}`));
  const valid = [], rejected = [];
  for (const row of parsed) {
    const invalid = invalidGroups.has(`${key(row.date)}|${row.acft}`) || invalidGroups.has(`${key(row.date)}|`);
    if (row.reasons.length || invalid) rejected.push({ rowNumber: row.rowNumber, reasons: row.reasons.length ? row.reasons : ["Another row for this aircraft/date is invalid; group preserved."] });
    else valid.push(row);
  }
  if (!valid.length) return { inserted: 0, rejected, dates: [], actualUploadId };
  const [fleet, networks, sectors, planned] = await Promise.all([
    Fleet.find({ userId, category: "Aircraft" }).lean(), Data.find({ userId }).lean(), Sector.find({ userId }).lean(),
    Flight.find({ userId, category: "Planned", date: { $in: valid.map((row) => row.date) } }).lean(),
  ]);
  const documents = [];
  for (const row of valid) {
    const aircraft = fleet.find((asset) => norm(asset.regn) === row.acft && (!asset.entry || key(asset.entry) <= key(row.date)) && (!asset.exit || key(asset.exit) >= key(row.date)));
    const network = networks.find((item) => norm(item.flight) === row.flight && norm(item.depStn) === row.depStn && norm(item.arrStn) === row.arrStn
      && (!item.effFromDt || key(item.effFromDt) <= key(row.date)) && (!item.effToDt || key(item.effToDt) >= key(row.date)));
    const snapshot = planned.find((item) => key(item.date) === key(row.date) && norm(item.flight) === row.flight && norm(item.depStn) === row.depStn);
    const sector = sectors.find((item) => norm(item.sector1) === row.depStn && norm(item.sector2) === row.arrStn);
    const fallbackMetrics = row.ft ? null : await calculateBH_FH(row.depStn, row.arrStn, row.bt, userId);
    const dist = Number(sector?.gcd ?? network?.gcd ?? snapshot?.dist) || 0;
    const common = {
      userId, date: row.date, day: moment.utc(row.date).format("ddd"), flight: row.flight,
      depStn: row.depStn, arrStn: row.arrStn, std: row.std, sta: row.sta, bt: row.bt,
      ft: row.ft || fallbackMetrics.ft, sector: `${row.depStn}-${row.arrStn}`, domIntl: row.domIntl,
      rotationNumber: row.rotationNumber, seats: row.seats, CargoCapT: row.CargoCapT, pax: row.pax, CargoT: row.CargoT,
      bh: timeStrToMinutes(row.bt) / 60, fh: row.ft ? timeStrToMinutes(row.ft) / 60 : fallbackMetrics.fh,
      paxLF: row.seats ? row.pax / row.seats * 100 : 0, dist, ask: row.seats * dist, rsk: row.pax * dist,
      cargoAtk: row.CargoCapT * dist, cargoRtk: row.CargoT * dist,
      variant: aircraft?.variant || network?.variant || snapshot?.variant || "", acftType: aircraft?.variant || network?.variant || snapshot?.acftType || "",
      aircraft: { registration: row.acft, ...(aircraft && Number.isFinite(Number(aircraft.sn)) ? { msn: Number(aircraft.sn) } : {}) },
      isComplete: true, actualUploadId, pairId: `${actualUploadId}:${row.rowNumber}`,
    };
    for (const field of ["userTag1", "userTag2", "remarks1", "remarks2"]) common[field] = network?.[field] ?? snapshot?.[field] ?? "";
    const actual = { ...common, category: "Actual" };
    for (const field of ["crewAllowances", "layoverCost", "crewPositioningCost"]) {
      for (const suffix of ["", "CCY", "RCCY"]) if (snapshot?.[field + suffix] !== undefined) actual[field + suffix] = snapshot[field + suffix];
    }
    documents.push(actual, { ...common, category: "Invoiced" });
  }
  const actualDocuments = documents.filter((row) => row.category === "Actual");
  const groups = [...new Map(actualDocuments.map((row) => {
    const registration = row.aircraft.registration, msn = row.aircraft.msn;
    return [`${key(row.date)}|${msn ?? registration}`, { date: row.date, ...(msn == null
      ? { "aircraft.registration": registration }
      : { $or: [{ "aircraft.msn": msn }, { "aircraft.msn": null, "aircraft.registration": registration }] }) }];
  })).values()];
  const assignmentDocuments = actualDocuments.map((row) => ({ userId, date: row.date, flightNumber: row.flight, aircraft: row.aircraft,
    metrics: { blockHours: row.bh, flightHours: row.fh, cycles: 1 }, isValid: true, source: "ACTUALS", actualUploadId }));
  const hello = await mongoose.connection.db.admin().command({ hello: 1 });
  const transactional = Boolean(hello.setName || hello.msg === "isdbgrid");
  if (!transactional) throw new Error("Actuals uploads require a MongoDB replica set or sharded cluster so aircraft/date replacement remains atomic if the server stops.");
  const session = await mongoose.startSession();
  const replace = async () => {
    const options = { session };
    await Flight.insertMany(documents, options);
    await Assignment.deleteMany({ userId, $or: groups }, options);
    await Assignment.insertMany(assignmentDocuments, options);
    await Flight.deleteMany({ userId, category: { $in: ["Actual", "Invoiced"] }, $or: groups, actualUploadId: { $ne: actualUploadId } }, options);
  };
  try {
    await session.withTransaction(replace);
  } finally { await session.endSession(); }
  await refreshMaintenanceReserves(userId, { recalculate: false });
  await recalculateActualCosts(userId);
  await require("./invoiceService").refillLockedInvoices(userId);
  await require("../controller/maintenanceController").recomputeMaintenanceTimeline({ userId, recomputeFromDate: valid.reduce((min, row) => row.date < min ? row.date : min, valid[0].date) });
  return { inserted: documents.length, flights: valid.length, rejected, dates: [...new Set(valid.map((row) => key(row.date)))].sort(), actualUploadId,
    transactional };
}
module.exports = { importActuals };
