const moment = require("moment");
const Flight = require("../model/flight");
const CostConfig = require("../model/costConfigSchema");
const Invoice = require("../model/maintenanceReserveInvoice");
const Draft = require("../model/invoiceDraft");
const { normalizeCostConfig, computeFlightCostsBatch, generateMaintenanceReserveScheduleWithContributions } = require("../utils/costLogic");
const { MR_FIELDS } = require("../utils/costFields");
const { loadCostContext, refreshMaintenanceReserves } = require("./maintenanceReserveRecalculation");
const norm = (value) => String(value ?? "").trim().toUpperCase();
const round2 = (value) => Math.round((value + Number.EPSILON) * 100) / 100;
function resolveInvoiceLines(lines, config, regn) {
  if (!Array.isArray(lines) || !lines.length) throw new Error("Confirm at least one maintenance event.");
  const settings = normalizeCostConfig(config).leasedReserve;
  const ids = new Set();
  return lines.map((line, index) => {
    const matches = settings.filter((setting) => norm(setting.acftRegn) === regn
      && (line.mrAccId ? norm(setting.mrAccId) === norm(line.mrAccId) : norm(setting.schMxEvent) === norm(line.schMxEvent))
      && (!line.sn || norm(setting.sn) === norm(line.sn)));
    if (matches.length !== 1) throw new Error(`Event ${index + 1}: select an unambiguous MR setting for this aircraft, event and SN.`);
    const setting = matches[0];
    if (ids.has(norm(setting.mrAccId))) throw new Error(`Duplicate MR account ${setting.mrAccId}.`);
    ids.add(norm(setting.mrAccId));
    for (const field of ["rate", "utilisation", "contribution"]) {
      if (line[field] === "" || line[field] == null || !Number.isFinite(Number(line[field])) || Number(line[field]) < 0) throw new Error(`Event ${index + 1}: ${field} must be a non-negative number.`);
    }
    return { mrAccId: norm(setting.mrAccId), schMxEvent: setting.schMxEvent, sn: setting.sn,
      rate: Number(line.rate), utilisation: Number(line.utilisation), contribution: Number(line.contribution), ccy: setting.ccy, driver: setting.driver,
      extracted: line.extracted || {}, difference: round2(Number(line.contribution) - Number(line.rate) * Number(line.utilisation)) };
  });
}
async function fillInvoiceFlights(invoice) {
  const userId = invoice.userId;
  const start = moment.utc({ year: invoice.year, month: invoice.month - 1, date: 1 });
  const flights = await Flight.find({ userId, category: "Invoiced", "aircraft.registration": invoice.acftRegn,
    date: { $gte: start.toDate(), $lt: start.clone().add(1, "month").toDate() } }).lean();
  const config = await loadCostContext(userId, flights);
  const totals = new Map(flights.map((row) => [String(row._id), Object.fromEntries(MR_FIELDS.map((field) => [field, field.endsWith("CCY") && !field.endsWith("RCCY") ? "" : 0]))]));
  const checks = [];
  for (const ccy of [...new Set(invoice.lines.map((line) => line.ccy))]) {
    const accounts = new Set(invoice.lines.filter((line) => line.ccy === ccy).map((line) => line.mrAccId));
    const schedule = config.maintenanceReserveSchedule.filter((row) => row.invoiceId === String(invoice._id) && accounts.has(norm(row.mrAccId)))
      .map((row) => ({ ...row, openingBalance: 0, openingBal: 0 }));
    const calculated = computeFlightCostsBatch(flights, { ...config, leasedReserve: config.leasedReserve.filter((row) => accounts.has(norm(row.mrAccId))), maintenanceReserveSchedule: schedule });
    for (const row of calculated) {
      const total = totals.get(String(row._id));
      for (const field of MR_FIELDS) {
        if (field.endsWith("CCY") && !field.endsWith("RCCY")) {
          const value = row[field] || "";
          if (value) total[field] = total[field] && total[field] !== value ? "MIXED" : value;
        } else total[field] = round2(total[field] + Number(row[field] || 0));
      }
    }
    for (const monthly of [false, true]) {
      const scheduleTotal = round2(invoice.lines.filter((line) => line.ccy === ccy && (norm(line.driver) === "MONTH") === monthly).reduce((sum, line) => sum + line.contribution, 0));
      const masterTotal = round2(calculated.reduce((sum, row) => sum + Number(row[monthly ? "mrMonthly" : "maintenanceReserveContribution"] || 0), 0));
      const diff = round2(scheduleTotal - masterTotal);
      checks.push({ type: monthly ? "Month" : "Usage", currency: ccy, scheduleTotal, masterTotal, diff, ok: Math.abs(diff) <= 0.005 * flights.length + 1e-8 });
    }
  }
  if (flights.length) await Flight.bulkWrite(flights.map((row) => ({ updateOne: { filter: { userId, category: "Invoiced", _id: row._id },
    update: { $set: { ...totals.get(String(row._id)), invoiceId: String(invoice._id) } } } })));
  const status = flights.length && checks.every((check) => check.ok) ? "Reconciled" : "Unreconciled";
  await Invoice.updateOne({ _id: invoice._id, userId }, { $set: { checks, status } });
  const saved = await CostConfig.findOne({ userId }).lean();
  if (saved) await CostConfig.updateOne({ userId }, { $set: { maintenanceReserveSchedule: saved.maintenanceReserveSchedule.map((row) =>
    row.invoiceId === String(invoice._id) ? { ...row, invoiceStatus: status } : row) } });
  return { checks, status, warning: !flights.length ? `No Invoiced flights for ${invoice.acftRegn} ${start.format("MMM-YY")}. Upload actuals first.`
    : status === "Unreconciled" ? "Invoice applied with a totals warning. Review the actuals and invoice values to align them." : "" };
}
async function applyInvoice({ userId, body }) {
  const acftRegn = norm(body.acftRegn), month = Number(body.month), year = Number(body.year);
  if (!acftRegn || !Number.isInteger(month) || month < 1 || month > 12 || !Number.isInteger(year) || year < 1900 || year > 3000) throw new Error("Confirm the aircraft and a valid utilisation month/year.");
  const draft = await Draft.findOne({ _id: body.draftId, userId }).lean();
  if (!draft) throw new Error("Invoice draft not found for this user.");
  const original = await CostConfig.findOne({ userId }).lean();
  if (!original) throw new Error("Save MR settings before applying an invoice.");
  const lines = resolveInvoiceLines(body.lines, original, acftRegn);
  await refreshMaintenanceReserves(userId, { recalculate: false });
  const config = await CostConfig.findOne({ userId }).lean();
  const postingDate = moment.utc({ year, month: month - 1, date: 1 }).add(1, "month").format("YYYY-MM-DD");
  for (const line of lines) {
    if (!config.maintenanceReserveSchedule.some((row) => norm(row.mrAccId) === line.mrAccId && row.date === postingDate && norm(row.transactionType) === "MONTHLY CONTRIBUTION")) {
      throw new Error(`No Monthly Contribution row for ${line.mrAccId} on ${postingDate}. Extend the MR setting dates first.`);
    }
  }
  const invoice = await Invoice.findOneAndUpdate({ userId, acftRegn, month, year }, { $set: { msn: norm(body.msn), lines,
    fileName: draft.fileName, storedFileKey: draft.storedFileKey, appliedAt: new Date(), appliedBy: userId } }, { upsert: true, new: true });
  const schedule = config.maintenanceReserveSchedule.map((row) => {
    if (row.invoiceId === String(invoice._id)) {
      row = { ...row, invoiceLocked: false, invoiceId: "", invoiceFileName: "", invoiceStatus: "", rate: row.generatedRate, driverValue: row.generatedDriverValue, contribution: row.generatedContribution };
    }
    const line = lines.find((entry) => entry.mrAccId === norm(row.mrAccId));
    if (!line || row.date !== postingDate || norm(row.transactionType) !== "MONTHLY CONTRIBUTION") return row;
    return { ...row, generatedRate: row.generatedRate ?? row.rate, generatedDriverValue: row.generatedDriverValue ?? row.driverValue,
      generatedContribution: row.generatedContribution ?? row.contribution, invoiceLocked: true, invoiceId: String(invoice._id), invoiceFileName: draft.fileName,
      invoiceAppliedAt: invoice.appliedAt, invoiceDifference: line.difference, rate: line.rate, driverValue: line.utilisation,
      contribution: line.contribution };
  });
  // Regeneration refreshes running balances while preserving invoice-locked values.
  await CostConfig.updateOne({ userId }, { $set: { maintenanceReserveSchedule: schedule } });
  await refreshMaintenanceReserves(userId, { recalculate: false });
  const result = await fillInvoiceFlights(invoice);
  await Draft.updateOne({ _id: draft._id, userId }, { $set: { appliedAt: new Date() } });
  return { invoiceId: invoice._id, ...result, schedule: (await CostConfig.findOne({ userId }).lean()).maintenanceReserveSchedule };
}
async function refillLockedInvoices(userId) {
  const invoices = await Invoice.find({ userId }).lean();
  for (const invoice of invoices) await fillInvoiceFlights(invoice);
}
module.exports = { applyInvoice, fillInvoiceFlights, refillLockedInvoices, resolveInvoiceLines };
