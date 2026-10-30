const Fleet = require("../model/fleet");
const Onwing = require("../model/aircraftOnwing");
const Reset = require("../model/maintenanceReset");
const Movement = require("../model/rotableMovementSchema");
const norm = (value) => String(value ?? "").trim().toUpperCase().replace(/^0+(?=\d+$)/, "");
const day = (value) => new Date(value).toISOString().slice(0, 10);
function buildOwnershipCache({ fleet = [], onwings = [], resets = [], movements = [] }) {
  const eventsBySn = new Map();
  for (const row of movements) {
    const effective = new Date(`${day(row.date)}T00:00:00Z`);
    effective.setUTCDate(effective.getUTCDate() + 1);
    for (const [field, type] of [["removedSN", "removed"], ["installedSN", "installed"]]) {
      const sn = norm(row[field]);
      if (!sn) continue;
      if (!eventsBySn.has(sn)) eventsBySn.set(sn, []);
      eventsBySn.get(sn).push({ row, type, effective: day(effective) });
    }
  }
  for (const events of eventsBySn.values()) events.sort((a, b) => a.effective.localeCompare(b.effective)
    || (a.type === b.type ? 0 : a.type === "removed" ? -1 : 1)
    || new Date(a.row.createdAt || 0) - new Date(b.row.createdAt || 0) || String(a.row._id || "").localeCompare(String(b.row._id || "")));
  return { fleet, onwings, resets, movements, eventsBySn, resolved: new Map() };
}
async function loadOwnershipCache(userId) {
  const [fleet, onwings, resets, movements] = await Promise.all([
    Fleet.find({ userId }).lean(), Onwing.find({ userId }).sort({ date: -1 }).lean(),
    Reset.find({ userId }).sort({ date: -1 }).lean(), Movement.find({ userId }).lean(),
  ]);
  return buildOwnershipCache({ fleet, onwings, resets, movements });
}
function resolveFromCache({ sn, date, cache }) {
  const target = day(date), original = norm(sn), memo = `${original}|${target}`;
  if (cache.resolved.has(memo)) return cache.resolved.get(memo);
  const chain = [], visited = new Set();
  let current = original, rootMsn = null;
  while (current) {
    if (visited.has(current) || chain.length >= 10) {
      console.warn(`Maintenance ownership cycle/depth limit: ${[...chain, current].join(" -> ")}`);
      break;
    }
    visited.add(current); chain.push(current);
    const event = (cache.eventsBySn.get(current) || []).filter((item) => item.effective <= target).at(-1);
    if (event) {
      if (event.type === "removed") break;
      const position = String(event.row.position || "").trim();
      if (["#1", "#2"].includes(position)) {
        const latestSlot = cache.movements.filter((row) => norm(row.msn) === norm(event.row.msn) && row.position === position
          && day(row.date) < target).sort((a, b) => new Date(a.date) - new Date(b.date) || new Date(a.createdAt || 0) - new Date(b.createdAt || 0)).at(-1);
        if (latestSlot && norm(latestSlot.installedSN) !== current) break;
      }
      current = norm(event.row.msn); continue;
    }
    if (cache.fleet.some((row) => norm(row.sn) === current && (!row.category || row.category === "Aircraft"))) { rootMsn = current; break; }
    const latestByMsn = new Map();
    for (const row of [...cache.onwings].sort((a, b) => new Date(b.date) - new Date(a.date))) {
      if (day(row.date) <= target && !latestByMsn.has(norm(row.msn))) latestByMsn.set(norm(row.msn), row);
    }
    const wing = [...latestByMsn.values()].find((row) => [row.pos1Esn, row.pos2Esn, row.apun].some((value) => norm(value) === current));
    if (wing) { current = norm(wing.msn); continue; }
    const reset = [...cache.resets].filter((row) => norm(row.snBn) === current && day(row.date) <= target)
      .sort((a, b) => new Date(b.date) - new Date(a.date) || new Date(b.createdAt || 0) - new Date(a.createdAt || 0))[0];
    if (reset && norm(reset.msnEsn) && norm(reset.msnEsn) !== current) { current = norm(reset.msnEsn); continue; }
    break;
  }
  const result = { rootMsn, chain };
  cache.resolved.set(memo, result);
  return result;
}
async function resolveUtilisationOwner({ userId, sn, date, cache }) {
  return resolveFromCache({ sn, date, cache: cache || await loadOwnershipCache(userId) });
}

async function validateOwnershipHistory({ userId, movements, resets, deleteMovementId }) {
  const cache = await loadOwnershipCache(userId);
  const incoming = movements || [];
  const movementKey = (row) => [norm(row.msn), norm(row.pn), norm(row.position), day(row.date)].join("|");
  const history = cache.movements.filter((row) => String(row._id) !== String(deleteMovementId)
    && !incoming.some((next) => (next.id && String(next.id) === String(row._id)) || (!next.id && movementKey(next) === movementKey(row))));
  const resetHistory = cache.resets.filter((row) => !(resets || []).some((next) => norm(next.snBn) === norm(row.snBn) && norm(next.pn) === norm(row.pn)));
  const events = [];
  for (const [index, row] of [...resetHistory, ...(resets || [])].entries()) {
    const child = norm(row.snBn), parent = norm(row.msnEsn);
    if (!child || !parent) continue;
    if (child === parent && (cache.fleet.some((asset) => norm(asset.sn) === child)
      || cache.resets.some(saved => norm(saved.snBn) === child && norm(saved.msnEsn) === child))) continue;
    events.push({ date: day(row.date), priority: -1, child, parent, type: "reset", label: `Set/Reset ${index + 1}` });
  }
  const earliestMovement = new Map();
  for (const row of [...history, ...incoming]) {
    const msn = norm(row.msn), date = day(row.date);
    if (!earliestMovement.has(msn) || date < earliestMovement.get(msn)) earliestMovement.set(msn, date);
  }
  const initialOnwings = new Map();
  for (const row of [...cache.onwings].sort((a, b) => new Date(a.date) - new Date(b.date))) {
    const msn = norm(row.msn);
    if (!initialOnwings.has(msn)) initialOnwings.set(msn, row);
  }
  for (const row of initialOnwings.values()) for (const child of [row.pos1Esn, row.pos2Esn, row.apun].map(norm).filter(Boolean)) {
    // A legacy first snapshot can be dated at the incoming engine change's
    // effective day while still containing its pre-change slot value.
    const snapshotDay = day(row.date), firstChange = earliestMovement.get(norm(row.msn));
    events.push({ date: firstChange && firstChange < snapshotDay ? firstChange : snapshotDay, priority: -2, child, parent: norm(row.msn), type: "baseline", label: "On-wing baseline" });
  }
  for (const [index, row] of [...history, ...incoming].entries()) {
    if (!row.date || Number.isNaN(new Date(row.date).getTime()) || !norm(row.msn) || !norm(row.pn) || (!norm(row.removedSN) && !norm(row.installedSN))) throw new Error(`Rotable row ${index + 1}: date, host, PN and an installed or removed SN are required.`);
    for (const [field, type, priority] of [["removedSN", "remove", 0], ["installedSN", "install", 1]]) if (norm(row[field])) {
      events.push({ date: day(row.date), priority, child: norm(row[field]), parent: norm(row.msn), type, label: `Rotable row ${index + 1}`,
        createdAt: row.createdAt, id: row._id || row.id || String(index).padStart(10, "0") });
    }
  }
  events.sort((a, b) => a.date.localeCompare(b.date) || a.priority - b.priority || new Date(a.createdAt || 0) - new Date(b.createdAt || 0) || String(a.id || "").localeCompare(String(b.id || "")));
  const parents = new Map();
  for (const event of events) {
    if (event.type === "remove") {
      if (parents.get(event.child) !== event.parent) throw new Error(`${event.label}: removed SN ${event.child} is not installed on ${event.parent}.`);
      parents.delete(event.child); continue;
    }
    if (parents.has(event.child) && parents.get(event.child) !== event.parent && event.type !== "baseline") throw new Error(`${event.label}: SN ${event.child} already has parent ${parents.get(event.child)}; remove it first.`);
    let parent = event.parent;
    const seen = new Set([event.child]);
    for (let depth = 0; parent; depth += 1) {
      if (seen.has(parent) || depth >= 10) throw new Error(`${event.label}: ownership cycle/depth limit for SN ${event.child}.`);
      seen.add(parent); parent = parents.get(parent);
    }
    parents.set(event.child, event.parent);
  }
}
module.exports = { buildOwnershipCache, loadOwnershipCache, resolveFromCache, resolveUtilisationOwner, validateOwnershipHistory };
