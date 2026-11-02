/**
 * createConnections.js — builds beyond/behind connections for a user's flights.
 *
 * Key features:
 * 1. Loads the user's flights once and evaluates each flight's connection
 *    query in memory against flights indexed by departure station and date,
 *    instead of issuing one or two MongoDB queries per flight.
 * 2. Writes connections and flight flags in chunked bulk operations.
 * 3. A per-user ConnectionJob row acts as a lock, so repeated or concurrent
 *    requests (from any instance) join the running build instead of deleting
 *    its output midway.
 * 4. Requests wait up to CONNECTION_JOB_WAIT_MS for the build; longer builds
 *    return 202 and clients poll /createConnections/status, so large schedules
 *    no longer hit the load balancer's idle timeout.
 * 5. Restored midnight crossover logic to accurately catch next-day connections.
 */

const mongoose = require('mongoose');

const User = require('../model/userSchema');
const Stations = require('../model/stationSchema');
const Flights = require('../model/flight');
const Connections = require('../model/connectionSchema');
const ConnectionJob = require('../model/connectionJob');

/* ─────────────────────────────────────────────
   CONFIGURATION
──────────────────────────────────────────── */

const WRITE_CHUNK_SIZE = 5000;
const YIELD_EVERY_FLIGHTS = 1000;
const JOB_STALE_MS = 60 * 60 * 1000;
const STATUS_POLL_MS = 1000;

const getJobWaitMs = () => {
  const configured = Number(process.env.CONNECTION_JOB_WAIT_MS);
  return Number.isFinite(configured) && configured >= 0 ? configured : 45000;
};

const sleep = (ms) => new Promise((resolve) => {
  const timer = setTimeout(resolve, ms);
  if (typeof timer.unref === 'function') timer.unref();
});

const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve));

/* ─────────────────────────────────────────────
   IN-MEMORY QUERY EVALUATION
──────────────────────────────────────────── */

const bucketKey = (depStn, date) => `${depStn}\u0000${new Date(date).getTime()}`;

const matchesStdRange = (std, range) => (
  typeof std === 'string' && std >= range.$gte && std <= range.$lte
);

const isSameDate = (value, date) => (
  value instanceof Date && value.getTime() === date.getTime()
);

// Evaluates a query produced by buildDomQuery/buildIntlQuery against a lean
// flight exactly as MongoDB would: equality on depStn, $ne on arrStn,
// $regex on domIntl (strings only), string range on std, equality on date.
function matchesConnectionQuery(candidate, query) {
  if (candidate.depStn !== query.depStn) return false;
  if (candidate.arrStn === query.arrStn.$ne) return false;
  if (typeof candidate.domIntl !== 'string' || !query.domIntl.$regex.test(candidate.domIntl)) return false;
  if (query.$or) {
    return query.$or.some((branch) => (
      matchesStdRange(candidate.std, branch.std) && isSameDate(candidate.date, branch.date)
    ));
  }
  return matchesStdRange(candidate.std, query.std) && isSameDate(candidate.date, query.date);
}

const queryDates = (query) => (query.$or ? query.$or.map((branch) => branch.date) : [query.date]);

function findConnectionMatches(query, flightsByStationDate) {
  const matches = [];
  for (const date of queryDates(query)) {
    const candidates = flightsByStationDate.get(bucketKey(query.depStn, date)) || [];
    for (const candidate of candidates) {
      if (matchesConnectionQuery(candidate, query)) matches.push(candidate._id);
    }
  }
  return matches;
}

/* ─────────────────────────────────────────────
   BUILD
──────────────────────────────────────────── */

async function insertConnectionsChunk(entries) {
  if (entries.length === 0) return;
  // Rows are plain strings built above, so skip Mongoose hydration/validation.
  await Connections.insertMany(entries, { ordered: false, lean: true });
}

async function setFlightFlag(ids, field) {
  for (let i = 0; i < ids.length; i += WRITE_CHUNK_SIZE) {
    await Flights.updateMany(
      { _id: { $in: ids.slice(i, i + WRITE_CHUNK_SIZE) } },
      { $set: { [field]: true } }
    );
  }
}

async function buildConnectionsForUser(userId) {
  const user = await User.findById(userId, { hometimeZone: 1 }).lean();
  const hometimeZone = user?.hometimeZone;

  // Reset connections and flags in parallel
  await Promise.all([
    Connections.deleteMany({ userId }),
    Flights.updateMany({ userId }, { $set: { beyondODs: false, behindODs: false } })
  ]);

  const [stations, flights] = await Promise.all([
    Stations.find({ userId }).lean(),
    Flights.find(
      { userId },
      { _id: 1, depStn: 1, arrStn: 1, std: 1, sta: 1, bt: 1, date: 1, domIntl: 1, userId: 1 }
    ).lean(),
  ]);

  const stationsMap = {};
  stations.forEach(s => {
    stationsMap[s.stationName.trim().toUpperCase()] = s;
  });

  const flightsByStationDate = new Map();
  for (const flight of flights) {
    if (!(flight.date instanceof Date)) continue;
    const key = bucketKey(flight.depStn, flight.date);
    if (!flightsByStationDate.has(key)) flightsByStationDate.set(key, []);
    flightsByStationDate.get(key).push(flight);
  }

  let connectionBuffer = [];
  let connectionCount = 0;
  const beyondFlightIds = [];
  const behindFlightIds = new Map();

  for (let index = 0; index < flights.length; index++) {
    if (index > 0 && index % YIELD_EVERY_FLIGHTS === 0) await yieldToEventLoop();

    const flight = flights[index];
    if (typeof flight.arrStn !== 'string' || typeof flight.depStn !== 'string') continue;

    const stationArr = stationsMap[flight.arrStn.trim().toUpperCase()];
    const stationDep = stationsMap[flight.depStn.trim().toUpperCase()];
    if (!stationArr || !stationDep) continue;

    const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
    const domQ = buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);
    const intlQ = buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);

    const beyondIds = new Map();
    for (const id of findConnectionMatches(domQ, flightsByStationDate)) beyondIds.set(id.toString(), id);
    for (const id of findConnectionMatches(intlQ, flightsByStationDate)) beyondIds.set(id.toString(), id);
    if (beyondIds.size === 0) continue;

    beyondFlightIds.push(flight._id);
    for (const [beyondId, beyondObjectId] of beyondIds) {
      behindFlightIds.set(beyondId, beyondObjectId);
      connectionBuffer.push({
        flightID: flight._id.toString(),
        beyondOD: beyondId,
        userId: flight.userId
      });
    }

    if (connectionBuffer.length >= WRITE_CHUNK_SIZE) {
      await insertConnectionsChunk(connectionBuffer);
      connectionCount += connectionBuffer.length;
      connectionBuffer = [];
    }
  }

  await insertConnectionsChunk(connectionBuffer);
  connectionCount += connectionBuffer.length;

  await setFlightFlag(beyondFlightIds, 'beyondODs');
  await setFlightFlag([...behindFlightIds.values()], 'behindODs');

  console.log(
    `✅ Connections built: ${connectionCount} connections | ` +
    `${beyondFlightIds.length} beyond flights | ${flights.length} flights`
  );

  return { flightCount: flights.length, connectionCount };
}

/* ─────────────────────────────────────────────
   JOB LOCK + STATUS
──────────────────────────────────────────── */

const localRuns = new Map();

const isStale = (job) => (
  job?.status === 'processing' && job.startedAt && Date.now() - new Date(job.startedAt).getTime() > JOB_STALE_MS
);

async function acquireJob(userId) {
  const runId = new mongoose.Types.ObjectId().toString();
  const now = new Date();
  try {
    const job = await ConnectionJob.findOneAndUpdate(
      {
        userId,
        $or: [
          { status: { $ne: 'processing' } },
          { startedAt: { $lt: new Date(now.getTime() - JOB_STALE_MS) } },
        ],
      },
      {
        $set: {
          runId,
          status: 'processing',
          startedAt: now,
          finishedAt: null,
          error: null,
          flightCount: 0,
          connectionCount: 0,
        },
      },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    ).lean();
    return { job, acquired: job.runId === runId };
  } catch (error) {
    // A processing row already exists, so the upsert collided on the unique userId.
    if (error?.code === 11000) {
      return { job: await ConnectionJob.findOne({ userId }).lean(), acquired: false };
    }
    throw error;
  }
}

function startRun(userId, runId) {
  const run = (async () => {
    try {
      const result = await buildConnectionsForUser(userId);
      await ConnectionJob.updateOne(
        { userId, runId },
        { $set: { status: 'completed', finishedAt: new Date(), ...result } }
      );
    } catch (error) {
      console.error('Error in createConnections:', error.message);
      await ConnectionJob.updateOne(
        { userId, runId },
        { $set: { status: 'failed', finishedAt: new Date(), error: error.message } }
      ).catch(() => {});
    }
  })();
  localRuns.set(runId, run);
  run.then(() => localRuns.delete(runId));
  return run;
}

async function waitForJob(userId, runId, waitMs) {
  const deadline = Date.now() + waitMs;
  const localRun = localRuns.get(runId);
  if (localRun) {
    await Promise.race([localRun, sleep(waitMs)]);
  }

  for (;;) {
    const job = await ConnectionJob.findOne({ userId }).lean();
    if (!job || job.runId !== runId || job.status !== 'processing') return job;
    if (Date.now() >= deadline) return job;
    await sleep(Math.min(STATUS_POLL_MS, Math.max(0, deadline - Date.now())));
  }
}

const describeJob = (job) => {
  if (!job) return { status: 'idle' };
  if (isStale(job)) {
    return { status: 'failed', message: 'Connection processing was interrupted. Please run it again.' };
  }
  if (job.status === 'processing') {
    return { status: 'processing', message: 'Connection processing is still running.' };
  }
  if (job.status === 'failed') {
    return { status: 'failed', message: 'Connection processing failed.' };
  }
  return {
    status: 'completed',
    message: `Successfully processed ${job.flightCount} flights and created connections.`,
  };
};

/* ─────────────────────────────────────────────
   CONTROLLERS
──────────────────────────────────────────── */

async function createConnections(req, res) {
  try {
    console.log('createConnections called');
    const userId = req.user.id;

    const { job, acquired } = await acquireJob(userId);
    if (acquired) startRun(userId, job.runId);

    // Wait for completion so downstream POO refreshes do not read partial connection data.
    // Builds that outlive the wait return 202 and the client polls /createConnections/status.
    const latest = await waitForJob(userId, job.runId, getJobWaitMs());
    const { status, message } = describeJob(latest);

    if (status === 'completed') {
      return res.status(200).json({ success: true, status, message });
    }
    if (status === 'failed') {
      return res.status(500).json({ error: 'Internal server error', status });
    }
    return res.status(202).json({ success: true, status: 'processing', message });
  } catch (error) {
    console.error('Error in createConnections:', error.message);
    if (!res.headersSent) {
      res.status(error.statusCode || 500).json({ error: 'Internal server error' });
    }
  }
}

async function getConnectionStatus(req, res) {
  try {
    const job = await ConnectionJob.findOne({ userId: req.user.id }).lean();
    res.status(200).json({
      ...describeJob(job),
      startedAt: job?.startedAt || null,
      finishedAt: job?.finishedAt || null,
    });
  } catch (error) {
    console.error('Error reading connection status:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
}

// Builds run in-process; on shutdown, let any running build finish its writes.
const closeFlightQueue = async () => {
  await Promise.allSettled([...localRuns.values()]);
};

/* ─────────────────────────────────────────────
   QUERY BUILDERS (With Midnight Crossover Logic)
──────────────────────────────────────────── */

function buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
  const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
  const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
  const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);

  const sameDayDom = compareTimes(domConnectingTimeMax, '23:59') <= 0;
  const nextDayDom = compareTimes(domConnectingTimeMin, '23:59') > 0;
  const partialDayDom = !sameDayDom && !nextDayDom;

  const q = {
    depStn: flight.arrStn,
    arrStn: { $ne: flight.depStn },
    domIntl: { $regex: /dom/i },
    userId: flight.userId
  };

  if (sameDayDom) {
    q.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
    q.date = new Date(flight.date);
  } else if (nextDayDom) {
    q.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
    q.date = new Date(addDays(flight.date, 1));
  } else if (partialDayDom) {
    const ddminPlusB = addTimeStrings(ddMinStdLT, calculateTimeDifference(domConnectingTimeMin, '23:59'));
    const ddmaxMinusB = calculateTimeDifference('23:59', ddMaxStdLT);
    q.$or = [
      { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
      { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
    ];
  }
  return q;
}

function buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
  const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);
  const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
  const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

  const sameDayInt = compareTimes(intConnectingTimeMax, '23:59') <= 0;
  const nextDayInt = compareTimes(intConnectingTimeMin, '23:59') > 0;
  const partialDayInt = !sameDayInt && !nextDayInt;

  const q = {
    depStn: flight.arrStn,
    arrStn: { $ne: flight.depStn },
    domIntl: { $regex: /intl/i },
    userId: flight.userId
  };

  if (sameDayInt) {
    q.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
    q.date = new Date(flight.date);
  } else if (nextDayInt) {
    q.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
    q.date = new Date(addDays(flight.date, 1));
  } else if (partialDayInt) {
    const ininminPlusB = addTimeStrings(inInMinStdLT, calculateTimeDifference(intConnectingTimeMin, '23:59'));
    const ininmaxMinusB = calculateTimeDifference('23:59', inInMaxStdLT);
    q.$or = [
      { std: { $gte: inInMinStdLT, $lte: ininmaxMinusB }, date: new Date(flight.date) },
      { std: { $gte: ininminPlusB, $lte: inInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
    ];
  }
  return q;
}

/* ─────────────────────────────────────────────
   TIME HELPERS
──────────────────────────────────────────── */

function calculateTimeDifference(time1, time2) {
  const [h1, m1] = time1.split(':').map(Number);
  const [h2, m2] = time2.split(':').map(Number);
  let diff = (h2 * 60 + m2) - (h1 * 60 + m1);
  if (diff < 0) diff += 24 * 60;
  return `${String(Math.floor(diff / 60)).padStart(2, '0')}:${String(diff % 60).padStart(2, '0')}`;
}

function convertTimeToTZ(originalTime, originalUTCOffset, targetUTCOffset) {
  const [oh, om] = originalTime.split(':').map(Number);
  const sign = s => s.startsWith('UTC-') ? -1 : 1;
  const parseOffset = s => {
    const parts = s.replace('UTC', '').split(':');
    return [Number(parts[0]), Number(parts[1] || 0)];
  };

  const [oH, oM] = parseOffset(originalUTCOffset);
  const [tH, tM] = parseOffset(targetUTCOffset);

  let h = oh - (oH * sign(originalUTCOffset)) + (tH * sign(targetUTCOffset));
  let m = om - (oM * sign(originalUTCOffset)) + (tM * sign(targetUTCOffset));

  if (m >= 60) { h++; m -= 60; }
  if (m < 0) { h--; m += 60; }
  h = (h + 24) % 24;

  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

function compareTimes(t1, t2) {
  const toMs = t => { const [h, m] = t.split(':').map(Number); return h * 60 + m; };
  return toMs(t1) - toMs(t2);
}

function addTimeStrings(t1, t2, t3 = '00:00') {
  const toMin = t => { const [h, m] = t.split(':').map(Number); return h * 60 + m; };
  const total = toMin(t1) + toMin(t2) + toMin(t3);
  return `${String(Math.floor(total / 60)).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

module.exports = createConnections;
module.exports.getConnectionStatus = getConnectionStatus;
module.exports.buildConnectionsForUser = buildConnectionsForUser;
module.exports.matchesConnectionQuery = matchesConnectionQuery;
module.exports.closeFlightQueue = closeFlightQueue;
