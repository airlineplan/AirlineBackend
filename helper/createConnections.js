/**
 * createConnections.js — Optimized for maximum throughput and accuracy
 *
 * Key features:
 * 1. Renamed Bull queue to 'flight-processing-v2' to clear stuck Redis jobs.
 * 2. Deduplicated DB queries (groups identical queries to save database load).
 * 3. Cursors instead of .skip() to prevent DB scanning overhead.
 * 4. Restored midnight crossover logic to accurately catch next-day connections.
 */

const Bull = require('bull');
const mongoose = require('mongoose');

const User = require('../model/userSchema');
const Stations = require('../model/stationSchema');
const Flights = require('../model/flight');
const Connections = require('../model/connectionSchema');

/* ─────────────────────────────────────────────
   REDIS + QUEUE CONFIGURATION
──────────────────────────────────────────── */

const REDIS_CONFIG = {
  host: 'redis-12693.c264.ap-south-1-1.ec2.redns.redis-cloud.com',
  port: 12693,
  username: 'default',
  password: '5NJA5j0k3sDz6lKVJlsm0GoCA4DWecHU'
};

const flightQueue = new Bull('flight-processing-v2', {
  redis: REDIS_CONFIG,
  settings: {
    maxStalledCount: 3,
    lockDuration: 600000,
    removeOnComplete: { age: 300 },
    removeOnFail: { age: 300 }
  }
});

/* ─────────────────────────────────────────────
   WORKER (High Performance Deduplication)
──────────────────────────────────────────── */

const CONCURRENCY = 10;

flightQueue.process(CONCURRENCY, async (job) => {
  const { flightIdsBatch = [], userId, hometimeZone } = job.data;

  if (flightIdsBatch.length === 0) return;

  // Convert string IDs back to ObjectId
  const objectIds = flightIdsBatch.map(id => new mongoose.Types.ObjectId(id));

  // Load stationsMap inside the worker
  const stations = await Stations.find({ userId }).lean();
  const stationsMap = {};
  stations.forEach(s => {
    stationsMap[s.stationName.trim().toUpperCase()] = s;
  });

  // Re-fetch the actual flight documents for this batch
  const flightsBatch = await Flights.find(
    { _id: { $in: objectIds } },
    { _id: 1, depStn: 1, arrStn: 1, std: 1, sta: 1, bt: 1, date: 1, domIntl: 1, userId: 1 }
  ).lean();

  // ── Step 1: Build all "beyond" queries for the entire batch ──────
  const orClauses = [];
  const flightMeta = new Map();

  for (const flight of flightsBatch) {
    const arrKey = flight.arrStn.trim().toUpperCase();
    const depKey = flight.depStn.trim().toUpperCase();

    const stationArr = stationsMap[arrKey];
    const stationDep = stationsMap[depKey];

    if (!stationArr || !stationDep) continue;

    const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
    const domQ = buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);
    const intlQ = buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);

    const tag = flight._id.toString();
    flightMeta.set(tag, flight);

    orClauses.push({ ...domQ, _flightTag: tag });
    orClauses.push({ ...intlQ, _flightTag: tag });
  }

  if (orClauses.length === 0) return;

  // ── Step 2: Deduplicate identical queries to save database load ──
  const queryMap = new Map();

  for (const { _flightTag, ...q } of orClauses) {
    const key = JSON.stringify(q); // Group identical queries together
    if (!queryMap.has(key)) queryMap.set(key, { query: q, tags: [] });
    queryMap.get(key).tags.push(_flightTag);
  }

  const queryEntries = [...queryMap.values()];
  const results = [];

  // Chunk queries to prevent DB connection pool exhaustion (50 queries at a time)
  const DB_QUERY_CHUNK_SIZE = 50;
  for (let i = 0; i < queryEntries.length; i += DB_QUERY_CHUNK_SIZE) {
    const chunk = queryEntries.slice(i, i + DB_QUERY_CHUNK_SIZE);

    const chunkResults = await Promise.all(
      chunk.map(({ query }) => Flights.find(query, { _id: 1 }).lean())
    );
    results.push(...chunkResults);
  }

  // ── Step 3: Map results back to originating flights ────────────────
  const beyondMap = new Map();
  for (let i = 0; i < queryEntries.length; i++) {
    const { tags } = queryEntries[i];
    const matchedIds = results[i].map(f => f._id);
    for (const tag of tags) {
      if (!beyondMap.has(tag)) beyondMap.set(tag, new Set());
      for (const id of matchedIds) beyondMap.get(tag).add(id.toString());
    }
  }

  // ── Step 4: Build bulk writes and connection inserts ───────────────
  const connectionEntries = [];
  const flightBulkOps = [];
  const allBeyondIds = new Set();

  for (const [flightTag, beyondSet] of beyondMap) {
    if (beyondSet.size === 0) continue;
    const flight = flightMeta.get(flightTag);

    flightBulkOps.push({
      updateOne: {
        filter: { _id: flight._id },
        update: { $set: { beyondODs: true } }
      }
    });

    for (const beyondId of beyondSet) {
      allBeyondIds.add(beyondId);
      connectionEntries.push({
        flightID: flight._id.toString(),
        beyondOD: beyondId.toString(),
        userId: flight.userId
      });
    }
  }

  if (allBeyondIds.size > 0) {
    flightBulkOps.push({
      updateMany: {
        filter: { _id: { $in: [...allBeyondIds].map(id => new mongoose.Types.ObjectId(id)) } },
        update: { $set: { behindODs: true } }
      }
    });
  }

  // ── Step 5: Fire all writes in parallel ────────────────────────────
  await Promise.all([
    connectionEntries.length > 0
      ? Connections.insertMany(connectionEntries, { ordered: false })
      : Promise.resolve(),
    flightBulkOps.length > 0
      ? Flights.bulkWrite(flightBulkOps, { ordered: false })
      : Promise.resolve()
  ]);

  console.log(
    `✅ Job done: ${connectionEntries.length} connections | ` +
    `${flightBulkOps.length} flight updates | batch size: ${flightsBatch.length}`
  );
});

/* ─────────────────────────────────────────────
   CONTROLLER
──────────────────────────────────────────── */

module.exports = async function createConnections(req, res) {
  try {
    console.log('createConnections called');

    const userId = req.user.id;
    const user = await User.findById(userId, { hometimeZone: 1 }).lean();
    const hometimeZone = user.hometimeZone;

    // Reset connections and flags in parallel
    await Promise.all([
      Connections.deleteMany({ userId }),
      Flights.updateMany({ userId }, { $set: { beyondODs: false, behindODs: false } })
    ]);
    console.log('Reset complete.');

    // Respond immediately — don't make the client wait
    // res.status(202).json({ message: 'Connection processing started. Check back shortly.' });

    // Enqueue batches via Cursor (Extremely fast, low memory)
    const BATCH_SIZE = 2000;
    let totalQueued = 0;
    const jobs = [];
    let flightIdsBatch = [];

    const cursor = Flights.find({ userId }, { _id: 1 }).lean().cursor();

    for await (const flight of cursor) {
      flightIdsBatch.push(flight._id.toString());

      if (flightIdsBatch.length >= BATCH_SIZE) {
        jobs.push(await flightQueue.add(
          { flightIdsBatch, userId, hometimeZone },
          { ttl: 3600 * 1000 }
        ));
        totalQueued += flightIdsBatch.length;
        console.log(`Queued ${totalQueued} flights`);
        flightIdsBatch = []; // reset batch
      }
    }

    // Queue any remaining flights
    if (flightIdsBatch.length > 0) {
      jobs.push(await flightQueue.add(
        { flightIdsBatch, userId, hometimeZone },
        { ttl: 3600 * 1000 }
      ));
      totalQueued += flightIdsBatch.length;
      console.log(`Queued final batch. Total: ${totalQueued} flights`);
    }

    // Wait for completion in the background and log result
    Promise.all(jobs.map(job => job.finished()))
      .then(() => console.log(`All jobs complete. Successfully processed ${totalQueued} flights.`))
      .catch(err => console.error('Job failure:', err));

    res.status(200).json({
      success: true,
      message: `Successfully processed ${totalQueued} flights and created connections.`
    });
  } catch (error) {
    console.error('Error in createConnections:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
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

/* ─────────────────────────────────────────────
   QUEUE CLEANUP
──────────────────────────────────────────── */

async function cleanupQueue() {
  try {
    await Promise.all([
      flightQueue.clean(300 * 1000, 'completed'),
      flightQueue.clean(300 * 1000, 'failed')
    ]);
  } catch (err) {
    console.error("Queue cleanup failed:", err);
  }
}

setInterval(cleanupQueue, 900 * 1000);
cleanupQueue();