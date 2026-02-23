const Bull = require('bull');
const User = require("../model/userSchema");
const Stations = require("../model/stationSchema");
const Flights = require("../model/flight");
const Connections = require("../model/connectionSchema");

const flightQueue = new Bull('flight-processing', {
  redis: {
    host: 'redis-12693.c264.ap-south-1-1.ec2.redns.redis-cloud.com',
    port: 12693,
    username: 'default',
    password: '5NJA5j0k3sDz6lKVJlsm0GoCA4DWecHU'
  },
  settings: {
    maxStalledCount: 3,
    lockDuration: 3000000, 
    removeOnComplete: { age: 300 },  
    removeOnFail: { age: 300 }  
  }
});

const concurrency = 5;

flightQueue.process(concurrency, async (job) => {
  const { flightsBatch, stationsMap, hometimeZone } = job.data;
  
  const connectionEntriesBatch = [];
  const bulkOps = []; // 🔥 NEW: Array to batch update the Flights collection

  for (const flight of flightsBatch) {
    const stationArr = stationsMap[flight.arrStn];
    const stationDep = stationsMap[flight.depStn];

    if (!stationArr) {
      console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
      continue; 
    }

    const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);

    const domQuery = buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);
    const intlQuery = buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);

    const [domFlights, intlFlights] = await Promise.all([
      Flights.find(domQuery),
      Flights.find(intlQuery)
    ]);

    const beyondODs = [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)];

    // 🔥 NEW: If connections exist, mark the Flags on the Flights table for the Dashboard
    if (beyondODs.length > 0) {
      // 1. Mark the origin flight as having a "Beyond" connection
      bulkOps.push({
        updateOne: {
          filter: { _id: flight._id },
          update: { $set: { beyondODs: true } }
        }
      });

      // 2. Mark all the destination flights as having a "Behind" connection
      beyondODs.forEach(targetId => {
        bulkOps.push({
          updateOne: {
            filter: { _id: targetId },
            update: { $set: { behindODs: true } }
          }
        });
      });
    }

    const connectionEntries = beyondODs.map(beyondOD => ({
      flightID: flight._id,
      beyondOD,
      userId: flight.userId
    }));

    connectionEntriesBatch.push(...connectionEntries);
  }

  // Execute inserts for Connections table
  if (connectionEntriesBatch.length > 0) {
    await Connections.insertMany(connectionEntriesBatch);
  }

  // 🔥 NEW: Execute bulk updates for the Flights table so Dashboard can see them
  if (bulkOps.length > 0) {
    await Flights.bulkWrite(bulkOps, { ordered: false });
  }
  
  console.log(`Job completed: Added ${connectionEntriesBatch.length} connections & updated ${bulkOps.length} flights.`);
});

module.exports = async function createConnections(req, res) {
  try {
    console.log("Create Connection called");

    const userId = req.user.id;
    const user = await User.findById(userId);
    const hometimeZone = user.hometimeZone;

    const stationsMap = {};
    const stations = await Stations.find({ userId: userId });
    stations.forEach(station => {
      stationsMap[station.stationName] = station;
    });

    // Reset Connections table
    await Connections.deleteMany({ userId: userId });

    // 🔥 NEW: Reset the boolean flags on ALL flights before we recalculate
    await Flights.updateMany(
      { userId },
      { $set: { beyondODs: false, behindODs: false } }
    );
    console.log("Reset beyondODs and behindODs for all flights.");

    const batchSize = 10000;
    let skip = 0;
    let totalFlights = 0;
    const jobs = [];

    while (true) {
      const flightsBatch = await Flights.find({ userId: userId })
        .skip(skip)
        .limit(batchSize)
        .lean();

      if (flightsBatch.length === 0) break; 

      const job = await flightQueue.add({
        flightsBatch,
        stationsMap,
        hometimeZone
      },
      {
        ttl: 3600 
      });

      jobs.push(job);
      skip += batchSize;
      totalFlights += flightsBatch.length;
      console.log(`Processed ${totalFlights} flights`);
    }

    await Promise.all(jobs.map(job => job.finished()));
    console.log('All jobs completed');
    
    res.status(200).json({ message: "Connections processing completed successfully!" });
  } catch (error) {
    console.error('Error processing flight connections:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

// ... [Keep buildDomQuery, buildIntlQuery, and all your other time helpers exactly as they were] ...

function buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
  const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
  const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
  const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);

  const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
  const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
  const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

  const domQuery = {
    depStn: flight.arrStn,
    arrStn: { $ne: flight.depStn },
    domIntl: { $regex: new RegExp('dom', 'i') },
    userId: flight.userId
  };

  if (sameDayDom) {
    domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
    domQuery.date = new Date(flight.date);
  } else if (nextDayDom) {
    domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
    domQuery.date = new Date(addDays(flight.date, 1));
  } else if (partialDayDom) {
    const ddminPlusB = addTimeStrings(ddMinStdLT, calculateTimeDifference(domConnectingTimeMin, "23:59"));
    const ddmaxMinusB = calculateTimeDifference("23:59", ddMaxStdLT);
    domQuery.$or = [
      { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
      { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
    ];
  }
  return domQuery;
}

function buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
  const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);
  const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
  const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

  const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
  const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
  const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

  const intlQuery = {
    depStn: flight.arrStn,
    arrStn: { $ne: flight.depStn },
    domIntl: { $regex: new RegExp('intl', 'i') },
    userId: flight.userId
  };

  if (sameDayInt) {
    intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
    intlQuery.date = new Date(flight.date);
  } else if (nextDayInt) {
    intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
    intlQuery.date = new Date(addDays(flight.date, 1));
  } else if (partialDayInt) {
    const ininminPlusB = addTimeStrings(inInMinStdLT, calculateTimeDifference(intConnectingTimeMin, "23:59"));
    const ininmaxMinusB = calculateTimeDifference("23:59", inInMaxStdLT);
    intlQuery.$or = [
      { std: { $gte: inInMinStdLT, $lte: ininmaxMinusB }, date: new Date(flight.date) },
      { std: { $gte: ininminPlusB, $lte: inInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
    ];
  }
  return intlQuery;
}

function calculateTimeDifference(time1, time2) {
  const [hour1, minute1] = time1.split(":").map(Number);
  const [hour2, minute2] = time2.split(":").map(Number);
  let differenceInMinutes = (hour2 * 60 + minute2) - (hour1 * 60 + minute1);
  if (differenceInMinutes < 0) differenceInMinutes += 24 * 60; 
  const differenceHours = Math.floor(differenceInMinutes / 60);
  const paddedHours = differenceHours.toString().padStart(2, '0'); 
  const differenceMinutes = differenceInMinutes % 60;
  const paddedMinutes = differenceMinutes.toString().padStart(2, '0'); 
  return `${paddedHours}:${paddedMinutes}`;
}

function convertTimeToTZ(originalTime, originalUTCOffset, targetUTCOffset) {
  const [originalHours, originalMinutes] = originalTime.split(':').map(Number);
  const originalOffsetSign = originalUTCOffset.startsWith('UTC-') ? -1 : 1;
  const targetOffsetSign = targetUTCOffset.startsWith('UTC-') ? -1 : 1;
  const originalOffsetHours = Number(originalUTCOffset.split(':')[0].slice(4)) * originalOffsetSign;
  const originalOffsetMinutes = Number(originalUTCOffset.split(':')[1]) * originalOffsetSign;
  const targetOffsetHours = Number(targetUTCOffset.split(':')[0].slice(4)) * targetOffsetSign;
  const targetOffsetMinutes = Number(targetUTCOffset.split(':')[1]) * targetOffsetSign;

  let utcHours = originalHours - originalOffsetHours;
  let utcMinutes = originalMinutes - originalOffsetMinutes;
  let targetHours = utcHours + targetOffsetHours;
  let targetMinutes = utcMinutes + targetOffsetMinutes;

  if (targetMinutes >= 60) {
    targetHours += 1;
    targetMinutes -= 60;
  } else if (targetMinutes < 0) {
    targetHours -= 1;
    targetMinutes += 60;
  }

  targetHours = (targetHours + 24) % 24;
  return `${targetHours < 10 ? '0' : ''}${targetHours}:${targetMinutes < 10 ? '0' : ''}${targetMinutes}`;
}

function parseTimeString(timeString) {
  const [hours, minutes] = timeString.split(':').map(Number);
  return new Date(0, 0, 0, hours, minutes); 
}

function compareTimes(time1, time2) {
  const date1 = parseTimeString(time1);
  const date2 = parseTimeString(time2);
  return date1.getTime() - date2.getTime();
}

function addTimeStrings(time1, time2, time3 = '00:00') {
  function timeToMinutes(timeString) {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
  }
  function minutesToTime(totalMinutes) {
    const hours = Math.floor(totalMinutes / 60);
    const paddedHours = hours.toString().padStart(2, '0'); 
    const minutes = totalMinutes % 60;
    const paddedMinutes = minutes.toString().padStart(2, '0'); 
    return `${paddedHours}:${paddedMinutes}`;
  }
  const totalMinutes = timeToMinutes(time1) + timeToMinutes(time2) + timeToMinutes(time3);
  return minutesToTime(totalMinutes);
}

function addDays(date, days) {
  const result = new Date(date); 
  result.setDate(result.getDate() + days); 
  return result; 
}

async function cleanupQueue() {
  const stalledJobs = await flightQueue.getJobs(['stalled']);
  if (stalledJobs.length > 0) {
    console.log(`Found ${stalledJobs.length} stalled jobs. Cleaning up...`);
  }

  await Promise.all([
    flightQueue.clean(300 * 1000, 'completed'),
    flightQueue.clean(300 * 1000, 'failed'),
    flightQueue.clean(1800 * 1000, 'stalled')
  ]);
}

setInterval(cleanupQueue, 900 * 1000);
cleanupQueue();