const Bull = require('bull');
const User = require("../model/userSchema");
const Stations = require("../model/stationSchema");
const Flights = require("../model/flight");
const Connections = require("../model/connectionSchema")


// const createConnections = async (req, res) => {
//   try {
//     console.log("Create Connection called");

//     const userId = req.user.id;
//     const user = await User.findById(userId).lean();

//     if (!user) {
//       return res.status(404).send("User not found"); // Handle case when user is not found
//     }

//     // if (!user.todoConnection) {
//     //   return res.status(200).json({ message: "No changes Seen." });
//     // }

//     // Step 1: Pre-fetch stations data and create a map for quick access
//     const stations = await Stations.find({ userId }).lean();
//     const stationsMap = {};
//     stations.forEach(station => {
//       stationsMap[station.stationName] = station;
//     });
//     console.log(`Fetched and mapped ${stations.length} stations.`);

//     // Step 2: Reset beyondODs and behindODs fields for all user flights using bulk update
//     await Flights.updateMany(
//       { userId },
//       { $set: { beyondODs: false, behindODs: false } }
//     );
//     console.log("Reset beyondODs and behindODs for all flights.");

//     // Step 3: Fetch all flights and build in-memory index
//     const allFlights = await Flights.find({ userId }).select('_id depStn arrStn domIntl date std sta bt').lean();
//     console.log(`Fetched ${allFlights.length} flights for processing.`);

//     // Build in-memory index: depStn_date -> array of flights sorted by std
//     const flightsByDepStnDate = {};

//     allFlights.forEach(flight => {
//       const depStn = flight.depStn;
//       const dateKey = normalizeDate(flight.date);
//       const key = `${depStn}_${dateKey}`;

//       if (!flightsByDepStnDate[key]) {
//         flightsByDepStnDate[key] = [];
//       }
//       flightsByDepStnDate[key].push(flight);
//     });

//     // Sort each array by std
//     for (const key in flightsByDepStnDate) {
//       flightsByDepStnDate[key].sort((a, b) => compareTimes(a.std, b.std));
//     }
//     console.log("Built in-memory index for flights.");

//     // Prepare sets to track which flights need to have beyondODs and behindODs set to true
//     const flightsWithBeyondODs = new Set();
//     const flightsWithBehindODs = new Set();

//     console.log("Processing connections...");

//     // Iterate over each flight to determine connections
//     for (const flight of allFlights) {
//       const { _id, arrStn, depStn, domIntl, std, sta, bt, date } = flight;

//       const stationArr = stationsMap[arrStn];
//       const stationDep = stationsMap[depStn];

//       if (!stationArr || !stationDep) {
//         console.error(`Station not found for flight ${_id}: arrStn=${arrStn}, depStn=${depStn}`);
//         continue; // Skip flights with missing station data
//       }

//       // Convert standard time to home timezone
//       const stdHTZ = convertTimeToTZ(std, stationDep.stdtz, stationArr.stdtz);

//       // Initialize query conditions
//       let domConditions = [];
//       let intlConditions = [];

//       if (domIntl.toLowerCase() === 'dom') {
//         // DOM-specific calculations
//         const ddMinStdLT = addTimeStrings(sta, stationArr.ddMinCT);
//         const ddMaxStdLT = addTimeStrings(sta, stationArr.ddMaxCT);
//         const dInMinStdLT = addTimeStrings(sta, stationArr.dInMinCT);
//         const dInMaxStdLT = addTimeStrings(sta, stationArr.dInMaxCT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, bt, stationArr.ddMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, bt, stationArr.ddMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, bt, stationArr.dInMinCT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, bt, stationArr.dInMaxCT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = !sameDayDom && !nextDayDom;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = !sameDayInt && !nextDayInt;

//         if (sameDayDom) {
//           domConditions.push({
//             date: new Date(date),
//             std: { $gte: ddMinStdLT, $lte: ddMaxStdLT }
//           });
//         } else if (nextDayDom) {
//           domConditions.push({
//             date: new Date(addDays(date, 1)),
//             std: { $gte: ddMinStdLT, $lte: ddMaxStdLT }
//           });
//         } else if (partialDayDom) {
//           const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");
//           const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//           const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

//           domConditions.push(
//             {
//               std: { $gte: ddMinStdLT, $lte: ddmaxMinusB },
//               date: new Date(date)
//             },
//             {
//               std: { $gte: ddminPlusB, $lte: ddMaxStdLT },
//               date: new Date(addDays(date, 1))
//             }
//           );
//         }

//         if (sameDayInt) {
//           intlConditions.push({
//             date: new Date(date),
//             std: { $gte: dInMinStdLT, $lte: dInMaxStdLT }
//           });
//         } else if (nextDayInt) {
//           intlConditions.push({
//             date: new Date(addDays(date, 1)),
//             std: { $gte: dInMinStdLT, $lte: dInMaxStdLT }
//           });
//         } else if (partialDayInt) {
//           const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");
//           const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//           const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

//           intlConditions.push(
//             {
//               std: { $gte: dInMinStdLT, $lte: dinmaxMinusB },
//               date: new Date(date)
//             },
//             {
//               std: { $gte: dinminPlusB, $lte: dInMaxStdLT },
//               date: new Date(addDays(date, 1))
//             }
//           );
//         }
//       } else if (domIntl.toLowerCase() === 'intl') {
//         // INTL-specific calculations
//         const inDMinStdLT = addTimeStrings(sta, stationArr.inDMinCT);
//         const inDMaxStdLT = addTimeStrings(sta, stationArr.inDMaxCT);
//         const inInMinStdLT = addTimeStrings(sta, stationArr.inInMinDT);
//         const inInMaxStdLT = addTimeStrings(sta, stationArr.inInMaxDT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, bt, stationArr.inDMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, bt, stationArr.inDMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, bt, stationArr.inInMinDT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, bt, stationArr.inInMaxDT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = !sameDayDom && !nextDayDom;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = !sameDayInt && !nextDayInt;

//         const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domConditions.push({
//             date: new Date(date),
//             std: { $gte: inDMinStdLT, $lte: inDMaxStdLT }
//           });
//         } else if (nextDayDom) {
//           domConditions.push({
//             date: new Date(addDays(date, 1)),
//             std: { $gte: inDMinStdLT, $lte: inDMaxStdLT }
//           });
//         } else if (partialDayDom) {
//           const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//           const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

//           const flightDateUTC = new Date(date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           domConditions.push(
//             {
//               std: { $gte: inDMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: indmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           );
//         }

//         if (sameDayInt) {
//           intlConditions.push({
//             date: new Date(date),
//             std: { $gte: inInMinStdLT, $lte: inInMaxStdLT }
//           });
//         } else if (nextDayInt) {
//           intlConditions.push({
//             date: new Date(addDays(date, 1)),
//             std: { $gte: inInMinStdLT, $lte: inInMaxStdLT }
//           });
//         } else if (partialDayInt) {
//           const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//           const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

//           const flightDateUTC = new Date(date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           intlConditions.push(
//             {
//               std: { $gte: inInMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: ininmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           );
//         }
//       }

//       // Function to find all matching flights in memory based on conditions
//       const findFlightsInMemory = (conditions) => {
//         const results = new Set();
//         for (const condition of conditions) {
//           let depStn = flight.arrStn;
//           let dateKey;
//           let stdGte, stdLte;

//           if (condition.date instanceof Date) {
//             dateKey = normalizeDate(condition.date);
//             stdGte = condition.std.$gte;
//             stdLte = condition.std.$lte;
//           } else {
//             // Handle cases where date is an object (e.g., { $gte: ..., $lt: ... })
//             // Assuming the condition has $gte and $lte for date
//             dateKey = normalizeDate(condition.date.$gte);
//             stdGte = condition.std.$gte;
//             stdLte = condition.std.$lte;
//           }

//           const key = `${depStn}_${dateKey}`;
//           const flightsArray = flightsByDepStnDate[key] || [];

//           const startIdx = binarySearchByStd(flightsArray, stdGte, true);
//           const endIdx = binarySearchByStd(flightsArray, stdLte, false);

//           if (startIdx !== -1 && endIdx !== -1 && startIdx <= endIdx) {
//             for (let idx = startIdx; idx <= endIdx; idx++) {
//               const flightItem = flightsArray[idx];

//               // Check that arrStn is not equal to flight.depStn
//               if (flightItem.arrStn !== flight.depStn) {
//                 results.add(flightItem._id.toString());
//               }
//             }
//           }
//         }
//         return Array.from(results);
//       };

//       // Find domFlights and intlFlights
//       const domFlightsIds = findFlightsInMemory(domConditions);
//       const intlFlightsIds = findFlightsInMemory(intlConditions);

//       // Aggregate beyondODs
//       if (domFlightsIds.length > 0 || intlFlightsIds.length > 0) {
//         flightsWithBeyondODs.add(flight._id.toString());
//       }

//       // Aggregate behindODs
//       domFlightsIds.forEach(fId => flightsWithBehindODs.add(fId));
//       intlFlightsIds.forEach(fId => flightsWithBehindODs.add(fId));
//     }

//     console.log("All connections processed. Preparing bulk updates...");

//     // Prepare bulk operations
//     const bulkOps = [];

//     // Prepare beyondODs updates
//     flightsWithBeyondODs.forEach(flightId => {
//       bulkOps.push({
//         updateOne: {
//           filter: { _id: new mongoose.Types.ObjectId(flightId) },
//           update: { $set: { beyondODs: true } }
//         }
//       });
//     });

//     // Prepare behindODs updates
//     flightsWithBehindODs.forEach(flightId => {
//       bulkOps.push({
//         updateOne: {
//           filter: { _id: new mongoose.Types.ObjectId(flightId) },
//           update: { $set: { behindODs: true } }
//         }
//       });
//     });

//     console.log(`Total bulk operations to perform: ${bulkOps.length}`);

//     // Execute bulk operations in batches to avoid exceeding MongoDB's limit
//     const BATCH_SIZE = 1000; // Adjust based on MongoDB's max bulk size
//     let totalExecuted = 0;

//     for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
//       const batch = bulkOps.slice(i, i + BATCH_SIZE);
//       await Flights.bulkWrite(batch, { ordered: false });
//       totalExecuted += batch.length;
//       console.log(`Bulk write batch ${Math.floor(i / BATCH_SIZE) + 1} executed. Batch operations: ${batch.length}. Total executed: ${totalExecuted}`);
//     }

//     console.log("Connections Completed Successfully.");
//     res.status(200).json({ message: "Connections Completed Successfully." });

//   } catch (error) {
//     console.error('Error processing flight connections:', error);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// };




// const createConnections = async (req, res) => {
//   try {

//     console.log("Create Connection called")

//     const userId = req.user.id;
//     // Fetch user's hometimeZone
//     const user = await User.findById(userId);
//     // const hometimeZone = user.hometimeZone;

//     // Pre-fetch stations data
//     const stationsMap = {};
//     const stations = await Stations.find({ userId: userId });
//     for (const station of stations) {
//       stationsMap[station.stationName] = station;
//     }

//     // Fetch all flight entries in a single query
//     const allFlights = await Flights.find({ userId: userId });

//     console.log("Flights loaded  : ", allFlights.length);

//     for (const flight of allFlights) {
//       if (flight.userId === userId) {
//         flight.beyondODs = [];
//         flight.behindODs = [];
//         await flight.save();
//       }
//     }

//     console.log("beyondODs , behindODs added  : ");

//     let count  = 0;
//     // Iterate over each flight
//     for (const flight of allFlights) {

//       console.log("Iteration : ", count);
//       count++;

//       const stationArr = stationsMap[flight.arrStn];
//       const stationDep = stationsMap[flight.depStn];

//       if (!stationArr) {
//         console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
//         continue; // Skip to the next flight
//       }

//       const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
//       // const staHTZ = convertTimeToTZ(flight.sta, stationArr.stdtz, hometimeZone);



//       const domQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('dom', 'i') }
//       };

//       const intlQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('intl', 'i') }
//       };

//       if (flight.domIntl.toLowerCase() === 'dom') {
//         const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
//         const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
//         const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
//         const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;


//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         // B = 23:59 - domConnectingTimeMin
//         const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

//         if (sameDayDom) {

//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(flight.date)

//         } else if (nextDayDom) {
//           // min to max on the next day
//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//           const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

//           domQuery.$or = [
//             { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//         if (sameDayInt) {
//           // min to max on the same day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           // min to max on the next day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//           const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

//           intlQuery.$or = [
//             { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//       } else if (flight.domIntl.toLowerCase() === 'intl') {
//         const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
//         const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
//         const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
//         const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(flight.date);

//         } else if (nextDayDom) {

//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {

//           const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//           const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           domQuery.$or = [
//             {
//               std: { $gte: inDMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: indmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];
//         }

//         if (sameDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {

//           const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//           const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           intlQuery.$or = [
//             {
//               std: { $gte: inInMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: ininmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];
//         }
//       }

//       console.log("domQuery is : " + JSON.stringify(domQuery));
//       console.log("intlQuery is : " + JSON.stringify(intlQuery));
//       const domFlights = await Flights.find(domQuery);
//       const intlFlights = await Flights.find(intlQuery);

//       const update = {
//         $set: {
//           beyondODs: [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)],
//         },
//       };

//       await Flights.updateOne({ _id: flight._id }, update);

//       // Update behindODs field in domFlights and intlFlights
//       if (!flight._id) {
//         console.error('Flight _id is undefined or null');
//         // Handle the error accordingly, for example, by skipping this update operation
//       } else {
//         // Update documents with $addToSet only if flight._id is valid
//         for (const f of domFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//         for (const f of intlFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//       }
//     };


//     console.log("Connections Completed")
//     res.status(200).json({ message: "Connections Completed" });
//   } catch (error) {
//     console.error('Error processing flight connections:', error);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// };

// const createConnections = async (req, res) => {
//   try {

//     console.log("Create Connection called")

//     const userId = req.user.id;
//     // Fetch user's hometimeZone
//     const user = await User.findById(userId);
//     // const hometimeZone = user.hometimeZone;

//     // Pre-fetch stations data
//     const stationsMap = {};
//     const stations = await Stations.find({ userId: userId });
//     for (const station of stations) {
//       stationsMap[station.stationName] = station;
//     }

//     // Fetch all flight entries in a single query
//     const allFlights = await Flights.find({ userId: userId });

//     for (const flight of allFlights) {
//       if (flight.userId === userId) {
//         flight.beyondODs = [];
//         flight.behindODs = [];
//         await flight.save();
//       }
//     }

//     // Iterate over each flight
//     for (const flight of allFlights) {
//       const stationArr = stationsMap[flight.arrStn];
//       const stationDep = stationsMap[flight.depStn];

//       if (!stationArr) {
//         console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
//         continue; // Skip to the next flight
//       }

//       const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
//       // const staHTZ = convertTimeToTZ(flight.sta, stationArr.stdtz, hometimeZone);



//       const domQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('dom', 'i') }
//       };

//       const intlQuery = {
//         depStn: flight.arrStn,
//         arrStn: { $ne: flight.depStn },
//         domIntl: { $regex: new RegExp('intl', 'i') }
//       };

//       if (flight.domIntl.toLowerCase() === 'dom') {
//         const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
//         const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
//         const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
//         const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;


//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         // B = 23:59 - domConnectingTimeMin
//         const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

//         if (sameDayDom) {

//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(flight.date)

//         } else if (nextDayDom) {
//           // min to max on the next day
//           domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//           const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

//           domQuery.$or = [
//             { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//         if (sameDayInt) {
//           // min to max on the same day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           // min to max on the next day
//           intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {
//           // minstd to max - B on the same date
//           // min + B to max on the next date
//           const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//           const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

//           intlQuery.$or = [
//             { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
//             { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//           ];
//         }

//       } else if (flight.domIntl.toLowerCase() === 'intl') {
//         const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
//         const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
//         const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
//         const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

//         const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
//         const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
//         const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
//         const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

//         const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//         const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//         const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//         const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//         const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//         const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//         const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//         if (sameDayDom) {
//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(flight.date);

//         } else if (nextDayDom) {

//           domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//           domQuery.date = new Date(addDays(flight.date, 1))
//         } else if (partialDayDom) {

//           const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//           const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           domQuery.$or = [
//             {
//               std: { $gte: inDMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: indmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];
//         }

//         if (sameDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(flight.date);
//         } else if (nextDayInt) {
//           intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//           intlQuery.date = new Date(addDays(flight.date, 1));
//         } else if (partialDayInt) {

//           const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//           // const dinmaxMinusB = calculateTimeDifference(paramBInt, inInMaxStdLT);
//           const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

//           const flightDateUTC = new Date(flight.date);
//           flightDateUTC.setUTCHours(0, 0, 0, 0);

//           // Calculate the next day in UTC for comparison
//           const nextDayDateUTC = new Date(flightDateUTC);
//           nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//           intlQuery.$or = [
//             {
//               std: { $gte: inInMinStdLT, $lte: "23:59" },
//               date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//             },
//             {
//               std: { $gte: "00:00", $lte: ininmaxMinusB },
//               date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//             }
//           ];

//           // intlQuery.$or = [
//           //   { std: { $gte: inInMinStdLT, $lte: "23:59" }, date: new Date(flight.date) },
//           //   { std: { $gte: "00:00", $lte: ininmaxMinusB }, date: new Date(addDays(flight.date, 1)) }
//           // ];

//         }
//       }

//       // const [domFlights, intlFlights] = await Promise.all([
//       //     Flights.find(domQuery),
//       //     Flights.find(intlQuery)
//       // ]);

//       console.log("domQuery is : " + JSON.stringify(domQuery));
//       console.log("intlQuery is : " + JSON.stringify(intlQuery));
//       const domFlights = await Flights.find(domQuery);
//       const intlFlights = await Flights.find(intlQuery);

//       const update = {
//         $set: {
//           beyondODs: [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)],
//         },
//       };

//       await Flights.updateOne({ _id: flight._id }, update);

//       // Update behindODs field in domFlights and intlFlights
//       if (!flight._id) {
//         console.error('Flight _id is undefined or null');
//         // Handle the error accordingly, for example, by skipping this update operation
//       } else {
//         // Update documents with $addToSet only if flight._id is valid
//         for (const f of domFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//         for (const f of intlFlights) {
//           await Flights.updateOne({ _id: f._id }, { $addToSet: { behindODs: flight._id } });
//         }
//       }
//     };


//     console.log("Connections Completed")
//     res.status(200).json({ message: "Connections Completed" });
//   } catch (error) {
//     console.error('Error processing flight connections:', error);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// };



// module.exports = async function createConnections(req, res) {
//     try {

//         console.log("Create Connection called")

//         const userId = req.user.id;
//         // Fetch user's hometimeZone
//         const user = await User.findById(userId);
//         // const hometimeZone = user.hometimeZone;

//         // Pre-fetch stations data
//         const stationsMap = {};
//         const stations = await Stations.find({ userId: userId });
//         for (const station of stations) {
//             stationsMap[station.stationName] = station;
//         }

//         // Fetch all flight entries in a single query
//         const allFlights = await Flights.find(
//             { userId: userId },
//             { date: 1, depStn: 1, std: 1, bt: 1, sta: 1, arrStn: 1, domIntl:1 }
//         );

//         await Connections.deleteMany({ userId: userId });

//         // Iterate over each flight
//         for (const flight of allFlights) {
//             const stationArr = stationsMap[flight.arrStn];
//             const stationDep = stationsMap[flight.depStn];

//             if (!stationArr) {
//                 console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
//                 continue; // Skip to the next flight
//             }


//             const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);
//             // const staHTZ = convertTimeToTZ(flight.sta, stationArr.stdtz, hometimeZone);

//             const domQuery = {
//                 depStn: flight.arrStn,
//                 arrStn: { $ne: flight.depStn },
//                 domIntl: { $regex: new RegExp('dom', 'i') },
//                 userId: userId 
//             };

//             const intlQuery = {
//                 depStn: flight.arrStn,
//                 arrStn: { $ne: flight.depStn },
//                 domIntl: { $regex: new RegExp('intl', 'i') },
//                 userId: userId 
//             };

//             if (flight.domIntl.toLowerCase() === 'dom') {
//                 const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
//                 const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
//                 const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
//                 const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

//                 const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
//                 const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
//                 const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
//                 const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

//                 const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//                 const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//                 const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;


//                 const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//                 const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//                 const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//                 // B = 23:59 - domConnectingTimeMin
//                 const paramBDom = calculateTimeDifference(domConnectingTimeMin, "23:59");

//                 if (sameDayDom) {

//                     domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//                     domQuery.date = new Date(flight.date)

//                 } else if (nextDayDom) {
//                     // min to max on the next day
//                     domQuery.std = { $gte: ddMinStdLT, $lte: ddMaxStdLT };
//                     domQuery.date = new Date(addDays(flight.date, 1))
//                 } else if (partialDayDom) {
//                     // minstd to max - B on the same date
//                     // min + B to max on the next date
//                     const ddminPlusB = addTimeStrings(ddMinStdLT, paramBDom);
//                     const ddmaxMinusB = calculateTimeDifference(paramBDom, ddMaxStdLT);

//                     domQuery.$or = [
//                         { std: { $gte: ddMinStdLT, $lte: ddmaxMinusB }, date: new Date(flight.date) },
//                         { std: { $gte: ddminPlusB, $lte: ddMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//                     ];
//                 }

//                 if (sameDayInt) {
//                     // min to max on the same day
//                     intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//                     intlQuery.date = new Date(flight.date);
//                 } else if (nextDayInt) {
//                     // min to max on the next day
//                     intlQuery.std = { $gte: dInMinStdLT, $lte: dInMaxStdLT };
//                     intlQuery.date = new Date(addDays(flight.date, 1));
//                 } else if (partialDayInt) {
//                     // minstd to max - B on the same date
//                     // min + B to max on the next date
//                     const dinminPlusB = addTimeStrings(dInMinStdLT, paramBDom);
//                     const dinmaxMinusB = calculateTimeDifference(paramBDom, dInMaxStdLT);

//                     intlQuery.$or = [
//                         { std: { $gte: dInMinStdLT, $lte: dinmaxMinusB }, date: new Date(flight.date) },
//                         { std: { $gte: dinminPlusB, $lte: dInMaxStdLT }, date: new Date(addDays(flight.date, 1)) }
//                     ];
//                 }

//             } else if (flight.domIntl.toLowerCase() === 'intl') {
//                 const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
//                 const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
//                 const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
//                 const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

//                 const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
//                 const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
//                 const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMinDT);
//                 const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inInMaxDT);

//                 const sameDayDom = compareTimes(domConnectingTimeMax, "23:59") <= 0;
//                 const nextDayDom = compareTimes(domConnectingTimeMin, "23:59") > 0;
//                 const partialDayDom = compareTimes(domConnectingTimeMin, "23:59") <= 0 && compareTimes(domConnectingTimeMax, "23:59") > 0;

//                 const sameDayInt = compareTimes(intConnectingTimeMax, "23:59") <= 0;
//                 const nextDayInt = compareTimes(intConnectingTimeMin, "23:59") > 0;
//                 const partialDayInt = compareTimes(intConnectingTimeMin, "23:59") <= 0 && compareTimes(intConnectingTimeMax, "23:59") > 0;

//                 const paramBInt = calculateTimeDifference(intConnectingTimeMin, "23:59");

//                 if (sameDayDom) {
//                     domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//                     domQuery.date = new Date(flight.date);

//                 } else if (nextDayDom) {

//                     domQuery.std = { $gte: inDMinStdLT, $lte: inDMaxStdLT };
//                     domQuery.date = new Date(addDays(flight.date, 1))
//                 } else if (partialDayDom) {

//                     const indminPlusB = addTimeStrings(inDMinStdLT, paramBInt);
//                     const indmaxMinusB = calculateTimeDifference("24:00", inDMaxStdLT);

//                     const flightDateUTC = new Date(flight.date);
//                     flightDateUTC.setUTCHours(0, 0, 0, 0);

//                     // Calculate the next day in UTC for comparison
//                     const nextDayDateUTC = new Date(flightDateUTC);
//                     nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//                     domQuery.$or = [
//                         {
//                             std: { $gte: inDMinStdLT, $lte: "23:59" },
//                             date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//                         },
//                         {
//                             std: { $gte: "00:00", $lte: indmaxMinusB },
//                             date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//                         }
//                     ];
//                 }

//                 if (sameDayInt) {
//                     intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//                     intlQuery.date = new Date(flight.date);
//                 } else if (nextDayInt) {
//                     intlQuery.std = { $gte: inInMinStdLT, $lte: inInMaxStdLT };
//                     intlQuery.date = new Date(addDays(flight.date, 1));
//                 } else if (partialDayInt) {

//                     const dinminPlusB = addTimeStrings(inInMinStdLT, paramBInt);
//                     // const dinmaxMinusB = calculateTimeDifference(paramBInt, inInMaxStdLT);
//                     const ininmaxMinusB = calculateTimeDifference("24:00", inInMaxStdLT);

//                     const flightDateUTC = new Date(flight.date);
//                     flightDateUTC.setUTCHours(0, 0, 0, 0);

//                     // Calculate the next day in UTC for comparison
//                     const nextDayDateUTC = new Date(flightDateUTC);
//                     nextDayDateUTC.setDate(nextDayDateUTC.getDate() + 1);

//                     intlQuery.$or = [
//                         {
//                             std: { $gte: inInMinStdLT, $lte: "23:59" },
//                             date: { $gte: flightDateUTC, $lt: nextDayDateUTC }
//                         },
//                         {
//                             std: { $gte: "00:00", $lte: ininmaxMinusB },
//                             date: { $gte: nextDayDateUTC, $lt: addDays(nextDayDateUTC, 1) }
//                         }
//                     ];

//                     // intlQuery.$or = [
//                     //   { std: { $gte: inInMinStdLT, $lte: "23:59" }, date: new Date(flight.date) },
//                     //   { std: { $gte: "00:00", $lte: ininmaxMinusB }, date: new Date(addDays(flight.date, 1)) }
//                     // ];

//                 }
//             }

//             // const [domFlights, intlFlights] = await Promise.all([
//             //     Flights.find(domQuery),
//             //     Flights.find(intlQuery)
//             // ]);

//             console.log("domQuery is : " + JSON.stringify(domQuery));
//             console.log("intlQuery is : " + JSON.stringify(intlQuery));
//             const domFlights = await Flights.find(domQuery);
//             const intlFlights = await Flights.find(intlQuery);

//             const beyondODs = [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)];

//             const connectionEntries = beyondODs.map(beyondOD => ({
//                 flightID: flight._id,
//                 beyondOD,
//                 userId: userId 
//             }));

//             // Insert the entries into the connections collection
//             Connections.insertMany(connectionEntries)
//                 .then(() => {
//                     console.log('Connections added');
//                 })
//                 .catch(error => {
//                     console.error('Error creating connections:', error);
//                 });
//         };


//         console.log("Connections Completed")
//         res.status(200).json({ message: "Connections Completed" });
//     } catch (error) {
//         console.error('Error processing flight connections:', error);
//         res.status(500).json({ error: 'Internal server error' });
//     }
// };


//---------------------------------

const flightQueue = new Bull('flight-processing', {
  redis: {
    host: 'redis-12693.c264.ap-south-1-1.ec2.redns.redis-cloud.com',
    port: 12693,
    username: 'default',
    password: '5NJA5j0k3sDz6lKVJlsm0GoCA4DWecHU'
  }
});

const concurrency = 10;

// Define the job processor once outside of any route or function
flightQueue.process(concurrency, async (job) => {
  const { flightsBatch, stationsMap, hometimeZone } = job.data;
  const connectionEntriesBatch = [];

  for (const flight of flightsBatch) {
    const stationArr = stationsMap[flight.arrStn];
    const stationDep = stationsMap[flight.depStn];

    if (!stationArr) {
      console.error(`Station not found for flight with arrStn: ${flight.arrStn}`);
      continue; // Skip to the next flight
    }

    const stdHTZ = convertTimeToTZ(flight.std, stationDep.stdtz, stationArr.stdtz);

    // Build queries for domestic and international flights
    const domQuery = buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);
    const intlQuery = buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone);

    // Fetch connected flights in parallel
    const [domFlights, intlFlights] = await Promise.all([
      Flights.find(domQuery),
      Flights.find(intlQuery)
    ]);

    // Map connected flights to connection entries
    const beyondODs = [...domFlights.map(f => f._id), ...intlFlights.map(f => f._id)];

    const connectionEntries = beyondODs.map(beyondOD => ({
      flightID: flight._id,
      beyondOD,
      userId: flight.userId
    }));

    connectionEntriesBatch.push(...connectionEntries);
  }

  // Batch insert connections into the database
  if (connectionEntriesBatch.length > 0) {
    await Connections.insertMany(connectionEntriesBatch);
    console.log('Connections added');
  }
});

// Define the route for creating connections
module.exports = async function createConnections(req, res) {
  try {
    console.log("Create Connection called");

    const userId = req.user.id;

    // Fetch user's hometimeZone
    const user = await User.findById(userId);
    const hometimeZone = user.hometimeZone;

    // Pre-fetch stations data
    const stationsMap = {};
    const stations = await Stations.find({ userId: userId });
    stations.forEach(station => {
      stationsMap[station.stationName] = station;
    });

    await Connections.deleteMany({ userId: userId });

    // Fetch flight data in batches to avoid memory issues
    const batchSize = 10000;
    let skip = 0;
    let totalFlights = 0;

    const jobs = [];
    // Paginate through all flights
    while (true) {
      const flightsBatch = await Flights.find({ userId: userId })
        .skip(skip)
        .limit(batchSize)
        .lean();

      if (flightsBatch.length === 0) break; // Stop when no flights are left

      // Add the batch of flights to the queue for processing
      const job = await flightQueue.add({
        flightsBatch,
        stationsMap,
        hometimeZone
      });

      jobs.push(job);

      // Increment skip for the next batch
      skip += batchSize;
      totalFlights += flightsBatch.length;

      // Log progress
      console.log(`Processed ${totalFlights} flights`);
    }

    // Wait for all jobs to complete
    await Promise.all(jobs.map(job => job.finished()));
    console.log('All jobs completed');
    // res.status(200).json({ message: "Connections Created" });
  } catch (error) {
    console.error('Error processing flight connections:', error);
    // res.status(500).json({ error: 'Internal server error' });
  }
};

// Helper function to build the domestic flight query
function buildDomQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const ddMinStdLT = addTimeStrings(flight.sta, stationArr.ddMinCT);
  const ddMaxStdLT = addTimeStrings(flight.sta, stationArr.ddMaxCT);
  const dInMinStdLT = addTimeStrings(flight.sta, stationArr.dInMinCT);
  const dInMaxStdLT = addTimeStrings(flight.sta, stationArr.dInMaxCT);

  const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMinCT);
  const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.ddMaxCT);
  const intConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMinCT);
  const intConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.dInMaxCT);

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

// Helper function to build the international flight query
function buildIntlQuery(flight, stationDep, stationArr, stdHTZ, hometimeZone) {
  const inDMinStdLT = addTimeStrings(flight.sta, stationArr.inDMinCT);
  const inDMaxStdLT = addTimeStrings(flight.sta, stationArr.inDMaxCT);
  const inInMinStdLT = addTimeStrings(flight.sta, stationArr.inInMinDT);
  const inInMaxStdLT = addTimeStrings(flight.sta, stationArr.inInMaxDT);

  const domConnectingTimeMin = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMinCT);
  const domConnectingTimeMax = addTimeStrings(stdHTZ, flight.bt, stationArr.inDMaxCT);
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

    // Handling negative difference (i.e., crossing over to the previous day)
    if (differenceInMinutes < 0) {
        differenceInMinutes += 24 * 60; // Add a day's worth of minutes
    }

    const differenceHours = Math.floor(differenceInMinutes / 60);
    const paddedHours = differenceHours.toString().padStart(2, '0'); // Ensure hours are 2 digits with leading zero if needed
    const differenceMinutes = differenceInMinutes % 60;
    const paddedMinutes = differenceMinutes.toString().padStart(2, '0'); // Ensure minutes are 2 digits with leading zero if needed

    return `${paddedHours}:${paddedMinutes}`;
}

function convertTimeToTZ(originalTime, originalUTCOffset, targetUTCOffset) {
    // Extract hours and minutes from original time
    const [originalHours, originalMinutes] = originalTime.split(':').map(Number);

    // Extract hours and minutes from UTC offsets
    const originalOffsetSign = originalUTCOffset.startsWith('UTC-') ? -1 : 1;
    const targetOffsetSign = targetUTCOffset.startsWith('UTC-') ? -1 : 1;
    const originalOffsetHours = Number(originalUTCOffset.split(':')[0].slice(4)) * originalOffsetSign;
    const originalOffsetMinutes = Number(originalUTCOffset.split(':')[1]) * originalOffsetSign;
    const targetOffsetHours = Number(targetUTCOffset.split(':')[0].slice(4)) * targetOffsetSign;
    const targetOffsetMinutes = Number(targetUTCOffset.split(':')[1]) * targetOffsetSign;

    // Convert time from original timezone to UTC
    let utcHours = originalHours - originalOffsetHours;
    let utcMinutes = originalMinutes - originalOffsetMinutes;

    // Convert time from UTC to target timezone
    let targetHours = utcHours + targetOffsetHours;
    let targetMinutes = utcMinutes + targetOffsetMinutes;

    // Handle overflow and underflow of minutes
    if (targetMinutes >= 60) {
        targetHours += 1;
        targetMinutes -= 60;
    } else if (targetMinutes < 0) {
        targetHours -= 1;
        targetMinutes += 60;
    }

    // Handle overflow and underflow of hours
    targetHours = (targetHours + 24) % 24;

    // Format the result
    const convertedTime = `${targetHours < 10 ? '0' : ''}${targetHours}:${targetMinutes < 10 ? '0' : ''}${targetMinutes}`;

    return convertedTime;
}

function parseTimeString(timeString) {
    const [hours, minutes] = timeString.split(':').map(Number);
    return new Date(0, 0, 0, hours, minutes); // Month and year are set to 0, day to 0 is equivalent to the previous day
}

function compareTimes(time1, time2) {
    const date1 = parseTimeString(time1);
    const date2 = parseTimeString(time2);
    return date1.getTime() - date2.getTime();
}

function timeToMinutes(timeString) {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
}


function addTimeStrings(time1, time2, time3 = '00:00') {
    // Function to convert time string to minutes
    function timeToMinutes(timeString) {
        const [hours, minutes] = timeString.split(':').map(Number);
        return hours * 60 + minutes;
    }

    // Function to convert minutes to time string
    function minutesToTime(totalMinutes) {
        const hours = Math.floor(totalMinutes / 60);
        const paddedHours = hours.toString().padStart(2, '0'); // Ensure hours are 2 digits with leading zero if needed
        const minutes = totalMinutes % 60;
        const paddedMinutes = minutes.toString().padStart(2, '0'); // Ensure minutes are 2 digits with leading zero if needed
        return `${paddedHours}:${paddedMinutes}`;
    }

    // Convert time strings to total minutes
    const totalMinutes = timeToMinutes(time1) + timeToMinutes(time2) + timeToMinutes(time3);

    // Convert total minutes back to time string
    const resultTime = minutesToTime(totalMinutes);

    return resultTime;
}

function addDays(date, days) {
    const result = new Date(date); // Create a new Date object to avoid modifying the original date
    result.setDate(result.getDate() + days); // Set the date to be days days ahead
    return result; // Return the new date object
}