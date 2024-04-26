const mongoose = require("mongoose");
const FLIGHT = require("../model/flight");
const FLIGHTHISTORY = require("../model/flightHistory");
// const createConnections = require('../helper/createConnections');

const sectorSchema = new mongoose.Schema({
  sector1: {
    type: String,
  },
  sector2: {
    type: String,
  },
  acftType: {
    type: String,
  },
  variant: {
    type: String,
  },
  bt: {
    type: String,
  },
  gcd: {
    type: String,
  },
  paxCapacity: {
    type: String,
  },
  CargoCapT: {
    type: String,
  },
  paxLF: {
    type: String,
  },
  cargoLF: {
    type: String,
  },
  fromDt: {
    type: Date,
    default: null,
  },
  toDt: {
    type: Date,
    default: null,
  },
  flight: {
    type: String,
  },
  std: {
    type: String,
  },
  sta: {
    type: String,
  },
  dow: {
    type: String,
  },
  domINTL: {
    type: String,
  },
  userTag1: {
    type: String,
  },
  userTag2: {
    type: String,
  },
  remarks1: {
    type: String,
  },
  remarks2: {
    type: String,
  },
  networkId: {
    type: String,
  },
  userId: {
    type: String,
  },
  isScheduled: {
    type: Boolean,
    default: false, // Set a default value if needed
  },
  rotationNumber: {
    type: String
  },
  addedByRotation: {
    type: String
  }
});

const startDate = new Date("2023-08-01");
const endDate = new Date("2023-08-07");
const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const dow = 135;
const digitArray = String(dow).split("").map(Number);
const firstElement = digitArray[0];
const lastElement = digitArray[digitArray.length - 1];

let currentDate = new Date(startDate);

while (currentDate <= endDate) {
  const dayOfWeek = daysOfWeek[currentDate.getDay()];
  const formattedDate = currentDate.toLocaleDateString("en-US");

  if (digitArray.includes(currentDate.getDay())) {
    console.log(formattedDate); // Output the formatted date
  }

  // Move to the next day
  currentDate.setDate(currentDate.getDate() + 1);
}

// sectorSchema.post("findOneAndUpdate", async function (doc) {
//   const startDate = new Date(doc.fromDt);
//   const endDate = new Date(doc.toDt);
//   const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

//   // Delete existing documents with the same sectorId
//   try {
//     await FLIGHT.deleteMany({ networkId: doc.networkId });
//     // await FLIGHT.deleteMany({ networkId: doc.networkId });
//     console.log("Existing flight entries deleted.");
//   } catch (error) {
//     console.error("Error deleting existing flight entries:", error);
//   }

//   const dow = parseInt(doc.dow);
//   console.log(dow, "this is dow");
//   const digitArray = String(dow).split("").map(Number);
//   const firstElement = digitArray[0];
//   const lastElement = digitArray[digitArray.length - 1];

//   let currentDate = new Date(startDate);

//   while (currentDate <= endDate) {
//     const dayOfWeek = daysOfWeek[currentDate.getDay()];
//     const formattedDate = currentDate.toLocaleDateString("en-US");

//     if (digitArray.includes(currentDate.getDay() !== 0 ? currentDate.getDay() : 7)) {
//       const newFlight = new FLIGHT({
//         date: currentDate,
//         day: dayOfWeek,
//         flight: doc.flight,
//         depStn: doc.sector1,
//         std: doc.std,
//         bt: doc.bt,
//         sta: doc.sta,
//         arrStn: doc.sector2,
//         sector: `${doc.sector1}-${doc.sector2}`,
//         variant: doc.variant,
//         seats: doc.paxCapacity,
//         CargoCapT: doc.CargoCapT,
//         dist: doc.gcd,
//         pax: doc.paxCapacity * (doc.paxLF / 100),
//         CargoT: doc.CargoCapT * (doc.cargoLF / 100),
//         ask: doc.paxCapacity * doc.gcd,
//         rsk: doc.paxCapacity * (doc.paxLF / 100) * doc.gcd,
//         cargoAtk: doc.CargoCapT * doc.gcd,
//         cargoRtk: doc.CargoCapT * (doc.cargoLF / 100) * doc.gcd,
//         domIntl: doc.domINTL.toLowerCase(),
//         userTag1: doc.userTag1,
//         userTag2: doc.userTag2,
//         remarks1: doc.remarks1,
//         remarks2: doc.remarks2,
//         sectorId: doc._id,
//         userId: doc.userId,
//         networkId: doc.networkId,
//         rotationNumber: doc.rotationNumber, // Retain rotationNumber
//         addedByRotation: doc.addedByRotation, // Retain addedByRotation
//         effFromDt: doc.fromDt,
//         effToDt: doc.toDt,
//         dow: doc.dow
//       });

//       if (
//         !isNaN(newFlight.pax) &&
//         !isNaN(newFlight.CargoT) &&
//         !isNaN(newFlight.rsk) &&
//         !isNaN(newFlight.ask) &&
//         !isNaN(newFlight.cargoAtk) &&
//         !isNaN(newFlight.cargoRtk)
//       ) {
//         newFlight.isComplete = true;
//       } else {
//         newFlight.isComplete = false;
//       }

//       try {
//         await newFlight.save();
//         console.log("New flight entry created.");
//       } catch (error) {
//         console.error("Error creating new flight entry:", error);
//       }
//     }

//     currentDate.setDate(currentDate.getDate() + 1);
//   }

//   try {
//     await createConnections(doc.userId);
//     console.log("createConnections completed successfully.");
//   } catch (error) {
//     console.error("Error in createConnections:", error);
//   }
// });

sectorSchema.post("findOneAndUpdate", async function (doc) {
  try {
    const updatedFields = {
      seats: doc.paxCapacity,
      pax: doc.paxCapacity * (doc.paxLF / 100),
      dist: doc.gcd,
      CargoCapT: doc.CargoCapT,
      CargoT: doc.CargoCapT * (doc.cargoLF / 100),
      ask: doc.paxCapacity * doc.gcd,
      rsk: doc.paxCapacity * (doc.paxLF / 100) * doc.gcd,
      cargoAtk: doc.CargoCapT * doc.gcd,
      cargoRtk: doc.CargoCapT * (doc.cargoLF / 100) * doc.gcd
    };

    const allFieldsValid = Object.entries(updatedFields).every(([key, value]) => (
      key in updatedFields && (typeof value === 'string' || typeof value === 'number')
    ));

    updatedFields.isComplete = allFieldsValid;

    // Update flights associated with this sector
    await FLIGHT.updateMany({ networkId: doc.networkId }, { $set: updatedFields });

    console.log("Flights updated successfully.");
  } catch (error) {
    console.error("Error updating flights:", error);
  }
});

sectorSchema.post("save", async function (doc) {
  try {
    const updatedFields = {
      seats: doc.paxCapacity,
      pax: doc.paxCapacity * (doc.paxLF / 100),
      dist: doc.gcd,
      CargoCapT: doc.CargoCapT,
      CargoT: doc.CargoCapT * (doc.cargoLF / 100),
      ask: doc.paxCapacity * doc.gcd,
      rsk: doc.paxCapacity * (doc.paxLF / 100) * doc.gcd,
      cargoAtk: doc.CargoCapT * doc.gcd,
      cargoRtk: doc.CargoCapT * (doc.cargoLF / 100) * doc.gcd
    };

    const allFieldsValid = Object.entries(updatedFields).every(([key, value]) => (
      key in updatedFields && (typeof value === 'string' || typeof value === 'number')
    ));

    updatedFields.isComplete = allFieldsValid;

    // Update flights associated with this sector
    await FLIGHT.updateMany({ networkId: doc.networkId }, { $set: updatedFields });

    console.log("Flights updated successfully.");
  } catch (error) {
    console.error("Error updating flights:", error);
  }
});

// sectorSchema.post("save", async function (doc) {
//   // if (doc.isScheduled) {
//   const startDate = new Date(doc.fromDt);
//   const endDate = new Date(doc.toDt);
//   const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

//   // Delete existing documents with the same sectorId
//   try {
//     if (!doc.rotationNumber) {

//     } else {
//       const flightEntries = await FLIGHT.find({ networkId: doc.networkId });

//       for (const entry of flightEntries) {

//         entry.addedByRotation = `${doc.rotationNumber}+${doc.depNumber - 1}`;
//         await FLIGHTHISTORY.create(entry);
//       }
//     }

//     await FLIGHT.deleteMany({ networkId: doc.networkId });
//     console.log("Existing flight entries deleted.");
//   } catch (error) {
//     console.error("Error deleting existing flight entries:", error);
//   }

//   const dow = parseInt(doc.dow);
//   console.log(dow, "this is dow");
//   const digitArray = String(dow).split("").map(Number);
//   const firstElement = digitArray[0];
//   const lastElement = digitArray[digitArray.length - 1];

//   let currentDate = new Date(startDate);

//   while (currentDate <= endDate) {
//     const dayOfWeek = daysOfWeek[currentDate.getDay()];
//     const formattedDate = currentDate.toLocaleDateString("en-US");

//     if (digitArray.includes(currentDate.getDay() !== 0 ? currentDate.getDay() : 7)) {
//       const newFlight = new FLIGHT({
//         date: currentDate,
//         day: dayOfWeek,
//         flight: doc.flight,
//         depStn: doc.sector1,
//         std: doc.std,
//         bt: doc.bt,
//         sta: doc.sta,
//         arrStn: doc.sector2,
//         sector: `${doc.sector1}-${doc.sector2}`,
//         variant: doc.variant,
//         seats: doc.paxCapacity,
//         CargoCapT: doc.CargoCapT,
//         dist: doc.gcd,
//         pax: doc.paxCapacity * (doc.paxLF / 100),
//         CargoT: doc.CargoCapT * (doc.cargoLF / 100),
//         ask: doc.paxCapacity * doc.gcd,
//         rsk: doc.paxCapacity * (doc.paxLF / 100) * doc.gcd,
//         cargoAtk: doc.CargoCapT * doc.gcd,
//         cargoRtk: doc.CargoCapT * (doc.cargoLF / 100) * doc.gcd,
//         domIntl: doc.domINTL.toLowerCase(),
//         userTag1: doc.userTag1,
//         userTag2: doc.userTag2,
//         remarks1: doc.remarks1,
//         remarks2: doc.remarks2,
//         sectorId: doc._id,
//         userId: doc.userId,
//         networkId: doc.networkId,
//         rotationNumber: doc.rotationNumber,
//         addedByRotation: (doc.rotationNumber && doc.depNumber) ? (doc.rotationNumber.toString() + '-' + doc.depNumber.toString()) : '',
//         effFromDt: doc.fromDt,
//         effToDt: doc.toDt,
//         dow: doc.dow
//       });

//       if (
//         !isNaN(newFlight.pax) &&
//         !isNaN(newFlight.CargoT) &&
//         !isNaN(newFlight.rsk) &&
//         !isNaN(newFlight.ask) &&
//         !isNaN(newFlight.cargoAtk) &&
//         !isNaN(newFlight.cargoRtk)
//       ) {
//         newFlight.isComplete = true;
//       } else {
//         newFlight.isComplete = false;
//       }

//       try {
//         await newFlight.save();
//         console.log("New flight entry created.");
//       } catch (error) {
//         console.error("Error creating new flight entry:", error);
//       }
//     }

//     currentDate.setDate(currentDate.getDate() + 1);
//   }

//   try {
//     await createConnections(doc.userId);
//     console.log("createConnections completed successfully.");
//   } catch (error) {
//     console.error("Error in createConnections:", error);
//   }
// });

module.exports = mongoose.model("Sector", sectorSchema);
