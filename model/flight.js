const mongoose = require("mongoose");
const Schema = mongoose.Schema;
const Stations = require("./stationSchema");


const flightSchema = new mongoose.Schema(
  {
    date: {
      type: Date,
    },
    day: {
      type: String,
    },
    flight: {
      type: String,
    },
    depStn: {
      type: String,
    },
    std: {
      type: String,
    },
    bt: {
      type: String,
    },
    sta: {
      type: String,
    },
    arrStn: {
      type: String,
    },
    sector: {
      type: String,
    },
    variant: {
      type: String,
    },
    seats: {
      type: String,
    },
    CargoCapT: {
      type: String,
    },
    dist: {
      type: String,
    },
    pax: {
      type: String,
    },
    CargoT: {
      type: String,
    },
    ask: {
      type: String,
    },
    rsk: {
      type: String,
    },
    cargoAtk: {
      type: String,
    },
    cargoRtk: {
      type: String,
    },
    domIntl: {
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
    sectorId: {
      type: String,
    },
    networkId: {
      type: String,
    },
    userId: {
      type: String,
    },
    isComplete: {
      type: Boolean,
    },
    rotationNumber: {
      type: String
    },
    // beyondODs: [
    //   {
    //     type: mongoose.Schema.Types.ObjectId,
    //     ref: 'FLIGHT',
    //   },
    // ],
    // behindODs: [
    //   {
    //     type: mongoose.Schema.Types.ObjectId,
    //     ref: 'FLIGHT',
    //   },
    // ],
    addedByRotation : {
      type: String 
    },
    effFromDt: {
      type: Date,
    },
    effToDt: {
      type: Date,
    },
    dow: {
      type: String,
    }
  }
);

// flightSchema.post('save', async function (doc, next) {
//   try {
//     console.log('Post-save hook called for flight:', doc._id);

//     await updateConnectionAfterFlightAddition(doc._id);
    
//     next();
//   } catch (error) {
//     console.error('Error in post-save hook:', error);
//     next(error);
//   }
// });

// flightSchema.pre('deleteMany', async function (next) {
//   try {
//     const query = this.getQuery();
//     const flights = await this.model.find(query, '_id');
//     this.deletedFlightIds = flights.map(flight => flight._id);
//     next();
//   } catch (error) {
//     console.error('Error in pre-deleteMany hook:', error);
//     next(error);
//   }
// });

// Post-deleteMany hook to access captured IDs after deletion
// flightSchema.post('deleteMany', async function (result, next) {
//   try {
//     if (this.deletedFlightIds) {
//       console.log('Post-deleteMany hook called. Deleted flight IDs:', this.deletedFlightIds);
      
//       for (const flightId of this.deletedFlightIds) {
//         await updateConnectionAfterFlightDelete(flightId); // Pass the flight ID to the function
//       }
//       this.deletedFlightIds = null;
//     }
//     next();
//   } catch (error) {
//     console.error('Error in post-deleteMany hook:', error);
//     next(error);
//   }
// });


//-----------------------------------------------------------


flightSchema.index({ userId: 1 });
flightSchema.index({ depStn: 1 });
flightSchema.index({ arrStn: 1 });
flightSchema.index({ domIntl: 1 });
flightSchema.index({ std: 1 });
flightSchema.index({ date: 1 });

module.exports = mongoose.model("FLIGHT", flightSchema);
