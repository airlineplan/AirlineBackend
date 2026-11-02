const mongoose = require('mongoose');

const rotationDetailsSchema = new mongoose.Schema({
    rotationNumber: {
        type: String,
    },
    depNumber: {
        type: String,
    },
    flightNumber: {
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
    domIntl : {
        type: String
    },
    gt: {
        type: String,
    },
    connection: {
        type: String,
    },
    userId: {
        type: String,
    },
});

rotationDetailsSchema.index({ userId: 1, rotationNumber: 1, depNumber: 1 });

const RotationDetails = mongoose.model('RotationDetails', rotationDetailsSchema);

module.exports = RotationDetails;
