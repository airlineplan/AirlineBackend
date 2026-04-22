// model/maintenanceTargetSchema.js
const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const maintenanceTargetSchema = new Schema({
    label: { type: String, trim: true },
    msnEsn: { type: String, trim: true },
    pn: { type: String, trim: true },
    snBn: { type: String, trim: true },
    category: { type: String, trim: true },
    date: { type: Date, required: true },
    tsn: { type: String, trim: true },
    csn: { type: String, trim: true },
    dsn: { type: String, trim: true },
    tso: { type: String, trim: true },
    cso: { type: String, trim: true },
    dso: { type: String, trim: true },
    tsRplmt: { type: String, trim: true },
    csRplmt: { type: String, trim: true },
    dsRplmt: { type: String, trim: true },
    userId: { type: Schema.Types.ObjectId, ref: "User", required: true }
}, { timestamps: true });

maintenanceTargetSchema.index({ userId: 1, msnEsn: 1, pn: 1, snBn: 1 }, { unique: true });

module.exports = mongoose.model("MaintenanceTarget", maintenanceTargetSchema);
