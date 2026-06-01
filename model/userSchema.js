const mongoose = require("mongoose");
const { ASSIGNABLE_PAGE_IDS, PAGE_ACCESS_LEVELS } = require("../config/pageAccess");

const userSchema = new mongoose.Schema({
  firstName: {
    type: String,
    // required: true,
  },
  lastName: {
    type: String,
  },
  email: {
    type: String,
    required: true,
    lowercase: true,
    trim: true,
  },
  password: {
    type: String,
    required: true,
  },
  role: {
    type: String,
    enum: ["tenant_admin", "user"],
    default: "user",
    index: true,
  },
  isActive: {
    type: Boolean,
    default: true,
    index: true,
  },
  pageAccess: {
    type: Map,
    of: {
      type: String,
      enum: PAGE_ACCESS_LEVELS,
    },
    default: undefined,
  },
  pageAccessConfigured: {
    type: Boolean,
    default: false,
  },
  createdBy: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    default: null,
  },
  lastLoginAt: {
    type: Date,
  },
  hometimeZone: {
    type: String,
    default: 'UTC+5:30'
  },
  todoConnection : {
    type: Boolean,
    default: false
  }
}, { timestamps: true });

userSchema.path("pageAccess").validate(function validatePageAccessFeatures(pageAccess) {
  if (pageAccess === undefined || pageAccess === null) return true;
  const keys = pageAccess instanceof Map ? [...pageAccess.keys()] : Object.keys(pageAccess);
  return keys.every((key) => ASSIGNABLE_PAGE_IDS.has(key));
}, "Invalid page access feature");

userSchema.index({ email: 1 }, { unique: true });

module.exports = mongoose.model("User", userSchema);
