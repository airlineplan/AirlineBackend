const mongoose = require("mongoose");

const provisioningLogSchema = new mongoose.Schema(
  {
    level: {
      type: String,
      enum: ["info", "warning", "error"],
      default: "info",
    },
    message: {
      type: String,
      required: true,
    },
    meta: {
      type: mongoose.Schema.Types.Mixed,
      default: undefined,
    },
  },
  { timestamps: true }
);

const tenantSchema = new mongoose.Schema(
  {
    tenantName: {
      type: String,
      required: true,
      trim: true,
    },
    subdomain: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
      unique: true,
    },
    fullDomain: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
      unique: true,
    },
    adminEmail: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
    },
    status: {
      type: String,
      enum: ["pending", "provisioning", "dns_pending", "ssl_pending", "active", "failed", "deactivated"],
      default: "pending",
      index: true,
    },
    aws: {
      region: String,
      instanceType: String,
      instanceId: String,
      publicIp: String,
    },
    atlas: {
      projectId: String,
      clusterName: String,
      clusterId: String,
      databaseName: String,
      username: String,
    },
    dns: {
      provider: {
        type: String,
        default: "godaddy",
      },
      recordType: {
        type: String,
        default: "A",
      },
      recordValue: String,
      lastCheckedAt: Date,
    },
    runtimeEnv: {
      tenantDomain: String,
      tenantSubdomain: String,
      viteApiUrl: String,
      scheduleUploadLimit: String,
      flightLimit: String,
    },
    failureReason: String,
    lastProvisioningStartedAt: Date,
    lastProvisioningFinishedAt: Date,
    logs: [provisioningLogSchema],
  },
  { timestamps: true }
);

tenantSchema.index({ subdomain: 1 }, { unique: true });
tenantSchema.index({ fullDomain: 1 }, { unique: true });

module.exports = mongoose.model("Tenant", tenantSchema);
