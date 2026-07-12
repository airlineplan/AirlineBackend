const mongoose = require("mongoose");
const {
  FEATURE_IDS,
  normalizeFeatureMap,
} = require("../config/featureCatalog");

const TENANT_STATUSES = Object.freeze([
  "PENDING",
  "PROVISIONING",
  "MIGRATING_DB",
  "SEEDING_ADMIN",
  "HEALTH_CHECKING",
  "ACTIVE",
  "FAILED",
  "ROLLING_BACK",
  "SUSPENDED",
  "DELETING",
  "DELETED",
]);

const auditEventSchema = new mongoose.Schema(
  {
    type: { type: String, required: true },
    message: { type: String, required: true },
    actor: String,
    step: String,
    meta: mongoose.Schema.Types.Mixed,
  },
  { timestamps: true }
);

const tenantSchema = new mongoose.Schema(
  {
    tenantId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    slug: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
      unique: true,
      index: true,
    },
    companyName: {
      type: String,
      required: true,
      trim: true,
    },
    domain: {
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
    plan: {
      type: String,
      default: "enterprise-dedicated",
      trim: true,
    },
    branding: {
      companyName: String,
      logoUrl: String,
      primaryColor: {
        type: String,
        default: "#0B3B75",
      },
    },
    features: {
      type: mongoose.Schema.Types.Mixed,
      default: () => normalizeFeatureMap({}, { defaultEnabled: true }),
      validate: {
        validator(value) {
          return (
            value &&
            typeof value === "object" &&
            FEATURE_IDS.every((featureId) => typeof value[featureId] === "boolean")
          );
        },
        message: "Tenant features must contain every canonical feature flag",
      },
    },
    status: {
      type: String,
      enum: TENANT_STATUSES,
      default: "PENDING",
      index: true,
    },
    currentStep: {
      type: String,
      default: "PENDING",
    },
    executionArn: String,
    attempt: {
      type: Number,
      default: 1,
      min: 1,
    },
    albRulePriority: {
      type: Number,
      required: true,
      unique: true,
    },
    deployment: {
      desiredAppVersion: String,
      desiredImageTag: String,
      deployedAppVersion: String,
      deployedImageTag: String,
      deployedImageDigest: String,
      previousImageTag: String,
      history: [
        {
          appVersion: String,
          imageTag: String,
          imageDigest: String,
          deployedAt: Date,
        },
      ],
    },
    resources: {
      awsRegion: {
        type: String,
        default: "ap-south-1",
      },
      ecsClusterArn: String,
      ecsServiceArn: String,
      ecsServiceName: String,
      taskDefinitionArn: String,
      taskSecurityGroupId: String,
      privateSubnetIds: [String],
      targetGroupArn: String,
      albRuleArn: String,
      route53Record: String,
      secretArn: String,
      redisReplicationGroupId: String,
      redisEndpoint: String,
      logGroupName: String,
      atlasProjectId: String,
      atlasProjectName: String,
      atlasClusterName: String,
      atlasDatabaseName: String,
      terraformStateKey: String,
      provisioningOutputKey: String,
    },
    provisioning: {
      bootstrapSecretArn: {
        type: String,
        select: false,
      },
      lastStartedAt: Date,
      lastFinishedAt: Date,
    },
    failure: {
      step: String,
      message: String,
      code: String,
      occurredAt: Date,
    },
    auditEvents: [auditEventSchema],
  },
  { timestamps: true, minimize: false }
);

tenantSchema.index({ slug: 1 }, { unique: true });
tenantSchema.index({ domain: 1 }, { unique: true });
tenantSchema.index({ albRulePriority: 1 }, { unique: true });

module.exports = mongoose.model("Tenant", tenantSchema);
module.exports.TENANT_STATUSES = TENANT_STATUSES;
