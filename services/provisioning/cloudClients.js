const crypto = require("crypto");
const https = require("https");
const { EC2Client, RunInstancesCommand, DescribeInstancesCommand } = require("@aws-sdk/client-ec2");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const requireEnv = (name) => {
  const value = process.env[name];
  if (!value) {
    const error = new Error(`${name} is required`);
    error.code = "CONFIG_MISSING";
    throw error;
  }
  return value;
};

const requestJson = ({ method = "GET", hostname, path, headers = {}, body }) =>
  new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : undefined;
    const req = https.request(
      {
        hostname,
        path,
        method,
        headers: {
          Accept: "application/json",
          ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
          ...headers,
        },
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          data += chunk;
        });
        res.on("end", () => {
          let parsed = {};
          if (data) {
            try {
              parsed = JSON.parse(data);
            } catch {
              parsed = { raw: data };
            }
          }
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve({ statusCode: res.statusCode, headers: res.headers, data: parsed });
            return;
          }
          const error = new Error(parsed.detail || parsed.message || `Request failed with ${res.statusCode}`);
          error.statusCode = res.statusCode;
          error.response = parsed;
          error.headers = res.headers;
          reject(error);
        });
      }
    );
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });

const md5 = (value) => crypto.createHash("md5").update(value).digest("hex");

const parseDigestHeader = (header = "") => {
  const challenge = {};
  header.replace(/^Digest\s+/i, "").replace(/(\w+)=(?:"([^"]+)"|([^,]+))/g, (_, key, quoted, plain) => {
    challenge[key] = quoted || plain;
    return "";
  });
  return challenge;
};

const digestRequestJson = async ({ method = "GET", path, body }) => {
  const username = requireEnv("ATLAS_PUBLIC_KEY");
  const password = requireEnv("ATLAS_PRIVATE_KEY");
  const hostname = "cloud.mongodb.com";

  let challengeResponse;
  try {
    challengeResponse = await requestJson({ method, hostname, path, body });
    return challengeResponse.data;
  } catch (error) {
    const digestHeader = error.headers?.["www-authenticate"];
    if (error.statusCode !== 401 || !digestHeader) throw error;

    const challenge = parseDigestHeader(digestHeader);
    const nc = "00000001";
    const cnonce = crypto.randomBytes(12).toString("hex");
    const qop = String(challenge.qop || "auth").split(",")[0].trim();
    const ha1 = md5(`${username}:${challenge.realm}:${password}`);
    const ha2 = md5(`${method}:${path}`);
    const response = md5(`${ha1}:${challenge.nonce}:${nc}:${cnonce}:${qop}:${ha2}`);
    const authorization = [
      `Digest username="${username}"`,
      `realm="${challenge.realm}"`,
      `nonce="${challenge.nonce}"`,
      `uri="${path}"`,
      `qop=${qop}`,
      `nc=${nc}`,
      `cnonce="${cnonce}"`,
      `response="${response}"`,
      `algorithm=MD5`,
    ].join(", ");

    const authed = await requestJson({
      method,
      hostname,
      path,
      headers: {
        Authorization: authorization,
        "Accept-Version": "2024-08-05",
      },
      body,
    });
    return authed.data;
  }
};

const createAtlasCluster = async ({ clusterName, databaseName, username, password, log }) => {
  const groupId = process.env.ATLAS_PROJECT_ID || process.env.ATLAS_PROJECT_TEMPLATE_ID;
  if (!groupId) throw new Error("ATLAS_PROJECT_ID or ATLAS_PROJECT_TEMPLATE_ID is required");

  const providerName = process.env.ATLAS_PROVIDER_NAME || "AWS";
  const regionName = process.env.ATLAS_REGION_NAME || "AP_SOUTH_1";
  const instanceSizeName = process.env.ATLAS_INSTANCE_SIZE || "M10";

  log("info", "Creating MongoDB Atlas cluster", { clusterName, groupId, regionName, instanceSizeName });
  await digestRequestJson({
    method: "POST",
    path: `/api/atlas/v2/groups/${groupId}/clusters`,
    body: {
      name: clusterName,
      clusterType: "REPLICASET",
      mongoDBMajorVersion: process.env.ATLAS_MONGODB_MAJOR_VERSION || "7.0",
      replicationSpecs: [
        {
          regionConfigs: [
            {
              providerName,
              regionName,
              priority: 7,
              electableSpecs: {
                instanceSize: instanceSizeName,
                nodeCount: 3,
              },
            },
          ],
        },
      ],
    },
  });

  log("info", "Creating Atlas database user", { username });
  await digestRequestJson({
    method: "POST",
    path: `/api/atlas/v2/groups/${groupId}/databaseUsers`,
    body: {
      databaseName: "admin",
      groupId,
      password,
      roles: [{ databaseName, roleName: "readWrite" }],
      username,
    },
  });

  const maxChecks = Number(process.env.ATLAS_CLUSTER_POLL_ATTEMPTS || 60);
  for (let attempt = 1; attempt <= maxChecks; attempt += 1) {
    const cluster = await digestRequestJson({
      method: "GET",
      path: `/api/atlas/v2/groups/${groupId}/clusters/${encodeURIComponent(clusterName)}`,
    });
    log("info", "Waiting for Atlas cluster", { attempt, stateName: cluster.stateName });
    if (cluster.stateName === "IDLE" && cluster.connectionStrings?.standardSrv) {
      const standardSrv = cluster.connectionStrings.standardSrv.replace("mongodb+srv://", "");
      return {
        projectId: groupId,
        clusterName,
        clusterId: cluster.id,
        databaseName,
        username,
        mongoUri: `mongodb+srv://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${standardSrv}/${databaseName}?retryWrites=true&w=majority`,
      };
    }
    await sleep(Number(process.env.ATLAS_CLUSTER_POLL_INTERVAL_MS || 60000));
  }

  throw new Error("Atlas cluster did not become ready within the polling window");
};

const createGoDaddyRecord = async ({ subdomain, ipAddress }) => {
  const rootDomain = process.env.ROOT_DOMAIN || "airlineplan.com";
  const key = requireEnv("GODADDY_API_KEY");
  const secret = requireEnv("GODADDY_API_SECRET");

  await requestJson({
    method: "PUT",
    hostname: "api.godaddy.com",
    path: `/v1/domains/${encodeURIComponent(rootDomain)}/records/A/${encodeURIComponent(subdomain)}`,
    headers: {
      Authorization: `sso-key ${key}:${secret}`,
    },
    body: [{ data: ipAddress, ttl: Number(process.env.GODADDY_DNS_TTL || 600) }],
  });
};

const launchTenantInstance = async ({ tenant, mongoUri, tenantJwtSecret, log }) => {
  const region = tenant.aws.region || process.env.AWS_REGION || "ap-south-1";
  const client = new EC2Client({ region });
  const userData = Buffer.from(buildTenantUserData({ tenant, mongoUri, tenantJwtSecret })).toString("base64");

  const command = new RunInstancesCommand({
    ImageId: requireEnv("AWS_AMI_ID"),
    InstanceType: tenant.aws.instanceType || "t3.small",
    MinCount: 1,
    MaxCount: 1,
    KeyName: process.env.AWS_KEY_PAIR_NAME || undefined,
    SecurityGroupIds: [requireEnv("AWS_SECURITY_GROUP_ID")],
    SubnetId: process.env.AWS_SUBNET_ID || undefined,
    UserData: userData,
    TagSpecifications: [
      {
        ResourceType: "instance",
        Tags: [
          { Key: "Name", Value: `airlineplan-${tenant.subdomain}` },
          { Key: "Tenant", Value: tenant.subdomain },
        ],
      },
    ],
  });

  log("info", "Launching EC2 instance", { region, instanceType: tenant.aws.instanceType });
  const response = await client.send(command);
  const instanceId = response.Instances?.[0]?.InstanceId;
  if (!instanceId) throw new Error("AWS did not return an EC2 instance id");

  const maxChecks = Number(process.env.EC2_POLL_ATTEMPTS || 60);
  for (let attempt = 1; attempt <= maxChecks; attempt += 1) {
    const described = await client.send(new DescribeInstancesCommand({ InstanceIds: [instanceId] }));
    const instance = described.Reservations?.[0]?.Instances?.[0];
    const state = instance?.State?.Name;
    log("info", "Waiting for EC2 public IP", { attempt, instanceId, state });
    if (state === "running" && instance.PublicIpAddress) {
      return { instanceId, publicIp: instance.PublicIpAddress, region };
    }
    await sleep(Number(process.env.EC2_POLL_INTERVAL_MS || 15000));
  }

  throw new Error("EC2 instance did not become ready within the polling window");
};

const buildTenantUserData = ({ tenant, mongoUri, tenantJwtSecret }) => {
  const repoUrl = requireEnv("APP_REPO_URL");
  const repoBranch = process.env.APP_REPO_BRANCH || "main";
  const appDir = "/opt/airlineplan";
  const email = process.env.CERTBOT_EMAIL || tenant.adminEmail;

  return `#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y curl git nginx certbot python3-certbot-nginx
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs
npm install -g pm2
rm -rf ${appDir}
git clone --branch ${repoBranch} ${repoUrl} ${appDir}
cat > ${appDir}/AirlineBackend/.env <<'ENV'
MONGO_URI=${mongoUri}
JWT_SECRET=${tenantJwtSecret}
SCHEDULE_UPLOAD_LIMIT=${process.env.SCHEDULE_UPLOAD_LIMIT || ""}
FLIGHT_LIMIT=${process.env.FLIGHT_LIMIT || ""}
TENANT_SUBDOMAIN=${tenant.subdomain}
TENANT_DOMAIN=${tenant.fullDomain}
ENV
cat > ${appDir}/Airlineplan/.env.production <<'ENV'
VITE_API_URL=https://${tenant.fullDomain}
ENV
cd ${appDir}/Airlineplan
npm ci
npm run build
cd ${appDir}/AirlineBackend
npm ci --omit=dev
cat > /etc/nginx/sites-available/airlineplan <<'NGINX'
server {
  listen 80;
  server_name ${tenant.fullDomain};
  client_max_body_size 100m;
  location / {
    proxy_pass http://127.0.0.1:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection 'upgrade';
    proxy_set_header Host $host;
    proxy_cache_bypass $http_upgrade;
  }
}
NGINX
ln -sf /etc/nginx/sites-available/airlineplan /etc/nginx/sites-enabled/airlineplan
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx
pm2 start index.js --name airlineplan
pm2 save
pm2 startup systemd -u root --hp /root
certbot --nginx -d ${tenant.fullDomain} --non-interactive --agree-tos -m ${email} --redirect || true
`;
};

module.exports = {
  createAtlasCluster,
  createGoDaddyRecord,
  launchTenantInstance,
  requestJson,
  sleep,
};
