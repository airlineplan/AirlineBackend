const dns = require("node:dns");
const net = require("node:net");

const DEFAULT_DNS_SERVERS = ["1.1.1.1", "8.8.8.8"];

const configureDns = () => {
  const configuredValue = process.env.NODE_DNS_SERVERS;
  const candidates = configuredValue?.trim()
    ? configuredValue.split(",")
    : DEFAULT_DNS_SERVERS;

  const validServers = [];
  const invalidServers = [];

  for (const candidate of candidates) {
    const server = candidate.trim();
    if (!server) continue;

    if (net.isIP(server)) {
      if (!validServers.includes(server)) validServers.push(server);
    } else {
      invalidServers.push(server);
    }
  }

  if (invalidServers.length > 0) {
    console.warn(
      `Ignoring invalid NODE_DNS_SERVERS value(s): ${invalidServers.join(", ")}`
    );
  }

  if (validServers.length === 0) {
    console.warn(
      `No valid NODE_DNS_SERVERS values were provided; keeping system DNS servers: ${dns
        .getServers()
        .join(", ")}`
    );
    return dns.getServers();
  }

  try {
    dns.setServers(validServers);
    console.log(`DNS servers configured: ${validServers.join(", ")}`);
    return validServers;
  } catch (error) {
    console.warn(
      `Unable to configure Node.js DNS servers; keeping system DNS settings (${error.code || error.name})`
    );
    return dns.getServers();
  }
};

const configuredDnsServers = configureDns();

module.exports = {
  DEFAULT_DNS_SERVERS,
  configureDns,
  configuredDnsServers,
};
