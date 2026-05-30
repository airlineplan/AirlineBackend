const { TENANT_ADMIN_ROLES } = require("../middlware/auth");

const isTenantAdmin = (req) => TENANT_ADMIN_ROLES.has(req.user?.role);

const scopedUserQuery = (req, query = {}) => {
  if (isTenantAdmin(req)) {
    return { ...query };
  }

  return { ...query, userId: req.user.id };
};

module.exports = {
  isTenantAdmin,
  scopedUserQuery,
};
