require("dotenv").config();

module.exports = {
  get secret() {
    return process.env.JWT_SECRET || process.env.ADMIN_JWT_SECRET;
  },
};
