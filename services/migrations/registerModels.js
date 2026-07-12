const fs = require("fs");
const path = require("path");

const registerModels = () => {
  const modelDirectory = path.resolve(__dirname, "../../model");
  fs.readdirSync(modelDirectory)
    .filter((fileName) => fileName.endsWith(".js"))
    .filter(
      (fileName) =>
        !["tenantSchema.js", "controlPlaneCounterSchema.js"].includes(fileName)
    )
    .forEach((fileName) => {
      require(path.join(modelDirectory, fileName));
    });
};

module.exports = {
  registerModels,
};
