const DEFAULT_CTD = Object.freeze({ condition: "Normal", thrust: 0, derate: 0 });
function validateCtdValue(metric, value) {
  if (!Object.hasOwn(DEFAULT_CTD, metric)) throw new Error("Unknown CTD metric.");
  if (metric === "condition") {
    if (typeof value !== "string" || !/^[a-z0-9 %.-]{1,40}$/i.test(value.trim()) || !value.trim()) {
      throw new Error("Condition is required, maximum 40 characters; use letters, numbers, spaces, %, - or .");
    }
    return value.trim();
  }
  if (value === "" || value == null || !Number.isFinite(Number(value))) throw new Error(`${metric} is required and must be numeric.`);
  const number = Number(value);
  if (number < 0 || number > (metric === "thrust" ? 500 : 100) || (metric === "thrust" && !Number.isInteger(number))) {
    throw new Error(metric === "thrust" ? "Thrust must be an integer between 0 and 500." : "Derate must be between 0 and 100%.");
  }
  return number;
}
module.exports = { DEFAULT_CTD, validateCtdValue };
