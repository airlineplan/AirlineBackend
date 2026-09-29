const assert = require("node:assert/strict");
const test = require("node:test");

const User = require("../model/userSchema");
const { getStationsTableData, saveStation } = require("../controller/stationController");

test("User schema defaults hometimeZone to UTC+5:30", () => {
  const user = new User({ email: "test-tz@example.com" });
  assert.equal(user.hometimeZone, "UTC+5:30");
});

test("User schema stores configured hometimeZone", () => {
  const user = new User({ email: "test-bkk@example.com", hometimeZone: "UTC+7:00" });
  assert.equal(user.hometimeZone, "UTC+7:00");
});
