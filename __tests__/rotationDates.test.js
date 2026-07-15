const assert = require("node:assert/strict");
const test = require("node:test");

const {
  dayNamesForDow,
  occurrenceDates,
  scheduleDowForDate,
} = require("../utils/rotationDates");

test("rotation DOW uses 1=Monday and 7=Sunday", () => {
  assert.equal(scheduleDowForDate("2026-06-01"), 1);
  assert.equal(scheduleDowForDate("2026-06-07"), 7);
  assert.deepEqual(dayNamesForDow("1357"), ["Mon", "Wed", "Fri", "Sun"]);
});

test("rotation occurrence dates match the June 2026 1357 schedule", () => {
  assert.deepEqual(
    occurrenceDates("2026-06-01", "2026-06-07", "1357").map((date) => date.toISOString().slice(0, 10)),
    ["2026-06-01", "2026-06-03", "2026-06-05", "2026-06-07"]
  );
});
