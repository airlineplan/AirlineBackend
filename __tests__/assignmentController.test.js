const assert = require("node:assert/strict");
const { test } = require("node:test");

const { __testables__ } = require("../controller/assignmentController");

const { parseExcelDate } = __testables__;

const isoDate = (value) => value.toISOString().slice(0, 10);

test("assignment upload parses single-digit hyphenated Excel dates", () => {
  assert.equal(isoDate(parseExcelDate("1-Apr-26")), "2026-04-01");
  assert.equal(isoDate(parseExcelDate("9-Apr-26")), "2026-04-09");
  assert.equal(isoDate(parseExcelDate("1-May-26")), "2026-05-01");
});

test("assignment upload still parses two-digit hyphenated Excel dates", () => {
  assert.equal(isoDate(parseExcelDate("10-Apr-26")), "2026-04-10");
  assert.equal(isoDate(parseExcelDate("15-May-26")), "2026-05-15");
});
